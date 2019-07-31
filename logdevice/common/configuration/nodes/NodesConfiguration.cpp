/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of tnhis source tree.
 */
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

#include <chrono>

#include <folly/hash/SpookyHashV2.h>

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using facebook::logdevice::toString;
namespace MembershipVersion = membership::MembershipVersion;

namespace {
template <typename C>
bool valid(C config) {
  return config != nullptr && config->validate();
}

template <typename M>
bool serviceDiscoveryhasMembershipNodes(const ServiceDiscoveryConfig& serv_conf,
                                        const M& membership) {
  for (const node_index_t node : membership) {
    auto serv_discovery = serv_conf.getNodeAttributes(node);
    bool node_exist = serv_discovery.hasValue();
    if (!node_exist) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Membership for role %s has node %u which is not in "
                      "service discovery config.",
                      toString(membership.getType()).str().c_str(),
                      node);
      return false;
    }

    if (!serv_discovery->hasRole(membership.getType())) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Membership for role %s has node %u whose service "
                      "discovery does not contain the provisioned role.",
                      toString(membership.getType()).str().c_str(),
                      node);
      return false;
    }
  }
  return true;
}

} // namespace

NodesConfiguration::NodesConfiguration()
    : service_discovery_(std::make_shared<const ServiceDiscoveryConfig>()),
      sequencer_config_(std::make_shared<const SequencerConfig>()),
      storage_config_(std::make_shared<const StorageConfig>()),
      metadata_logs_rep_(std::make_shared<const MetaDataLogsReplication>()),
      version_(MembershipVersion::EMPTY_VERSION),
      storage_hash_(0),
      num_shards_(0),
      max_node_index_(0) {
  ld_check(validate());
}

bool NodesConfiguration::Update::isValid() const {
  // we don't check validity for each update of the sub config, as we check each
  // one of them in applyUpdate();
  return !empty();
}

bool NodesConfiguration::Update::hasAllUpdates() const {
  return service_discovery_update != nullptr &&
      sequencer_config_update != nullptr && storage_config_update != nullptr &&
      metadata_logs_rep_update != nullptr;
}

bool NodesConfiguration::Update::empty() const {
  return service_discovery_update == nullptr &&
      sequencer_config_update == nullptr && storage_config_update == nullptr &&
      metadata_logs_rep_update == nullptr;
}

std::string NodesConfiguration::Update::toString() const {
  return folly::sformat(
      "[Svc:{},Seq:{},Sto:{},Meta:{}]",
      service_discovery_update ? service_discovery_update->toString() : "",
      sequencer_config_update ? sequencer_config_update->toString() : "",
      storage_config_update ? storage_config_update->toString() : "",
      metadata_logs_rep_update ? metadata_logs_rep_update->toString() : "");
}

bool NodesConfiguration::serviceDiscoveryConsistentWithMembership() const {
  return serviceDiscoveryhasMembershipNodes(
             *service_discovery_, *sequencer_config_->getMembership()) &&
      serviceDiscoveryhasMembershipNodes(
             *service_discovery_, *storage_config_->getMembership());
}

bool NodesConfiguration::membershipVersionsConsistentWithVersion() const {
  return getSequencerMembership()->getVersion() <= version_ &&
      getStorageMembership()->getVersion() <= version_;
}

bool NodesConfiguration::validateConfigMetadata() const {
  if (version_ == MembershipVersion::EMPTY_VERSION) {
    // every component should be empty and (if versioned) with EMPTY_VERSION
    if (!service_discovery_->isEmpty() || !sequencer_config_->isEmpty() ||
        !storage_config_->isEmpty() ||
        sequencer_config_->getMembership()->getVersion() !=
            MembershipVersion::EMPTY_VERSION ||
        storage_config_->getMembership()->getVersion() !=
            MembershipVersion::EMPTY_VERSION ||
        metadata_logs_rep_->getVersion() != MembershipVersion::EMPTY_VERSION) {
      err = E::INVALID_CONFIG;
      return false;
    }
  }

  // check all storage nodes must have the same number of shards as the
  // num_shards_
  for (const node_index_t node : *getStorageMembership()) {
    const auto& storage_attr =
        getStorageConfig()->getAttributes()->nodeAttributesAt(node);
    if (storage_attr.num_shards != num_shards_) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          5,
          "validation failed! node %hu has num_shards attribute of %hd "
          "while num_shards for the config is %hd.",
          node,
          storage_attr.num_shards,
          num_shards_);
      err = E::INVALID_CONFIG;
      return false;
    }
  }

  return true;
}

bool NodesConfiguration::validate(bool validate_config_metadata) const {
  if (!valid(service_discovery_) || !valid(sequencer_config_) ||
      !valid(storage_config_) || !valid(metadata_logs_rep_)) {
    err = E::INVALID_CONFIG;
    return false;
  }

  // for each node in membership for each role:
  // 1) there must be service discovery info;
  // 2) service discovery info must contain the role for the membership
  if (!serviceDiscoveryConsistentWithMembership()) {
    err = E::INVALID_CONFIG;
    return false;
  }

  // Sequencer & Storage membership versions can't be larger that the NC's main
  // version;
  if (!membershipVersionsConsistentWithVersion()) {
    err = E::INVALID_CONFIG;
    return false;
  }

  // TODO T33035439: XXX: if the config is not EMPTY, metadata nodes must be
  // able to replicate metadata logs.
  // if (version_ != MembershipVersion::EMPTY_VERSION) {
  //   if (!canReplicatteMetaDataLogs(
  //           storage_config_->getMembership(), metadata_logs_rep_)) {
  //     return false;
  //   }
  // }

  if (validate_config_metadata && !validateConfigMetadata()) {
    err = E::INVALID_CONFIG;
    return false;
  }

  return true;
}

std::shared_ptr<const NodesConfiguration> NodesConfiguration::applyUpdate(
    const NodesConfiguration::Update& update) const {
  if (!service_discovery_ || !sequencer_config_ || !storage_config_ ||
      !metadata_logs_rep_) {
    // there is no way we could end up in such invalid state
    err = E::INTERNAL;
    return nullptr;
  }

  if (!update.isValid()) {
    err = E::INVALID_PARAM;
    return nullptr;
  }

  ld_assert(validate());

  // 0) make a copy of the current configuration
  std::shared_ptr<NodesConfiguration> new_config(
      std::make_shared<NodesConfiguration>(*this));

  // 1) apply service discovery updates
  if (update.service_discovery_update) {
    auto new_service_discovery = applyConfigUpdate(
        *service_discovery_, *update.service_discovery_update);
    if (new_service_discovery == nullptr) {
      return nullptr;
    } else {
      new_config->service_discovery_ = std::move(new_service_discovery);
    }
  }

  // 2) apply sequencer config update
  if (update.sequencer_config_update) {
    auto new_sequencer_config =
        sequencer_config_->applyUpdate(*update.sequencer_config_update);
    if (new_sequencer_config == nullptr) {
      return nullptr;
    } else {
      new_config->sequencer_config_ = std::move(new_sequencer_config);
    }
  }

  // 3) apply storage config update
  if (update.storage_config_update) {
    auto new_storage_config =
        storage_config_->applyUpdate(*update.storage_config_update);
    if (new_storage_config == nullptr) {
      return nullptr;
    } else {
      new_config->storage_config_ = std::move(new_storage_config);
    }
  }

  // 4) apply metadata logs replication config update
  if (update.metadata_logs_rep_update) {
    auto new_metadata_logs_rep = applyConfigUpdate(
        *metadata_logs_rep_, *update.metadata_logs_rep_update);
    if (new_metadata_logs_rep == nullptr) {
      return nullptr;
    } else {
      new_config->metadata_logs_rep_ = std::move(new_metadata_logs_rep);
    }
  }

  // 5) bump the config version and updates the configuration metadata
  new_config->touch(update.context);

  // 6) validate the config but ignoring config metadata. e.g., check if the
  // service discovery is consistent with membership. we will validate the
  // config metadata after we update the config metadata in the next step
  if (!new_config->validate(/*validate config metadata*/ false)) {
    return nullptr;
  }

  // update storage_hash, num_shards and addr_to_index
  new_config->recomputeConfigMetadata();
  new_config->last_maintenance_ = update.maintenance;

  // 7) Finally check the validaity of the new config, for example
  // consistency b/w service discovery, membership and metadata logs
  // replication
  if (!new_config->validateConfigMetadata()) {
    return nullptr;
  }

  ld_assert(new_config->validate());
  return new_config;
}

uint64_t NodesConfiguration::computeStorageNodesHash() const {
  const auto& storage_mem = getStorageMembership();
  auto storage_nodes = storage_mem->getMembershipNodes();
  std::sort(storage_nodes.begin(), storage_nodes.end());

  if (storage_nodes.empty()) {
    return 0;
  }

  std::string hashable_string;

  // for each node, writing out the attributes being hashed
  auto append = [&](const void* ptr, size_t num_bytes) {
    size_t old_size = hashable_string.size();
    size_t new_size = old_size + num_bytes;
    if (new_size > hashable_string.capacity()) {
      size_t new_capacity = std::max(256ul, hashable_string.capacity() * 2);
      hashable_string.reserve(new_capacity);
    }
    hashable_string.resize(new_size);
    memcpy(&hashable_string[old_size], ptr, num_bytes);
  };

  for (auto node_id : storage_nodes) {
    const auto& serv_disc = getServiceDiscovery()->nodeAttributesAt(node_id);
    const auto& storage_attr =
        getStorageConfig()->getAttributes()->nodeAttributesAt(node_id);

    append(&node_id, sizeof(node_id));
    append(&storage_attr.capacity, sizeof(storage_attr.capacity));
    append(&storage_attr.exclude_from_nodesets,
           sizeof(storage_attr.exclude_from_nodesets));
    append(&storage_attr.num_shards, sizeof(storage_attr.num_shards));

    // appending location str and the terminating null-byte
    std::string location_str = serv_disc.locationStr();
    append(location_str.c_str(), location_str.size() + 1);

    // append per-shard storage states
    const auto shard_map = storage_mem->getShardStates(node_id);
    const std::map<shard_index_t, membership::ShardState> ordered_shard_map(
        shard_map.begin(), shard_map.end());
    for (const auto& kv : ordered_shard_map) {
      append(&kv.second.storage_state, sizeof(kv.second.storage_state));
    }
  }
  const uint64_t SEED = 0x9a6bf3f8ebcd8cdfL; // random
  return folly::hash::SpookyHashV2::Hash64(
      hashable_string.data(), hashable_string.size(), SEED);
}

shard_size_t NodesConfiguration::computeNumShards() const {
  for (const node_index_t node : *getStorageMembership()) {
    const auto& storage_attr =
        getStorageConfig()->getAttributes()->nodeAttributesAt(node);
    ld_check(storage_attr.num_shards > 0);
    // return as soon as we got one valid num_shards
    return storage_attr.num_shards;
  }
  return 0;
}

node_index_t NodesConfiguration::computeMaxNodeIndex() const {
  node_index_t res = 0;
  for (const auto& kv : *service_discovery_) {
    res = std::max(res, kv.first);
  }
  return res;
}

SequencersConfig NodesConfiguration::computeSequencersConfig() const {
  // sequencersConfig_ needs consecutive node indexes, see comment in
  // SequencersConfig.h.
  // Pad with zero-weight invalid nodes if there are gaps in numbering.

  // note: this is actually computed eariler in recomputeConfigMetadata(),
  // however, do it again now for safety just in case people forget about
  // such assupmption
  SequencersConfig result;

  // TODO T41571347 use max node id in sequencer membership instead of the
  // max node id of the whole cluster
  size_t max_node = computeMaxNodeIndex();
  result.nodes.resize(max_node + 1);
  result.weights.resize(max_node + 1);

  const auto sequencer_membership = getSequencerMembership();
  for (const auto node : *sequencer_membership) {
    if (sequencer_membership->isSequencingEnabled(node)) {
      result.nodes[node] = getNodeID(node);
      result.weights[node] =
          sequencer_membership->getNodeStatePtr(node)->getEffectiveWeight();
    }
  }

  // Scale all weights to the [0, 1] range. Note that increasing the maximum
  // weight will cause all nodes' weights to change, possibly resulting in
  // many sequencers being relocated.
  auto max_it = std::max_element(result.weights.begin(), result.weights.end());
  if (max_it != result.weights.end() && *max_it > 0) {
    double max_weight = *max_it;
    for (double& weight : result.weights) {
      weight /= max_weight;
    }
  }

  return result;
}

void NodesConfiguration::touch(std::string context) {
  version_ = MembershipVersion::Type(version_.val() + 1);
  last_change_timestamp_ = SystemTimestamp{std::chrono::system_clock::now()}
                               .toMilliseconds()
                               .count();
  last_change_context_ = std::move(context);
}

void NodesConfiguration::recomputeConfigMetadata() {
  storage_hash_ = computeStorageNodesHash();
  num_shards_ = computeNumShards();
  max_node_index_ = computeMaxNodeIndex();
  sequencer_locator_config_ = computeSequencersConfig();
}

std::shared_ptr<const NodesConfiguration>
NodesConfiguration::withIncrementedVersionAndTimestamp(
    folly::Optional<membership::MembershipVersion::Type> new_nc_version,
    folly::Optional<membership::MembershipVersion::Type>
        new_sequencer_membership_version,
    folly::Optional<membership::MembershipVersion::Type>
        new_storage_membership_version,
    std::string context) const {
  if (new_nc_version.hasValue() && new_nc_version.value() <= version_) {
    return nullptr;
  }

  auto new_config = std::make_shared<NodesConfiguration>(*this);
  new_config->touch(std::move(context));
  if (new_nc_version.hasValue()) {
    new_config->setVersion(new_nc_version.value());
  }

  // We also check the invariant that nc_version >=
  // max(sequencer_config_version, storage_config_version)
  if (sequencer_config_) {
    new_config->sequencer_config_ = sequencer_config_->withIncrementedVersion(
        new_sequencer_membership_version);
    if (!new_config->sequencer_config_) {
      return nullptr;
    }
    ld_check_ge(new_config->getVersion(),
                new_config->sequencer_config_->getMembership()->getVersion());
  }

  if (storage_config_) {
    new_config->storage_config_ =
        storage_config_->withIncrementedVersion(new_storage_membership_version);
    if (!new_config->storage_config_) {
      return nullptr;
    }
    ld_check_ge(new_config->getVersion(),
                new_config->storage_config_->getMembership()->getVersion());
  }

  return new_config;
}

std::shared_ptr<const NodesConfiguration> NodesConfiguration::withVersion(
    membership::MembershipVersion::Type version) const {
  auto config = std::make_shared<NodesConfiguration>(*this);
  config->setVersion(version);
  return config;
}

bool NodesConfiguration::operator==(const NodesConfiguration& rhs) const {
  return equalWithTimestampIgnored(rhs) &&
      last_change_timestamp_ == rhs.last_change_timestamp_;
}

bool NodesConfiguration::equalWithTimestampIgnored(
    const NodesConfiguration& rhs) const {
  return equalWithTimestampAndVersionIgnored(rhs) && version_ == rhs.version_;
}

bool NodesConfiguration::equalWithTimestampAndVersionIgnored(
    const NodesConfiguration& rhs) const {
  return compare_obj_ptrs(service_discovery_, rhs.service_discovery_) &&
      compare_obj_ptrs(sequencer_config_, rhs.sequencer_config_) &&
      compare_obj_ptrs(storage_config_, rhs.storage_config_) &&
      compare_obj_ptrs(metadata_logs_rep_, rhs.metadata_logs_rep_) &&
      storage_hash_ == rhs.storage_hash_ && num_shards_ == rhs.num_shards_ &&
      last_maintenance_ == rhs.last_maintenance_ &&
      last_change_context_ == rhs.last_change_context_;
}

const NodeServiceDiscovery*
NodesConfiguration::getNodeServiceDiscovery(node_index_t node) const {
  ld_check(service_discovery_ != nullptr);
  return service_discovery_->getNodeAttributesPtr(node);
}

const StorageNodeAttribute*
NodesConfiguration::getNodeStorageAttribute(node_index_t node) const {
  const auto& storage_attrs = getStorageAttributes();
  ld_check(storage_attrs != nullptr);
  return storage_attrs->getNodeAttributesPtr(node);
}

double NodesConfiguration::getWritableStorageCapacity(ShardID shard) const {
  const auto& storage_membership = getStorageMembership();
  if (!storage_membership->canWriteToShard(shard)) {
    return 0.0;
  }

  const auto* node_attr = getNodeStorageAttribute(shard.node());
  // node must exist in storage attribute if in membership
  ld_check(node_attr != nullptr);
  return node_attr->capacity;
}

node_gen_t NodesConfiguration::getNodeGeneration(node_index_t node) const {
  const auto* node_attr = getNodeStorageAttribute(node);
  return node_attr != nullptr ? node_attr->generation : 1;
}

shard_size_t NodesConfiguration::getNumShards(node_index_t node) const {
  const auto* node_attr = getNodeStorageAttribute(node);
  return node_attr != nullptr ? node_attr->num_shards : 0;
}

const NodeServiceDiscovery*
NodesConfiguration::getNodeServiceDiscovery(NodeID node) const {
  const NodeServiceDiscovery* node_discovery =
      getNodeServiceDiscovery(node.index());
  if (node_discovery && getNodeGeneration(node.index()) == node.generation()) {
    return node_discovery;
  }
  return nullptr;
}

}}}} // namespace facebook::logdevice::configuration::nodes
