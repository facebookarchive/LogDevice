/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of tnhis source tree.
 */
#include "NodesConfiguration.h"

#include <chrono>

#include <folly/hash/SpookyHashV2.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/configuration/ServerConfig.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using facebook::logdevice::toString;
namespace MembershipVersion = membership::MembershipVersion;

namespace {
template <typename C>
bool valid(C config) {
  return config != nullptr && config->validate();
}

bool serviceDiscoveryhasMembershipNodes(
    const ServiceDiscoveryConfig& serv_conf,
    const membership::Membership& membership) {
  for (node_index_t node : membership.getMembershipNodes()) {
    bool node_exist;
    NodeServiceDiscovery serv_discovery;
    std::tie(node_exist, serv_discovery) = serv_conf.getNodeAttributes(node);

    if (!node_exist) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Membership for role %s has node %u which is not in "
                      "service discovery config.",
                      toString(membership.getType()).str().c_str(),
                      node);
      return false;
    }

    if (!serv_discovery.hasRole(membership.getType())) {
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
      num_shards_(0) {
  ld_check(validate());
}

bool NodesConfiguration::Update::isValid() const {
  // we don't check validity for each update of the sub config, as we check each
  // one of them in applyUpdate();
  return service_discovery_update != nullptr ||
      sequencer_config_update != nullptr || storage_config_update != nullptr ||
      metadata_logs_rep_update != nullptr;
}

bool NodesConfiguration::Update::hasAllUpdates() const {
  return service_discovery_update != nullptr &&
      sequencer_config_update != nullptr && storage_config_update != nullptr &&
      metadata_logs_rep_update != nullptr;
}

bool NodesConfiguration::serviceDiscoveryConsistentWithMembership() const {
  return serviceDiscoveryhasMembershipNodes(
             *service_discovery_, *sequencer_config_->getMembership()) &&
      serviceDiscoveryhasMembershipNodes(
             *service_discovery_, *storage_config_->getMembership());
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

  // TODO T15517759: check all storage nodes must have the same number
  // of shards as the num_shards_
  auto storage_nodes = getStorageMembership()->getMembershipNodes();
  for (const auto n : storage_nodes) {
    const auto& storage_attr =
        getStorageConfig()->getAttributes()->nodeAttributesAt(n);
    if (storage_attr.num_shards != num_shards_) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          5,
          "validation failed! node %hu has num_shards attribute of %hd "
          "while num_shards for the config is %hd.",
          n,
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

std::shared_ptr<const NodesConfiguration>
NodesConfiguration::applyUpdate(NodesConfiguration::Update update) const {
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

  if (version_ == MembershipVersion::EMPTY_VERSION && !update.hasAllUpdates()) {
    // the first version update to nodes configuration must provision all four
    // sub-configs
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

  // 5) validate the config but ignoring config metadata. e.g., check if the
  // service discovery is consistent with membership. we will validate the
  // config metadata after we update the config metadata in the next step
  if (!new_config->validate(/*validate config metadata*/ false)) {
    return nullptr;
  }

  // 6) bump the config version and updates the configuration metadata
  new_config->version_ = MembershipVersion::Type(version_.val() + 1);
  // update storage_hash, num_shards and addr_to_index
  new_config->recomputeConfigMetadata();
  new_config->last_change_timestamp_ =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  new_config->last_maintenance_ = update.maintenance;
  new_config->last_change_context_ = std::move(update.context);

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
  }
  const uint64_t SEED = 0x9a6bf3f8ebcd8cdfL; // random
  return folly::hash::SpookyHashV2::Hash64(
      hashable_string.data(), hashable_string.size(), SEED);
}

shard_size_t NodesConfiguration::computeNumShards() const {
  const auto& storage_mem = getStorageMembership();
  auto storage_nodes = storage_mem->getMembershipNodes();
  for (const auto n : storage_nodes) {
    const auto& storage_attr =
        getStorageConfig()->getAttributes()->nodeAttributesAt(n);
    ld_check(storage_attr.num_shards > 0);
    return storage_attr.num_shards;
  }
  return 0;
}

void NodesConfiguration::recomputeConfigMetadata() {
  storage_hash_ = computeStorageNodesHash();
  num_shards_ = computeNumShards();

  auto add_addr_index = [this](const std::vector<node_index_t>& nodes) {
    for (auto n : nodes) {
      const auto& serv_disc = getServiceDiscovery()->nodeAttributesAt(n);
      addr_to_index_.insert(std::make_pair(serv_disc.address, n));
    }
  };

  add_addr_index(getSequencerMembership()->getMembershipNodes());
  add_addr_index(getStorageMembership()->getMembershipNodes());
}

std::shared_ptr<const NodesConfiguration> NodesConfiguration::withVersion(
    membership::MembershipVersion::Type version) const {
  auto config = std::make_shared<NodesConfiguration>(*this);
  config->setVersion(version);
  return config;
}

}}}} // namespace facebook::logdevice::configuration::nodes
