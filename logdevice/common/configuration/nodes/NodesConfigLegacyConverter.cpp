/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of tnhis source tree.
 */
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"

#include <chrono>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/NodesConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/utils.h"
#include "logdevice/common/toString.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

/*static*/
membership::StorageState NodesConfigLegacyConverter::fromLegacyStorageState(
    configuration::StorageState ls) {
  switch (ls) {
    case configuration::StorageState::READ_WRITE:
      return membership::StorageState::READ_WRITE;
    case configuration::StorageState::READ_ONLY:
      return membership::StorageState::READ_ONLY;
    case configuration::StorageState::DISABLED:
      return membership::StorageState::NONE;
  }
  ld_check(false);
  return membership::StorageState::INVALID;
}

/*static*/
configuration::StorageState
NodesConfigLegacyConverter::toLegacyStorageState(membership::StorageState ss) {
  switch (ss) {
    case membership::StorageState::PROVISIONING:
    case membership::StorageState::NONE:
      return configuration::StorageState::DISABLED;
    case membership::StorageState::NONE_TO_RO:
      return configuration::StorageState::READ_ONLY;
    case membership::StorageState::READ_ONLY:
      return configuration::StorageState::READ_ONLY;
    case membership::StorageState::READ_WRITE:
      return configuration::StorageState::READ_WRITE;
    case membership::StorageState::RW_TO_RO:
      return configuration::StorageState::READ_ONLY;
    case membership::StorageState::DATA_MIGRATION:
      return configuration::StorageState::READ_ONLY;
    case membership::StorageState::INVALID:
      return configuration::StorageState::DISABLED;
  }
  ld_check(false);
  return configuration::StorageState::DISABLED;
}

/*static*/
int NodesConfigLegacyConverter::toLegacyNodesConfig(
    const NodesConfiguration& config,
    NodesConfig* legacy_config_out) {
  ld_check(legacy_config_out != nullptr);
  if (!config.validate()) {
    err = E::INVALID_CONFIG;
    return -1;
  }

  std::unordered_set<node_index_t> all_nodes;
  configuration::Nodes res_nodes;

  // sequencer membership
  const auto& seq_mem = config.getSequencerMembership();
  for (auto qn : seq_mem->getMembershipNodes()) {
    all_nodes.insert(qn);
    auto res = seq_mem->getNodeState(qn);
    ld_check(res.hasValue());
    res_nodes[qn].addSequencerRole(
        res->sequencer_enabled, res->getConfiguredWeight());

    // generation is not meaningful for sequencer only node, however
    // to maintain compatibility with legacy format, we set it to 1.
    // for nodes also have storage role, their generation will be reset
    // below
    res_nodes[qn].generation = 1;
  }

  // storage membership / attributes
  const auto& storage_mem = config.getStorageMembership();
  const auto& storage_attr = config.getStorageConfig()->getAttributes();
  for (auto sn : storage_mem->getMembershipNodes()) {
    all_nodes.insert(sn);
    auto& node = res_nodes[sn];
    ld_check(storage_attr->hasNode(sn));
    const auto& attr = storage_attr->nodeAttributesAt(sn);
    node.addStorageRole(attr.num_shards);
    node.storage_attributes->capacity = attr.capacity;
    node.generation = attr.generation;
    node.storage_attributes->exclude_from_nodesets = attr.exclude_from_nodesets;

    folly::Optional<configuration::StorageState> ss;
    for (shard_index_t sid = 0; sid < attr.num_shards; ++sid) {
      auto res = storage_mem->getShardState(ShardID(sn, sid));
      if (!res.hasValue()) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        5,
                        "Shard %s does not exist!",
                        toString(ShardID(sn, sid)).c_str());
        err = E::INVALID_CONFIG;
        return -1;
      }

      auto shard_ss = toLegacyStorageState(res->storage_state);
      if (ss.hasValue() && ss.value() != shard_ss) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            5,
            "Shard %s has a different storage state %s that other "
            "shards (state: %s) of the same node!",
            toString(ShardID(sn, sid)).c_str(),
            configuration::storageStateToString(shard_ss).c_str(),
            configuration::storageStateToString(ss.value()).c_str());
        err = E::NOTSUPPORTED;
        return -1;
      }

      ss = shard_ss;
    }

    if (!ss.hasValue()) {
      err = E::INVALID_CONFIG;
      return -1;
    }

    node.storage_attributes->state = ss.value();
  }

  // service discovery
  const auto& serv_disc = config.getServiceDiscovery();
  for (auto n : all_nodes) {
    // must have service discovery info in config
    ld_check(serv_disc->hasNode(n));
    auto serv = serv_disc->nodeAttributesAt(n);
    auto& node = res_nodes[n];
    node.name = serv.name;
    node.address = serv.address;
    // During parsing the ServerConfig, if the gossip field is missing it's set
    // to the data address. There's no way to know if the field was actually set
    // or not. As a result, converting from an NC with a folly::none to
    // a ServerConfig and then back to the NC, you won't get the exact same
    // structure as the gossip address will be set in the returned NC.
    node.gossip_address = serv.getGossipAddress();
    node.ssl_address = serv.ssl_address;
    node.location = serv.location;
    node.roles = serv.roles;
  }

  legacy_config_out->setNodes(std::move(res_nodes));
  return 0;
}

/*static*/
std::shared_ptr<const NodesConfiguration>
NodesConfigLegacyConverter::fromLegacyNodesConfig(
    const NodesConfig& config_legacy,
    const MetaDataLogsConfig& meta_config,
    config_version_t version) {
  auto serv_disc = std::make_shared<ServiceDiscoveryConfig>();
  auto seq_mem = std::make_shared<membership::SequencerMembership>();
  auto seq_attr = std::make_shared<SequencerAttributeConfig>();
  auto storage_mem = std::make_shared<membership::StorageMembership>();
  auto storage_attr = std::make_shared<StorageAttributeConfig>();
  auto meta_rep = std::make_shared<MetaDataLogsReplication>();

  // setting all membership versions to `version'
  seq_mem->version_ = membership::MembershipVersion::Type(version.val());
  seq_mem->bootstrapping_ = false;
  storage_mem->version_ = membership::MembershipVersion::Type(version.val());
  storage_mem->bootstrapping_ = false;

  std::unordered_set<node_index_t> meta_nodes(
      meta_config.metadata_nodes.begin(), meta_config.metadata_nodes.end());

  for (const auto& it : config_legacy.getNodes()) {
    node_index_t nid = it.first;
    const auto& node = it.second;
    // service discovery
    serv_disc->setNodeAttributes(
        nid,
        {node.name,
         node.address,
         node.gossip_address.valid()
             ? folly::Optional<Sockaddr>(node.gossip_address)
             : folly::none,
         node.ssl_address,
         node.location,
         node.roles});

    if (node.hasRole(configuration::NodeRole::SEQUENCER)) {
      if (node.sequencer_attributes == nullptr) {
        err = E::INVALID_CONFIG;
        return nullptr;
      }

      bool seq_enabled = node.sequencer_attributes->enabled();
      double seq_w = node.sequencer_attributes->getConfiguredWeight();
      seq_mem->setNodeState(
          nid,
          {seq_enabled, seq_w, membership::MaintenanceID::MAINTENANCE_NONE});
      seq_attr->setNodeAttributes(nid, {});
    }

    if (node.hasRole(configuration::NodeRole::STORAGE)) {
      if (node.storage_attributes == nullptr) {
        err = E::INVALID_CONFIG;
        return nullptr;
      }

      // check if the node is part of the metadata nodeset
      membership::MetaDataStorageState ms =
          (meta_nodes.count(nid) > 0
               ? membership::MetaDataStorageState::METADATA
               : membership::MetaDataStorageState::NONE);

      // storage membership are derived from legacy storage states
      // and DISABLED is translated to NONE
      for (shard_index_t sid = 0; sid < node.getNumShards(); ++sid) {
        storage_mem->setShardState(
            ShardID(nid, sid),
            {fromLegacyStorageState(node.getStorageState()),
             membership::StorageStateFlags::NONE,
             ms,
             membership::MaintenanceID::MAINTENANCE_NONE,
             membership::MembershipVersion::Type(version.val())});
      }

      // storage attributes
      storage_attr->setNodeAttributes(
          nid,
          {node.storage_attributes->capacity,
           node.storage_attributes->num_shards,
           static_cast<node_gen_t>(node.generation),
           node.storage_attributes->exclude_from_nodesets});
    }
  }

  // metadata replication config
  if (meta_config.metadata_log_group == nullptr) {
    // TODO(T33035439): this happens in unit tests where metadata logs section
    // is omitted. Use an empty metadata logs replication.
    meta_rep->version_ = membership::MembershipVersion::EMPTY_VERSION;
    ld_check(meta_rep->validate());
  } else {
    meta_rep->version_ = membership::MembershipVersion::Type(version.val());
    meta_rep->replication_ = ReplicationProperty::fromLogAttributes(
        meta_config.metadata_log_group->attrs());
  }

  auto res = std::make_shared<NodesConfiguration>();
  res->version_ = membership::MembershipVersion::Type(version.val());
  res->service_discovery_ = std::move(serv_disc);
  res->sequencer_config_ = std::make_shared<const SequencerConfig>(
      std::move(seq_mem), std::move(seq_attr));
  res->storage_config_ = std::make_shared<const StorageConfig>(
      std::move(storage_mem), std::move(storage_attr));
  res->metadata_logs_rep_ = std::move(meta_rep);

  res->last_change_timestamp_ =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  res->last_maintenance_ = membership::MaintenanceID::MAINTENANCE_NONE;
  res->last_change_context_ = "";
  res->recomputeConfigMetadata();

  if (!res->validate()) {
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return res;
}

/*static*/
bool NodesConfigLegacyConverter::testWithServerConfig(
    const ServerConfig& server_config) {
  // test legacy conversion
  auto convert_start_time = std::chrono::steady_clock::now();
  auto converted_nodes_config =
      fromLegacyNodesConfig(server_config.getNodesConfig(),
                            server_config.getMetaDataLogsConfig(),
                            server_config.getVersion());
  if (converted_nodes_config == nullptr) {
    return false;
  }

  if (converted_nodes_config->getStorageMembership()->isBootstrapping() ||
      converted_nodes_config->getSequencerMembership()->isBootstrapping()) {
    ld_error(
        "NodesConfiguration converted from the legacy NodesConfig should not "
        "have the bootstrapping flag set. Got: sequencer_boostrapping: "
        "%d, storage_bootstrapping: %d",
        converted_nodes_config->getSequencerMembership()->isBootstrapping(),
        converted_nodes_config->getStorageMembership()->isBootstrapping());
    return false;
  }

  auto convert_end_time = std::chrono::steady_clock::now();

  ld_info("Convertion to the new format took %lu usec for config with cluster "
          "name %s.",
          SystemTimestamp(convert_end_time - convert_start_time)
              .toMicroseconds()
              .count(),
          server_config.getClusterName().c_str());

  auto new_nodes_config =
      server_config.getNodesConfigurationFromServerConfigSource();
  ld_check(new_nodes_config != nullptr);

  {
    auto old_hash = server_config.getNodesConfig().calculateHash();
    if (new_nodes_config->getStorageNodesHash() != old_hash) {
      ld_info(
          "(expected) Nodes config hash mismatch after conversion. original: "
          "%lx, new: %lx.",
          old_hash,
          new_nodes_config->getStorageNodesHash());

      // TODO T33035439: NodesConfiguration uses a new way to compute storage
      // node hash so this is expected. We should unify how storage is computed
      // in the future.

      // return false;
    }
  }

  {
    auto old_num_shards = server_config.getNodesConfig().calculateNumShards();
    if (new_nodes_config->getNumShards() != old_num_shards) {
      ld_error(
          "Nodes config num of shards mismatch after conversion! original: "
          "%u, new: %u.",
          old_num_shards,
          new_nodes_config->getNumShards());
      return false;
    }
  }

  {
    auto old_max_idx =
        server_config.getNodesConfig().getMaxNodeIdx_DEPRECATED();
    if (new_nodes_config->getMaxNodeIndex() != old_max_idx) {
      ld_error(
          "Nodes config max node index mismatch after conversion! original: "
          "%lu, new: %u.",
          old_max_idx,
          new_nodes_config->getMaxNodeIndex());
      return false;
    }
  }

  if (new_nodes_config->getSequencersConfig() !=
      server_config.getSequencers_DEPRECATED()) {
    ld_error(
        "Sequencer config does not match b/w new NC and legacy server config!");
    return false;
  }

  // check metadata shards
  std::unordered_set<node_index_t> meta_nodeset_orig(
      server_config.getMetaDataNodeIndices().begin(),
      server_config.getMetaDataNodeIndices().end());
  std::unordered_set<node_index_t> meta_nodeset_new;
  for (auto sid :
       new_nodes_config->getStorageMembership()->getMetaDataStorageSet()) {
    meta_nodeset_new.insert(sid.node());
  }

  if (meta_nodeset_orig != meta_nodeset_new) {
    ld_error("Metadata nodeset is inconsistent after conversion. original: %s, "
             "new: %s.",
             toString(meta_nodeset_orig).c_str(),
             toString(meta_nodeset_new).c_str());
    return false;
  }

  if (new_nodes_config->getStorageMembership()->getMetaDataNodeIndices() !=
      server_config.getMetaDataNodeIndices()) {
    ld_error("Metadata nodeset indices is inconsistent after conversion");
    return false;
  }

  const auto meta_log_group = server_config.getMetaDataLogGroup();
  const auto meta_rep_from_server_config =
      ReplicationProperty::fromLogAttributes(meta_log_group->attrs());
  const auto meta_rep_from_nc =
      new_nodes_config->getMetaDataLogsReplication()->getReplicationProperty();
  if (meta_rep_from_server_config != meta_rep_from_nc) {
    ld_error(
        "Metadata replication is inconsistent after conversion. original: %s, "
        "new: %s.",
        toString(meta_rep_from_server_config).c_str(),
        toString(meta_rep_from_nc).c_str());
    return false;
  }

  NodesConfig converted_back;
  int rv = toLegacyNodesConfig(*new_nodes_config, &converted_back);
  if (rv != 0) {
    return false;
  }

  auto cfg_to_compare = server_config.withNodes(std::move(converted_back));
  if (cfg_to_compare == nullptr) {
    return false;
  }

  // to compare, normalized to string and compare
  auto orig_str = server_config.toString();
  auto compare_str = cfg_to_compare->toString();
  if (compare_str != orig_str) {
    ld_error("Conversion error for config %s!",
             server_config.getClusterName().c_str());
    std::cerr << "<<<<<< Original:" << std::endl;
    std::cerr << orig_str << std::endl;
    std::cerr << "<<<<<< Converted:" << std::endl;
    std::cerr << compare_str << std::endl;
    return false;
  }

  return true;
}

/*static*/
bool NodesConfigLegacyConverter::testSerialization(
    const ServerConfig& server_config,
    bool compress) {
  auto nodes_config =
      server_config.getNodesConfigurationFromServerConfigSource();
  ld_check(nodes_config != nullptr);

  auto serialization_start_time = std::chrono::steady_clock::now();
  auto config_str =
      NodesConfigurationCodec::serialize(*nodes_config, {compress});
  auto serialization_end_time = std::chrono::steady_clock::now();
  if (config_str.empty()) {
    ld_error("Serialization failed for config %s.",
             server_config.getClusterName().c_str());
    return false;
  }

  ld_info(
      "Serialization took %lu usec with compression %s for config with cluster "
      "name %s. Blob size: %lu.",
      SystemTimestamp(serialization_end_time - serialization_start_time)
          .toMicroseconds()
          .count(),
      compress ? "ON" : "OFF",
      server_config.getClusterName().c_str(),
      config_str.size());

  auto deserialization_start_time = std::chrono::steady_clock::now();
  auto deseriazed_config = NodesConfigurationCodec::deserialize(config_str);
  auto deserialization_end_time = std::chrono::steady_clock::now();

  ld_info("Deserialization took %lu usec with compression %s for config with "
          "cluster "
          "name %s.",
          SystemTimestamp(deserialization_end_time - deserialization_start_time)
              .toMicroseconds()
              .count(),
          compress ? "ON" : "OFF",
          server_config.getClusterName().c_str());

  if (deseriazed_config == nullptr) {
    ld_error("Deserialization failed for config %s.",
             server_config.getClusterName().c_str());
    return false;
  }

  if (!(*nodes_config == *deseriazed_config)) {
    ld_error("Deserialized config is different from original for config %s.",
             server_config.getClusterName().c_str());
    return false;
  }

  return true;
}

}}}} // namespace facebook::logdevice::configuration::nodes
