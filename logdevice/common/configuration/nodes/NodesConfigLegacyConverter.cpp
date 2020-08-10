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
#include "logdevice/include/NodeLocationScope.h"

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
         /*version*/ 0,
         node.address,
         node.gossip_address.valid()
             ? folly::Optional<Sockaddr>(node.gossip_address)
             : folly::none,
         node.ssl_address,
         node.admin_address,
         node.server_to_server_address,
         node.server_thrift_api_address,
         node.client_thrift_api_address,
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
          nid, {seq_enabled, seq_w, /*manual_override=*/false});
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
             membership::MembershipVersion::Type(version.val()),
             /*manual_override=*/false});
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
  res->recomputeConfigMetadata();

  if (!res->validate()) {
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return res;
}

}}}} // namespace facebook::logdevice::configuration::nodes
