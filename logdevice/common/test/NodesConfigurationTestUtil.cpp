/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

#include <folly/Format.h>
#include <glog/logging.h>

namespace facebook {
  namespace logdevice {
    namespace NodesConfigurationTestUtil {

using namespace configuration::nodes;
using namespace membership;
using RoleSet = NodeServiceDiscovery::RoleSet;

constexpr configuration::nodes::NodeServiceDiscovery::RoleSet seq_role{1};
constexpr configuration::nodes::NodeServiceDiscovery::RoleSet storage_role{2};
constexpr configuration::nodes::NodeServiceDiscovery::RoleSet both_role{3};

const membership::MaintenanceID::Type DUMMY_MAINTENANCE{2333};
const membership::MaintenanceID::Type DUMMY_MAINTENANCE2{2334};

NodeServiceDiscovery genDiscovery(node_index_t n,
                                  RoleSet roles,
                                  std::string location) {
  folly::Optional<NodeLocation> l;
  if (!location.empty()) {
    l = NodeLocation();
    l.value().fromDomainString(location);
  }
  std::string addr = folly::sformat("127.0.0.{}", n);
  return NodeServiceDiscovery{Sockaddr(addr, 4440),
                              Sockaddr(addr, 4441),
                              /*ssl address*/ folly::none,
                              l,
                              roles,
                              "host" + std::to_string(n)};
}

NodesConfiguration::Update
initialProvisionUpdate(std::vector<NodeTemplate> nodes,
                       ReplicationProperty metadata_rep) {
  NodesConfiguration::Update update{};

  // 1. provision service discovery config
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  for (const auto& node : nodes) {
    update.service_discovery_update->addNode(
        node.id,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(
                genDiscovery(node.id, node.roles, node.location))});
  }

  // 2. provision sequencer config
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.sequencer_config_update->attributes_update =
      std::make_unique<SequencerAttributeConfig::Update>();

  for (const auto& node : nodes) {
    if (hasRole(node.roles, NodeRole::SEQUENCER)) {
      update.sequencer_config_update->membership_update->addNode(
          node.id,
          {SequencerMembershipTransition::ADD_NODE,
           true,
           node.sequencer_weight,
           MaintenanceID::MAINTENANCE_PROVISION});

      update.sequencer_config_update->attributes_update->addNode(
          node.id,
          {SequencerAttributeConfig::UpdateType::PROVISION,
           std::make_unique<SequencerNodeAttribute>()});
    }
  }

  // 3. provision storage config
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();

  for (const auto& node : nodes) {
    if (hasRole(node.roles, NodeRole::STORAGE)) {
      for (int s = 0; s < node.num_shards; ++s) {
        update.storage_config_update->membership_update->addShard(
            ShardID(node.id, s),
            {node.metadata_node
                 ? StorageStateTransition::PROVISION_METADATA_SHARD
                 : StorageStateTransition::PROVISION_SHARD,
             (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
              Condition::NO_SELF_REPORT_MISSING_DATA |
              Condition::LOCAL_STORE_WRITABLE),
             MaintenanceID::MAINTENANCE_PROVISION,
             /* state_override = */ folly::none});
      }
      update.storage_config_update->attributes_update->addNode(
          node.id,
          {StorageAttributeConfig::UpdateType::PROVISION,
           std::make_unique<StorageNodeAttribute>(
               StorageNodeAttribute{/*capacity=*/256.0,
                                    /*num_shards*/ node.num_shards,
                                    /*generation*/ 1,
                                    /*exclude_from_nodesets*/ false})});
    }
  }

  // 4. provisoin metadata logs replication
  update.metadata_logs_rep_update =
      std::make_unique<MetaDataLogsReplication::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.metadata_logs_rep_update->replication = metadata_rep;

  // 5. fill other update metadata
  update.maintenance = MaintenanceID::MAINTENANCE_PROVISION;
  update.context = "initial provision";

  VLOG(1) << "update: " << update.toString();
  return update;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration> provisionNodes(
    configuration::nodes::NodesConfiguration::Update provision_update) {
  auto config = std::make_shared<const NodesConfiguration>();
  auto new_config = config->applyUpdate(std::move(provision_update));
  ld_assert(new_config != nullptr);
  return new_config;
}

NodesConfiguration::Update initialProvisionUpdate() {
  std::vector<NodeTemplate> nodes;
  std::map<node_index_t, RoleSet> role_map = {{1, both_role},
                                              {2, storage_role},
                                              {7, seq_role},
                                              {9, storage_role},
                                              {11, storage_role},
                                              {13, storage_role}};
  for (node_index_t n : NodeSetIndices({1, 2, 7, 9, 11, 13})) {
    nodes.push_back({n,
                     role_map[n],
                     n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff",
                     n == 1 ? 1.0 : 7.0,
                     /*num_shards=*/1,
                     n == 2 || n == 9 ? true : false});
  }

  return initialProvisionUpdate(
      std::move(nodes), ReplicationProperty{{NodeLocationScope::RACK, 2}});
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes() {
  return provisionNodes(initialProvisionUpdate());
}

configuration::nodes::NodesConfiguration::Update
addNewNodeUpdate(const configuration::nodes::NodesConfiguration& existing,
                 NodeTemplate node) {
  NodesConfiguration::Update update{};
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();
  update.service_discovery_update->addNode(
      node.id,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::PROVISION,
          std::make_unique<NodeServiceDiscovery>(
              genDiscovery(node.id, node.roles, node.location))});

  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          existing.getSequencerMembership()->getVersion());
  update.sequencer_config_update->attributes_update =
      std::make_unique<SequencerAttributeConfig::Update>();

  if (hasRole(node.roles, NodeRole::SEQUENCER)) {
    update.sequencer_config_update->membership_update->addNode(
        node.id,
        {SequencerMembershipTransition::ADD_NODE,
         true,
         node.sequencer_weight,
         DUMMY_MAINTENANCE});

    update.sequencer_config_update->attributes_update->addNode(
        node.id,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});
  }

  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          existing.getStorageMembership()->getVersion());
  if (hasRole(node.roles, NodeRole::STORAGE)) {
    for (int s = 0; s < node.num_shards; ++s) {
      update.storage_config_update->membership_update->addShard(
          ShardID(node.id, s),
          {node.metadata_node ? StorageStateTransition::ADD_EMPTY_METADATA_SHARD
                              : StorageStateTransition::ADD_EMPTY_SHARD,
           Condition::FORCE,
           DUMMY_MAINTENANCE,
           /* state_override = */ folly::none});
    }
    update.storage_config_update->attributes_update->addNode(
        node.id,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ node.num_shards,
                                  /*generation*/ 1,
                                  /*exclude_from_nodesets*/ false})});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update
addNewNodeUpdate(const configuration::nodes::NodesConfiguration& existing) {
  return addNewNodeUpdate(
      existing, {17, both_role, "aa.bb.cc.dd.ee", 0.0, 1, false});
}

NodesConfiguration::Update
enablingReadUpdate(MembershipVersion::Type base_version) {
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(base_version);

  update.storage_config_update->membership_update->addShard(
      ShardID{17, 0},
      {StorageStateTransition::ENABLING_READ,
       Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
           Condition::NO_SELF_REPORT_MISSING_DATA |
           Condition::CAUGHT_UP_LOCAL_CONFIG,
       DUMMY_MAINTENANCE2,
       /* state_override = */ folly::none});
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update
disablingWriteUpdate(membership::MembershipVersion::Type base_version) {
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(base_version);

  for (node_index_t n : NodeSetIndices({11, 13})) {
    update.storage_config_update->membership_update->addShard(
        ShardID{n, 0},
        {StorageStateTransition::DISABLING_WRITE,
         Condition::WRITE_AVAILABILITY_CHECK | Condition::CAPACITY_CHECK,
         DUMMY_MAINTENANCE2,
         /* state_override = */ folly::none});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

}}} // namespace facebook::logdevice::NodesConfigurationTestUtil
