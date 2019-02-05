/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

#include <folly/Format.h>

namespace facebook { namespace logdevice {

using namespace configuration::nodes;
using namespace membership;
using RoleSet = NodeServiceDiscovery::RoleSet;

const configuration::nodes::NodeServiceDiscovery::RoleSet seq_role{1};
const configuration::nodes::NodeServiceDiscovery::RoleSet storage_role{2};
const configuration::nodes::NodeServiceDiscovery::RoleSet both_role{3};

const membership::MaintenanceID::Type DUMMY_MAINTENANCE{2333};

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

NodesConfiguration::Update initialProvisionUpdate() {
  NodesConfiguration::Update update;

  // 1. provision service discovery config
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  std::map<node_index_t, RoleSet> role_map = {
      {1, both_role}, {2, storage_role}, {7, seq_role}, {9, storage_role}};

  for (node_index_t n : NodeSetIndices({1, 2, 7, 9})) {
    update.service_discovery_update->addNode(
        n,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(genDiscovery(
                n,
                role_map[n],
                n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff"))});
  }
  // 2. provision sequencer config
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.sequencer_config_update->attributes_update =
      std::make_unique<SequencerAttributeConfig::Update>();

  for (node_index_t n : NodeSetIndices({1, 7})) {
    update.sequencer_config_update->membership_update->addNode(
        n,
        {SequencerMembershipTransition::ADD_NODE,
         n == 1 ? 1.0 : 7.0,
         MaintenanceID::MAINTENANCE_PROVISION});

    update.sequencer_config_update->attributes_update->addNode(
        n,
        {SequencerAttributeConfig::UpdateType::PROVISION,
         std::make_unique<SequencerNodeAttribute>()});
  }

  // 3. provision storage config
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();

  for (node_index_t n : NodeSetIndices({1, 2, 9})) {
    update.storage_config_update->membership_update->addShard(
        ShardID(n, 0),
        {n == 1 ? StorageStateTransition::PROVISION_SHARD
                : StorageStateTransition::PROVISION_METADATA_SHARD,
         (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
          Condition::NO_SELF_REPORT_MISSING_DATA |
          Condition::LOCAL_STORE_WRITABLE),
         MaintenanceID::MAINTENANCE_PROVISION,
         /* state_override = */ folly::none});

    update.storage_config_update->attributes_update->addNode(
        n,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ 1,
                                  /*generation*/ 1,
                                  /*exclude_from_nodesets*/ false})});
  }

  // 4. provisoin metadata logs replication
  update.metadata_logs_rep_update =
      std::make_unique<MetaDataLogsReplication::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.metadata_logs_rep_update->replication.assign(
      {{NodeLocationScope::RACK, 2}});

  // 5. fill other update metadata
  update.maintenance = MaintenanceID::MAINTENANCE_PROVISION;
  update.context = "initial provision";

  return update;
}

// provision a LD nodes config with:
// 1) four nodes N1, N2, N7, N9
// 2) N1 and N7 have sequencer role; while N1, N2 and N9 have storage role;
// 3) N2 and N9 are metadata storage nodes, metadata logs replicaton is
//    (rack, 2)
std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes() {
  auto config = std::make_shared<const NodesConfiguration>();
  NodesConfiguration::Update update = initialProvisionUpdate();
  // finally perform the update
  auto new_config = config->applyUpdate(std::move(update));
  assert(new_config != nullptr);
  return new_config;
}

NodesConfiguration::Update addNewNodeUpdate() {
  NodesConfiguration::Update update;
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();
  update.service_discovery_update->addNode(
      17,
      ServiceDiscoveryConfig::NodeUpdate{
          ServiceDiscoveryConfig::UpdateType::PROVISION,
          std::make_unique<NodeServiceDiscovery>(
              genDiscovery(17, both_role, "aa.bb.cc.dd.ee"))});
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();
  update.storage_config_update->attributes_update->addNode(
      17,
      {StorageAttributeConfig::UpdateType::PROVISION,
       std::make_unique<StorageNodeAttribute>(
           StorageNodeAttribute{/*capacity=*/256.0,
                                /*num_shards*/ 1,
                                /*generation*/ 1,
                                /*exclude_from_nodesets*/ false})});
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          MembershipVersion::MIN_VERSION);
  update.storage_config_update->membership_update->addShard(
      ShardID(17, 0),
      {StorageStateTransition::ADD_EMPTY_SHARD,
       Condition::FORCE,
       DUMMY_MAINTENANCE,
       /* state_override = */ folly::none});
  return update;
}
}} // namespace facebook::logdevice
