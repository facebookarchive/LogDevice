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

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"

namespace facebook {
  namespace logdevice {
    namespace NodesConfigurationTestUtil {

using namespace configuration::nodes;
using namespace membership;
using RoleSet = NodeServiceDiscovery::RoleSet;

constexpr configuration::nodes::NodeServiceDiscovery::RoleSet kSeqRole{1};
constexpr configuration::nodes::NodeServiceDiscovery::RoleSet kStorageRole{2};
constexpr configuration::nodes::NodeServiceDiscovery::RoleSet kBothRoles{3};

NodeServiceDiscovery genDiscovery(node_index_t n,
                                  RoleSet roles,
                                  std::string location) {
  folly::Optional<NodeLocation> l;
  if (!location.empty()) {
    l = NodeLocation();
    l.value().fromDomainString(location);
  }
  std::string addr = folly::sformat("127.0.0.{}", n);
  return NodeServiceDiscovery{folly::sformat("server-{}", n),
                              /*version*/ 0,
                              Sockaddr(addr, 4440),
                              Sockaddr(addr, 4441),
                              /*ssl address*/ folly::none,
                              /*admin address*/ Sockaddr(addr, 6440),
                              l,
                              roles};
}

configuration::nodes::NodesConfiguration::Update
markAllShardProvisionedUpdate(const NodesConfiguration& nc) {
  // MARK_SHARD_PROVISIONED transaction
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          nc.getStorageMembership()->getVersion());
  const auto& storage = nc.getStorageMembership();
  for (const auto nid : *storage) {
    for (const auto& [shard_idx, state] : storage->getShardStates(nid)) {
      if (state.storage_state != StorageState::PROVISIONING) {
        continue;
      }
      update.storage_config_update->membership_update->addShard(
          ShardID(nid, shard_idx),
          {StorageStateTransition::MARK_SHARD_PROVISIONED,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA),
           /* state_override = */ folly::none});
    }
  }
  return update;
}

configuration::nodes::NodesConfiguration::Update
bootstrapEnableAllShardsUpdate(const NodesConfiguration& nc,
                               std::unordered_set<ShardID> metadata_shards) {
  // BOOTSTRAP_ENABLE_SHARDS transaction
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          nc.getStorageMembership()->getVersion());
  const auto& storage = nc.getStorageMembership();
  for (const auto nid : *storage) {
    for (const auto& [shard_idx, state] : storage->getShardStates(nid)) {
      if (state.storage_state != StorageState::NONE) {
        continue;
      }
      auto shard = ShardID(nid, shard_idx);
      update.storage_config_update->membership_update->addShard(
          shard,
          {metadata_shards.find(shard) == metadata_shards.end()
               ? StorageStateTransition::BOOTSTRAP_ENABLE_SHARD
               : StorageStateTransition::BOOTSTRAP_ENABLE_METADATA_SHARD,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA |
            Condition::LOCAL_STORE_WRITABLE),
           /* state_override = */ folly::none});
    }
  }
  return update;
}

configuration::nodes::NodesConfiguration::Update
finalizeBootstrappingUpdate(const NodesConfiguration& nc) {
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(
          nc.getStorageMembership()->getVersion());
  update.storage_config_update->membership_update->finalizeBootstrapping();
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          nc.getSequencerMembership()->getVersion());
  update.sequencer_config_update->membership_update->finalizeBootstrapping();
  return update;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration> provisionNodes(
    configuration::nodes::NodesConfiguration::Update provision_update,
    std::unordered_set<ShardID> metadata_shards) {
  auto config = std::make_shared<const NodesConfiguration>();
  config = config->applyUpdate(std::move(provision_update));
  ld_assert(config != nullptr);
  config = config->applyUpdate(markAllShardProvisionedUpdate(*config));
  ld_assert(config != nullptr);
  config = config->applyUpdate(
      bootstrapEnableAllShardsUpdate(*config, std::move(metadata_shards)));
  ld_assert(config != nullptr);
  config = config->applyUpdate(finalizeBootstrappingUpdate(*config));
  ld_assert(config != nullptr);
  VLOG(1) << "config: " << NodesConfigurationCodec::debugJsonString(*config);
  return config;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes() {
  return provisionNodes(
      initialAddShardsUpdate(), {ShardID(2, 0), ShardID(9, 0)});
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes(std::vector<node_index_t> node_idxs,
               std::unordered_set<ShardID> metadata_shards) {
  return provisionNodes(
      initialAddShardsUpdate(std::move(node_idxs)), std::move(metadata_shards));
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes(std::vector<NodeTemplate> nodes,
               ReplicationProperty metadata_rep) {
  std::unordered_set<ShardID> metadata_shards;
  for (const auto& node : nodes) {
    for (int i = 0; i < node.num_shards; i++) {
      metadata_shards.insert(ShardID(node.id, i));
    }
  }
  return provisionNodes(
      initialAddShardsUpdate(std::move(nodes), std::move(metadata_rep)),
      std::move(metadata_shards));
}

NodesConfiguration::Update
initialAddShardsUpdate(std::vector<NodeTemplate> nodes,
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
           node.sequencer_weight});

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
            {StorageStateTransition::ADD_EMPTY_SHARD,
             Condition::NONE,
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

  VLOG(1) << "update: " << update.toString();
  return update;
}

NodesConfiguration::Update
initialAddShardsUpdate(std::vector<node_index_t> node_idxs) {
  std::vector<NodeTemplate> nodes;
  for (auto nid : node_idxs) {
    nodes.push_back({nid,
                     kBothRoles,
                     "aa.bb.cc.dd.ee",
                     1.0,
                     /* num_shard=*/1,
                     /*metadata_node=*/false /* doesn't matter */});
  }
  return initialAddShardsUpdate(
      std::move(nodes), ReplicationProperty{{NodeLocationScope::RACK, 2}});
}

NodesConfiguration::Update initialAddShardsUpdate() {
  std::vector<NodeTemplate> nodes;
  std::map<node_index_t, RoleSet> role_map = {{1, kBothRoles},
                                              {2, kStorageRole},
                                              {7, kSeqRole},
                                              {9, kStorageRole},
                                              {11, kStorageRole},
                                              {13, kStorageRole}};
  for (node_index_t n : NodeSetIndices({1, 2, 7, 9, 11, 13})) {
    nodes.push_back({n,
                     role_map[n],
                     n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff",
                     n == 1 ? 1.0 : 7.0,
                     /*num_shards=*/1,
                     /*metadata_node=*/false /* doesn't matter */});
  }

  return initialAddShardsUpdate(
      std::move(nodes), ReplicationProperty{{NodeLocationScope::RACK, 2}});
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
        {SequencerMembershipTransition::ADD_NODE, true, node.sequencer_weight});

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
          {StorageStateTransition::ADD_EMPTY_SHARD,
           Condition::FORCE,
           /* state_override = */ folly::none});
    }
    update.storage_config_update->attributes_update->addNode(
        node.id,
        {StorageAttributeConfig::UpdateType::PROVISION,
         std::make_unique<StorageNodeAttribute>(
             StorageNodeAttribute{/*capacity=*/256.0,
                                  /*num_shards*/ node.num_shards,
                                  /*generation*/ node.generation,
                                  /*exclude_from_nodesets*/ false})});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update
addNewNodeUpdate(const configuration::nodes::NodesConfiguration& existing,
                 node_index_t new_node_idx) {
  // We don't check whether the existing NC has the new node already since this
  // is something a user could do and we'd want to test for. In that case, when
  // the user tries to apply / propose the update, they'll get an error.
  return addNewNodeUpdate(
      existing, {new_node_idx, kBothRoles, "aa.bb.cc.dd.ee", 0.0, 1});
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
         /* state_override = */ folly::none});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}
}}} // namespace facebook::logdevice::NodesConfigurationTestUtil
