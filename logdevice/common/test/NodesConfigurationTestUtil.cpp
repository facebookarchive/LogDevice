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
using TagMap = NodeServiceDiscovery::TagMap;

using Node = configuration::Node;
using Nodes = configuration::Nodes;

constexpr RoleSet kSeqRole{1};
constexpr RoleSet kStorageRole{2};
constexpr RoleSet kBothRoles{3};
const TagMap kTestTags{{"key1", "value1"}, {"key_2", ""}};

NodeServiceDiscovery genDiscovery(node_index_t n, const Node& node) {
  std::string addr = folly::sformat("127.0.0.{}", n);
  return NodeServiceDiscovery{
      node.name,
      /*version*/ 0,
      node.address.valid() ? node.address : Sockaddr(addr, 4440),
      node.gossip_address.valid()
          ? folly::Optional<Sockaddr>(node.gossip_address)
          : folly::none,
      node.ssl_address,
      node.admin_address,
      node.server_to_server_address,
      node.server_thrift_api_address,
      node.client_thrift_api_address,
      node.location,
      node.roles,
      node.tags};
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
provisionNodes(configuration::Nodes nodes, ReplicationProperty metadata_rep) {
  // TODO(mbassem): Remove in the next diffs.
  // If the passed structure doesn't have any metadata node, consider all of
  // them metadata nodes.
  auto num_metadata = std::count_if(nodes.begin(), nodes.end(), [](auto it) {
    return it.second.metadata_node;
  });

  std::unordered_set<ShardID> metadata_shards;
  for (const auto& [nid, node] : nodes) {
    if (hasRole(node.roles, NodeRole::STORAGE) &&
        (node.metadata_node || num_metadata == 0)) {
      for (int i = 0; i < node.storage_attributes->num_shards; i++) {
        metadata_shards.insert(ShardID(nid, i));
      }
    }
  }
  return provisionNodes(
      initialAddShardsUpdate(std::move(nodes), std::move(metadata_rep)),
      std::move(metadata_shards));
}

NodesConfiguration::Update
initialAddShardsUpdate(configuration::Nodes nodes,
                       ReplicationProperty metadata_rep) {
  NodesConfiguration::Update update{};

  // 1. provision service discovery config
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  for (const auto& [nid, node] : nodes) {
    update.service_discovery_update->addNode(
        nid,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(genDiscovery(nid, node))});
  }

  // 2. provision sequencer config
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          MembershipVersion::EMPTY_VERSION);
  update.sequencer_config_update->attributes_update =
      std::make_unique<SequencerAttributeConfig::Update>();

  for (const auto& [nid, node] : nodes) {
    if (hasRole(node.roles, NodeRole::SEQUENCER)) {
      update.sequencer_config_update->membership_update->addNode(
          nid,
          {SequencerMembershipTransition::ADD_NODE,
           node.sequencer_attributes->enabled,
           node.sequencer_attributes->weight});

      update.sequencer_config_update->attributes_update->addNode(
          nid,
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

  for (const auto& [nid, node] : nodes) {
    if (hasRole(node.roles, NodeRole::STORAGE)) {
      for (int s = 0; s < node.storage_attributes->num_shards; ++s) {
        update.storage_config_update->membership_update->addShard(
            ShardID(nid, s),
            {StorageStateTransition::ADD_EMPTY_SHARD,
             Condition::NONE,
             /* state_override = */ folly::none});
      }
      update.storage_config_update->attributes_update->addNode(
          nid,
          {StorageAttributeConfig::UpdateType::PROVISION,
           std::make_unique<StorageNodeAttribute>(StorageNodeAttribute{
               /*capacity=*/node.storage_attributes->capacity,
               /*num_shards*/ node.storage_attributes->num_shards,
               /*generation*/ node_gen_t(node.generation),
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

namespace {
configuration::Node defaultNodeTemplate(node_index_t idx) {
  std::string addr = folly::sformat("127.0.0.{}", idx);
  return Node()
      .setAddress(Sockaddr(addr, 4440))
      .setGossipAddress(Sockaddr(addr, 4441))
      .setTags(kTestTags)
      .setLocation("aa.bb.cc.dd.ee")
      .addSequencerRole(true, 1)
      .addStorageRole(1, 1)
      .setIsMetadataNode(false);
}
} // namespace

NodesConfiguration::Update
initialAddShardsUpdate(std::vector<node_index_t> node_idxs) {
  configuration::Nodes nodes;
  for (auto nid : node_idxs) {
    nodes.emplace(nid, defaultNodeTemplate(nid));
  }
  return initialAddShardsUpdate(
      std::move(nodes), ReplicationProperty{{NodeLocationScope::RACK, 2}});
}

NodesConfiguration::Update initialAddShardsUpdate() {
  configuration::Nodes nodes;
  std::map<node_index_t, RoleSet> role_map = {{1, kBothRoles},
                                              {2, kStorageRole},
                                              {7, kSeqRole},
                                              {9, kStorageRole},
                                              {11, kStorageRole},
                                              {13, kStorageRole}};
  for (node_index_t n : NodeSetIndices({1, 2, 7, 9, 11, 13})) {
    Node node = Node::withTestDefaults(n, false, false);
    node.setTags(kTestTags).setIsMetadataNode(false).setLocation(
        n % 2 == 0 ? "aa.bb.cc.dd.ee" : "aa.bb.cc.dd.ff");
    if (hasRole(role_map[n], NodeRole::SEQUENCER)) {
      node.addSequencerRole(true, n == 1 ? 1.0 : 7.0);
    }
    if (hasRole(role_map[n], NodeRole::STORAGE)) {
      node.addStorageRole(1, 256.0);
    }
    nodes.emplace(n, std::move(node));
  }

  return initialAddShardsUpdate(
      std::move(nodes), ReplicationProperty{{NodeLocationScope::RACK, 2}});
}

configuration::nodes::NodesConfiguration::Update
addNewNodeUpdate(const configuration::nodes::NodesConfiguration& existing,
                 const Node& node,
                 folly::Optional<node_index_t> idx) {
  return addNewNodesUpdate(
      existing, {{idx.value_or(existing.getMaxNodeIndex() + 1), node}});
}
configuration::nodes::NodesConfiguration::Update
addNewNodesUpdate(const configuration::nodes::NodesConfiguration& existing,
                  configuration::Nodes nodes) {
  NodesConfiguration::Update update{};
  update.service_discovery_update =
      std::make_unique<ServiceDiscoveryConfig::Update>();

  for (auto [nid, node] : nodes) {
    update.service_discovery_update->addNode(
        nid,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::PROVISION,
            std::make_unique<NodeServiceDiscovery>(genDiscovery(nid, node))});

    if (hasRole(node.roles, NodeRole::SEQUENCER)) {
      if (update.sequencer_config_update == nullptr) {
        update.sequencer_config_update =
            std::make_unique<SequencerConfig::Update>();
        update.sequencer_config_update->membership_update =
            std::make_unique<SequencerMembership::Update>(
                existing.getSequencerMembership()->getVersion());
        update.sequencer_config_update->attributes_update =
            std::make_unique<SequencerAttributeConfig::Update>();
      }
      update.sequencer_config_update->membership_update->addNode(
          nid,
          {SequencerMembershipTransition::ADD_NODE,
           true,
           node.sequencer_attributes->weight});

      update.sequencer_config_update->attributes_update->addNode(
          nid,
          {SequencerAttributeConfig::UpdateType::PROVISION,
           std::make_unique<SequencerNodeAttribute>()});
    }

    if (hasRole(node.roles, NodeRole::STORAGE)) {
      if (update.storage_config_update == nullptr) {
        update.storage_config_update =
            std::make_unique<StorageConfig::Update>();
        update.storage_config_update->attributes_update =
            std::make_unique<StorageAttributeConfig::Update>();
        update.storage_config_update->membership_update =
            std::make_unique<StorageMembership::Update>(
                existing.getStorageMembership()->getVersion());
      }
      for (int s = 0; s < node.storage_attributes->num_shards; ++s) {
        update.storage_config_update->membership_update->addShard(
            ShardID(nid, s),
            {StorageStateTransition::ADD_EMPTY_SHARD,
             Condition::FORCE,
             /* state_override = */ folly::none});
      }
      update.storage_config_update->attributes_update->addNode(
          nid,
          {StorageAttributeConfig::UpdateType::PROVISION,
           std::make_unique<StorageNodeAttribute>(StorageNodeAttribute{
               /*capacity*/ node.storage_attributes->capacity,
               /*num_shards*/ node.storage_attributes->num_shards,
               /*generation*/ node_gen_t(node.generation),
               /*exclude_from_nodesets*/ false})});
    }
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
      existing, defaultNodeTemplate(new_node_idx), new_node_idx);
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

configuration::nodes::NodesConfiguration::Update setStorageMembershipUpdate(
    const configuration::nodes::NodesConfiguration& existing,
    std::vector<ShardID> shards,
    folly::Optional<membership::StorageState> target_storage_state,
    folly::Optional<membership::MetaDataStorageState> target_metadata_state) {
  std::vector<ShardID> target_shards;
  for (auto shard : shards) {
    if (shard.shard() != -1) {
      target_shards.push_back(shard);
    } else {
      auto storage_attr = existing.getNodeStorageAttribute(shard.node());
      ld_check(storage_attr);
      for (shard_index_t i = 0; i < storage_attr->num_shards; i++) {
        target_shards.emplace_back(shard.node(), i);
      }
    }
  }

  NodesConfiguration::Update update{};
  if (target_storage_state.hasValue() || target_metadata_state.hasValue()) {
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<StorageMembership::Update>(
            existing.getStorageMembership()->getVersion());
    for (auto shard : target_shards) {
      auto storage_mem = existing.getStorageMembership()->getShardState(shard);
      ld_check(storage_mem.hasValue());
      update.storage_config_update->membership_update->addShard(
          shard,
          {StorageStateTransition::OVERRIDE_STATE,
           Condition::FORCE,
           /* state_override = */
           ShardState::Update::StateOverride{
               target_storage_state.value_or(storage_mem->storage_state),
               storage_mem->flags,
               target_metadata_state.value_or(storage_mem->metadata_state)}});
    }
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update setSequencerEnabledUpdate(
    const configuration::nodes::NodesConfiguration& existing,
    std::vector<node_index_t> nodes,
    bool target_sequencer_enabled) {
  NodesConfiguration::Update update{};
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          existing.getSequencerMembership()->getVersion());
  for (auto node : nodes) {
    auto seq_mem = existing.getSequencerMembership()->getNodeState(node);
    ld_check(seq_mem.hasValue());
    update.sequencer_config_update->membership_update->addNode(
        node,
        {SequencerMembershipTransition::SET_ENABLED_FLAG,
         target_sequencer_enabled,
         /* sequencer_weight */ 0 /* doesn't matter */});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update setSequencerWeightUpdate(
    const configuration::nodes::NodesConfiguration& existing,
    std::vector<node_index_t> nodes,
    double target_sequencer_weight) {
  NodesConfiguration::Update update{};
  update.sequencer_config_update = std::make_unique<SequencerConfig::Update>();
  update.sequencer_config_update->membership_update =
      std::make_unique<SequencerMembership::Update>(
          existing.getSequencerMembership()->getVersion());
  for (auto node : nodes) {
    auto seq_mem = existing.getSequencerMembership()->getNodeState(node);
    ld_check(seq_mem.hasValue());
    update.sequencer_config_update->membership_update->addNode(
        node,
        {SequencerMembershipTransition::SET_WEIGHT,
         /* sequencer_enabled */ false /* doesn't matter */,
         target_sequencer_weight});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update excludeFromNodesetUpdate(
    const configuration::nodes::NodesConfiguration& existing,
    std::vector<node_index_t> nodes,
    bool target_exclude_from_nodeset) {
  NodesConfiguration::Update update{};
  update.storage_config_update = std::make_unique<StorageConfig::Update>();
  update.storage_config_update->attributes_update =
      std::make_unique<StorageAttributeConfig::Update>();

  for (auto node : nodes) {
    auto attrs = existing.getNodeStorageAttribute(node);
    ld_check(attrs);
    auto new_attrs = std::make_unique<StorageNodeAttribute>(*attrs);
    new_attrs->exclude_from_nodesets = target_exclude_from_nodeset;
    update.storage_config_update->attributes_update->addNode(
        node,
        {StorageAttributeConfig::UpdateType::RESET, std::move(new_attrs)});
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update
setMetadataReplicationPropertyUpdate(
    const configuration::nodes::NodesConfiguration& existing,
    ReplicationProperty metadata_rep) {
  NodesConfiguration::Update update{};
  update.metadata_logs_rep_update =
      std::make_unique<MetaDataLogsReplication::Update>(
          existing.getMetaDataLogsReplication()->getVersion());
  update.metadata_logs_rep_update->replication = metadata_rep;
  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update setNodeAttributesUpdate(
    node_index_t node,
    folly::Optional<configuration::nodes::NodeServiceDiscovery> svc_disc,
    folly::Optional<configuration::nodes::SequencerNodeAttribute> seq_attrs,
    folly::Optional<configuration::nodes::StorageNodeAttribute> storage_attrs) {
  NodesConfiguration::Update update{};

  if (svc_disc.has_value()) {
    update.service_discovery_update =
        std::make_unique<ServiceDiscoveryConfig::Update>();
    update.service_discovery_update->addNode(
        node,
        ServiceDiscoveryConfig::NodeUpdate{
            ServiceDiscoveryConfig::UpdateType::RESET,
            std::make_unique<NodeServiceDiscovery>(*svc_disc)});
  }

  if (seq_attrs.has_value()) {
    update.sequencer_config_update =
        std::make_unique<SequencerConfig::Update>();

    update.sequencer_config_update->attributes_update =
        std::make_unique<SequencerAttributeConfig::Update>();

    update.sequencer_config_update->attributes_update->addNode(
        node,
        {SequencerAttributeConfig::UpdateType::RESET,
         std::make_unique<SequencerNodeAttribute>(*seq_attrs)});
  }

  if (storage_attrs.has_value()) {
    update.storage_config_update = std::make_unique<StorageConfig::Update>();
    update.storage_config_update->attributes_update =
        std::make_unique<StorageAttributeConfig::Update>();

    update.storage_config_update->attributes_update->addNode(
        node,
        {StorageAttributeConfig::UpdateType::RESET,
         std::make_unique<StorageNodeAttribute>(*storage_attrs)});
  }

  VLOG(1) << "update: " << update.toString();
  return update;
}

configuration::nodes::NodesConfiguration::Update
shrinkNodesUpdate(const configuration::nodes::NodesConfiguration& existing,
                  std::vector<node_index_t> nodes) {
  NodesConfiguration::Update update{};

  for (auto node_idx : nodes) {
    // Service Discovery Update
    if (update.service_discovery_update == nullptr) {
      update.service_discovery_update =
          std::make_unique<ServiceDiscoveryConfig::Update>();
    }
    update.service_discovery_update->addNode(
        node_idx, {ServiceDiscoveryConfig::UpdateType::REMOVE, nullptr});

    // Sequencer Config Update
    if (existing.isSequencerNode(node_idx)) {
      // Sequencer Membership Update
      if (update.sequencer_config_update == nullptr) {
        update.sequencer_config_update =
            std::make_unique<SequencerConfig::Update>();
        update.sequencer_config_update->membership_update =
            std::make_unique<SequencerMembership::Update>(
                existing.getSequencerMembership()->getVersion());
        update.sequencer_config_update->attributes_update =
            std::make_unique<SequencerAttributeConfig::Update>();
      }
      update.sequencer_config_update->membership_update->addNode(
          node_idx,
          {SequencerMembershipTransition::REMOVE_NODE,
           /* sequencer_enabled= */ false,
           /* weight= */ 0});

      // Sequencer Config
      update.sequencer_config_update->attributes_update->addNode(
          node_idx, {SequencerAttributeConfig::UpdateType::REMOVE, nullptr});
    }

    // Storage Config Update
    if (existing.isStorageNode(node_idx)) {
      // Storage Membership Update
      if (update.storage_config_update == nullptr) {
        update.storage_config_update =
            std::make_unique<StorageConfig::Update>();
        update.storage_config_update->membership_update =
            std::make_unique<StorageMembership::Update>(
                existing.getStorageMembership()->getVersion());
        update.storage_config_update->attributes_update =
            std::make_unique<StorageAttributeConfig::Update>();
      }

      auto shards = existing.getStorageMembership()->getShardStates(node_idx);
      for (const auto& shard : shards) {
        update.storage_config_update->membership_update->addShard(
            ShardID(node_idx, shard.first),
            {StorageStateTransition::REMOVE_EMPTY_SHARD, Condition::NONE});
      }

      // Storage Attributes Update
      update.storage_config_update->attributes_update->addNode(
          node_idx, {StorageAttributeConfig::UpdateType::REMOVE, nullptr});
    }
  }
  VLOG(1) << "update: " << update.toString();
  return update;
}
}}} // namespace facebook::logdevice::NodesConfigurationTestUtil
