/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/MetadataNodesetSelector.h"

#include <gtest/gtest.h>

#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

namespace facebook { namespace logdevice {

using NodeTemplate = NodesConfigurationTestUtil::NodeTemplate;
class MetadataNodesetSelectorTest : public ::testing::Test {
 public:
  explicit MetadataNodesetSelectorTest() {}

  void init();
  void addNewRack(std::string rack_location, std::vector<node_index_t> nodes);

  void overrideStorageState(
      std::unordered_map<ShardID, membership::StorageState> map);
  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_config_;
  ReplicationProperty replication_property_;
};

void MetadataNodesetSelectorTest::init() {
  int i, j = 0;
  std::vector<NodeTemplate> nodes;
  std::unordered_set<ShardID> metadata_shards;
  for (auto loc : {"rg1.dc1.msb1.rw1.rk1",
                   "rg1.dc1.msb1.rw1.rk2",
                   "rg1.dc1.msb1.rw1.rk3"}) {
    i = 0;
    while (i < 2) {
      NodeTemplate node;
      node.id = j++;
      node.location = loc;
      node.num_shards = 1;
      if (i == 1) {
        metadata_shards.insert(ShardID(node.id, 0));
      }
      node.metadata_node = i == 1 ? true : false;
      nodes.push_back(node);
      i++;
    }
  }
  auto update = NodesConfigurationTestUtil::initialAddShardsUpdate(
      nodes, replication_property_);
  nodes_config_ = NodesConfigurationTestUtil::provisionNodes(
      std::move(update), metadata_shards);
  ld_check(nodes_config_);
}

void MetadataNodesetSelectorTest::addNewRack(std::string rack_location,
                                             std::vector<node_index_t> nodes) {
  std::unordered_map<ShardID, membership::StorageState> map;
  for (auto nid : nodes) {
    NodeTemplate node;
    node.id = nid;
    node.location = rack_location;
    node.num_shards = 1;
    auto update =
        NodesConfigurationTestUtil::addNewNodeUpdate(*nodes_config_, node);
    nodes_config_ = nodes_config_->applyUpdate(update);
    map[ShardID(nid, 0)] = membership::StorageState::READ_WRITE;
  }
  // Make all the nodes writable immediately
  overrideStorageState(map);
}

void MetadataNodesetSelectorTest::overrideStorageState(
    std::unordered_map<ShardID, membership::StorageState> map) {
  NodesConfiguration::Update update{};
  update.storage_config_update =
      std::make_unique<configuration::nodes::StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          nodes_config_->getStorageMembership()->getVersion());

  for (const auto& it : map) {
    auto shard = it.first;
    auto result = nodes_config_->getStorageMembership()->getShardState(shard);
    ld_check(result);

    auto shardState = result.value();
    membership::ShardState::Update::StateOverride s;
    s.storage_state = it.second;
    s.flags = shardState.flags;
    s.metadata_state = shardState.metadata_state;

    membership::ShardState::Update u;
    u.transition = membership::StorageStateTransition::OVERRIDE_STATE;
    u.conditions = membership::Condition::FORCE;
    u.state_override = s;

    update.storage_config_update->membership_update->addShard(shard, u);
  }

  nodes_config_ = nodes_config_->applyUpdate(std::move(update));
  ld_check(nodes_config_);
}

TEST_F(MetadataNodesetSelectorTest, BasicNoChange) {
  replication_property_ = ReplicationProperty({{NodeLocationScope::RACK, 2}});
  init();

  auto old_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();

  auto result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());
  EXPECT_FALSE(result.hasError());
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(old_metadata_nodeset, result.value());

  addNewRack("rg1.dc1.msb3.rw1.rk4", {13, 14});
  result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());

  EXPECT_FALSE(result.hasError());
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(old_metadata_nodeset, result.value());
}

TEST_F(MetadataNodesetSelectorTest, ReplaceDrainedNode) {
  replication_property_ = ReplicationProperty({{NodeLocationScope::RACK, 2}});
  init();

  auto old_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();

  // Say one of them is now drained. Overrite its status
  auto drained_node = *old_metadata_nodeset.begin();
  overrideStorageState(
      {{ShardID(drained_node, 0), membership::StorageState::NONE}});
  auto result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());
  EXPECT_FALSE(result.hasError());
  EXPECT_TRUE(result.hasValue());
  EXPECT_NE(old_metadata_nodeset, result.value());
  EXPECT_EQ(old_metadata_nodeset.size(), result.value().size());
  EXPECT_TRUE(old_metadata_nodeset.count(drained_node));
  EXPECT_FALSE(result.value().count(drained_node));
}

TEST_F(MetadataNodesetSelectorTest, ExcludeNode) {
  replication_property_ = ReplicationProperty({{NodeLocationScope::RACK, 2}});
  init();

  auto old_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();

  // Say we want to exclude one of them
  auto excluded_node = *old_metadata_nodeset.begin();
  auto result =
      MetadataNodeSetSelector::getNodeSet(nodes_config_, {excluded_node});
  EXPECT_FALSE(result.hasError());
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(old_metadata_nodeset.size(), result.value().size());
  EXPECT_NE(old_metadata_nodeset, result.value());
  EXPECT_TRUE(old_metadata_nodeset.count(excluded_node));
  EXPECT_FALSE(result.value().count(excluded_node));
}

TEST_F(MetadataNodesetSelectorTest, MultiScopeReplicationProperty) {
  replication_property_ =
      ReplicationProperty({{NodeLocationScope::CLUSTER, 3}});
  init();
  auto old_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();
  // init adds only 3 nodes across 3 different racks as metadata nodes
  EXPECT_EQ(old_metadata_nodeset.size(), 3);
  auto result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());
  // We should fail to pick a new nodeset because we do not have enough nodes
  // spread across multiple clusters
  EXPECT_TRUE(result.hasError());

  // Add a new rack in a new cluster
  addNewRack("rg1.dc1.msb2.rw1.rk1", {13, 14});
  result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());
  // We should fail to pick a new nodeset because we do not have enough nodes
  // spread across multiple clusters
  EXPECT_TRUE(result.hasError());

  // Add a new rack in a new cluster
  addNewRack("rg1.dc1.msb3.rw1.rk1", {15, 16});
  result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());
  EXPECT_FALSE(result.hasError());
  EXPECT_TRUE(result.hasValue());
  EXPECT_EQ(result.value().size(), 5);
}

TEST_F(MetadataNodesetSelectorTest, NodeScopeNodeset) {
  replication_property_ = ReplicationProperty({{NodeLocationScope::NODE, 3}});
  // Build a cluster with three nodes in the same domain
  std::vector<NodeTemplate> nodes;
  for (int i = 0; i < 3; i++) {
    NodeTemplate node;
    node.id = i;
    node.location = "rg1.dc1.msb3.rw1.rk1";
    node.num_shards = 1;
    nodes.push_back(node);
  }
  auto update = NodesConfigurationTestUtil::initialAddShardsUpdate(
      nodes, replication_property_);
  nodes_config_ =
      NodesConfigurationTestUtil::provisionNodes(std::move(update), {});
  ld_check(nodes_config_);

  // We should be able to build a nodeset that spawns the whole cluster
  auto result = MetadataNodeSetSelector::getNodeSet(
      nodes_config_, std::unordered_set<node_index_t>());

  ASSERT_TRUE(result.hasValue());
  EXPECT_EQ((std::set<node_index_t>{0, 1, 2}), result.value());
}

}} // namespace facebook::logdevice
