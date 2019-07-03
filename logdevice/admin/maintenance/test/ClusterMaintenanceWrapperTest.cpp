/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/test/NodesConfigurationTestUtil.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;
using namespace facebook::logdevice::NodesConfigurationTestUtil;
using facebook::logdevice::configuration::nodes::NodesConfiguration;

TEST(ClusterMaintenanceWrapperTest, Empty) {
  auto empty_config = std::make_shared<NodesConfiguration>();
  auto cluster_state = std::make_unique<thrift::ClusterMaintenanceState>();
  ClusterMaintenanceWrapper wrapper{std::move(cluster_state), empty_config};

  ASSERT_EQ(nullptr, wrapper.getMaintenanceByGroupID("911"));

  ASSERT_EQ(0, wrapper.getGroupsForShard(ShardID(0, 0)).size());
  ASSERT_EQ(0, wrapper.getGroupsForSequencer(0).size());

  folly::F14FastSet<ShardOperationalState> enabled = {
      ShardOperationalState::ENABLED};
  ASSERT_EQ(enabled, wrapper.getShardTargetStates(ShardID(0, 0)));
  ASSERT_EQ(SequencingState::ENABLED, wrapper.getSequencerTargetState(0));

  ASSERT_FALSE(wrapper.shouldSkipSafetyCheck(ShardID(0, 1)));
  ASSERT_FALSE(wrapper.shouldSkipSafetyCheck(1));

  ASSERT_FALSE(wrapper.isPassiveDrainAllowed(ShardID(1, 1)));
}

TEST(ClusterMaintenanceWrapperTest, ShardDefinitions) {
  auto config = provisionNodes();

  auto cluster_state = std::make_unique<thrift::ClusterMaintenanceState>();
  cluster_state->set_version(222);
  std::vector<MaintenanceDefinition> definitions;
  auto def1 = MaintenanceDefinition();
  def1.set_user("Automation");

  auto node1 = thrift::NodeID();
  node1.set_node_index(1);

  auto node2 = thrift::NodeID();
  node2.set_node_index(2);

  auto node3 = thrift::NodeID();
  auto node3_addr = thrift::SocketAddress();
  // this node will be matched by address.
  node3_addr.set_address("127.0.0.9");
  node3_addr.set_address_family(thrift::SocketAddressFamily::INET);
  node3.set_address(node3_addr);

  auto shard1 = thrift::ShardID();
  shard1.set_node(node1);
  shard1.set_shard_index(0);

  auto shard2 = thrift::ShardID();
  shard2.set_node(node2);
  shard2.set_shard_index(0);
  def1.set_shards({shard1, shard2});
  def1.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def1.set_group(true);
  // Group ID will be defined and set by the maintenance RSM.
  def1.set_group_id("911");
  definitions.push_back(def1);

  auto def2 = MaintenanceDefinition();
  def2.set_user("robots");
  def2.set_shards({shard2});
  def2.set_shard_target_state(ShardOperationalState::DRAINED);
  def2.set_group_id("122");
  def2.set_skip_safety_checks(true);
  def2.set_allow_passive_drains(true);
  definitions.push_back(def2);

  auto def3 = MaintenanceDefinition();
  auto shard3 = thrift::ShardID();
  shard3.set_node(node3);
  shard3.set_shard_index(0);
  def3.set_user("humans");
  def3.set_shards({shard3});
  def3.set_shard_target_state(ShardOperationalState::DRAINED);
  def3.set_group_id("520");
  def3.set_allow_passive_drains(true);
  // simulates an internal maintenance request where the shard is down.
  def3.set_force_restore_rebuilding(true);
  definitions.push_back(def3);

  // Non Existent node that will be added later
  auto node4 = thrift::NodeID();
  node4.set_node_index(17);
  auto def4 = MaintenanceDefinition();
  auto shard4 = thrift::ShardID();
  shard4.set_node(node4);
  shard4.set_shard_index(0);
  def4.set_user("humans");
  def4.set_shards({shard4});
  def4.set_shard_target_state(ShardOperationalState::DRAINED);
  def4.set_group_id("620");
  def4.set_allow_passive_drains(true);
  definitions.push_back(def4);

  cluster_state->set_maintenances(std::move(definitions));

  ClusterMaintenanceWrapper wrapper{std::move(cluster_state), config};

  ASSERT_EQ(222, wrapper.getVersion());

  ASSERT_NE(nullptr, wrapper.getMaintenanceByGroupID("911"));

  ASSERT_EQ("Automation", wrapper.getMaintenanceByGroupID("911")->get_user());

  ASSERT_EQ(1, wrapper.getGroupsForShard(ShardID(1, 0)).size());
  ASSERT_EQ(folly::F14FastSet<GroupID>{"911"},
            wrapper.getGroupsForShard(ShardID(1, 0)));

  ASSERT_EQ(2, wrapper.getGroupsForShard(ShardID(2, 0)).size());
  ASSERT_THAT(wrapper.getGroupsForShard(ShardID(2, 0)),
              UnorderedElementsAre("911", "122"));

  // N9 resolved by address.
  ASSERT_EQ(1, wrapper.getGroupsForShard(ShardID(9, 0)).size());
  ASSERT_EQ(folly::F14FastSet<GroupID>{"520"},
            wrapper.getGroupsForShard(ShardID(9, 0)));

  ASSERT_TRUE(wrapper.shouldSkipSafetyCheck(ShardID(2, 0)));
  ASSERT_FALSE(wrapper.shouldForceRestoreRebuilding(ShardID(2, 0)));
  ASSERT_TRUE(wrapper.isPassiveDrainAllowed(ShardID(2, 0)));

  ASSERT_FALSE(wrapper.isPassiveDrainAllowed(ShardID(1, 0)));
  ASSERT_FALSE(wrapper.shouldForceRestoreRebuilding(ShardID(1, 0)));
  ASSERT_FALSE(wrapper.shouldSkipSafetyCheck(ShardID(1, 0)));

  ASSERT_TRUE(wrapper.shouldForceRestoreRebuilding(ShardID(9, 0)));
  ASSERT_TRUE(wrapper.isPassiveDrainAllowed(ShardID(9, 0)));
  ASSERT_FALSE(wrapper.shouldSkipSafetyCheck(ShardID(9, 0)));

  folly::F14FastSet<ShardOperationalState> expected_states = {
      ShardOperationalState::DRAINED, ShardOperationalState::MAY_DISAPPEAR};
  ASSERT_EQ(expected_states, wrapper.getShardTargetStates(ShardID(2, 0)));
  ASSERT_EQ(
      folly::F14FastSet<ShardOperationalState>{ShardOperationalState::DRAINED},
      wrapper.getShardTargetStates(ShardID(9, 0)));
  ASSERT_EQ(
      folly::F14FastSet<ShardOperationalState>{
          ShardOperationalState::MAY_DISAPPEAR},
      wrapper.getShardTargetStates(ShardID(1, 0)));

  // N17 is not in the config. Querying for target state
  // should return ENABLED
  EXPECT_FALSE(config->getStorageConfig()->getMembership()->hasNode(17));
  ASSERT_EQ(
      folly::F14FastSet<ShardOperationalState>{ShardOperationalState::ENABLED},
      wrapper.getShardTargetStates(ShardID(17, 0)));

  // Added new node to config and ensure an updated config
  // results in regeneration of index definition
  config = config->applyUpdate(addNewNodeUpdate(*config));
  wrapper.updateNodesConfiguration(config);
  EXPECT_TRUE(config->getStorageConfig()->getMembership()->hasNode(17));
  ASSERT_EQ(
      folly::F14FastSet<ShardOperationalState>{ShardOperationalState::DRAINED},
      wrapper.getShardTargetStates(ShardID(17, 0)));

  auto grouped = wrapper.groupShardsByGroupID(
      {ShardID(9, 0), ShardID(1, 0), ShardID(2, 0)});
  // 3 groups.
  ASSERT_EQ(3, grouped.size());
  ASSERT_EQ(ShardSet({ShardID(9, 0)}), grouped["520"]);
  ASSERT_EQ(ShardSet({ShardID(1, 0), ShardID(2, 0)}), grouped["911"]);
  ASSERT_EQ(ShardSet({ShardID(2, 0)}), grouped["122"]);
}

TEST(ClusterMaintenanceWrapperTest, SequencerDefinitions) {
  auto cluster_state = std::make_unique<thrift::ClusterMaintenanceState>();
  auto config = provisionNodes();
  std::vector<MaintenanceDefinition> definitions;
  auto def1 = MaintenanceDefinition();
  def1.set_user("Automation");

  auto node1 = thrift::NodeID();
  // The Node Index _has_ to be set. The Maintenance RSM will materialize this.
  node1.set_node_index(1);

  auto node2 = thrift::NodeID();
  // The Node Index _has_ to be set. The Maintenance RSM will materialize this.
  node2.set_node_index(2);

  def1.set_sequencer_nodes({node1, node2});
  def1.set_sequencer_target_state(SequencingState::DISABLED);
  // A single group
  def1.set_group(true);
  // Group ID will be defined and set by the maintenance RSM.
  def1.set_group_id("911");
  definitions.push_back(def1);

  auto def2 = MaintenanceDefinition();
  auto node3 = thrift::NodeID();
  // The Node Index _has_ to be set. The Maintenance RSM will materialize this.
  node3.set_node_index(9);
  // A node that doesn't exist.
  auto unknown_node1 = thrift::NodeID();
  unknown_node1.set_node_index(900);
  // Another node that doesn't exist but matched by address.
  auto unknown_node2 = thrift::NodeID();
  auto unknown_addr = thrift::SocketAddress();
  // this node will be matched by address.
  unknown_addr.set_address("127.0.0.250");
  unknown_addr.set_address_family(thrift::SocketAddressFamily::INET);
  unknown_node2.set_address(unknown_addr);

  def2.set_user("robots");
  def2.set_sequencer_nodes({node2, node3, unknown_node1, unknown_node2});
  def2.set_sequencer_target_state(SequencingState::DISABLED);
  // A single group
  def2.set_group_id("122");
  def2.set_skip_safety_checks(true);
  definitions.push_back(def2);

  cluster_state->set_maintenances(std::move(definitions));

  ClusterMaintenanceWrapper wrapper{std::move(cluster_state), config};

  ASSERT_EQ(1, wrapper.getGroupsForSequencer(1).size());
  ASSERT_EQ(2, wrapper.getGroupsForSequencer(2).size());
  ASSERT_EQ(1, wrapper.getGroupsForSequencer(9).size());

  // The unknown node.
  ASSERT_EQ(0, wrapper.getGroupsForSequencer(900).size());

  ASSERT_EQ(
      folly::F14FastSet<GroupID>{"911"}, wrapper.getGroupsForSequencer(1));
  ASSERT_THAT(
      wrapper.getGroupsForSequencer(2), UnorderedElementsAre("911", "122"));
  ASSERT_EQ(
      folly::F14FastSet<GroupID>{"122"}, wrapper.getGroupsForSequencer(9));

  ASSERT_FALSE(wrapper.shouldSkipSafetyCheck(1));
  // Node ID 2 will skip safety since definition-2 includes it.
  ASSERT_TRUE(wrapper.shouldSkipSafetyCheck(2));
  ASSERT_TRUE(wrapper.shouldSkipSafetyCheck(9));

  ASSERT_EQ(SequencingState::DISABLED, wrapper.getSequencerTargetState(1));
  ASSERT_EQ(SequencingState::DISABLED, wrapper.getSequencerTargetState(2));
  ASSERT_EQ(SequencingState::DISABLED, wrapper.getSequencerTargetState(9));

  auto grouped = wrapper.groupSequencersByGroupID({1, 2, 9});
  // 3 groups.
  ASSERT_EQ(2, grouped.size());
  ASSERT_EQ(folly::F14FastSet<node_index_t>({1, 2}), grouped["911"]);
  ASSERT_EQ(folly::F14FastSet<node_index_t>({2, 9}), grouped["122"]);
}
