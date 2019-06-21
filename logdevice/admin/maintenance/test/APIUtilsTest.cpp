/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/APIUtils.h"

#include <folly/container/F14Set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/maintenance/test/MaintenanceTestUtil.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

TEST(APIUtilsTest, Validate) {
  // empty definition is invalid.
  {
    MaintenanceDefinition def1;
    auto failed = APIUtils::validateDefinition(def1);
    ASSERT_TRUE(failed.hasValue());
    ASSERT_EQ("At least one of shards or sequencer_nodes must be set",
              failed->get_message());
  }

  MaintenanceDefinition def1;
  thrift::ShardSet shards;
  thrift::NodeID node1 = mkNodeID(1);
  shards.push_back(mkShardID(1, -1));
  def1.set_shards(shards);
  def1.set_shard_target_state(ShardOperationalState::DRAINED);
  // user must be set
  auto failed = APIUtils::validateDefinition(def1);
  ASSERT_TRUE(failed.hasValue());
  ASSERT_EQ("user must be set for the definition to non-empty string",
            failed->get_message());
  def1.set_user("bunny");
  failed = APIUtils::validateDefinition(def1);
  ASSERT_EQ(folly::none, failed);

  def1.set_user("bun ny");
  failed = APIUtils::validateDefinition(def1);
  ASSERT_TRUE(failed.hasValue());
  ASSERT_EQ("user cannot contain whitespaces", failed->get_message());

  def1.set_user("bunny");
  def1.set_sequencer_nodes({node1});
  failed = APIUtils::validateDefinition(def1);
  ASSERT_TRUE(failed.hasValue());
  ASSERT_EQ("sequencer_target_state must be DISABLED if sequencer_nodes is set",
            failed->get_message());
  def1.set_sequencer_target_state(SequencingState::BOYCOTTED);
  failed = APIUtils::validateDefinition(def1);
  ASSERT_TRUE(failed.hasValue());
  ASSERT_EQ("sequencer_target_state must be DISABLED if sequencer_nodes is set",
            failed->get_message());
  def1.set_sequencer_target_state(SequencingState::DISABLED);
  ASSERT_EQ(folly::none, APIUtils::validateDefinition(def1));

  shards.push_back(mkShardID(2, -10));
  def1.set_shards(shards);
  failed = APIUtils::validateDefinition(def1);
  ASSERT_TRUE(failed.hasValue());
  ASSERT_EQ("Cannot accept shard_index smaller than -1", failed->get_message());
  shards.pop_back();
  def1.set_shards(shards);
  ASSERT_EQ(folly::none, APIUtils::validateDefinition(def1));

  {
    MaintenanceDefinition def2 = def1;
    def2.set_force_restore_rebuilding(true);
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ("Setting force_restore_rebuilding is not allowed",
              failed2->get_message());
  }
  {
    MaintenanceDefinition def2 = def1;
    def2.set_ttl_seconds(-1);
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ(
        "ttl_seconds must be a non-negative number", failed2->get_message());
  }
  {
    MaintenanceDefinition def2 = def1;
    def2.set_group_id("hola");
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ("group_id cannot be set by the user", failed2->get_message());
  }
  {
    MaintenanceDefinition def2 = def1;
    def2.set_last_check_impact_result(thrift::CheckImpactResponse());
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ("last_check_impact_result cannot be set by the user",
              failed2->get_message());
  }
  {
    MaintenanceDefinition def2 = def1;
    def2.set_expires_on(200);
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ("expires_on cannot be set by the user", failed2->get_message());
  }
  {
    MaintenanceDefinition def2 = def1;
    def2.set_created_on(100);
    auto failed2 = APIUtils::validateDefinition(def2);
    ASSERT_EQ("created_on cannot be set by the user", failed2->get_message());
  }
}

TEST(APIUtilsTest, RandomGroupID) {
  folly::F14FastSet<std::string> generated;
  std::string id = APIUtils::generateGroupID(8);
  ASSERT_EQ(8, id.size());
  generated.insert(std::move(id));
  for (int i = 0; i < 100000; ++i) {
    // We should be happy without collisions in 100k id space.
    id = APIUtils::generateGroupID(8);
    ASSERT_TRUE(generated.count(id) == 0);
    generated.insert(std::move(id));
  }
  ASSERT_EQ(100001, generated.size());
}

TEST(APIUtilsTest, ExpandMaintenances1) {
  /**
   * generate a single maintenance because all shards are in the same node,
   * whether we pass group with true or false.
   */
  std::shared_ptr<const NodesConfiguration> nodes_config =
      genNodesConfiguration();
  MaintenanceDefinition request;
  request.set_user("bunny");
  request.set_shard_target_state(ShardOperationalState::DRAINED);
  // expands to all shards of node 1
  request.set_shards({mkShardID(1, -1)});
  request.set_sequencer_nodes({mkNodeID(1)});
  request.set_sequencer_target_state(SequencingState::DISABLED);
  // to validate we correctly respect the attributes
  request.set_skip_safety_checks(true);
  request.set_group(false);
  {
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasValue());
    ASSERT_EQ(1, output.value().size());
    const MaintenanceDefinition& result = (*output)[0];
    ASSERT_EQ("bunny", result.get_user());
    ASSERT_TRUE(result.group_id_ref().has_value());
    ASSERT_EQ(8, result.group_id_ref().value().size());
    ASSERT_THAT(result.get_shards(), UnorderedElementsAre(mkShardID(1, 0)));
    ASSERT_EQ(ShardOperationalState::DRAINED, result.get_shard_target_state());
    ASSERT_EQ(SequencingState::DISABLED, result.get_sequencer_target_state());
    ASSERT_THAT(
        result.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
    ASSERT_TRUE(result.get_skip_safety_checks());
    ASSERT_TRUE(result.created_on_ref().has_value());
    ASSERT_TRUE(result.created_on_ref().value() > 0);
  }
  {
    // Let's try while group = true; Our expecations should be identical since
    // all maintenances in this request are on the same node.
    request.set_group(true);
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasValue());
    ASSERT_EQ(1, output.value().size());
    const MaintenanceDefinition& result = (*output)[0];
    ASSERT_EQ("bunny", result.get_user());
    ASSERT_TRUE(result.group_id_ref().has_value());
    ASSERT_EQ(8, result.group_id_ref().value().size());
    ASSERT_THAT(result.get_shards(), UnorderedElementsAre(mkShardID(1, 0)));
    ASSERT_EQ(ShardOperationalState::DRAINED, result.get_shard_target_state());
    ASSERT_EQ(SequencingState::DISABLED, result.get_sequencer_target_state());
    ASSERT_THAT(
        result.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
    ASSERT_TRUE(result.get_skip_safety_checks());
    ASSERT_TRUE(result.created_on_ref().has_value());
    ASSERT_TRUE(result.created_on_ref().value() > 0);
  }
  {
    // Let's add another sequenecer node and we should get two maintenances in
    // return if group=false, and 1 of group=true
    //
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(7)});
    request.set_group(true);
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasValue());
    ASSERT_EQ(1, output.value().size());
    const MaintenanceDefinition& result = (*output)[0];
    ASSERT_THAT(result.get_sequencer_nodes(),
                UnorderedElementsAre(mkNodeID(1), mkNodeID(7)));

    request.set_group(false);
    output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasValue());
    ASSERT_EQ(2, output.value().size());
    for (const auto& def : *output) {
      ASSERT_EQ(SequencingState::DISABLED, def.get_sequencer_target_state());
      if (def.get_shards().empty()) {
        // This has to be the sequencer only maintenance
        ASSERT_THAT(
            def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(7)));
      } else {
        ASSERT_THAT(
            def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
      }
    }
  }
  {
    // Let's add another shard in a different node and we should get three
    // maintenances in return if group=false
    //
    request.set_shards({mkShardID(1, -1), mkShardID(13, -1)});
    request.set_sequencer_nodes({mkNodeID(1), mkNodeID(7)});
    request.set_group(false);
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasValue());
    ASSERT_EQ(3, output.value().size());
    std::unordered_set<std::string> found_ids;
    for (const auto& def : *output) {
      ASSERT_EQ(SequencingState::DISABLED, def.get_sequencer_target_state());
      if (def.get_shards().empty()) {
        // This has to be the sequencer only maintenance
        ASSERT_THAT(
            def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(7)));
        ASSERT_EQ(0, def.get_shards().size());
      } else if (def.get_shards()[0].get_node().node_index_ref().value() == 1) {
        ASSERT_THAT(
            def.get_sequencer_nodes(), UnorderedElementsAre(mkNodeID(1)));
        ASSERT_THAT(def.get_shards(), UnorderedElementsAre(mkShardID(1, 0)));
      } else {
        ASSERT_EQ(0, def.get_sequencer_nodes().size());
        ASSERT_THAT(def.get_shards(), UnorderedElementsAre(mkShardID(13, 0)));
      }
      // For each of these maintenances, we should have a set of attributs set.
      // the maintnances must have unique ids
      ASSERT_EQ(0, found_ids.count(*def.group_id_ref()));
      ASSERT_TRUE(def.group_id_ref().value().size() > 0);
      ASSERT_EQ(0, found_ids.count(def.group_id_ref().value()));
      found_ids.insert(def.group_id_ref().value());
      ASSERT_TRUE(def.get_skip_safety_checks());
      ASSERT_TRUE(def.created_on_ref().has_value());
      ASSERT_TRUE(def.created_on_ref().value() > 0);
    }
  }
}

TEST(APIUtilsTest, ExpandMaintenances2) {
  // We can't reference shards in non-storage nodes, or sequencer nodes that do
  // not have this role in maintenances.
  std::shared_ptr<const NodesConfiguration> nodes_config =
      genNodesConfiguration();
  MaintenanceDefinition request;
  request.set_user("bunny");
  request.set_shard_target_state(ShardOperationalState::DRAINED);
  // expands to all shards of node 1
  request.set_shards({mkShardID(1, -1)});
  // node 9 is storage node only.
  request.set_sequencer_nodes({mkNodeID(9)});
  request.set_sequencer_target_state(SequencingState::DISABLED);
  // to validate we correctly respect the attributes
  request.set_skip_safety_checks(true);
  request.set_group(false);
  {
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasError());
    ASSERT_EQ("Node 9 is not a sequencer node", output.error().get_message());
  }
  {
    request.set_sequencer_nodes({mkNodeID(1)});
    // now let's try to set shards in non-storage nodes.
    request.set_shards({mkShardID(7, -1)});
    auto output = APIUtils::expandMaintenances(request, nodes_config);
    ASSERT_TRUE(output.hasError());
    ASSERT_EQ("Node 7 is not a storage node", output.error().get_message());
  }
}

TEST(APIUtilsTest, MaintenanceEquivalence) {
  std::shared_ptr<const NodesConfiguration> nodes_config =
      genNodesConfiguration();
  MaintenanceDefinition def1;
  def1.set_user("bunny");
  def1.set_shard_target_state(ShardOperationalState::DRAINED);
  // expands to all shards of node 1
  def1.set_shards({mkShardID(1, -1), mkShardID(13, -1)});
  def1.set_sequencer_nodes({mkNodeID(1)});
  def1.set_sequencer_target_state(SequencingState::DISABLED);
  // to validate we correctly respect the attributes
  def1.set_skip_safety_checks(true);
  def1.set_group(true);
  auto output = APIUtils::expandMaintenances(def1, nodes_config);
  ASSERT_TRUE(output.hasValue());
  ASSERT_EQ(1, output.value().size());

  MaintenanceDefinition def2;
  def2.set_user("bunny");
  def2.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // expands to all shards of node 1
  def2.set_shards({mkShardID(1, -1), mkShardID(13, -1)});
  def2.set_sequencer_nodes({mkNodeID(1)});
  def2.set_sequencer_target_state(SequencingState::DISABLED);
  // to validate we correctly respect the attributes
  def2.set_skip_safety_checks(true);
  def2.set_group(true);
  auto def2_output = APIUtils::expandMaintenances(def2, nodes_config);
  ASSERT_TRUE(def2_output.hasValue());
  ASSERT_EQ(1, def2_output.value().size());

  ASSERT_FALSE(
      APIUtils::areMaintenancesEquivalent((*output)[0], (*def2_output)[0]));

  def2.set_shard_target_state(ShardOperationalState::DRAINED);
  def2_output = APIUtils::expandMaintenances(def2, nodes_config);

  ASSERT_TRUE(
      APIUtils::areMaintenancesEquivalent((*output)[0], (*def2_output)[0]));

  ASSERT_EQ((*def2_output)[0],
            APIUtils::findEquivalentMaintenance(*def2_output, (*output)[0]));

  def2.set_user("what");
  def2_output = APIUtils::expandMaintenances(def2, nodes_config);

  ASSERT_FALSE(
      APIUtils::areMaintenancesEquivalent((*output)[0], (*def2_output)[0]));

  def2.set_user("bunny");
  def2.set_sequencer_nodes({mkNodeID(7)});
  def2_output = APIUtils::expandMaintenances(def2, nodes_config);
  ASSERT_FALSE(
      APIUtils::areMaintenancesEquivalent((*output)[0], (*def2_output)[0]));

  ASSERT_EQ(folly::none,
            APIUtils::findEquivalentMaintenance(*def2_output, (*output)[0]));
}

TEST(APIUtilsTest, MaintenanceFilter) {
  std::vector<MaintenanceDefinition> defs;
  MaintenanceDefinition def1;
  def1.set_user("bunny");
  def1.set_group_id("group1");
  defs.push_back(def1);

  MaintenanceDefinition def2;
  def2.set_user("bunny");
  def2.set_group_id("group2");
  defs.push_back(def2);

  MaintenanceDefinition def3;
  def3.set_user("funny");
  def3.set_group_id("group3");
  defs.push_back(def3);

  thrift::MaintenancesFilter filter1;

  ASSERT_EQ(defs, APIUtils::filterMaintenances(filter1, defs));
  filter1.set_user("bunny");
  auto res1 = APIUtils::filterMaintenances(filter1, defs);
  ASSERT_EQ(2, res1.size());
  ASSERT_THAT(res1, UnorderedElementsAre(def1, def2));
  filter1.set_group_ids({"group1"});
  res1 = APIUtils::filterMaintenances(filter1, defs);
  ASSERT_EQ(1, res1.size());
  ASSERT_THAT(res1, UnorderedElementsAre(def1));
}
