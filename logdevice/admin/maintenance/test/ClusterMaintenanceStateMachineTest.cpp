/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

using namespace ::testing;
using namespace apache::thrift;
using namespace facebook::logdevice;
using namespace facebook::logdevice::thrift;
using namespace facebook::logdevice::maintenance;

#define NUM_SHARDS 2

namespace facebook { namespace logdevice { namespace maintenance {

class ClusterMaintenanceStateMachineTest : public ::testing::Test {
 public:
  ClusterMaintenanceStateMachineTest() {}
  ~ClusterMaintenanceStateMachineTest() {}

  void init(int n);
  int applyMaintenances(std::vector<thrift::MaintenanceDefinition> defs);
  int removeMaintenance(thrift::RemoveMaintenancesRequest req);
  thrift::MaintenanceDefinition
  getMaintenanceDefinition(thrift::MaintenanceGroupID group);
  std::unique_ptr<thrift::ClusterMaintenanceState> cluster_state_;
  std::unique_ptr<ClusterMaintenanceStateMachine> sm_;
  std::vector<thrift::NodeID> node;
  std::vector<thrift::ShardID> shard;
};

void ClusterMaintenanceStateMachineTest::init(int nnodes) {
  for (int i = 0, j = 0; i < nnodes; i++) {
    node.push_back(thrift::NodeID());
    node[i].set_node_index(i);
    for (int k = 0; k < NUM_SHARDS; k++) {
      shard.push_back(thrift::ShardID());
      shard[j].set_node(node[i]);
      shard[j].set_shard_index(k);
      j++;
    }
  }
  sm_ = std::make_unique<ClusterMaintenanceStateMachine>(
      create_default_settings<AdminServerSettings>());
  cluster_state_ = sm_->makeDefaultState(1);
}

int ClusterMaintenanceStateMachineTest::applyMaintenances(
    std::vector<thrift::MaintenanceDefinition> defs) {
  auto delta = std::make_unique<MaintenanceDelta>();
  delta->set_apply_maintenances(std::move(defs));
  auto version = cluster_state_->get_version() + 1;
  std::string failure_str;
  return sm_->applyDelta(*delta,
                         *cluster_state_,
                         version,
                         SystemTimestamp::now().toMilliseconds(),
                         failure_str);
}

int ClusterMaintenanceStateMachineTest::removeMaintenance(
    thrift::RemoveMaintenancesRequest req) {
  auto delta = std::make_unique<MaintenanceDelta>();
  delta->set_remove_maintenances(std::move(req));
  auto version = cluster_state_->get_version() + 1;
  std::string failure_str;
  return sm_->applyDelta(*delta,
                         *cluster_state_,
                         version,
                         SystemTimestamp::now().toMilliseconds(),
                         failure_str);
}

thrift::MaintenanceDefinition
ClusterMaintenanceStateMachineTest::getMaintenanceDefinition(
    thrift::MaintenanceGroupID group) {
  thrift::MaintenanceDefinition result;
  auto defs = cluster_state_->get_maintenances();
  for (auto& def : defs) {
    if (def.group_id_ref().value() == group) {
      result = def;
      break;
    }
  }
  return result;
}

TEST_F(ClusterMaintenanceStateMachineTest, BasicApplyAndRemoval) {
  init(5);

  // Automation wants N0S1 and N2S1 set to MAY_DISAPPEAR
  auto def = MaintenanceDefinition();
  def.set_user("Automation");
  // N0S1, N2S1
  def.set_shards({shard[0], shard[4]});
  def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def.set_group(true);
  def.set_group_id("N0S1N2S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  ASSERT_EQ(getMaintenanceDefinition("N0S1N2S1"), def);
  auto prev = cluster_state_->get_version();

  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N1, N4
  def.set_sequencer_nodes({node[1], node[4]});
  def.set_sequencer_target_state(SequencingState::DISABLED);
  def.set_group_id("N1N4Seq");
  ASSERT_EQ(0, applyMaintenances({def}));
  ASSERT_EQ(getMaintenanceDefinition("N1N4Seq"), def);
  prev = cluster_state_->get_version();

  // Oncall wants N0S1 set to DRAINED
  def = MaintenanceDefinition();
  def.set_user("Oncall");
  // N0S1
  def.set_shards({shard[0]});
  def.set_shard_target_state(ShardOperationalState::DRAINED);
  // A single group
  def.set_group(true);
  def.set_group_id("N0S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  ASSERT_EQ(getMaintenanceDefinition("N0S1"), def);
  prev = cluster_state_->get_version();

  // Automation wants N1S1 and N3S1 set to MAY_DISAPPEAR
  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N1S1, N3S1
  def.set_shards({shard[2], shard[6]});
  def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def.set_group(true);
  def.set_group_id("N1S1N3S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  EXPECT_EQ(cluster_state_->get_version(), prev + 1);
  ASSERT_EQ(getMaintenanceDefinition("N1S1N3S1"), def);
  prev = cluster_state_->get_version();

  // Try to remove all nonexistent maintenances, that should return NOTFOUND
  thrift::MaintenancesFilter filter1;
  filter1.set_group_ids({"NonExistantGroup"});
  thrift::RemoveMaintenancesRequest req;
  req.set_filter(std::move(filter1));
  ASSERT_EQ(-1, removeMaintenance(req));
  ASSERT_EQ(E::NOTFOUND, err);
  // Since this is not really removing anything, we shouldn't bump the
  // maintenance state version.
  EXPECT_EQ(cluster_state_->get_version(), prev);

  thrift::MaintenanceDefinition empty;
  // Try removing valid group ids along with invalid one, that will remove the
  // valid one successfully and the non-existant will be ignored.
  filter1.set_group_ids({"N1S1N3S1", "NonExistantGroup"});
  req.set_filter(std::move(filter1));
  ASSERT_EQ(0, removeMaintenance(req));
  EXPECT_EQ(cluster_state_->get_version(), prev + 1);
  ASSERT_EQ(getMaintenanceDefinition("N1S1N3S1"), empty);
  prev = cluster_state_->get_version();

  // Now Automation wants to remove the previously applied
  // maintenance for N0S1N2S1
  thrift::MaintenancesFilter filter2;
  thrift::RemoveMaintenancesRequest req2;
  filter2.set_group_ids({"N0S1N2S1"});
  filter2.set_user("Automation");
  req2.set_filter(std::move(filter2));
  req2.set_user("Automation");
  removeMaintenance(req2);
  EXPECT_EQ(cluster_state_->get_version(), prev + 1);

  // Check the original group to see if N0S1N2S1 has been removed
  // And this should not affect Oncall's maintenance for N0S1
  def = getMaintenanceDefinition("N0S1N2S1");
  ASSERT_EQ(getMaintenanceDefinition("N0S1N2S1"), empty);
  def = getMaintenanceDefinition("N0S1");
  auto shards = def.get_shards();
  EXPECT_EQ(shards.size(), 1);
  EXPECT_EQ(shards[0], shard[0]);

  // None of these removal should have affected
  // N1S1N3S1, N1N4Seq
  def = getMaintenanceDefinition("N1N4Seq");
  ASSERT_NE(getMaintenanceDefinition("N1N4Seq"), empty);

  // Remove all maintenances for user Automation
  thrift::MaintenancesFilter filter3;
  thrift::RemoveMaintenancesRequest req3;
  filter3.set_user("Automation");
  req3.set_filter(std::move(filter3));
  req3.set_user("Automation");
  removeMaintenance(req3);

  // N1S1N3S1, N1N4Seq should now be gone
  def = getMaintenanceDefinition("N1S1N3S1");
  ASSERT_EQ(getMaintenanceDefinition("N1S1N3S1"), empty);
  def = getMaintenanceDefinition("N1N4Seq");
  ASSERT_EQ(getMaintenanceDefinition("N1N4Seq"), empty);
}

TEST_F(ClusterMaintenanceStateMachineTest, ApplyFailures) {
  init(5);

  // Automation wants N0S1 and N2S1 set to MAY_DISAPPEAR
  auto def = MaintenanceDefinition();
  def.set_user("Automation");
  // N0S1, N2S1
  def.set_shards({shard[0], shard[4]});
  def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def.set_group(true);
  def.set_group_id("N0S1N2S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  ASSERT_EQ(getMaintenanceDefinition("N0S1N2S1"), def);
  auto prev = cluster_state_->get_version();

  // Oncall wants N0S1 set to DRAINED.
  // No clash here since prev maintenance is from different user
  def = MaintenanceDefinition();
  def.set_user("Oncall");
  // N0S1
  def.set_shards({shard[0]});
  def.set_shard_target_state(ShardOperationalState::DRAINED);
  // A single group
  def.set_group(true);
  def.set_group_id("N0S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  ASSERT_EQ(getMaintenanceDefinition("N0S1"), def);
  prev = cluster_state_->get_version();

  // Automation wants N1S1 and N3S1 set to MAY_DISAPPEAR
  // Sequencers on N1,N3 set to DISABLED
  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N1S1, N3S1
  def.set_shards({shard[3], shard[6]});
  def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  def.set_sequencer_nodes({node[1], node[3]});
  def.set_sequencer_target_state(SequencingState::DISABLED);
  // A single group
  def.set_group(true);
  def.set_group_id("N1S1N3S1");
  ASSERT_EQ(0, applyMaintenances({def}));
  EXPECT_EQ(cluster_state_->get_version(), prev + 1);
  ASSERT_EQ(getMaintenanceDefinition("N1S1N3S1"), def);
  prev = cluster_state_->get_version();

  // Automation wants N0S1, N4S2 set to DRAINED
  // Should result in maintenance clash since
  // Automation already requested for N0S1 to be set to MAY_DISAPPEAR
  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N0S1, N3S2
  def.set_shards({shard[0], shard[7]});
  def.set_shard_target_state(ShardOperationalState::DRAINED);
  def.set_group_id("N0S1N4S2");
  ASSERT_EQ(-1, applyMaintenances({def}));
  ASSERT_EQ(E::MAINTENANCE_CLASH, err);
  EXPECT_EQ(cluster_state_->get_version(), prev);

  // Automation wants N1S1 set to DRAINED.
  // But had previously requested N1S1 be set to MAY_DISAPPEAR,
  // so thats a clash
  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N1S1
  def.set_shards({shard[3]});
  def.set_shard_target_state(ShardOperationalState::DRAINED);
  def.set_group_id("N1S1");
  ASSERT_EQ(-1, applyMaintenances({def}));
  ASSERT_EQ(E::MAINTENANCE_CLASH, err);
  EXPECT_EQ(cluster_state_->get_version(), prev);

  // Automation wants N2S1 and N4S1 set to MAY_DISAPPEAR
  // Sequencers on N1,N4 set to DISABLED
  // N1 sequencer disable clashes even though rest are fine
  def = MaintenanceDefinition();
  def.set_user("Automation");
  // N2S1, N4S1
  def.set_shards({shard[5], shard[8]});
  def.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  def.set_sequencer_nodes({node[1], node[4]});
  def.set_sequencer_target_state(SequencingState::DISABLED);
  def.set_group_id("N2s1N4S1");
  ASSERT_EQ(-1, applyMaintenances({def}));
  ASSERT_EQ(E::MAINTENANCE_CLASH, err);
  EXPECT_EQ(cluster_state_->get_version(), prev);

  // Multiple valid definitions with one invalid definition
  auto def1 = MaintenanceDefinition();
  def1.set_user("Automation");
  // N1S1
  def1.set_shards({shard[3]});
  def1.set_shard_target_state(ShardOperationalState::DRAINED);
  def1.set_group_id("N1S1");
  auto def2 = MaintenanceDefinition();
  def2.set_user("Automation");
  def2.set_sequencer_nodes({node[0], node[2]});
  def2.set_sequencer_target_state(SequencingState::DISABLED);
  def2.set_group_id("Seq0Seq2");
  ASSERT_EQ(-1, applyMaintenances({def, def2}));
  ASSERT_EQ(E::MAINTENANCE_CLASH, err);
  EXPECT_EQ(cluster_state_->get_version(), prev);

  // Apply of def2 alone from above should succeed
  ASSERT_EQ(0, applyMaintenances({def2}));
  prev = cluster_state_->get_version();
  // Applying with same group id should result in a clash
  def2 = MaintenanceDefinition();
  def2.set_user("Automation");
  def2.set_sequencer_nodes({node[4]});
  def2.set_sequencer_target_state(SequencingState::DISABLED);
  def2.set_group_id("Seq0Seq2");
  ASSERT_EQ(-1, applyMaintenances({def2}));
  ASSERT_EQ(E::MAINTENANCE_CLASH, err);
  EXPECT_EQ(cluster_state_->get_version(), prev);
}

}}} // namespace facebook::logdevice::maintenance
