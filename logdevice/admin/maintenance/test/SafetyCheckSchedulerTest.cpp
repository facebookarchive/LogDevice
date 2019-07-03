/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/SafetyCheckScheduler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/admin/maintenance/test/MaintenanceTestUtil.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;

namespace facebook { namespace logdevice { namespace maintenance {
class SafetyCheckSchedulerMock : public SafetyCheckScheduler {
 public:
  SafetyCheckSchedulerMock() {}

  // Canned impacts for shardsets.
  struct CannedCheckImpact {
    ShardSet disabled_shards;
    NodeIndexSet disabled_sequencers;

    // Shards to check
    ShardSet shards;
    NodeIndexSet sequencers;
    // Whether this check has failed in safety check or not.
    Status status{E::OK};
    // Returned Impact
    Impact impact;
  };
  std::vector<CannedCheckImpact> shard_impacts;

  virtual folly::SemiFuture<folly::Expected<Impact, Status>> performSafetyCheck(
      ShardSet disabled_shards,
      NodeIndexSet disabled_sequencers,
      ShardAuthoritativeStatusMap status_map,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>,
      ShardSet shards,
      NodeIndexSet sequencers) const override;

  virtual std::deque<std::pair<GroupID, ShardsAndSequencers>>
  buildExecutionPlan(
      const ClusterMaintenanceWrapper& maintenance_state,
      const std::vector<const ShardWorkflow*>& shard_wf,
      const std::vector<const SequencerWorkflow*>& seq_wf) const override;
};

folly::SemiFuture<folly::Expected<Impact, Status>>
SafetyCheckSchedulerMock::performSafetyCheck(
    ShardSet disabled_shards,
    SafetyCheckScheduler::NodeIndexSet disabled_sequencers,
    ShardAuthoritativeStatusMap /* unused */,
    std::shared_ptr<
        const configuration::nodes::NodesConfiguration> /* unused */,
    ShardSet shards,
    SafetyCheckScheduler::NodeIndexSet sequencers) const {
  auto promise_future_pair =
      folly::makePromiseContract<folly::Expected<Impact, Status>>();
  for (const auto& it : shard_impacts) {
    if (it.disabled_shards == disabled_shards &&
        it.disabled_sequencers == disabled_sequencers && it.shards == shards &&
        it.sequencers == sequencers) {
      if (it.status == E::OK) {
        promise_future_pair.first.setValue(it.impact);
      } else {
        promise_future_pair.first.setValue(folly::makeUnexpected(it.status));
      }
    }
  }
  if (!promise_future_pair.second.isReady()) {
    ld_assert(false);
  }
  return std::move(promise_future_pair.second);
}

std::deque<std::pair<GroupID, SafetyCheckScheduler::ShardsAndSequencers>>
SafetyCheckSchedulerMock::buildExecutionPlan(
    const ClusterMaintenanceWrapper& maintenance_state,
    const std::vector<const ShardWorkflow*>& shard_wf,
    const std::vector<const SequencerWorkflow*>& seq_wf) const {
  return SafetyCheckScheduler::buildExecutionPlan(
      maintenance_state, shard_wf, seq_wf);
}
}}} // namespace facebook::logdevice::maintenance

TEST(SafetyCheckerSchedulerTest, TestMock) {
  SafetyCheckSchedulerMock mock;
  Impact impact;
  impact.result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;

  SafetyCheckSchedulerMock::CannedCheckImpact canned = {
      .disabled_shards = {{ShardID(2, 0)}},
      .disabled_sequencers = {1},
      .shards = {{ShardID(0, 0), ShardID(0, 1)}},
      .status = E::OK,
      .sequencers = {1, 2, 3},
      .impact = impact,
  };

  mock.shard_impacts.push_back(canned);

  auto f = mock.performSafetyCheck(canned.disabled_shards,
                                   canned.disabled_sequencers,
                                   ShardAuthoritativeStatusMap(),
                                   nullptr,
                                   canned.shards,
                                   canned.sequencers);
  folly::Expected<Impact, Status> result = std::move(f).get();
  ASSERT_TRUE(result.hasValue());
  ASSERT_EQ(impact.result, result->result);

  Impact impact2;
  canned.impact = impact2;
  canned.shards = {{ShardID(0, 0), ShardID(0, 2)}};
  canned.status = E::NOBUFS;

  mock.shard_impacts.push_back(canned);

  auto f2 = mock.performSafetyCheck(canned.disabled_shards,
                                    canned.disabled_sequencers,
                                    ShardAuthoritativeStatusMap(),
                                    nullptr,
                                    canned.shards,
                                    canned.sequencers);

  ASSERT_TRUE(f2.isReady());
  result = std::move(f2).get();
  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(E::NOBUFS, result.error());
}

TEST(SafetyCheckerSchedulerTest, ShardPlanning1) {
  ClusterMaintenanceWrapper wrapper{
      genMaintenanceState(), genNodesConfiguration()};
  std::vector<ShardWorkflow> shard_wf_values = genShardWorkflows();
  std::vector<const ShardWorkflow*> shard_wf;
  for (const auto& workflow : shard_wf_values) {
    shard_wf.push_back(&workflow);
  }
  std::vector<SequencerWorkflow> seq_wf_values = genSequencerWorkflows();
  std::vector<const SequencerWorkflow*> seq_wf;
  for (const auto& workflow : seq_wf_values) {
    seq_wf.push_back(&workflow);
  }

  SafetyCheckSchedulerMock mock;
  auto plan = mock.buildExecutionPlan(wrapper, shard_wf, seq_wf);
  // We expect the following results:
  // G1 (N1S0 -> MAY_DISAPPEAR) + (N1 -> DISABLED)
  // G3 (N2:S0 -> DRAINED)
  // G2 (N2:S0, N11:S0 -> DRAINED) + (N11 -> DISABLED)
  // G4 (N7 -> DISABLED)
  ASSERT_EQ(4, plan.size());
  auto group1 = plan[0];
  auto shards1_sequencers1 = group1.second;
  ASSERT_EQ("G1", group1.first);
  ASSERT_EQ(ShardSet{{ShardID(1, 0)}}, shards1_sequencers1.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{1}, shards1_sequencers1.second);

  // G3 (N2:S0 -> DRAINED)
  auto group2 = plan[1];
  auto shards2_sequencers2 = group2.second;
  ASSERT_EQ("G3", group2.first);
  ASSERT_EQ(ShardSet{{ShardID(2, 0)}}, shards2_sequencers2.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{}, shards2_sequencers2.second);

  // G2 (N2:S0, N11:S0 -> DRAINED) + (N11 -> DISABLED)
  auto group3 = plan[2];
  auto shards3_sequencers3 = group3.second;
  ASSERT_EQ("G2", group3.first);
  ASSERT_EQ(
      ShardSet({ShardID(2, 0), ShardID(11, 0)}), shards3_sequencers3.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{11}, shards3_sequencers3.second);

  // G4 (N7 -> DISABLED)
  auto group4 = plan[3];
  auto shards4_sequencers4 = group4.second;
  ASSERT_EQ("G4", group4.first);
  ASSERT_EQ(ShardSet(), shards4_sequencers4.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{7}, shards4_sequencers4.second);
}

TEST(SafetyCheckerSchedulerTest, ShardPlanning2) {
  ClusterMaintenanceWrapper wrapper{
      genMaintenanceState(), genNodesConfiguration()};
  std::vector<ShardWorkflow> shard_wf_values = genShardWorkflows();
  std::vector<SequencerWorkflow> seq_wf_values = genSequencerWorkflows();

  std::vector<const ShardWorkflow*> shard_wf;
  for (const auto& workflow : shard_wf_values) {
    shard_wf.push_back(&workflow);
  }

  std::vector<const SequencerWorkflow*> seq_wf;
  for (const auto& workflow : seq_wf_values) {
    seq_wf.push_back(&workflow);
  }

  // Now assume the following:
  //   - N7 not needed (workflow doesn't request it.)
  //   - N11:S0 not needed (workflow doesn't request it.)
  //   - N2:S0 not needed (workflow doesn't request it.)
  {
    auto it = shard_wf.begin();
    while (it != shard_wf.end()) {
      if ((*it)->getShardID() == ShardID(11, 0) ||
          (*it)->getShardID() == ShardID(2, 0)) {
        it = shard_wf.erase(it);
      } else {
        ++it;
      }
    }
  }

  {
    auto it = seq_wf.begin();
    while (it != seq_wf.end()) {
      if ((*it)->getNodeIndex() == 7) {
        it = seq_wf.erase(it);
      } else {
        ++it;
      }
    }
  }
  SafetyCheckSchedulerMock mock;
  auto plan = mock.buildExecutionPlan(wrapper, shard_wf, seq_wf);
  // We expect the following results:
  // G1 (N1S0 -> MAY_DISAPPEAR) + (N1 -> DISABLED)
  // G2 () + (N11 -> DISABLED)
  ASSERT_EQ(2, plan.size());
  auto group1 = plan[0];
  auto shards1_sequencers1 = group1.second;
  ASSERT_EQ("G1", group1.first);
  ASSERT_EQ(ShardSet{{ShardID(1, 0)}}, shards1_sequencers1.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{1}, shards1_sequencers1.second);

  // G2 () + (N11 -> DISABLED)
  auto group2 = plan[1];
  auto shards2_sequencers2 = group2.second;
  ASSERT_EQ("G2", group2.first);
  ASSERT_EQ(ShardSet(), shards2_sequencers2.first);
  ASSERT_EQ(folly::F14FastSet<node_index_t>{11}, shards2_sequencers2.second);
}

TEST(SafetyCheckerSchedulerTest, EmptyPlanning) {
  ClusterMaintenanceWrapper wrapper{
      genMaintenanceState(), genNodesConfiguration()};

  SafetyCheckSchedulerMock mock;
  auto plan = mock.buildExecutionPlan(wrapper,
                                      std::vector<const ShardWorkflow*>(),
                                      std::vector<const SequencerWorkflow*>());
  ASSERT_EQ(0, plan.size());
}

TEST(SafetyCheckerSchedulerTest, Scheduling) {
  auto nodes_config = genNodesConfiguration();
  ClusterMaintenanceWrapper wrapper{genMaintenanceState(), nodes_config};
  ShardAuthoritativeStatusMap status_map;

  std::vector<ShardWorkflow> shard_wf_values = genShardWorkflows();
  std::vector<const ShardWorkflow*> shard_wf;
  for (const auto& workflow : shard_wf_values) {
    shard_wf.push_back(&workflow);
  }

  std::vector<SequencerWorkflow> seq_wf_values = genSequencerWorkflows();

  std::vector<const SequencerWorkflow*> seq_wf;
  for (const auto& workflow : seq_wf_values) {
    seq_wf.push_back(&workflow);
  }

  SafetyCheckSchedulerMock mock;
  // We expect the following results:
  // G1 (N1S0 -> MAY_DISAPPEAR) + (N1 -> DISABLED)
  // G3 (N2:S0 -> DRAINED)
  // G2 (N2:S0, N11:S0 -> DRAINED) + (N11 -> DISABLED)
  // G4 (N7 -> DISABLED)
  //
  // Our Safety Check Results:
  // G1 => SAFE (N1S0, N1) -> disabled.
  // G3 => SAFE (N2S0) -> disabled.
  // G2 => UNSAFE (N11:S0, N11) [blocked].
  // G4 => SAFE (, N7) -> disabled

  ShardSet safe_shards_to_disable;
  SafetyCheckScheduler::NodeIndexSet safe_sequencers_to_disable;

  Impact bad_impact;
  bad_impact.result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;

  Impact safe_impact;
  safe_impact.result = Impact::ImpactResult::NONE;

  // G1 test. => SAFE
  mock.shard_impacts.push_back(SafetyCheckSchedulerMock::CannedCheckImpact{
      .shards = {{ShardID(1, 0)}},
      .sequencers = {1},
      .impact = safe_impact,
  });

  safe_shards_to_disable.insert(ShardID(1, 0));
  safe_sequencers_to_disable.insert(1);

  // G3 test. => SAFE
  mock.shard_impacts.push_back(SafetyCheckSchedulerMock::CannedCheckImpact{
      .disabled_shards = safe_shards_to_disable,
      .disabled_sequencers = safe_sequencers_to_disable,
      .shards = {{ShardID(2, 0)}},
      .impact = safe_impact,
  });
  safe_shards_to_disable.insert(ShardID(2, 0));

  // G2 test. => UNSAFE
  mock.shard_impacts.push_back(SafetyCheckSchedulerMock::CannedCheckImpact{
      .disabled_shards = safe_shards_to_disable,
      .disabled_sequencers = safe_sequencers_to_disable,
      .shards = {{ShardID(2, 0), ShardID(11, 0)}},
      .sequencers = {11},
      .impact = bad_impact,
  });

  // G4 test. => SAFE
  mock.shard_impacts.push_back(SafetyCheckSchedulerMock::CannedCheckImpact{
      .disabled_shards = safe_shards_to_disable,
      .disabled_sequencers = safe_sequencers_to_disable,
      .sequencers = {7},
      .impact = safe_impact,
  });

  auto f = mock.schedule(wrapper, status_map, nodes_config, shard_wf, seq_wf);
  // in test everything happens sync.
  ASSERT_TRUE(f.isReady());

  // Check if the result has what we expact.
  folly::Expected<SafetyCheckScheduler::Result, Status> result =
      std::move(f).get();

  ASSERT_TRUE(result.hasValue());

  ASSERT_EQ(ShardSet({ShardID(1, 0), ShardID(2, 0)}), result->safe_shards);
  ASSERT_EQ(
      SafetyCheckScheduler::NodeIndexSet({1, 7}), result->safe_sequencers);

  ASSERT_EQ(1, result->unsafe_groups.size());
  ASSERT_EQ(1, result->unsafe_groups.count("G2"));
  ASSERT_EQ(bad_impact, result->unsafe_groups["G2"]);
}

TEST(SafetyCheckerSchedulerTest, EmptyScheduling) {
  auto nodes_config = genNodesConfiguration();
  ClusterMaintenanceWrapper wrapper{genMaintenanceState(), nodes_config};
  ShardAuthoritativeStatusMap status_map;
  SafetyCheckSchedulerMock mock;

  auto f = mock.schedule(wrapper, status_map, nodes_config, {}, {});
  // in test everything happens sync.
  ASSERT_TRUE(f.isReady());
  auto result = std::move(f).get();
  ASSERT_TRUE(result.hasError());
  ASSERT_EQ(E::INVALID_PARAM, result.error());
}
