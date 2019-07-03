/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>

#include <folly/Expected.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/ClusterMaintenanceWrapper.h"
#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice {
class Processor;
class SafetyChecker;
}} // namespace facebook::logdevice

namespace facebook { namespace logdevice { namespace maintenance {
// fwd declaration.
class ShardWorkflow;
class SequencerWorkflow;

/**
 * SafetyCheckScheduler is a long-living object that accepts a request to
 * perform a number of safety checks from several workflows at the same time.
 * The scheduler tries to figure out what is the possible combination of
 * storage state changes that are possible for a given set of groups of
 * operations.
 *
 * If some of the workflows return MaintenanceStatus::AWAITING_SAFETY_CHECK.
 * We need to schedule a safety check run for them, but the order and grouping
 * of these safety check runs is crucial.
 *
 * The goal is to ensure that maintenance groups are performed together on
 * single safety check run, and that safe operations are taken into account
 * into the subsequent tests. If the first group safety check passed and it
 * was requesting StorageState to be set to DISABLED. We then _assume_ that
 * these shards are already in this state in the following test within this
 * round of tests.
 *
 *
 * In order for the scheduler to do some tradeoffs, it needs to know some
 * metadata about the maintenance behind the safety check request. It will
 * help the scheduler decide on prioritizing some maintenances over.
 *
 * The SafetyCheckScheduler takes a SerialWorkContext pointer that it will use
 * to enqueue work to be processed on
 */
class SafetyCheckScheduler {
 public:
  using NodeIndexSet = folly::F14FastSet<node_index_t>;
  using ShardsAndSequencers = std::pair<ShardSet, NodeIndexSet>;
  /**
   * A structure holding the results of a safety checker run.
   */
  struct Result {
    folly::F14NodeMap<GroupID, Impact> unsafe_groups;
    ShardSet safe_shards;
    NodeIndexSet safe_sequencers;
  };

  SafetyCheckScheduler(Processor* processor,
                       UpdateableSettings<AdminServerSettings> settings,
                       std::shared_ptr<SafetyChecker> safety_checker)
      : processor_(processor),
        settings_(std::move(settings)),
        safety_checker_(std::move(safety_checker)) {}

  /**
   * This is the main function of this class. It accepts a
   * ClusterMaintenanceWrapper which is able to answer information about the
   * groups and the workflows that are currently requesting safety check runs.
   *
   * The schedule function will do some attempts to group the maintenance
   * requests internally and return a Result object that contains information
   * about which state transitions are safe for shards and sequencers.
   */
  folly::SemiFuture<folly::Expected<Result, Status>> schedule(
      const ClusterMaintenanceWrapper& maintenance_state,
      const ShardAuthoritativeStatusMap& status_map,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_config,
      const std::vector<const ShardWorkflow*>& shard_wf,
      const std::vector<const SequencerWorkflow*>& seq_wf) const;

  virtual ~SafetyCheckScheduler() {}

 protected:
  // Used by Tests.
  SafetyCheckScheduler() {}

  /**
   * A data structure that records the progress of the progressive safety check
   * operations. The `result` field has the final recorded result if the plan
   * is empty.
   */
  struct ExecutionState {
    struct LastCheck {
      // The last group-id that was checked.
      GroupID group_id;
      // The last set of sequencers
      SafetyCheckScheduler::NodeIndexSet sequencers;
      // The last set of shards
      ShardSet shards;
      // The safety check result.
      Impact impact;
    };
    // The execution plan. This gets updated as we progress, each time we check
    // one item we remove it and update the Result.
    std::deque<std::pair<GroupID, ShardsAndSequencers>> plan;
    // The last check_impact operation if any.
    folly::Optional<LastCheck> last_check{folly::none};

    // The accumulated result. This includes the shards and sequencers that we
    // should consider _disabled_ for the next checks.
    Result result;
  };

  /*
   * This method takes a plan, and the last impact and executes the next step in
   * the plan based on the results accumulated so far.
   */
  virtual folly::SemiFuture<folly::Expected<Result, Status>>
  executePlan(ExecutionState exec_state,
              ShardAuthoritativeStatusMap status_map,
              std::shared_ptr<const configuration::nodes::NodesConfiguration>
                  nodes_config) const;

  // Performs a single safety check round for the given shards + sequencers.
  // This method takes arguments by-value to ensure that we don't hold
  // references across the async boundary.
  virtual folly::SemiFuture<folly::Expected<Impact, Status>> performSafetyCheck(
      ShardSet disabled_shards,
      NodeIndexSet disabled_sequencers,
      ShardAuthoritativeStatusMap status_map,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>,
      ShardSet shards,
      NodeIndexSet sequencers) const;

  // Build execution plans for shard+sequencer safety checks.
  // This returns an ordered list of safety check operations that need to be
  // tested in order.
  //
  // Each item is a set of shards that need to be tested along with a list of
  // sequencer DISABLED checks that go with it.
  //
  // The safety checker will perform both checks combined, there is no point of
  // performing a disable on a sequencer if we know we can't transition to the
  // target state for shards within the same maintenanc group.
  virtual std::deque<std::pair<GroupID, ShardsAndSequencers>>
  buildExecutionPlan(const ClusterMaintenanceWrapper& maintenance_state,
                     const std::vector<const ShardWorkflow*>& shard_wf,
                     const std::vector<const SequencerWorkflow*>& seq_wf) const;

 private:
  // Reads the last set impact in the execution state object and updates the
  // internal results.
  void updateResult(ExecutionState& state) const;

  Processor* processor_;
  UpdateableSettings<AdminServerSettings> settings_;
  std::shared_ptr<SafetyChecker> safety_checker_;
};
}}} // namespace facebook::logdevice::maintenance
