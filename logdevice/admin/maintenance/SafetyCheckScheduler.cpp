/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/maintenance/SafetyCheckScheduler.h"

#include "logdevice/admin/maintenance/SequencerWorkflow.h"
#include "logdevice/admin/maintenance/ShardWorkflow.h"
#include "logdevice/admin/safety/CheckImpactForLogRequest.h"
#include "logdevice/admin/safety/CheckImpactRequest.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"

namespace facebook { namespace logdevice { namespace maintenance {

folly::SemiFuture<folly::Expected<SafetyCheckScheduler::Result, Status>>
SafetyCheckScheduler::schedule(
    const ClusterMaintenanceWrapper& maintenance_state,
    const ShardAuthoritativeStatusMap& status_map,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    const std::vector<const ShardWorkflow*>& shard_wf,
    const std::vector<const SequencerWorkflow*>& seq_wf) const {
  // Build an empty Result object
  Result result;
  // Build Plan.
  auto plan = buildExecutionPlan(maintenance_state, shard_wf, seq_wf);
  if (plan.size() == 0) {
    ld_warning(
        "We are producing an empty safety-check execution plan. This should "
        "only happen if the maintenance workflows supplied are empty!");
    auto [p, f] = folly::makePromiseContract<folly::Expected<Result, Status>>();
    p.setValue(folly::makeUnexpected(E::INVALID_PARAM));
    return std::move(f);
  }
  // Execute Progressively.
  ExecutionState state;
  state.plan = std::move(plan);
  return executePlan(state, status_map, nodes_config);
}

folly::SemiFuture<folly::Expected<SafetyCheckScheduler::Result, Status>>
SafetyCheckScheduler::executePlan(
    ExecutionState state,
    ShardAuthoritativeStatusMap status_map,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_config) const {
  // now, do we have more shards in plan?
  updateResult(state);

  if (state.plan.size() == 0) {
    auto [p, f] = folly::makePromiseContract<folly::Expected<Result, Status>>();
    p.setValue(std::move(state.result));
    return std::move(f);
  }

  auto [group_id, shards_and_sequencers] = state.plan.front();
  auto [shards_to_check, sequencers_to_check] = shards_and_sequencers;
  // remove it from the plan.
  state.plan.pop_front();
  // We have a plan to execute.
  folly::SemiFuture<folly::Expected<Impact, Status>> safety_future =
      // arguments as passed as copies.
      performSafetyCheck(state.result.safe_shards,
                         state.result.safe_sequencers,
                         status_map,
                         nodes_config,
                         shards_to_check,
                         sequencers_to_check);

  folly::SemiFuture<folly::Expected<Result, Status>> result_fut =
      std::move(safety_future)
          // We switch into unsafe future because we know that it's safe to do
          // so. The execution will happen on the worker of the safety checker.
          // This will be executed inline in the worker executing the
          // underlying CheckImpactRequest.
          .toUnsafeFuture()
          // If the future is set with exception(ExceptionStatus) it will be
          // passed through to the caller's SemiFuture.
          .thenValue([this,
                      status_map{std::move(status_map)},
                      nodes_config{std::move(nodes_config)},
                      state{std::move(state)},
                      group_id{std::move(group_id)},
                      sequencers_to_check{std::move(sequencers_to_check)},
                      shards_to_check{std::move(shards_to_check)}](
                         folly::Expected<Impact, Status>
                             expected_impact) mutable {
            if (expected_impact.hasValue()) {
              state.last_check = ExecutionState::LastCheck{
                  .group_id = group_id,
                  .sequencers = sequencers_to_check,
                  .shards = shards_to_check,
                  .impact = std::move(*expected_impact)};
              // Will schedule another parts of the plan via the processor.
              // (async)
              return executePlan(std::move(state), status_map, nodes_config);
            }
            // We don't want to continue execution if safety checker is
            // failing. Instead, we fail the entire execution stack.
            auto [p, f] =
                folly::makePromiseContract<folly::Expected<Result, Status>>();
            p.setValue(folly::makeUnexpected(expected_impact.error()));
            return std::move(f);
          });
  return result_fut;
}

void SafetyCheckScheduler::updateResult(ExecutionState& state) const {
  // We will update the impact of the last_checked sequencers and shards only
  // if:
  //  - We do not have previous results for them.
  //  - Or it's not safe and previous we recorded that it's unsafe.
  //
  if (state.last_check) {
    if (state.last_check->impact.result == Impact::ImpactResult::NONE) {
      // The operation was safe. Add shards and sequencers to the safe list.
      state.result.safe_shards.insert(
          state.last_check->shards.begin(), state.last_check->shards.end());

      state.result.safe_sequencers.insert(state.last_check->sequencers.begin(),
                                          state.last_check->sequencers.end());
    } else {
      // The operation was unsafe. Add the impact to the group-id results.
      state.result.unsafe_groups[state.last_check->group_id] =
          state.last_check->impact;
    }
  }
}

folly::SemiFuture<folly::Expected<Impact, Status>>
SafetyCheckScheduler::performSafetyCheck(
    ShardSet disabled_shards,
    NodeIndexSet /*disabled_sequencers*/,
    ShardAuthoritativeStatusMap status_map,
    std::shared_ptr<const configuration::nodes::NodesConfiguration>
        nodes_config,
    ShardSet shards,
    NodeIndexSet /*sequencers*/) const {
  // We must have nodes configuration to operate.
  ld_assert(nodes_config != nullptr);
  ld_assert(processor_);

  auto [promise, future] =
      folly::makePromiseContract<folly::Expected<Impact, Status>>();

  // The request callback fulfills the promise.
  auto cb = [p = std::move(promise)](Status status, Impact impact) mutable {
    if (status != E::OK) {
      p.setValue(folly::makeUnexpected(status));
    } else {
      p.setValue(std::move(impact));
    }
  };

  ld_info("Performing safety check for disabling shards %s while assuming that "
          "%s are already disabled",
          toString(shards).c_str(),
          toString(disabled_shards).c_str());

  // Combine the shards into a single input list to safety checker.
  shards.insert(disabled_shards.begin(), disabled_shards.end());

  WorkerType worker_type = CheckImpactRequest::workerType(processor_);
  std::unique_ptr<Request> request = std::make_unique<CheckImpactRequest>(
      status_map,
      shards,
      configuration::StorageState::DISABLED, // We always assume that nodes may
                                             // die.
      SafetyMargin(),
      /* check_metadata_logs= */ true,
      /* check_internal_logs= */ true,
      /* logids_to_check= */ folly::none,
      settings_->safety_max_logs_in_flight,
      /* abort_on_error= */ true,
      settings_->safety_check_timeout,
      settings_->safety_check_failure_sample_size,
      /* read_epoch_metadata_from_sequencer = */ true,
      worker_type,
      std::move(cb));
  int rv = processor_->postRequest(request);
  if (rv != 0) {
    // We couldn't submit the request to the processor.
    ld_error("We couldn't submit the CheckImpactRequest to the logdevice "
             "processor: %s",
             error_description(err));
    ld_check(err != E::OK);
    folly::Promise<folly::Expected<Impact, Status>> error_promise;
    error_promise.setValue(folly::makeUnexpected(err));
    return error_promise.getSemiFuture();
  }

  return std::move(future);
}

std::deque<std::pair<GroupID, SafetyCheckScheduler::ShardsAndSequencers>>
SafetyCheckScheduler::buildExecutionPlan(
    const ClusterMaintenanceWrapper& maintenance_state,
    const std::vector<const ShardWorkflow*>& shard_wf,
    const std::vector<const SequencerWorkflow*>& seq_wf) const {
  // We assume that all the workflows passed to us require safety checks.
  // We also assume that all these shards already exist in the
  // ClusterMaintenanceWrapper as well.
  //
  std::deque<std::pair<GroupID, ShardsAndSequencers>> plan;
  // Stage 1: Group the shards into their corresponding groups.
  std::vector<ShardID> all_shards;
  for (const auto* workflow : shard_wf) {
    ld_assert(workflow != nullptr);
    all_shards.push_back(workflow->getShardID());
  }

  std::vector<node_index_t> all_sequencers;
  for (const auto* workflow : seq_wf) {
    ld_assert(workflow != nullptr);
    all_sequencers.push_back(workflow->getNodeIndex());
  }

  std::unordered_map<GroupID, ShardSet> shards_in_group =
      maintenance_state.groupShardsByGroupID(all_shards);

  std::unordered_map<GroupID, NodeIndexSet> sequencers_in_group =
      maintenance_state.groupSequencersByGroupID(all_sequencers);

  // Stage 2: Order the shard sets as following:
  //   1. Group all MAY_DISAPPEAR groups first as we know that these are
  //   short-term maintenances.
  //   2. For the MAY_DISAPPEAR groups, order by creation_time. First-come
  //   should be allowed to be served first.
  //   3. DRAINED targets are grouped afterwards with the same sub-ordering
  //   strategy.

  std::vector<std::tuple<int64_t, GroupID, ShardsAndSequencers>> may_disappear;
  std::vector<std::tuple<int64_t, GroupID, ShardsAndSequencers>> drained;

  // Finding all may_disappear groups that have shards in the workflows, if
  for (const auto& it : shards_in_group) {
    const GroupID& group_id = it.first;
    auto* maintenance = maintenance_state.getMaintenanceByGroupID(group_id);
    ld_check(maintenance != nullptr);
    int64_t timestamp = maintenance->created_on_ref().value_or(0);
    // Do we have sequencer disable requests for this group?
    NodeIndexSet sequencers_to_disable;
    auto seq_it = sequencers_in_group.find(group_id);
    if (seq_it != sequencers_in_group.end()) {
      // We have sequencers to disable here.
      sequencers_to_disable = seq_it->second;
      // remove it from the groups, as we have picked that up already.
      sequencers_in_group.erase(seq_it);
    }

    ShardsAndSequencers v = std::make_pair(it.second, sequencers_to_disable);

    if (maintenance->shard_target_state ==
        ShardOperationalState::MAY_DISAPPEAR) {
      may_disappear.push_back(std::make_tuple(timestamp, group_id, v));
    } else {
      ld_check(maintenance->shard_target_state ==
               ShardOperationalState::DRAINED);
      drained.push_back(std::make_tuple(timestamp, group_id, v));
    }
  }

  // sort by creation timestamp
  std::sort(may_disappear.begin(),
            may_disappear.end(),
            [](const std::tuple<int64_t, GroupID, ShardsAndSequencers>& i,
               const std::tuple<int64_t, GroupID, ShardsAndSequencers>& j) {
              return std::get<0>(i) < std::get<0>(j);
            });

  // sort by creation timestamp
  std::sort(drained.begin(),
            drained.end(),
            [](const std::tuple<int64_t, GroupID, ShardsAndSequencers>& i,
               const std::tuple<int64_t, GroupID, ShardsAndSequencers>& j) {
              return std::get<0>(i) < std::get<0>(j);
            });

  // Build the plan
  std::for_each(
      may_disappear.begin(),
      may_disappear.end(),
      [&](std::tuple<int64_t, GroupID, ShardsAndSequencers> v) {
        plan.push_back(std::make_pair(std::get<1>(v), std::get<2>(v)));
      });

  std::for_each(
      drained.begin(),
      drained.end(),
      [&](std::tuple<int64_t, GroupID, ShardsAndSequencers> v) {
        plan.push_back(std::make_pair(std::get<1>(v), std::get<2>(v)));
      });

  // Do we have sequencer-only maintenances still left?
  if (sequencers_in_group.size() > 0) {
    for (const auto& it : sequencers_in_group) {
      ShardSet empty;
      plan.push_back(
          std::make_pair(it.first, std::make_pair(empty, it.second)));
    }
  }

  ld_check(plan.size() ==
           may_disappear.size() + drained.size() + sequencers_in_group.size());
  return plan;
}
}}} // namespace facebook::logdevice::maintenance
