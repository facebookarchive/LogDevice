/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FlowGroup.h"

#include <folly/Random.h>

namespace facebook { namespace logdevice {

bool FlowGroup::injectShapingEvent(Priority p, double error_chance) {
  if (error_chance != 0 &&
      reordering_allowed_at_priority_ == Priority::INVALID && configured() &&
      enabled() && folly::Random::randDouble(0, 100.0) <= error_chance) {
    RATELIMIT_DEBUG(
        std::chrono::seconds(10), 100, "Injecting traffic shaping event");
    // Empty the meter so that all subsequent messages see a shortage
    // until more bandwdith is added.
    resetMeter(p);
    return true;
  }
  return false;
}

bool FlowGroup::applyUpdate(FlowGroupsUpdate::GroupEntry& update,
                            StatsHolder* stats) {
  // Skip update work if shaping on this flow group has been and continues
  // to be disabled.
  if (!enabled_ && !update.policy.enabled()) {
    return false;
  }

  enabled_ = update.policy.enabled();

  if (!enabled_) {
    // Clear any accumulated bandwidth budget or debt so that any future enable
    // of this flow group has a clean starting state.
    for (auto& e : meter_.entries) {
      e.reset(0);
    }
    // Run one last time after disabling traffic shaping on a FlowGroup.
    // This ensures that any messages blocked due to past resource constraints
    // are now released.
    return true;
  }

  bool need_to_run = false;
  auto policy_it = update.policy.entries.begin();
  auto meter_it = meter_.entries.begin();
  Priority p = Priority::MAX;
  for (auto& entry : update.overflow_entries) {
    auto priorityq_was_blocked = !meter_it->canDrain() && !priorityq_.empty();

    meter_it->setCapacity(policy_it->capacity);

    // Allowed deposits in this quantum without exceeding maximum bandwidth cap.
    size_t budget = policy_it->max_bw + entry.last_deposit_budget_overflow;
    size_t starting_budget = budget;
    ld_check(budget >= policy_it->guaranteed_bw);

    // There are four sources of bandwidth credit for a bucket:
    //
    // SOURCE 1
    // ========
    // Every Sender gets the same guaranteed bandwidth credit in each quantum.
    //
    // Overflows of guaranteed credits are accumulated across all Senders to
    // become the last_overflow value used during the next quantum. cur_overflow
    // is not consumed directly in this quantum (i.e. by a Sender processed
    // later in the update) to ensure all Senders have an equal probability (via
    // randomize shuffle of the order in which updates are applied) of being
    // able to consume excess credit.
    entry.cur_overflow += meter_it->fill(policy_it->guaranteed_bw, budget);

    // SOURCE 2
    // ========
    // Consumption of credit returned to the Sender by clients during the
    // previous quantum.
    //
    // These "redeposits" occur when an operation doesn't consume as many
    // credits as estimated. When redeposits occurr, they are used to first
    // fill the traffic shaping bucket. Any overflow, which if used in the
    // current quantum would cause us to violate burst limits, is retained
    // for application in the next quantum. This is equivalent to the overflow
    // that happens when we fill the bucket during a normal TrafficShaper
    // deposit of guananteed_bw. But, since this overflow occured in the last
    // quantum, we add it to last_overflow. It is not subject to budget limits
    // since the credits were already allowed in a previous quantum.
    entry.last_overflow += meter_it->consumeReturnedCreditOverflow();

    // SOURCE 3
    // ========
    // Consumption of guaranteed credit that could not be used by other
    // Senders in the last quantum.
    //
    // First fit. Any overflow remaining at the end of the run is
    // given to the priority queue buckets. If it can't fit in the
    // priority queue buckets, it is discarded. This credit is subject
    // to the max bandwidth budget because it was never accounted for
    // in a previous budget.
    ld_check(entry.last_overflow >= 0);
    entry.last_overflow = meter_it->fill(entry.last_overflow, budget);
    ld_check(entry.last_overflow >= 0);

    // SOURCE 4
    // ========
    // Consumption of guaranteed credit that is not assigned directly to
    // a given priority (i.e. deposited directly into the priority queue meter)
    // or could not be used in previous quantums at this or other priority
    // classes.
    //
    // This credit is consumed in priority order so the highest priority
    // traffic flows have first access. This credit is subject to the max
    // bandwidth budget because it was never accounted for in a previous
    // budget.
    if (p != Priority::INVALID) {
      auto requested_amount = policy_it->capacity - meter_it->level();
      transferCredit(PRIORITYQ_PRIORITY, p, requested_amount, budget);
    }

    ld_check_ge(policy_it->capacity, meter_it->level());

    if (priorityq_was_blocked && meter_it->canDrain()) {
      need_to_run = true;
    }

    // Update budget accounting.
    auto fill_amount = starting_budget - budget;
    if (fill_amount < policy_it->max_bw) {
      // We couldn't use up our deposit budget. Transfer the budget credit so
      // it is accessible by meters for this same class that are on other
      // Senders.
      entry.cur_deposit_budget_overflow += policy_it->max_bw - fill_amount;
    } else {
      // Deduct any budget used from the last quantum.
      entry.last_deposit_budget_overflow -= fill_amount - policy_it->max_bw;
    }

    ++policy_it;
    ++meter_it;
    p = priorityBelow(p);
  }
  return need_to_run;
}

size_t FlowGroup::returnCredits(Priority p, size_t amount) {
  if (!enabled()) {
    return amount;
  }
  return meter_.entries[asInt(p)].returnCredits(amount);
}

bool FlowGroup::run(std::mutex& flow_meters_mutex,
                    SteadyTimestamp run_deadline) {
  // There's no work to perform if no callbacks are queued.
  if (priorityq_.empty()) {
    return false;
  }

  auto run_limits_exceeded = [&run_deadline]() {
    return SteadyTimestamp::now() > run_deadline;
  };

  // See if sufficient bandwidth has been added at each priority
  // to allow these queues to drain. We want to consume priority
  // reserved bandwidth first.
  for (Priority p = Priority::MAX; p < PRIORITYQ_PRIORITY;
       p = priorityBelow(p)) {
    auto& meter_entry = meter_.entries[asInt(p)];
    while (!priorityq_.empty(p) && (!enabled_ || meter_entry.canDrain()) &&
           !run_limits_exceeded()) {
      issueCallback(priorityq_.front(p), flow_meters_mutex);
    }

    // Getting priorityq size is a linear time operation. Instead, just depend
    // on whether request priorityq is empty or not to see if there is still
    // a backlog.
    deps_->statsSet(
        &PerShapingPriorityStats::pq_backlog, scope_, p, priorityq_.empty(p));
  }

  return run_limits_exceeded();
}

bool FlowGroup::drain(size_t cost, Priority p) {
  bool res = false;
  if (!wouldCutInLine(p)) {
    auto& meter = meter_.entries[asInt(p)];
    if (!enabled_ || meter.drain(cost)) {
      deps_->statsAdd(&PerShapingPriorityStats::bwconsumed, scope_, p, cost);
      res = true;
    }
  }

  if (assert_can_drain_ && reordering_allowed_at_priority_ == p) {
    // It's the first drain of a BWAvailableCallback at same priority.
    // Should be able to drain.
    ld_check(res);
    assert_can_drain_ = false;
  }

  return res;
}

}} // namespace facebook::logdevice
