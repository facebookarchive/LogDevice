/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FlowGroup.h"

#include <folly/Random.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

bool FlowGroup::injectShapingEvent(Priority p) {
  double chance_percent =
      Worker::settings().message_error_injection_chance_percent;
  if (chance_percent != 0 &&
      Worker::settings().message_error_injection_status == E::CBREGISTERED &&
      reordering_allowed_at_priority_ == Priority::INVALID && configured() &&
      enabled() && folly::Random::randDouble(0, 100.0) <= chance_percent) {
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

  // Run one last time once after disabling traffic shaping on a FlowGroup.
  // This ensures that any messages blocked due to past resource constraints
  // are now released.
  if (!enabled_) {
    // Clear any accumulated bandwidth budget or debt so that any future enable
    // of this flow group has a clean starting state.
    for (auto& e : meter_.entries) {
      e.reset(0);
    }
    return true;
  }

  bool need_to_run = false;
  auto policy_it = update.policy.entries.begin();
  auto meter_it = meter_.entries.begin();
  Priority p = Priority::MAX;
  for (auto& entry : update.overflow_entries) {
    auto priorityq_was_blocked = !meter_it->canDrain() && !priorityq_.empty();

    meter_it->setCapacity(policy_it->capacity);
    meter_it->resetDepositBudget(policy_it->max_bw);

    // Every Sender gets the same priority based increment.
    // Overflows are accumulated across all Senders to become
    // the overflow value used during the next quantum.
    entry.cur_overflow +=
        meter_it->fill(policy_it->guaranteed_bw, policy_it->capacity);

    // Any credit that was returned by FlowGroup's clients in the last quantum,
    // that couldn't fit in our bucket, since it would have violated burst
    // limit, is equivalent to the overflow that happens when we fill the bucket
    // during a normal TrafficShaper deposit. This overflow occured in the
    // last quantum, hence we add it to last_overflow.
    entry.last_overflow += meter_it->consumeReturnedCreditOverflow();

    // First fit. Any overflow remaining at the end of the run is
    // given to the priority queue buckets. If it can't fit in the
    // priority queue buckets, it is discarded.
    ld_check(entry.last_overflow >= 0);
    entry.last_overflow =
        meter_it->fill(entry.last_overflow, policy_it->capacity);
    ld_check(entry.last_overflow >= 0);

    // Move the bandwidth credits from priority queue class into current traffic
    // class.
    // Deposit budget for traffic class in an interval is bound by max bytes per
    // second. Assume that a traffic class has following bucket parameters,
    // burst capacity(100) > max bytes per second(10) >
    // guaranteed bytes per second(2). Let's assume an interval of 1 sec.
    // At every interval, deposit budget for the traffic class will be reset to
    // 10, and 2 credits will be added to the bucket. This reduces deposit
    // budget to 8. If none of this gets used in the current interval for the
    // traffic class, then in the next interval deposit budget will be reset and
    // another 2 credits will be added to the bucket. This reduces the deposit
    // budget to 8 credits again.
    // If a burst of 20 credits comes in for the traffic class, it will consume
    // 4 credits from the bucket and borrow 8 from pq class, if pq class has
    // them, and incur a debt of 8. Ideally, these 20 credits should not have
    // incurred any debt, because there was no traffic in the previous interval.
    // Requested bandwidth is 10 bytes per second, so 20 bytes in 2 seconds
    // is within bounds.
    // This transfer from pq class to the requesting traffic class helps to
    // tackle this issue. In the above example in the beginning of 1st interval,
    // 8 credits if available in pq class will be transferred to traffic class
    // bucket, after this number of credits in bucket is 10 and deposit budget
    // becomes 0. In the second interval, deposit budget for traffic class
    // bucket will be reset to 10, and it will get 2 credits as part of
    // guaranteed bandwidth. Again if 8 credits are available in pq class
    // bucket, they will be transferred to traffic class bucket. The total
    // credit count in bucket will be 20. Now, if a burst that consumes 20
    // credits comes in, it won't incur any debt. This way the traffic class
    // bandwidth can be reached or sustained even in presence of bursty traffic.
    // Generally speaking, this allows to accrue credits equivalent to max
    // requested bandwidth every iteration till bucket capacity is reached. If a
    // traffic class is not using it's credits entirely for multiple iterations,
    // it can still reach it's requested or target bandwidth because enough
    // credits get accumulated beforehand for this class during the lull period.
    // Other advantage of doing this is less contention, transferring credits
    // from pq class to traffic class when sending message acquires flow_meters
    // mutex which can be a point of contention. By preadding the credits to
    // traffic class will reduce the contention.
    if (p != Priority::INVALID) {
      auto requested_amount = policy_it->capacity - meter_it->level();
      transferCredit(PRIORITYQ_PRIORITY, p, requested_amount);
    }

    ld_check_ge(policy_it->capacity, meter_it->level());

    if (priorityq_was_blocked && meter_it->canDrain()) {
      need_to_run = true;
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

    // Getting priorityq size is linear time operation instead just depend on
    // whether request priorityq is empty or not to see if there is still
    // backlog
    deps_->statsSet(
        &PerShapingPriorityStats::pq_backlog, scope_, p, priorityq_.empty(p));
    // Reconfigure access to priority queue bandwidth. The traffic shaper
    // may have reset the deposit budget.
    priorityq_.enable(p, meter_entry.canFill());
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
