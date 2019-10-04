/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerHealthMonitor.h"

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/chrono_util.h"

namespace facebook { namespace logdevice {

ServerHealthMonitor::ServerHealthMonitor(folly::Executor& executor,
                                         std::chrono::milliseconds sleep_period,
                                         int num_workers,
                                         StatsHolder* stats)
    : executor_(executor),
      sleep_period_(sleep_period),
      stats_(stats),
      state_timer_(
          /*min=*/sleep_period, // cannot be unhealthy shorter than one loop
          /*initial=*/sleep_period,
          /*max=*/kMaxTimerValue,
          /*multiplier=*/kNumPeriods,
          /*decrease_rate=*/kDecreaseRate,
          /*fuzz_factor=*/kFuzzFactor) {
  internal_info_.num_workers_ = num_workers;
  internal_info_.worker_stalls_.reserve(num_workers);
  internal_info_.worker_stalls_.assign(
      num_workers, {kNumBuckets, kNumPeriods * sleep_period_});
  internal_info_.worker_queue_stalls_.reserve(num_workers);
  internal_info_.worker_queue_stalls_.assign(
      num_workers, {kNumBuckets, kNumPeriods * sleep_period_});
}

void ServerHealthMonitor::startUp() {
  auto now = SteadyTimestamp::now();
  updateVariables(now);
  monitorLoop();
}

void ServerHealthMonitor::monitorLoop() {
  last_entry_time_ = SteadyTimestamp::now();
  folly::futures::sleep(sleep_period_)
      .via(&executor_)
      .then([this](folly::Try<folly::Unit>) mutable {
        STAT_INCR(stats_, health_monitor_num_loops);

        if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
          shutdown_promise_.setValue();
          return;
        }
        int64_t loop_entry_delay = msec_since(last_entry_time_);
        internal_info_.health_monitor_delay_ =
            (loop_entry_delay - sleep_period_.count() > kMaxLoopStall.count())
            ? true
            : false;
        processReports();
        monitorLoop();
      });
}
void ServerHealthMonitor::updateVariables(TimePoint now) {
  std::for_each(internal_info_.worker_stalls_.begin(),
                internal_info_.worker_stalls_.end(),
                [now](TimeSeries& t) { t.update(now); });
  std::for_each(internal_info_.worker_queue_stalls_.begin(),
                internal_info_.worker_queue_stalls_.end(),
                [now](TimeSeries& t) { t.update(now); });
  state_timer_.positiveFeedback(now); // calc how much time has passed
}
bool ServerHealthMonitor::isOverloaded(TimePoint start_time,
                                       TimePoint end_time) {
  // If more than x percent of workers have queue stalls
  // status is overloaded
  return (std::count_if(internal_info_.worker_queue_stalls_.begin(),
                        internal_info_.worker_queue_stalls_.end(),
                        [start_time, end_time](TimeSeries& t) {
                          return t.count(start_time, end_time) > 0;
                        }) >=
          0.3 * internal_info_.worker_queue_stalls_.capacity());
}

void ServerHealthMonitor::calculateNegativeSignal(TimePoint now,
                                                  TimePoint start_time,
                                                  TimePoint end_time) {
  if (internal_info_.health_monitor_delay_ || internal_info_.watchdog_delay_ ||
      internal_info_.total_stalled_workers > 0 ||
      std::any_of(internal_info_.worker_stalls_.begin(),
                  internal_info_.worker_stalls_.end(),
                  [start_time, end_time](TimeSeries& t) {
                    return t.count(start_time, end_time) > 0;
                  })) {
    state_timer_.negativeFeedback();
    state_timer_.positiveFeedback(now); // for timekeeping purposes
  }
}

void ServerHealthMonitor::processReports() {
  auto now = SteadyTimestamp::now();
  updateVariables(now);
  auto start_time = now - kPeriodRange * sleep_period_;
  auto end_time = now + std::chrono::nanoseconds(1);
  overloaded_ = isOverloaded(start_time, end_time);
  if (overloaded_) {
    STAT_INCR(stats_, health_monitor_overload_indicator);
  }
  calculateNegativeSignal(now, start_time, end_time);
  node_state_ = (sleep_period_ < state_timer_.getCurrentValue())
      ? NodeState::UNHEALTHY
      : NodeState::HEALTHY;
  if (node_state_ == NodeState::HEALTHY) {
    STAT_INCR(stats_, health_monitor_state_indicator);
  }
}

folly::SemiFuture<folly::Unit> ServerHealthMonitor::shutdown() {
  shutdown_.exchange(true, std::memory_order::memory_order_relaxed);
  return shutdown_promise_.getSemiFuture();
}

void ServerHealthMonitor::reportWatchdogHealth(bool delayed) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  executor_.add([this, delayed]() mutable {
    if (delayed && !internal_info_.watchdog_delay_) {
      internal_info_.watchdog_delay_ = delayed;
    }
    if (!delayed && internal_info_.watchdog_delay_) {
      internal_info_.watchdog_delay_ = delayed;
    }
  });
}

void ServerHealthMonitor::reportStalledWorkers(int num_stalled) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  executor_.add([this, num_stalled]() mutable {
    internal_info_.total_stalled_workers = num_stalled;
  });
}

void ServerHealthMonitor::reportWorkerQueueStall(
    int idx,
    std::chrono::milliseconds duration) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  auto tp = SteadyTimestamp::now();
  executor_.add([this, idx, tp, duration]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_queue_stalls_.capacity()) {
      internal_info_.worker_queue_stalls_[idx].addValue(tp, duration);
    }
  });
}

void ServerHealthMonitor::reportWorkerStall(
    int idx,
    std::chrono::milliseconds duration) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  auto tp = SteadyTimestamp::now();
  executor_.add([this, idx, tp, duration]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_stalls_.capacity()) {
      internal_info_.worker_stalls_[idx].addValue(tp, duration);
    }
  });
}

}} // namespace facebook::logdevice
