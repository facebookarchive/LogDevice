/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerHealthMonitor.h"

#include "logdevice/common/chrono_util.h"

namespace facebook { namespace logdevice {

ServerHealthMonitor::ServerHealthMonitor(folly::Executor& executor,
                                         std::chrono::milliseconds sleep_period,
                                         int num_workers)
    : executor_(executor), sleep_period_(sleep_period) {
  internal_info_.num_workers_ = num_workers;
  internal_info_.worker_queue_delays_.reserve(num_workers);
  internal_info_.worker_queue_delays_.assign(num_workers, false);
  internal_info_.worker_stalls_.reserve(num_workers);
  internal_info_.worker_stalls_.assign(
      num_workers, {NUM_BUCKETS, NUM_PERIODS * sleep_period_});
}

void ServerHealthMonitor::startUp() {
  monitorLoop();
}

void ServerHealthMonitor::monitorLoop() {
  last_entry_time_ = std::chrono::steady_clock::now();
  folly::futures::sleep(sleep_period_)
      .via(&executor_)
      .then([this](folly::Try<folly::Unit>) mutable {
        if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
          shutdown_promise_.setValue();
          return;
        }
        int64_t loop_entry_delay = msec_since(last_entry_time_);
        internal_info_.health_monitor_delay_ =
            (loop_entry_delay - sleep_period_.count() > MAX_LOOP_STALL.count())
            ? true
            : false;
        processReports();
        monitorLoop();
      });
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

void ServerHealthMonitor::reportWorkerQueueHealth(int idx, bool delayed) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  executor_.add([this, idx, delayed]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_queue_delays_.capacity()) {
      if (delayed && !internal_info_.worker_queue_delays_[idx]) {
        internal_info_.worker_queue_delays_[idx] = delayed;
      }
      if (!delayed && internal_info_.worker_queue_delays_[idx]) {
        internal_info_.worker_queue_delays_[idx] = delayed;
      }
    }
  });
}

void ServerHealthMonitor::reportWorkerStall(
    int idx,
    std::chrono::milliseconds duration) {
  if (shutdown_.load(std::memory_order::memory_order_relaxed)) {
    return;
  }
  TimePoint tp = std::chrono::steady_clock::now();
  executor_.add([this, idx, tp, duration]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_queue_delays_.capacity()) {
      internal_info_.worker_stalls_[idx].addValue(tp, duration);
    }
  });
}

}} // namespace facebook::logdevice
