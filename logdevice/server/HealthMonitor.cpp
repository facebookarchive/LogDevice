/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/HealthMonitor.h"

#include <folly/synchronization/Baton.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/chrono_util.h"
#include "logdevice/server/FailureDetector.h"

namespace facebook { namespace logdevice {

HealthMonitor::HealthMonitor(folly::Executor& executor,
                             std::chrono::milliseconds sleep_period,
                             int num_workers,
                             StatsHolder* stats,
                             std::chrono::milliseconds max_queue_stalls_avg,
                             std::chrono::milliseconds max_queue_stall_duration,
                             double max_overloaded_worker_percentage,
                             std::chrono::milliseconds max_stalls_avg,
                             double max_stalled_worker_percentage,
                             std::chrono::milliseconds max_loop_stall)
    : executor_(executor),
      sleep_period_(sleep_period),
      state_timer_(
          /*min=*/sleep_period, // cannot be unhealthy shorter than one loop
          /*initial=*/sleep_period,
          /*max=*/kMaxTimerValue,
          /*multiplier=*/kMultiplier,
          /*decrease_rate=*/kDecreaseRate,
          /*fuzz_factor=*/kFuzzFactor),
      stats_(stats),
      max_queue_stalls_avg_(max_queue_stalls_avg),
      max_queue_stall_duration_(max_queue_stall_duration),
      max_overloaded_worker_percentage_(max_overloaded_worker_percentage),
      max_stalls_avg_(max_stalls_avg),
      max_stalled_worker_percentage_(max_stalled_worker_percentage),
      max_loop_stall_(max_loop_stall) {
  internal_info_.num_workers_ = num_workers;
  internal_info_.worker_stalls_.reserve(num_workers);
  internal_info_.worker_stalls_.assign(
      num_workers, {kNumBuckets, kNumPeriods * sleep_period_});
  internal_info_.worker_queue_stalls_.reserve(num_workers);
  internal_info_.worker_queue_stalls_.assign(
      num_workers, {kNumBuckets, kNumPeriods * sleep_period_});
  internal_info_.connection_limit_reached_ = {
      kNumBuckets, kNumPeriods * sleep_period_};
}

void HealthMonitor::startUp() {
  auto now = SteadyTimestamp::now();
  folly::Baton baton;
  executor_.add([this, now, &baton]() mutable {
    updateVariables(now);
    monitorLoop();
    baton.post();
  });
  baton.wait();
}
void HealthMonitor::monitorLoop() {
  last_entry_time_ = SteadyTimestamp::now();
  sleep_semifuture_ = folly::futures::sleep(sleep_period_)
                          .via(&executor_)
                          .then([this](folly::Try<folly::Unit>) mutable {
                            if (shutdown_.load(std::memory_order_relaxed)) {
                              removeFailureDetector();
                              shutdown_promise_.setValue();
                              return;
                            }
                            int64_t loop_entry_delay =
                                msec_since(last_entry_time_);
                            internal_info_.health_monitor_delay_ =
                                (loop_entry_delay - sleep_period_.count() >
                                 max_loop_stall_.count())
                                ? true
                                : false;
                            processReports();
                            if (shutdown_.load(std::memory_order_relaxed)) {
                              removeFailureDetector();
                              shutdown_promise_.setValue();
                              return;
                            }
                            monitorLoop();
                          });
}
void HealthMonitor::updateVariables(TimePoint now) {
  std::for_each(internal_info_.worker_stalls_.begin(),
                internal_info_.worker_stalls_.end(),
                [now](TimeSeries& t) { t.update(now); });
  std::for_each(internal_info_.worker_queue_stalls_.begin(),
                internal_info_.worker_queue_stalls_.end(),
                [now](TimeSeries& t) { t.update(now); });
  internal_info_.connection_limit_reached_.update(now);
  state_timer_.positiveFeedback(now); // calc how much time has passed
}

WorkerTimeSeriesStats
HealthMonitor::createWorkerTimeSeriesStats(int worker_id,
                                           TimeSeries t,
                                           TimePoint now) {
  WorkerTimeSeriesStats worker_stats;
  worker_stats.worker_id = worker_id;
  worker_stats.curr_num = t.count(now - kPeriodRange * sleep_period_, now);
  worker_stats.curr_sum =
      t.sum(now - kPeriodRange * sleep_period_, now).count() * 1000;
  if (worker_stats.curr_num > 0) {
    worker_stats.curr_avg = worker_stats.curr_sum / worker_stats.curr_num;
  }
  return worker_stats;
}

bool HealthMonitor::isOverloaded(TimePoint now,
                                 std::chrono::milliseconds half_period,
                                 NodeHealthStats& stats) {
  auto num_fd_overloads = internal_info_.connection_limit_reached_.count(
      now - kPeriodRange * sleep_period_, now);
  stats.curr_num_conn_req_rejected = num_fd_overloads;
  int worker_id = 0;
  auto num_overloaded_workers = std::count_if(
      internal_info_.worker_queue_stalls_.begin(),
      internal_info_.worker_queue_stalls_.end(),
      [this, now, half_period, &worker_id, &stats](TimeSeries& t) {
        // Detection of problematic queuing periods is done on the past
        // 2 HM loops (2*sleep_period_), taking into account time
        // intervals that are equivalent to sleep_period_ start and end
        // in neighboring loops.
        bool return_value = false;
        for (int p = 2; p <= 2 * kPeriodRange; ++p) {
          // Sum of queue stalls during this period.
          auto period_sum =
              t.sum(now - p * half_period, now - (p - 2) * half_period);
          // Number of queue stalls during this period.
          auto period_count =
              t.count(now - p * half_period, now - (p - 2) * half_period);
          if ((period_count > 0) && (period_sum >= max_queue_stall_duration_) &&
              (period_sum / period_count >= max_queue_stalls_avg_)) {
            return_value = true;
            break;
          }
        }
        if (return_value) {
          stats.overloaded_worker_stats.push_back(
              createWorkerTimeSeriesStats(worker_id, t, now));
        }
        worker_id++;
        return return_value;
      });

  stats.percent_workers_w_long_queued_req =
      num_overloaded_workers / internal_info_.worker_queue_stalls_.capacity();
  // A node is overloaded when more than max_overloaded_worker_percentage_% of
  // workers have overloaded request queues OR when the connection limit has
  // been reached.
  return (num_overloaded_workers >= max_overloaded_worker_percentage_ *
                  internal_info_.worker_queue_stalls_.capacity() ||
          num_fd_overloads > 0);
}

HealthMonitor::StallInfo
HealthMonitor::isStalled(TimePoint now,
                         std::chrono::milliseconds half_period,
                         NodeHealthStats& stats) {
  // A node is stalled when more than max_stalled_worker_percentage_% of
  // workers have stalled requests.
  HealthMonitor::StallInfo info{0, false};
  int worker_id = 0;
  int num_stalled = std::count_if(
      internal_info_.worker_stalls_.begin(),
      internal_info_.worker_stalls_.end(),
      [this, now, &info, half_period, &worker_id, &stats](TimeSeries& t) {
        // Similar to isOverloaded(), detection of problematic queuing
        // periods is done on the past 2 HM loops, and takes into account
        // additional sleep_period_ intervals that extend into neighboring
        // loops.
        bool return_value = false;
        for (int p = 2; p <= 2 * kPeriodRange; ++p) {
          // Sum of request stalls during this period.
          auto period_sum =
              t.sum(now - p * half_period, now - (p - 2) * half_period);
          // Number of request stalls during this period.
          auto period_count =
              t.count(now - p * half_period, now - (p - 2) * half_period);
          if ((period_count > 0) &&
              (period_sum / period_count >= max_stalls_avg_)) {
            // Critically stalled requests are those whose duration is
            // equal to or greater than the sleep_period_. These represent
            // a serious concern and have priority over shorter stalls.
            info.critically_stalled_ +=
                period_sum / period_count >= sleep_period_ ? 1 : 0;
            return_value = true;
            break;
          }
        }
        if (return_value) {
          stats.stalled_worker_stats.push_back(
              createWorkerTimeSeriesStats(worker_id, t, now));
        }
        worker_id++;
        return return_value;
      });
  stats.percent_workers_w_long_exe_req =
      num_stalled / internal_info_.worker_stalls_.capacity();
  info.stalled_ = num_stalled >=
      max_stalled_worker_percentage_ * internal_info_.worker_stalls_.capacity();
  return info;
}
void HealthMonitor::calculateNegativeSignal(TimePoint now,
                                            NodeHealthStats& stats) {
  auto half_period = sleep_period_ / 2;
  stall_info_ = isStalled(now, half_period, stats);
  overloaded_ = isOverloaded(now, half_period, stats);
  if (internal_info_.health_monitor_delay_ || internal_info_.watchdog_delay_ ||
      internal_info_.total_stalled_workers > 0 || stall_info_.stalled_) {
    state_timer_.negativeFeedback();
    state_timer_.positiveFeedback(now); // for timekeeping purposes
  }
  if (stall_info_.critically_stalled_ > 0) {
    state_timer_.negativeFeedback();
    state_timer_.positiveFeedback(now); // for timekeeping purposes
  }
}

void HealthMonitor::processReports() {
  auto now = SteadyTimestamp::now();
  updateVariables(now);
  auto start_time = now - kPeriodRange * sleep_period_;
  auto end_time = now + std::chrono::nanoseconds(1);
  NodeHealthStats stats = calculateGlobalNodeHealthStats();
  calculateNegativeSignal(now, stats);
  node_status_ = (sleep_period_ < state_timer_.getCurrentValue())
      ? NodeHealthStatus::UNHEALTHY
      : overloaded_ ? NodeHealthStatus::OVERLOADED : NodeHealthStatus::HEALTHY;
  stats.curr_state_timer_value_ms = state_timer_.getCurrentValue().count();
  updateFailureDetectorStatus(node_status_);
  updateNodeHealthStats(stats);
  STAT_ADD(stats_,
           health_monitor_unhealthy,
           node_status_ == NodeHealthStatus::UNHEALTHY ? 1 : 0);
  STAT_ADD(stats_, health_monitor_overloaded, overloaded_ ? 1 : 0);
}

NodeHealthStats HealthMonitor::calculateGlobalNodeHealthStats() {
  NodeHealthStats stats;
  stats.observed_time_period_ms = kPeriodRange * sleep_period_.count();
  stats.health_monitor_delayed = internal_info_.health_monitor_delay_;
  stats.watchdog_delayed = internal_info_.watchdog_delay_;
  stats.curr_total_stalled_workers = internal_info_.total_stalled_workers;
  return stats;
}

void HealthMonitor::updateNodeHealthStats(NodeHealthStats stats) {
  folly::SharedMutex::WriteHolder write_lock(stats_mutex_);
  health_stats_ = stats;
}

NodeHealthStats HealthMonitor::getNodeHealthStats() {
  folly::SharedMutex::ReadHolder read_lock(stats_mutex_);
  return health_stats_;
}

folly::SemiFuture<folly::Unit> HealthMonitor::shutdown() {
  shutdown_.exchange(true, std::memory_order_relaxed);
  executor_.add([this]() mutable { sleep_semifuture_.cancel(); });
  return shutdown_promise_.getSemiFuture();
}

void HealthMonitor::reportWatchdogHealth(bool delayed) {
  if (shutdown_.load(std::memory_order_relaxed)) {
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

void HealthMonitor::reportStalledWorkers(int num_stalled) {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return;
  }
  executor_.add([this, num_stalled]() mutable {
    internal_info_.total_stalled_workers = num_stalled;
  });
}

void HealthMonitor::reportWorkerQueueStall(int idx,
                                           std::chrono::milliseconds duration) {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return;
  }
  auto tp = SteadyTimestamp::now();
  executor_.add([this, idx, tp, duration]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_queue_stalls_.capacity()) {
      internal_info_.worker_queue_stalls_[idx].addValue(tp, duration);
    }
  });
}

void HealthMonitor::reportWorkerStall(int idx,
                                      std::chrono::milliseconds duration) {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return;
  }
  auto tp = SteadyTimestamp::now();
  executor_.add([this, idx, tp, duration]() mutable {
    if (idx >= 0 && idx < internal_info_.worker_stalls_.capacity()) {
      internal_info_.worker_stalls_[idx].addValue(tp, duration);
    }
  });
}

void HealthMonitor::reportConnectionLimitReached() {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return;
  }
  auto tp = SteadyTimestamp::now();
  executor_.add([this, tp]() mutable {
    internal_info_.connection_limit_reached_.addValue(tp, 1);
  });
}

void HealthMonitor::setFailureDetector(FailureDetector* failure_detector) {
  if (failure_detector) {
    folly::SharedMutex::WriteHolder write_lock(mutex_);
    failure_detector_ = failure_detector;
  }
}

void HealthMonitor::removeFailureDetector() {
  folly::SharedMutex::WriteHolder write_lock(mutex_);
  failure_detector_ = nullptr;
}

void HealthMonitor::updateFailureDetectorStatus(NodeHealthStatus status) {
  folly::SharedMutex::ReadHolder read_lock(mutex_);
  if (failure_detector_) {
    failure_detector_->setNodeStatus(status);
  }
}

}} // namespace facebook::logdevice
