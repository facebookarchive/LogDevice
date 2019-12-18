/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <folly/executors/InlineExecutor.h>

#include "logdevice/common/chrono_util.h"
#include "logdevice/server/HealthMonitor.h"

namespace facebook { namespace logdevice {
class MockHealthMonitor : public HealthMonitor {
 public:
  static constexpr std::chrono::milliseconds kLoopDuration{50};
  static constexpr std::chrono::milliseconds kLongLoopDuration{100};
  static constexpr std::chrono::milliseconds kExtraLongLoopDuration{500};
  static constexpr int kStalledWorkers{2};
  static constexpr std::chrono::milliseconds kWorkerStallDuration{200};
  static constexpr std::chrono::milliseconds kMaxQueueStallsAvg{60};
  static constexpr std::chrono::milliseconds kMaxQueueStallDuration{200};
  static constexpr double kMaxOverloadedPercentage{0.3};
  static constexpr std::chrono::milliseconds kMaxStallsAvg{45};
  static constexpr double kMaxStalledPercentage{0.3};
  static constexpr std::chrono::milliseconds kMaxLoopStall{50};

  explicit MockHealthMonitor(folly::InlineExecutor& executor,
                             std::chrono::milliseconds sleep_period)
      : HealthMonitor(executor,
                      sleep_period,
                      kStalledWorkers,
                      nullptr,
                      kMaxQueueStallsAvg,
                      kMaxQueueStallDuration,
                      kMaxOverloadedPercentage,
                      kMaxStallsAvg,
                      kMaxStalledPercentage,
                      kMaxLoopStall) {}
  explicit MockHealthMonitor(folly::Executor& executor,
                             std::chrono::milliseconds sleep_period,
                             int num_workers,
                             StatsHolder* stats,
                             std::chrono::milliseconds max_queue_stalls_avg,
                             std::chrono::milliseconds max_queue_stall_duration,
                             double max_overloaded_worker_percentage,
                             std::chrono::milliseconds max_stalls_avg,
                             double max_stalled_worker_percentage,
                             std::chrono::milliseconds max_loop_stall)
      : HealthMonitor(executor,
                      sleep_period,
                      num_workers,
                      stats,
                      max_queue_stalls_avg,
                      max_queue_stall_duration,
                      max_overloaded_worker_percentage,
                      max_stalls_avg,
                      max_stalled_worker_percentage,
                      max_loop_stall) {}

  NodeHealthStatus
  calculateHealthMonitorState(std::chrono::milliseconds sleep_period) {
    return sleep_period_ < state_timer_.getCurrentValue()
        ? NodeHealthStatus::UNHEALTHY
        : overloaded_ ? NodeHealthStatus::OVERLOADED
                      : NodeHealthStatus::HEALTHY;
  }

  void updateNodeStatus(NodeHealthStatus status, NodeHealthStats stats) {
    node_status_ = status;
    stats.curr_state_timer_value_ms = state_timer_.getCurrentValue().count();
    updateFailureDetectorStatus(node_status_);
    updateNodeHealthStats(stats);
  }

  void simulateLoop(HealthMonitor::TimePoint now) {
    updateVariables(now);
    NodeHealthStats stats = calculateGlobalNodeHealthStats();
    calculateNegativeSignal(now, stats);
    updateNodeStatus(
        calculateHealthMonitorState(kExtraLongLoopDuration), stats);
  }
};
}} // namespace facebook::logdevice
