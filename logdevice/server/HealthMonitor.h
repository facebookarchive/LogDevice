/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <vector>

#include <folly/Executor.h>
#include <folly/SharedMutex.h>
#include <folly/futures/Promise.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/NodeHealthStatus.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {
class FailureDetector;
class HealthMonitor {
 public:
  HealthMonitor(folly::Executor& executor,
                std::chrono::milliseconds sleep_period,
                int num_workers,
                StatsHolder* stats,
                std::chrono::milliseconds max_queue_stalls_avg,
                std::chrono::milliseconds max_queue_stall_duration,
                double max_overloaded_worker_percentage,
                std::chrono::milliseconds max_stalls_avg,
                double max_stalled_worker_percentage);
  ~HealthMonitor() {}

  void startUp();
  folly::SemiFuture<folly::Unit> shutdown();
  void setFailureDetector(FailureDetector* failure_detector);

  // reporter methods
  void reportWatchdogHealth(bool delayed);
  void reportStalledWorkers(int num_stalled);
  void reportWorkerStall(int idx, std::chrono::milliseconds duration);
  void reportWorkerQueueStall(int idx, std::chrono::milliseconds duration);

 protected:
  using TimeSeries = folly::BucketedTimeSeries<std::chrono::duration<float>,
                                               std::chrono::steady_clock>;
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock,
                                            std::chrono::milliseconds>;
  struct StallInfo {
    int critically_stalled_{0};
    bool stalled_{false};
  };

  void monitorLoop();
  // void resetInternalState(); // to be added later if needed.
  void processReports();

  bool isOverloaded(TimePoint now, std::chrono::milliseconds half_period);
  StallInfo isStalled(TimePoint now, std::chrono::milliseconds half_period);
  void updateVariables(TimePoint now);
  void calculateNegativeSignal(TimePoint now);
  void updateFailureDetectorStatus(NodeHealthStatus status);

 private:
  friend class HealthMonitorTest;

  static constexpr int kNumBuckets = 12;
  static constexpr int kNumPeriods = 6;
  static constexpr int kMultiplier = 3;
  static constexpr int kPeriodRange = 3;
  static constexpr int kDecreaseRate = 1000; // decrease is just time passed
  static constexpr int kFuzzFactor = 0;      // no uncertainty
  static constexpr std::chrono::milliseconds kMaxTimerValue =
      std::chrono::milliseconds(100000);
  static constexpr std::chrono::milliseconds kMaxLoopStall =
      std::chrono::milliseconds(50);

  folly::Promise<folly::Unit> shutdown_promise_;
  folly::SemiFuture<folly::Unit> sleep_semifuture_;

  folly::Executor& executor_;
  std::atomic_bool shutdown_{false};
  std::chrono::steady_clock::time_point last_entry_time_;

  StallInfo stall_info_{0, false};
  folly::SharedMutex mutex_;
  FailureDetector* failure_detector_{nullptr};

  void removeFailureDetector();

  struct HMInfo {
    int num_workers_{};
    bool health_monitor_delay_{false};
    bool watchdog_delay_{false};
    int total_stalled_workers{0};
    std::vector<TimeSeries> worker_stalls_;
    std::vector<TimeSeries> worker_queue_stalls_;
  };
  HMInfo internal_info_;

 protected:
  NodeHealthStatus node_status_{NodeHealthStatus::HEALTHY};
  bool overloaded_{false};
  const std::chrono::milliseconds sleep_period_;
  ChronoExponentialBackoffAdaptiveVariable<std::chrono::milliseconds>
      state_timer_;

 private:
  StatsHolder* stats_;
  const std::chrono::milliseconds max_queue_stalls_avg_;
  const std::chrono::milliseconds max_queue_stall_duration_;
  const double max_overloaded_worker_percentage_;

  const std::chrono::milliseconds max_stalls_avg_;
  const double max_stalled_worker_percentage_;
};

}} // namespace facebook::logdevice
