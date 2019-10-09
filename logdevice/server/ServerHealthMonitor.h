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
#include <folly/futures/Promise.h>
#include <folly/stats/BucketedTimeSeries.h>

#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"
#include "logdevice/common/HealthMonitor.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

class ServerHealthMonitor : public HealthMonitor {
 public:
  enum class NodeState { HEALTHY, UNHEALTHY };
  ServerHealthMonitor(folly::Executor& executor,
                      std::chrono::milliseconds sleep_period,
                      int num_workers,
                      StatsHolder* stats);
  ~ServerHealthMonitor() override {}

  void startUp() override;
  folly::SemiFuture<folly::Unit> shutdown() override;

  // reporter methods
  void reportWatchdogHealth(bool delayed) override;
  void reportStalledWorkers(int num_stalled) override;
  void reportWorkerStall(int idx, std::chrono::milliseconds duration) override;
  void reportWorkerQueueStall(int idx,
                              std::chrono::milliseconds duration) override;

 protected:
  void monitorLoop();
  // void resetInternalState(); // to be added later if needed.
  void processReports();

 private:
  friend class ServerHealthMonitorTest;
  using TimeSeries = folly::BucketedTimeSeries<std::chrono::milliseconds,
                                               std::chrono::steady_clock>;
  using TimePoint = std::chrono::time_point<std::chrono::steady_clock,
                                            std::chrono::milliseconds>;
  static constexpr int kNumBuckets = 6;
  static constexpr int kNumPeriods = 3;
  static constexpr int kPeriodRange = 2;
  static constexpr int kDecreaseRate = 1000; // decrease is just time passed
  static constexpr int kFuzzFactor = 0;      // no uncertainty
  static constexpr std::chrono::milliseconds kMaxTimerValue =
      std::chrono::milliseconds(10000);
  static constexpr std::chrono::milliseconds kMaxLoopStall =
      std::chrono::milliseconds(10);

  folly::Executor& executor_;
  std::chrono::milliseconds sleep_period_;
  std::atomic_bool shutdown_{false};
  folly::Promise<folly::Unit> shutdown_promise_;
  std::chrono::steady_clock::time_point last_entry_time_;
  StatsHolder* stats_;
  ChronoExponentialBackoffAdaptiveVariable<std::chrono::milliseconds>
      state_timer_;
  NodeState node_state_{NodeState::HEALTHY};
  bool overloaded_{false};

  bool isOverloaded(TimePoint start_time, TimePoint end_time);
  void updateVariables(TimePoint now);
  void calculateNegativeSignal(TimePoint now,
                               TimePoint start_time,
                               TimePoint end_time);

  struct HMInfo {
    int num_workers_{};
    bool health_monitor_delay_{false};
    bool watchdog_delay_{false};
    int total_stalled_workers{0};
    std::vector<TimeSeries> worker_stalls_;
    std::vector<TimeSeries> worker_queue_stalls_;
  };
  HMInfo internal_info_;
};

}} // namespace facebook::logdevice
