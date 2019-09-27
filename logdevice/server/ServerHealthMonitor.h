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

#include "logdevice/common/HealthMonitor.h"

namespace facebook { namespace logdevice {

class ServerHealthMonitor : public HealthMonitor {
 public:
  ServerHealthMonitor(folly::Executor& executor,
                      std::chrono::milliseconds sleep_period,
                      int num_workers);
  ~ServerHealthMonitor() override {}

  void startUp() override;
  folly::SemiFuture<folly::Unit> shutdown() override;

  // reporter methods
  void reportWatchdogHealth(bool delayed) override;
  void reportStalledWorkers(int num_stalled) override;
  void reportWorkerStall(int idx, std::chrono::milliseconds duration) override;
  void reportWorkerQueueHealth(int idx, bool delayed) override;

 protected:
  void monitorLoop();
  // void resetInternalState(); // to be added later if needed.
  void processReports() {}

 private:
  using TimeSeries = folly::BucketedTimeSeries<std::chrono::milliseconds,
                                               std::chrono::steady_clock>;
  using TimePoint = std::chrono::steady_clock::time_point;
  static constexpr int NUM_BUCKETS = 6;
  static constexpr int NUM_PERIODS = 3;
  static constexpr std::chrono::milliseconds MAX_LOOP_STALL =
      std::chrono::milliseconds(10);

  friend class ServerHealthMonitorTest;
  folly::Executor& executor_;
  std::chrono::milliseconds sleep_period_;
  std::atomic_bool shutdown_{false};
  folly::Promise<folly::Unit> shutdown_promise_;
  std::chrono::steady_clock::time_point last_entry_time_;

  struct HMInfo {
    int num_workers_{};
    bool health_monitor_delay_{false};
    bool watchdog_delay_{false};
    int total_stalled_workers{};
    std::vector<bool> worker_queue_delays_{};
    std::vector<TimeSeries> worker_stalls_;
  };
  HMInfo internal_info_;
};

}} // namespace facebook::logdevice
