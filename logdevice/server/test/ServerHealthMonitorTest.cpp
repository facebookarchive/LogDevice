/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerHealthMonitor.h"

#include <folly/executors/InlineExecutor.h>
#include <gtest/gtest.h>

#include "logdevice/common/chrono_util.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
class ServerHealthMonitorTest : public testing::Test {
 public:
  const std::chrono::milliseconds loop_duration{50};
  const int stalled_workers{2};
  const std::chrono::milliseconds worker_stall_duration{50};

  explicit ServerHealthMonitorTest() {}
  // Tests case for when no changes are reported to the health monitor.
  // Waits for HM shutdown.
  void noUpdatesTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        std::make_unique<ServerHealthMonitor>(
            inlineExecutor,
            std::chrono::milliseconds(loop_duration),
            stalled_workers);

    shm->startUp();
    auto health_monitor_closed = shm->shutdown();
    health_monitor_closed.wait();
    EXPECT_EQ(false, shm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, shm->internal_info_.worker_queue_delays_[0]);
    EXPECT_EQ(false, shm->internal_info_.worker_queue_delays_[1]);
    EXPECT_EQ(0, shm->internal_info_.total_stalled_workers);
    EXPECT_EQ(true, shm->internal_info_.worker_stalls_[0].empty());
    EXPECT_EQ(true, shm->internal_info_.worker_stalls_[1].empty());
  }

  void totalStalledWorkersTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        std::make_unique<ServerHealthMonitor>(
            inlineExecutor,
            std::chrono::milliseconds(loop_duration),
            stalled_workers);
    shm->startUp();
    shm->reportStalledWorkers(stalled_workers);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(3 * loop_duration))
            .via(&inlineExecutor)
            .then([&shm](folly::Try<folly::Unit>) mutable {
              return shm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(2, shm->internal_info_.total_stalled_workers);
  }

  void watchdogDelayTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        std::make_unique<ServerHealthMonitor>(
            inlineExecutor, loop_duration, stalled_workers);

    shm->startUp();
    shm->reportWatchdogHealth(/*delayed = */ true);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(2 * loop_duration))
            .via(&inlineExecutor)
            .then([&shm](folly::Try<folly::Unit>) mutable {
              return shm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(true, shm->internal_info_.watchdog_delay_);
  }

  void workerStallsTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    auto now = std::chrono::steady_clock::now();
    std::unique_ptr<ServerHealthMonitor> shm =
        std::make_unique<ServerHealthMonitor>(
            inlineExecutor, loop_duration, stalled_workers);
    shm->startUp();

    shm->reportWorkerStall(0, worker_stall_duration);

    folly::futures::sleep(std::chrono::milliseconds(loop_duration))
        .via(&inlineExecutor)
        .then([=, &shm](folly::Try<folly::Unit>) mutable {
          shm->reportWorkerStall(0, worker_stall_duration);
          shm->reportWorkerStall(1, worker_stall_duration);
        });

    folly::futures::sleep(std::chrono::milliseconds(2 * loop_duration))
        .via(&inlineExecutor)
        .then([&shm](folly::Try<folly::Unit>) mutable {
          EXPECT_EQ(2, shm->internal_info_.worker_stalls_[0].count());
          EXPECT_EQ(1, shm->internal_info_.worker_stalls_[1].count());
        });

    auto health_monitor_closed =

        folly::futures::sleep(std::chrono::milliseconds(4 * loop_duration))
            .via(&inlineExecutor)
            .then([=, &shm](folly::Try<folly::Unit>) mutable {
              shm->internal_info_.worker_stalls_[0].update(now +
                                                           4 * loop_duration);
              shm->internal_info_.worker_stalls_[1].update(now +
                                                           4 * loop_duration);
              return shm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[1].count());
  }

  // Tests case for when no changes are reported to the health monitor, but
  // health monitor itself is delayed
  // Waits for HM shutdown.
  void healthMonitorDelayTest() {
    const std::chrono::milliseconds long_loop_duration{100};
    const std::chrono::milliseconds wake_up{150};
    const std::chrono::milliseconds offset{40};
    const int wait_ticks{80};

    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        std::make_unique<ServerHealthMonitor>(
            inlineExecutor, long_loop_duration, stalled_workers);

    shm->startUp();
    auto health_monitor_closed =
        folly::futures::sleep(wake_up)
            .via(&inlineExecutor)
            .then([&shm](folly::Try<folly::Unit>) mutable {
              return shm->shutdown();
            });

    folly::futures::sleep(std::chrono::milliseconds(offset))
        .via(&inlineExecutor)
        .then([=](folly::Try<folly::Unit>) mutable {
          auto entry_time = std::chrono::steady_clock::now();
          while (msec_since(entry_time) < wait_ticks) {
          }
        });

    health_monitor_closed.wait();
    EXPECT_EQ(true, shm->internal_info_.health_monitor_delay_);
  }
};

TEST_F(ServerHealthMonitorTest, NoUpdatesTest) {
  noUpdatesTest();
}

TEST_F(ServerHealthMonitorTest, HealthMonitorDelayTest) {
  healthMonitorDelayTest();
}

TEST_F(ServerHealthMonitorTest, TotalStalledWorkersTest) {
  totalStalledWorkersTest();
}

TEST_F(ServerHealthMonitorTest, WatchdogDelayTest) {
  watchdogDelayTest();
}

TEST_F(ServerHealthMonitorTest, WorkerStallsTest) {
  workerStallsTest();
}
}} // namespace facebook::logdevice
