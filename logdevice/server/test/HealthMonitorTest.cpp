/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/HealthMonitor.h"

#include <folly/executors/InlineExecutor.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;

#include "logdevice/common/chrono_util.h"
#include "logdevice/server/test/MockHealthMonitor.h"

namespace facebook { namespace logdevice {
class HealthMonitorTest : public testing::Test {
 public:
  explicit HealthMonitorTest() {}
  // Tests case for when no changes are reported to the health monitor.
  // Waits for HM shutdown.
  void noUpdatesTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kLoopDuration);
    hm->startUp();
    auto health_monitor_closed = hm->shutdown();
    health_monitor_closed.wait();
    EXPECT_EQ(false, hm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, hm->internal_info_.health_monitor_delay_);
    EXPECT_EQ(0, hm->internal_info_.total_stalled_workers);
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(0, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(0, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
  }

  void totalStalledWorkersTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kLoopDuration);
    hm->startUp();
    hm->reportStalledWorkers(MockHealthMonitor::kStalledWorkers);
    auto health_monitor_closed =
        folly::futures::sleep(
            std::chrono::milliseconds(3 * MockHealthMonitor::kLoopDuration))
            .via(&inlineExecutor)
            .then([&hm](folly::Try<folly::Unit>) mutable {
              return hm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(2, hm->internal_info_.total_stalled_workers);
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
  }

  void watchdogDelayTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kLoopDuration);
    hm->startUp();
    hm->reportWatchdogHealth(/*delayed = */ true);
    auto health_monitor_closed =
        folly::futures::sleep(
            std::chrono::milliseconds(2 * MockHealthMonitor::kLoopDuration))
            .via(&inlineExecutor)
            .then([&hm](folly::Try<folly::Unit>) mutable {
              return hm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(true, hm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
  }

  void connectionLimitReachedTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kLoopDuration);
    hm->startUp();
    hm->reportConnectionLimitReached();
    auto health_monitor_closed =
        folly::futures::sleep(
            std::chrono::milliseconds(MockHealthMonitor::kLoopDuration))
            .via(&inlineExecutor)
            .then([&hm](folly::Try<folly::Unit>) mutable {
              return hm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(1, hm->internal_info_.connection_limit_reached_.count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
  }

  void stateTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    hm->updateVariables(now);
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 1
    now += MockHealthMonitor::kExtraLongLoopDuration;
    auto start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // report stalls
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    hm->internal_info_.worker_stalls_[1].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 2
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 - 1) * MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 3
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 * (3 - 1) - 1) *
                  MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 4
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 * (3 * (3 - 1) - 1) - 1) *
                  MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 5
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ((3 * (3 * (3 * (3 - 1) - 1) - 1) - 1) *
                  MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 6-47
    now += 42 * MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
  }

  void overloadTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    hm->updateVariables(now);
    hm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 1
    now += MockHealthMonitor::kExtraLongLoopDuration;
    auto start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // report stalls
    hm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    hm->internal_info_.worker_queue_stalls_[1].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 2
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 3
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 4
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 5
    now += MockHealthMonitor::kExtraLongLoopDuration;
    start_time = now -
        HealthMonitor::kPeriodRange * MockHealthMonitor::kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
  }
  // Tests case for when no changes are reported to the health monitor, but
  // health monitor itself is delayed
  // Waits for HM shutdown.
  void healthMonitorDelayTest() {
    const std::chrono::milliseconds wake_up{150};
    const std::chrono::milliseconds offset{40};
    const int wait_ticks{80};

    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kLongLoopDuration);
    hm->startUp();
    auto health_monitor_closed =
        folly::futures::sleep(wake_up)
            .via(&inlineExecutor)
            .then([&hm](folly::Try<folly::Unit>) mutable {
              return hm->shutdown();
            });

    folly::futures::sleep(std::chrono::milliseconds(offset))
        .via(&inlineExecutor)
        .then([=](folly::Try<folly::Unit>) mutable {
          auto entry_time = std::chrono::steady_clock::now();
          while (msec_since(entry_time) < wait_ticks) {
          }
        });

    health_monitor_closed.wait();
    EXPECT_EQ(true, hm->internal_info_.health_monitor_delay_);
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
  }
};

TEST_F(HealthMonitorTest, NoUpdatesTest) {
  noUpdatesTest();
}

TEST_F(HealthMonitorTest, HealthMonitorDelayTest) {
  healthMonitorDelayTest();
}

TEST_F(HealthMonitorTest, TotalStalledWorkersTest) {
  totalStalledWorkersTest();
}

TEST_F(HealthMonitorTest, WatchdogDelayTest) {
  watchdogDelayTest();
}

TEST_F(HealthMonitorTest, ConnectionLimitReachedTest) {
  connectionLimitReachedTest();
}

TEST_F(HealthMonitorTest, StateTest) {
  stateTest();
}

TEST_F(HealthMonitorTest, OverloadTest) {
  overloadTest();
}

}} // namespace facebook::logdevice
