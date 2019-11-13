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

#include "logdevice/common/chrono_util.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
class HealthMonitorTest : public testing::Test {
 public:
  const std::chrono::milliseconds kLoopDuration{50};
  const std::chrono::milliseconds kLongLoopDuration{100};
  const std::chrono::milliseconds kExtraLongLoopDuration{500};
  const int kStalledWorkers{2};
  const std::chrono::milliseconds kWorkerStallDuration{200};
  const std::chrono::milliseconds kMaxQueueStallsAvg{60};
  const std::chrono::milliseconds kMaxQueueStallDuration{200};
  const double kMaxOverloadedPercentage{0.3};
  const std::chrono::milliseconds kMaxStallsAvg{45};
  const double kMaxStalledPercentage{0.3};

  explicit HealthMonitorTest() {}
  // Tests case for when no changes are reported to the health monitor.
  // Waits for HM shutdown.
  std::unique_ptr<HealthMonitor>
  createHM(folly::InlineExecutor& executor,
           const std::chrono::milliseconds loop_duration) {
    return std::make_unique<HealthMonitor>(executor,
                                           loop_duration,
                                           kStalledWorkers,
                                           nullptr,
                                           kMaxQueueStallsAvg,
                                           kMaxQueueStallDuration,
                                           kMaxOverloadedPercentage,
                                           kMaxStallsAvg,
                                           kMaxStalledPercentage);
  }

  void noUpdatesTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<HealthMonitor> hm = createHM(inlineExecutor, kLoopDuration);
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
    std::unique_ptr<HealthMonitor> hm = createHM(inlineExecutor, kLoopDuration);
    hm->startUp();
    hm->reportStalledWorkers(kStalledWorkers);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(3 * kLoopDuration))
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
    std::unique_ptr<HealthMonitor> hm = createHM(inlineExecutor, kLoopDuration);
    hm->startUp();
    hm->reportWatchdogHealth(/*delayed = */ true);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(2 * kLoopDuration))
            .via(&inlineExecutor)
            .then([&hm](folly::Try<folly::Unit>) mutable {
              return hm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(true, hm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
  }

  NodeHealthStatus
  calculateHealthMonitorState(HealthMonitor& hm,
                              std::chrono::milliseconds sleep_period) {
    return hm.sleep_period_ < hm.state_timer_.getCurrentValue()
        ? NodeHealthStatus::UNHEALTHY
        : hm.overloaded_ ? NodeHealthStatus::OVERLOADED
                         : NodeHealthStatus::HEALTHY;
  }

  void simulateLoop(HealthMonitor& hm, HealthMonitor::TimePoint now) {
    hm.updateVariables(now);
    hm.calculateNegativeSignal(now);
    hm.node_status_ = calculateHealthMonitorState(hm, kExtraLongLoopDuration);
  }

  void stateTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<HealthMonitor> hm =
        createHM(inlineExecutor, kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    hm->updateVariables(now);
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 1
    now += kExtraLongLoopDuration;
    auto start_time =
        now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // report stalls
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    hm->internal_info_.worker_stalls_[1].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 2
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 - 1) * kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 3
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 * (3 - 1) - 1) * kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 4
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(3 * (3 * (3 * (3 - 1) - 1) - 1) * kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    // fake loop 5
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(
        (3 * (3 * (3 * (3 - 1) - 1) - 1) - 1) * kExtraLongLoopDuration.count(),
        hm->state_timer_.getCurrentValue().count());
    // fake loop 6-47
    now += 42 * kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
  }

  void overloadTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<HealthMonitor> hm =
        createHM(inlineExecutor, kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    hm->updateVariables(now);
    hm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 1
    now += kExtraLongLoopDuration;
    auto start_time =
        now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // report stalls
    hm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    hm->internal_info_.worker_queue_stalls_[1].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 2
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 3
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 4
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 5
    now += kExtraLongLoopDuration;
    start_time = now - HealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*hm, now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(kExtraLongLoopDuration.count(),
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
    std::unique_ptr<HealthMonitor> hm =
        createHM(inlineExecutor, kLongLoopDuration);
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

TEST_F(HealthMonitorTest, StateTest) {
  stateTest();
}

TEST_F(HealthMonitorTest, OverloadTest) {
  overloadTest();
}
TEST_F(HealthMonitorTest, WatchdogDelayTest) {
  watchdogDelayTest();
}

}} // namespace facebook::logdevice
