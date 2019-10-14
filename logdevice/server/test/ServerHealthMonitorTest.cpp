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

  explicit ServerHealthMonitorTest() {}
  // Tests case for when no changes are reported to the health monitor.
  // Waits for HM shutdown.
  std::unique_ptr<ServerHealthMonitor>
  createHM(folly::InlineExecutor executor,
           const std::chrono::milliseconds loop_duration) {
    return std::make_unique<ServerHealthMonitor>(executor,
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
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kLoopDuration);
    shm->startUp();
    auto health_monitor_closed = shm->shutdown();
    health_monitor_closed.wait();
    EXPECT_EQ(false, shm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, shm->internal_info_.health_monitor_delay_);
    EXPECT_EQ(0, shm->internal_info_.total_stalled_workers);
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(0, shm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(0, shm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(false, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::HEALTHY, shm->node_state_);
  }

  void totalStalledWorkersTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kLoopDuration);
    shm->startUp();
    shm->reportStalledWorkers(kStalledWorkers);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(3 * kLoopDuration))
            .via(&inlineExecutor)
            .then([&shm](folly::Try<folly::Unit>) mutable {
              return shm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(2, shm->internal_info_.total_stalled_workers);
    EXPECT_EQ(false, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
  }

  void watchdogDelayTest() {
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kLoopDuration);
    shm->startUp();
    shm->reportWatchdogHealth(/*delayed = */ true);
    auto health_monitor_closed =
        folly::futures::sleep(std::chrono::milliseconds(2 * kLoopDuration))
            .via(&inlineExecutor)
            .then([&shm](folly::Try<folly::Unit>) mutable {
              return shm->shutdown();
            });

    health_monitor_closed.wait();
    EXPECT_EQ(true, shm->internal_info_.watchdog_delay_);
    EXPECT_EQ(false, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
  }

  ServerHealthMonitor::NodeState
  calculateHealthMonitorState(ServerHealthMonitor& shm,
                              std::chrono::milliseconds sleep_period) {
    return shm.sleep_period_ < shm.state_timer_.getCurrentValue()
        ? ServerHealthMonitor::NodeState::UNHEALTHY
        : shm.overloaded_ ? ServerHealthMonitor::NodeState::OVERLOADED
                          : ServerHealthMonitor::NodeState::HEALTHY;
  }

  void simulateLoop(ServerHealthMonitor& shm,
                    ServerHealthMonitor::TimePoint now,
                    ServerHealthMonitor::TimePoint start_time,
                    ServerHealthMonitor::TimePoint end_time) {
    shm.updateVariables(now);
    shm.calculateNegativeSignal(now);
    shm.node_state_ = calculateHealthMonitorState(shm, kExtraLongLoopDuration);
  }

  void stateTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    shm->updateVariables(now);
    shm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 1
    now += kExtraLongLoopDuration;
    auto start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // report stalls
    shm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    shm->internal_info_.worker_stalls_[1].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 2
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
    EXPECT_EQ(3 * (3 - 1) * kExtraLongLoopDuration.count(),
              shm->state_timer_.getCurrentValue().count());
    // fake loop 3
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
    EXPECT_EQ(3 * (3 * (3 - 1) - 1) * kExtraLongLoopDuration.count(),
              shm->state_timer_.getCurrentValue().count());
    // fake loop 4
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
    EXPECT_EQ(3 * (3 * (3 * (3 - 1) - 1) - 1) * kExtraLongLoopDuration.count(),
              shm->state_timer_.getCurrentValue().count());
    // fake loop 5
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
    EXPECT_EQ(
        (3 * (3 * (3 * (3 - 1) - 1) - 1) - 1) * kExtraLongLoopDuration.count(),
        shm->state_timer_.getCurrentValue().count());
    // fake loop 6-47
    now += 42 * kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, shm->internal_info_.worker_stalls_[1].count());
    EXPECT_EQ(ServerHealthMonitor::NodeState::HEALTHY, shm->node_state_);
    EXPECT_EQ(kExtraLongLoopDuration.count(),
              shm->state_timer_.getCurrentValue().count());
  }

  void overloadTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kExtraLongLoopDuration);
    auto now = SteadyTimestamp::now();
    shm->updateVariables(now);
    shm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 1
    now += kExtraLongLoopDuration;
    auto start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    auto end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // report stalls
    shm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, kWorkerStallDuration);
    shm->internal_info_.worker_queue_stalls_[1].addValue(
        now + delay_time, kWorkerStallDuration);
    // fake loop 2
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::OVERLOADED, shm->node_state_);
    // fake loop 3
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::OVERLOADED, shm->node_state_);
    // fake loop 4
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(true, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::OVERLOADED, shm->node_state_);
    // fake loop 5
    now += kExtraLongLoopDuration;
    start_time =
        now - ServerHealthMonitor::kPeriodRange * kExtraLongLoopDuration;
    end_time = now + std::chrono::nanoseconds(1);
    simulateLoop(*shm, now, start_time, end_time);
    // check values
    EXPECT_EQ(2, shm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, shm->internal_info_.worker_queue_stalls_[1].count());
    EXPECT_EQ(false, shm->overloaded_);
    EXPECT_EQ(ServerHealthMonitor::NodeState::HEALTHY, shm->node_state_);
    EXPECT_EQ(kExtraLongLoopDuration.count(),
              shm->state_timer_.getCurrentValue().count());
  }
  // Tests case for when no changes are reported to the health monitor, but
  // health monitor itself is delayed
  // Waits for HM shutdown.
  void healthMonitorDelayTest() {
    const std::chrono::milliseconds wake_up{150};
    const std::chrono::milliseconds offset{40};
    const int wait_ticks{80};

    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<ServerHealthMonitor> shm =
        createHM(inlineExecutor, kLongLoopDuration);
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
    EXPECT_EQ(ServerHealthMonitor::NodeState::UNHEALTHY, shm->node_state_);
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

TEST_F(ServerHealthMonitorTest, StateTest) {
  stateTest();
}

TEST_F(ServerHealthMonitorTest, OverloadTest) {
  overloadTest();
}
TEST_F(ServerHealthMonitorTest, WatchdogDelayTest) {
  watchdogDelayTest();
}

}} // namespace facebook::logdevice
