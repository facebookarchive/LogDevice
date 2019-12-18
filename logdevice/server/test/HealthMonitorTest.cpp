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
#include "logdevice/server/test/MockHealthMonitor.h"

namespace facebook { namespace logdevice {
class HealthMonitorTest : public testing::Test {
 public:
  explicit HealthMonitorTest() {}

  void checkVectors(const std::vector<WorkerTimeSeriesStats>& left,
                    const std::vector<WorkerTimeSeriesStats>& right) {
    EXPECT_EQ(left.size(), right.size());
    if (left.size() != right.size()) {
      return;
    }
    auto lit = left.begin();
    auto rit = right.begin();
    while (lit != left.end() && rit != right.end()) {
      EXPECT_EQ((*lit).worker_id, (*rit).worker_id);
      EXPECT_EQ((*lit).curr_num, (*rit).curr_num);
      EXPECT_EQ((*lit).curr_sum, (*rit).curr_sum);
      EXPECT_EQ((*lit).curr_avg, (*rit).curr_avg);
      ++lit;
      ++rit;
    }
  }

  void checkNodeHealthStatsPair(const NodeHealthStats& left,
                                const NodeHealthStats& right) {
    EXPECT_EQ(left.observed_time_period_ms, right.observed_time_period_ms);
    EXPECT_EQ(left.health_monitor_delayed, right.health_monitor_delayed);
    EXPECT_EQ(left.watchdog_delayed, right.watchdog_delayed);
    EXPECT_EQ(
        left.curr_total_stalled_workers, right.curr_total_stalled_workers);
    EXPECT_EQ(left.percent_workers_w_long_exe_req,
              right.percent_workers_w_long_exe_req);
    checkVectors(left.stalled_worker_stats, right.stalled_worker_stats);
    EXPECT_EQ(left.percent_workers_w_long_queued_req,
              right.percent_workers_w_long_queued_req);
    checkVectors(left.overloaded_worker_stats, right.overloaded_worker_stats);
    EXPECT_EQ(
        left.curr_num_conn_req_rejected, right.curr_num_conn_req_rejected);
    // very hard to predict -- ?
    // EXPECT_EQ(left.curr_state_timer_value_ms,
    // right.curr_state_timer_value_ms);
  }
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
    checkNodeHealthStatsPair(NodeHealthStats(), hm->getNodeHealthStats());
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
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
    NodeHealthStats stats;
    stats.health_monitor_delayed = true;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kLongLoopDuration.count();
    EXPECT_EQ(true, hm->internal_info_.health_monitor_delay_);
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
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
    NodeHealthStats stats;
    stats.curr_total_stalled_workers = MockHealthMonitor::kStalledWorkers;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kLoopDuration.count();
    EXPECT_EQ(2, hm->internal_info_.total_stalled_workers);
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
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
    NodeHealthStats stats;
    stats.watchdog_delayed = true;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kLoopDuration.count();
    EXPECT_EQ(true, hm->internal_info_.watchdog_delay_);
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
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
    NodeHealthStats stats;
    stats.curr_num_conn_req_rejected = 1;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kLoopDuration.count();
    EXPECT_EQ(1, hm->internal_info_.connection_limit_reached_.count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
  }

  void stateTest() {
    const std::chrono::milliseconds delay_time{250};
    auto& inlineExecutor = folly::InlineExecutor::instance();
    std::unique_ptr<MockHealthMonitor> hm = std::make_unique<MockHealthMonitor>(
        inlineExecutor, MockHealthMonitor::kExtraLongLoopDuration);
    NodeHealthStats stats;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kExtraLongLoopDuration.count();
    auto now = SteadyTimestamp::now();
    hm->updateVariables(now);
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 1
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // report stalls
    hm->internal_info_.worker_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    hm->internal_info_.worker_stalls_[1].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 2
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // Set expected stats
    stats.percent_workers_w_long_exe_req = 1;
    stats.stalled_worker_stats.resize(2);
    stats.stalled_worker_stats[0].worker_id = 0;
    stats.stalled_worker_stats[0].curr_num = 2;
    stats.stalled_worker_stats[0].curr_sum =
        2 * MockHealthMonitor::kWorkerStallDuration.count();
    stats.stalled_worker_stats[0].curr_avg =
        2 * MockHealthMonitor::kWorkerStallDuration.count() / 2;
    stats.stalled_worker_stats[1].worker_id = 1;
    stats.stalled_worker_stats[1].curr_num = 1;
    stats.stalled_worker_stats[1].curr_sum =
        MockHealthMonitor::kWorkerStallDuration.count();
    stats.stalled_worker_stats[1].curr_avg =
        MockHealthMonitor::kWorkerStallDuration.count();

    auto expected_timer_mul = 3 * (3 - 1);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->state_timer_.getCurrentValue().count());
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->getNodeHealthStats().curr_state_timer_value_ms);
    // fake loop 3
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    expected_timer_mul = 3 * (expected_timer_mul - 1);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->state_timer_.getCurrentValue().count());
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->getNodeHealthStats().curr_state_timer_value_ms);
    // fake loop 4
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    expected_timer_mul = 3 * (expected_timer_mul - 1);
    // Set expected stats
    stats.stalled_worker_stats[0].curr_num = 1;
    stats.stalled_worker_stats[0].curr_sum =
        MockHealthMonitor::kWorkerStallDuration.count();
    stats.stalled_worker_stats[0].curr_avg =
        MockHealthMonitor::kWorkerStallDuration.count();
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->state_timer_.getCurrentValue().count());
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->getNodeHealthStats().curr_state_timer_value_ms);
    // fake loop 5
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    expected_timer_mul = (expected_timer_mul - 1);
    // Set expected stats
    stats.percent_workers_w_long_exe_req = 0;
    stats.stalled_worker_stats.resize(0);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::UNHEALTHY, hm->node_status_);
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->state_timer_.getCurrentValue().count());
    EXPECT_EQ(
        expected_timer_mul * MockHealthMonitor::kExtraLongLoopDuration.count(),
        hm->getNodeHealthStats().curr_state_timer_value_ms);
    // fake loop 6-47
    now += 42 * MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[0].count());
    EXPECT_EQ(0, hm->internal_info_.worker_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
    EXPECT_EQ(MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->getNodeHealthStats().curr_state_timer_value_ms);
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
    hm->simulateLoop(now);
    // report stalls
    hm->internal_info_.worker_queue_stalls_[0].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    hm->internal_info_.worker_queue_stalls_[1].addValue(
        now + delay_time, MockHealthMonitor::kWorkerStallDuration);
    // fake loop 2
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // Set expected stats
    NodeHealthStats stats;
    stats.observed_time_period_ms = MockHealthMonitor::kPeriodRange *
        MockHealthMonitor::kExtraLongLoopDuration.count();
    stats.percent_workers_w_long_queued_req = 1;
    stats.overloaded_worker_stats.resize(2);
    stats.overloaded_worker_stats[0].worker_id = 0;
    stats.overloaded_worker_stats[0].curr_num = 2;
    stats.overloaded_worker_stats[0].curr_sum =
        2 * MockHealthMonitor::kWorkerStallDuration.count();
    stats.overloaded_worker_stats[0].curr_avg =
        2 * MockHealthMonitor::kWorkerStallDuration.count() / 2;
    stats.overloaded_worker_stats[1].worker_id = 1;
    stats.overloaded_worker_stats[1].curr_num = 1;
    stats.overloaded_worker_stats[1].curr_sum =
        MockHealthMonitor::kWorkerStallDuration.count();
    stats.overloaded_worker_stats[1].curr_avg =
        MockHealthMonitor::kWorkerStallDuration.count();
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 3
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 4
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // Set expected stats
    stats.overloaded_worker_stats[0].curr_num = 1;
    stats.overloaded_worker_stats[0].curr_sum =
        MockHealthMonitor::kWorkerStallDuration.count();
    stats.overloaded_worker_stats[0].curr_avg =
        MockHealthMonitor::kWorkerStallDuration.count();
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(true, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::OVERLOADED, hm->node_status_);
    // fake loop 5
    now += MockHealthMonitor::kExtraLongLoopDuration;
    hm->simulateLoop(now);
    // Set expected stats
    stats.percent_workers_w_long_queued_req = 0;
    stats.overloaded_worker_stats.resize(0);
    // check values
    EXPECT_EQ(2, hm->internal_info_.worker_queue_stalls_[0].count());
    EXPECT_EQ(1, hm->internal_info_.worker_queue_stalls_[1].count());
    checkNodeHealthStatsPair(stats, hm->getNodeHealthStats());
    EXPECT_EQ(false, hm->overloaded_);
    EXPECT_EQ(NodeHealthStatus::HEALTHY, hm->node_status_);
    EXPECT_EQ(MockHealthMonitor::kExtraLongLoopDuration.count(),
              hm->state_timer_.getCurrentValue().count());
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
