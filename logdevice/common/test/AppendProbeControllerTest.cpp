/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AppendProbeController.h"

#include <chrono>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"

using namespace facebook::logdevice;

class TestAppendProbeController : public AppendProbeController {
 public:
  using TimePoint = AppendProbeController::TimePoint;
  TestAppendProbeController(std::chrono::milliseconds recovery_interval,
                            std::function<TimePoint()> time_cb)
      : AppendProbeController(recovery_interval), time_cb_(time_cb) {}

 protected:
  TimePoint now() const override {
    return TimePoint(time_cb_());
  }

 private:
  std::function<TimePoint()> time_cb_;
};

TEST(AppendProbeControllerTest, Basic) {
  std::chrono::milliseconds t(0);
  auto time_cb = [&]() { return TestAppendProbeController::TimePoint(t); };
  TestAppendProbeController controller(std::chrono::seconds(1), time_cb);

  const NodeID N1(1, 1), N2(2, 1);
  const logid_t LOG_ID(1);

  // No probes initially
  ASSERT_FALSE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=100ms: append fails
  t = std::chrono::milliseconds(100);
  controller.onAppendReply(N1, LOG_ID, E::SEQNOBUFS);
  // t=100ms: append to N1 should probe
  ASSERT_TRUE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=150ms: still should probe
  t = std::chrono::milliseconds(150);
  ASSERT_TRUE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=200ms: append succeeded, still should probe
  t = std::chrono::milliseconds(200);
  controller.onAppendReply(N1, LOG_ID, E::OK);
  ASSERT_TRUE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=400ms: failure and success should reset recovery interval
  t = std::chrono::milliseconds(400);
  controller.onAppendReply(N1, LOG_ID, E::SEQNOBUFS);
  controller.onAppendReply(N1, LOG_ID, E::OK);
  ASSERT_TRUE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=1399ms: still should probe (recovery interval 1s)
  t = std::chrono::milliseconds(1399);
  ASSERT_TRUE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));

  // t=1400ms: recovery interval elapsed, stop probing
  t = std::chrono::milliseconds(1400);
  ASSERT_FALSE(controller.shouldProbe(N1, LOG_ID));
  ASSERT_FALSE(controller.shouldProbe(N2, LOG_ID));
}

// Test that nothing crashes or locks up under stress
TEST(AppendProbeControllerTest, MultiThreadedStressTest) {
  // dbg::currentLevel = dbg::Level::DEBUG;
  using namespace std::chrono;
  const duration<double> TEST_DURATION(0.2);
  const seconds RECOVERY_INTERVAL(1);
  const int NNODES = 10;
  auto random_node = [] {
    node_index_t node_index = folly::Random::rand32(1, NNODES + 1);
    return NodeID(node_index, 1);
  };
  const int NTHREADS = 16;
  std::atomic<int> time_ms(0);
  auto time_cb = [&]() {
    // Advance time occasionally
    if (folly::Random::oneIn(10000)) {
      time_ms += duration_cast<milliseconds>(RECOVERY_INTERVAL).count();
    } else if (folly::Random::oneIn(100)) {
      ++time_ms;
    }
    return TestAppendProbeController::TimePoint(milliseconds(time_ms.load()));
  };
  TestAppendProbeController controller(RECOVERY_INTERVAL, time_cb);
  const auto deadline = steady_clock::now() + TEST_DURATION;
  auto client_thread_fn = [&]() {
    for (int64_t i = 0;; ++i) {
      if (i % 1024 == 0 && steady_clock::now() >= deadline) {
        fprintf(stderr, "Client thread exiting after %ld calls\n", i);
        break;
      }
      bool failure = folly::Random::oneIn(100);
      controller.onAppendReply(
          random_node(), logid_t(1), failure ? E::SEQNOBUFS : E::OK);
      controller.shouldProbe(random_node(), logid_t(1));
    }
  };

  std::vector<std::thread> threads;
  while (threads.size() < NTHREADS) {
    threads.emplace_back(client_thread_fn);
  }
  for (auto& th : threads) {
    th.join();
  }
}
