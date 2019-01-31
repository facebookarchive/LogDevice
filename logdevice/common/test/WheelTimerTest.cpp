/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WheelTimer.h"

#include <atomic>
#include <chrono>

#include <folly/futures/Promise.h>
#include <gtest/gtest.h>

using namespace facebook::logdevice;
using namespace std::chrono_literals;
using namespace std::chrono;

TEST(WheelTimer, TimerCreation) {
  folly::Promise<folly::Unit> promise;
  auto future = promise.getSemiFuture();
  WheelTimer wheel;
  wheel.createTimer(
      [promise = std::move(promise)]() mutable { promise.setValue(); }, 10ms);

  ASSERT_EQ(std::move(future).wait(1s), true);
}

TEST(WheelTimer, Polling) {
  std::atomic<int> tries{0};
  folly::Function<void()> creator;
  {
    WheelTimer wheel;
    creator = [&tries, &wheel, &creator] {
      // i.e some call here
      tries++;
      wheel.createTimer([&creator] { creator(); }, 10ms);
    };
    creator();
    while (tries < 10) {
      std::this_thread::yield();
    }
  }
}
