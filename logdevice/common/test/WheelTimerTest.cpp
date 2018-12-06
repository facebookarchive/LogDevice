/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WheelTimer.h"

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
