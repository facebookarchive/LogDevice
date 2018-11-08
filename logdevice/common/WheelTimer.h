/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>

#include "folly/Function.h"
#include "folly/Synchronized.h"
#include "folly/io/async/HHWheelTimer.h"

namespace folly {
class EventBase;
}

namespace facebook { namespace logdevice {

class WheelTimer {
 public:
  WheelTimer();

  // async
  void createTimer(folly::Function<void()>&& callback,
                   std::chrono::milliseconds timeout);

  ~WheelTimer();

 private:
  WheelTimer(const WheelTimer&) = delete;
  WheelTimer(WheelTimer&&) = delete;
  WheelTimer& operator=(const WheelTimer&) = delete;
  WheelTimer& operator=(WheelTimer&&) = delete;

  static folly::HHWheelTimer::UniquePtr& getWheelTimer();

  constexpr int static kNumberOfThreads = 1;
  std::thread timer_thread_;
  std::atomic<folly::EventBase*> executor_{};
};

}} // namespace facebook::logdevice
