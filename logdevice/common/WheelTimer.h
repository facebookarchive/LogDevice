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
#include "folly/io/async/DelayedDestruction.h"
#include "folly/io/async/HHWheelTimer-fwd.h"

namespace folly {
class EventBase;
} // namespace folly

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

  constexpr int static kNumberOfThreads = 1;

  std::unique_ptr<folly::EventBase> executor_;
  std::unique_ptr<folly::HHWheelTimer, folly::DelayedDestruction::Destructor>
      wheel_timer_;
  std::thread timer_thread_;
};

}} // namespace facebook::logdevice
