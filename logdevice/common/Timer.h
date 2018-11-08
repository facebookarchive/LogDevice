/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>

#include "logdevice/common/TimerInterface.h"

/**
 * @file
 * Convenience class for a timer that runs on libevent.
 *
 * This class is not thread-safe in general.  All operations (including
 * construction, and destruction if the timer is active) should be mode
 * on a single thread.
 */

namespace facebook { namespace logdevice {

/**
 * All methods and ctors should be called from the Worker which is going
 * to use Timer, exception is default ctor which provides delayed init.
 */
class Timer : public TimerInterface {
 public:
  Timer();

  explicit Timer(std::function<void()> callback);

  virtual void activate(std::chrono::microseconds delay,
                        TimeoutMap* timeout_map = nullptr) override {
    impl_->activate(delay, timeout_map);
  }

  virtual void cancel() override {
    getTimerImpl().cancel();
  }

  virtual bool isActive() const override {
    return getTimerImpl().isActive();
  }

  virtual void setCallback(std::function<void()> callback) override {
    getTimerImpl().setCallback(callback);
  }

  virtual void assign(std::function<void()> callback) override {
    getTimerImpl().assign(callback);
  }

  virtual bool isAssigned() const override {
    return getTimerImpl().isAssigned();
  }

 private:
  TimerInterface& getTimerImpl() const;
  mutable std::unique_ptr<TimerInterface> impl_;
};

}} // namespace facebook::logdevice
