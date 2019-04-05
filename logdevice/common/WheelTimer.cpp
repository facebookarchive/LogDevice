/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WheelTimer.h"

#include <memory>

#include <folly/IntrusiveList.h>
#include <folly/futures/Future.h>
#include <folly/futures/Promise.h>
#include <folly/io/async/EventBase.h>
#include <folly/synchronization/LifoSem.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

namespace {
struct WheelTimerTag {};
constexpr long kMaxTimeoutMs = 100L * 366 * 24 * 3600 * 1000;

class Callback : public folly::HHWheelTimer::Callback {
 public:
  friend Callback* createCallback(folly::Function<void()>&& callback);

  void timeoutExpired() noexcept override {
    callback_();
    delete this;
  }

  void callbackCanceled() noexcept override {
    delete this;
  }

 private:
  explicit Callback(folly::Function<void()>&& callback)
      : callback_(std::move(callback)) {}

  ~Callback() override = default;
  folly::Function<void()> callback_;
};

Callback* createCallback(folly::Function<void()>&& callback) {
  return new Callback(std::move(callback));
}

constexpr auto kDefaultTickInterval = std::chrono::milliseconds(1);

} // namespace

WheelTimer::WheelTimer() : executor_(std::make_unique<folly::EventBase>()) {
  wheel_timer_ =
      folly::HHWheelTimer::newTimer(executor_.get(), kDefaultTickInterval);
  folly::Promise<folly::Unit> promise;
  auto ready = promise.getSemiFuture();
  timer_thread_ = std::thread([this, promise = std::move(promise)]() mutable {
    ThreadID::set(ThreadID::Type::WHEEL_TIMER, "ld:wheel_timer");
    promise.setValue();
    executor_->loopForever();
  });

  ready.wait();
}

WheelTimer::~WheelTimer() {
  shutdown();
}

void WheelTimer::shutdown() {
  if (shutdown_.exchange(true)) {
    return;
  }
  executor_->terminateLoopSoon();
  timer_thread_.join();
}

void WheelTimer::createTimer(folly::Function<void()>&& callback,
                             std::chrono::milliseconds timeout) {
  if (shutdown_.load()) {
    return;
  }

  timeout = std::chrono::milliseconds(std::min(timeout.count(), kMaxTimeoutMs));
  executor_->add([callback = std::move(callback), this, timeout]() mutable {
    wheel_timer_->scheduleTimeout(createCallback(std::move(callback)), timeout);
  });
}

}} // namespace facebook::logdevice
