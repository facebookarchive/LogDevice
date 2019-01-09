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
#include <folly/io/async/EventBaseManager.h>
#include <folly/synchronization/LifoSem.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

namespace {
struct WheelTimerTag {};
constexpr long kMaxTimeoutMs = 100L * 366 * 24 * 3600 * 1000;

class Callback : public folly::HHWheelTimer::Callback {
 public:
  friend Callback* createCallback(folly::Function<void()>&& callback);

  virtual void timeoutExpired() noexcept override {
    if (canceled_.load()) {
      return;
    }

    callback_();
    delete this;
  }

  virtual void callbackCanceled() noexcept override {
    delete this;
  }

 private:
  explicit Callback(folly::Function<void()>&& callback)
      : callback_(std::move(callback)) {}

  ~Callback() = default;

  std::atomic<bool> canceled_{false};
  folly::Function<void()> callback_;
};

Callback* createCallback(folly::Function<void()>&& callback) {
  return new Callback(std::move(callback));
}

constexpr auto kDefaultTickInterval = std::chrono::milliseconds(1);

} // namespace

WheelTimer::WheelTimer() {
  folly::Promise<folly::Unit> promise;
  auto ready = promise.getSemiFuture();

  timer_thread_ = std::thread([this, promise = std::move(promise)]() mutable {
    executor_.store(folly::EventBaseManager::get()->getEventBase());
    wheel_timer_ = folly::HHWheelTimer::newTimer(
        folly::EventBaseManager::get()->getEventBase(), kDefaultTickInterval);
    promise.setValue();
    executor_.load()->loopForever();
  });

  ready.wait();
}

WheelTimer::~WheelTimer() {
  executor_.load()->terminateLoopSoon();
  timer_thread_.join();
}

void WheelTimer::createTimer(folly::Function<void()>&& callback,
                             std::chrono::milliseconds timeout) {
  timeout = std::chrono::milliseconds(std::min(timeout.count(), kMaxTimeoutMs));
  executor_.load()->add(
      [callback = std::move(callback), timeout, this]() mutable {
        wheel_timer_->scheduleTimeout(
            createCallback(std::move(callback)), timeout);
      });
}

}} // namespace facebook::logdevice
