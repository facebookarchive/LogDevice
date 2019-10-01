/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LibeventTimer.h"

#include <cstring>

#include "logdevice/common/TimeoutMap.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

LibeventTimer::LibeventTimer() {}

LibeventTimer::LibeventTimer(EvBase* base) : LibeventTimer(base, nullptr) {}

LibeventTimer::LibeventTimer(EvBase* base, std::function<void()> callback) {
  assign(base, callback);
}

LibeventTimer::~LibeventTimer() {
  cancel();
}

void LibeventTimer::assign(EvBase* base, std::function<void()> callback) {
  callback_ = callback;

  worker_ = Worker::onThisThread(false /*enforce*/);
  ld_check(!isAssigned());
  timer_ = std::make_unique<EvTimer>(base);
  timer_->attachCallback([&] { libeventCallback(); });

  ld_assert(!timer_->isScheduled());
}

void LibeventTimer::activate(std::chrono::microseconds delay,
                             TimeoutMap* /* timeout_map */) {
  ld_check(isAssigned());
  ld_check(callback_);
  timer_->scheduleTimeout(
      std::chrono::duration_cast<std::chrono::milliseconds>(delay));
  auto w = Worker::onThisThread(false /*enforce*/);
  workerRunContext_ = w ? w->currentlyRunning_ : RunContext();
}

void LibeventTimer::cancel() {
  if (isActive()) {
    ld_check(isAssigned());
    timer_->cancelTimeout();
  }
}

bool LibeventTimer::isActive() const {
  return isAssigned() && timer_->isScheduled();
}

bool LibeventTimer::isAssigned() const {
  return timer_ != nullptr;
}

void LibeventTimer::libeventCallback() {
  ld_assert(!timer_->isScheduled());

  RunContext run_context = workerRunContext_;
  if (!ThreadID::isEventLoop()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "LibeventTimer not used on a EventLoop, timer source: %s",
                    run_context.describe().c_str());
  }

  ld_check(callback_);
  if (worker_) {
    WorkerContextScopeGuard g(worker_);
    worker_->onStartedRunning(run_context);
    callback_();
    worker_->onStoppedRunning(run_context);
  } else {
    callback_();
  }
}

}} // namespace facebook::logdevice
