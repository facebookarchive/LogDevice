/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "LibeventTimer.h"

#include <cstring>

#include "logdevice/common/debug.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/TimeoutMap.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

LibeventTimer::LibeventTimer() {
  memset(&timer_, 0, sizeof(timer_));
}

LibeventTimer::LibeventTimer(struct event_base* base)
    : LibeventTimer(base, nullptr) {}

LibeventTimer::LibeventTimer(struct event_base* base,
                             std::function<void()> callback) {
  assign(base, callback);
}

LibeventTimer::~LibeventTimer() {
  cancel();
}

void LibeventTimer::assign(struct event_base* base,
                           std::function<void()> callback) {
  callback_ = callback;
  ld_check(!initialized_);
  // Passing `this` as the callback arg is safe.  If the timer fires, we know
  // the instance still exists.  The destructor would have cancelled the timer
  // otherwise.
  int rv = evtimer_assign(&timer_,
                          base,
                          (EventHandler<LibeventTimer::libeventCallback>),
                          reinterpret_cast<void*>(this));
  ld_check(rv == 0);
  ld_assert(!evtimer_pending(&timer_, nullptr));

  initialized_ = true;
}

void LibeventTimer::activate(std::chrono::microseconds delay,
                             TimeoutMap* timeout_map) {
  struct timeval tv_buf;
  const struct timeval* tv;

  if (timeout_map != nullptr) {
    tv = timeout_map->get(delay, &tv_buf);
  } else {
    tv_buf.tv_sec = delay.count() / 1000000;
    tv_buf.tv_usec = delay.count() % 1000000;
    tv = &tv_buf;
  }
  activate(tv);
}

void LibeventTimer::activate(const struct timeval* delay) {
  if (!ThreadID::isEventLoop()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1), 5, "LibeventTimer used outside event loop");
  }

  ld_check(initialized_);
  ld_check(callback_);
  evtimer_add(&timer_, delay);
  ld_assert(evtimer_pending(&timer_, nullptr));

  Worker* w = Worker::onThisThread(false);
  workerRunState_ = w ? w->currentlyRunning_ : RunState();
  active_ = true;
}

void LibeventTimer::cancel() {
  if (isActive()) {
    ld_check(initialized_);
    ld_assert(evtimer_pending(&timer_, nullptr));
    evtimer_del(&timer_);
    active_ = false;
  }
}

bool LibeventTimer::isActive() const {
  return active_;
}

bool LibeventTimer::isAssigned() const {
  ld_assert(initialized_ == LD_EV(event_initialized)(&timer_));
  return initialized_;
}

void LibeventTimer::libeventCallback(void* instance, short) {
  auto self = reinterpret_cast<LibeventTimer*>(instance);
  ld_assert(!evtimer_pending(&self->timer_, nullptr));
  self->active_ = false;

  RunState run_state = self->workerRunState_;

  if (!ThreadID::isEventLoop()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "LibeventTimer not used on a worker, timer source: %s",
                    run_state.describe().c_str());
  }

  Worker* w = Worker::onThisThread(false);
  if (w) {
    Worker::onStartedRunning(run_state);
  }
  ld_check(self->callback_);
  self->callback_(); // self could be destroyed after this
  if (w) {
    Worker::onStoppedRunning(run_state);
  }
}

}} // namespace facebook::logdevice
