/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EventLoop.h"

#include <errno.h>
#include <unistd.h>

#include <event2/event.h>
#include <folly/Memory.h>
#include <folly/container/Array.h>
#include <folly/io/async/Request.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

__thread EventLoop* EventLoop::thisThreadLoop;

static struct event_base* createEventBase() {
  int rv;
  struct event_base* base = LD_EV(event_base_new)();

  if (!base) {
    ld_error("Failed to create an event base for an EventLoop thread");
    err = E::NOMEM;
    return nullptr;
  }

  rv = LD_EV(event_base_priority_init)(base, EventLoop::NUM_PRIORITIES);
  if (rv != 0) { // unlikely
    ld_error("event_base_priority_init() failed");
    err = E::SYSLIMIT;
    LD_EV(event_base_free)(base);
    return nullptr;
  }

  return base;
}

static void deleteEventBase(struct event_base* base) {
  if (base) {
    // libevent-2.1 does not destroy bufferevents when bufferevent_free() is
    // called.  Instead it schedules a callback to be run at the next
    // iteration of event loop.  Run that iteration now.
    LD_EV(event_base_loop)(base, EVLOOP_ONCE | EVLOOP_NONBLOCK);

    LD_EV(event_base_free)(base);
  }
}

EventLoop::EventLoop(std::string thread_name, ThreadID::Type thread_type)
    : base_(createEventBase(), deleteEventBase),
      thread_type_(thread_type),
      thread_name_(thread_name),
      thread_(pthread_self()), // placeholder serving as "invalid" value
      running_(false),
      shutting_down_(false),
      disposer_(this),
      common_timeouts_(base_.get(), kMaxFastTimeouts) {
  int rv;
  pthread_attr_t attr;

  if (!base_) {
    // logdevice::err was set by createEventBase()
    throw ConstructorFailed();
  }

  ld_check(common_timeouts_.get(std::chrono::milliseconds(0)));

  scheduled_event_ = LD_EV(event_new)(
      base_.get(), -1, 0, EventHandler<EventLoop::delayCheckCallback>, this);
  if (!scheduled_event_) {
    ld_error("Failed to initialize scheduled event.");
    throw ConstructorFailed();
  }

  rv = pthread_attr_init(&attr);
  if (rv != 0) { // unlikely
    ld_check(rv == ENOMEM);
    ld_error("Failed to initialize a pthread attributes struct");
    err = E::NOMEM;
    throw ConstructorFailed();
  }

  rv = pthread_attr_setstacksize(&attr, EventLoop::STACK_SIZE);
  if (rv != 0) {
    ld_check(rv == EINVAL);
    ld_error("Failed to set stack size for an EventLoop thread. "
             "Stack size %lu is out of range",
             EventLoop::STACK_SIZE);
    err = E::SYSLIMIT;
    throw ConstructorFailed();
  }

  rv = pthread_create(&thread_, &attr, EventLoop::enter, this);
  if (rv != 0) {
    ld_error(
        "Failed to start an EventLoop thread, errno=%d (%s)", rv, strerror(rv));
    err = E::SYSLIMIT;
    throw ConstructorFailed();
  }
}

EventLoop::~EventLoop() {
  // Shutdown drains all the work contexts before invoking this destructor.
  ld_check(num_references_.load() == 0);
  LD_EV(event_free)(scheduled_event_);

  if (running_) {
    // start() was already invoked, it's EventLoopHandle's responsibility to
    // destroy the object now (by stopping the event loop)
    return;
  }

  // force thread_ to exit
  shutting_down_ = true;
  start_sem_.post();

  pthread_join(thread_, nullptr);
  // Shutdown drains all the work contexts before invoking this destructor.

  if (event_handlers_called_.load() != event_handlers_completed_.load()) {
    ld_info("EventHandlers called: %lu, EventHandlers completed: %lu",
            event_handlers_called_.load(),
            event_handlers_completed_.load());
  }
}

void EventLoop::add(folly::Function<void()> func) {
  addWithPriority(std::move(func), folly::Executor::LO_PRI);
}

void EventLoop::addWithPriority(folly::Function<void()> func, int8_t priority) {
  task_queue_->addWithPriority(std::move(func), priority);
}

void EventLoop::start() {
  start_sem_.post();
}

void* EventLoop::enter(void* self) {
  reinterpret_cast<EventLoop*>(self)->run();
  return nullptr;
}

void EventLoop::delayCheckCallback(void* arg, short) {
  EventLoop* self = (EventLoop*)arg;
  using namespace std::chrono;
  using namespace std::chrono_literals;
  auto now = steady_clock::now();
  if (self->scheduled_event_start_time_ != steady_clock::time_point::min()) {
    evtimer_add(self->scheduled_event_, self->getCommonTimeout(1s));
    if (now > self->scheduled_event_start_time_) {
      auto diff = now - self->scheduled_event_start_time_;
      auto cur_delay = duration_cast<microseconds>(diff);
      self->delay_us_.store(cur_delay);
    }
    self->scheduled_event_start_time_ = steady_clock::time_point::min();
  } else {
    evtimer_add(self->scheduled_event_, self->getZeroTimeout());
    self->scheduled_event_start_time_ = now;
  }
}

void EventLoop::run() {
  int rv;
  tid_ = syscall(__NR_gettid);

  // Wait for EventLoopHandle to give us the go-ahead
  start_sem_.wait();

  if (shutting_down_) {
    // this EventLoop is being destroyed, stop immediately
    return;
  }

  running_ = true;

  ThreadID::set(thread_type_, thread_name_);

  EventLoop::thisThreadLoop = this; // save in a thread-local

  // Initiate runs to detect eventloop delays.
  using namespace std::chrono_literals;
  delay_us_.store(std::chrono::milliseconds(0));
  scheduled_event_start_time_ = std::chrono::steady_clock::time_point::min();
  evtimer_add(scheduled_event_, getCommonTimeout(1s));
  onThreadStarted();

  ld_check(base_);
  // this runs until our EventLoopHandle closes its end of the pipe or there
  // is a fatal error
  rv = LD_EV(event_base_loop)(base_.get(), 0);
  if (rv != 0) {
    ld_error("event_base_loop() exited abnormally with return value %d.", rv);
  }
  ld_check_ge(rv, 0);

  // the thread on which this EventLoop ran terminates here
}

void EventLoop::dispose(ZeroCopyPayload* payload) {
  disposer_.dispose(payload);
}
}} // namespace facebook::logdevice
