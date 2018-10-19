/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "EventLoop.h"

#include <errno.h>
#include <unistd.h>

#include <folly/Memory.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestPump.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

__thread EventLoop* EventLoop::thisThreadLoop;

static const struct timeval tv_zero { 0, 0 };

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
      zero_timeout_(
          base_ ? LD_EV(event_base_init_common_timeout)(base_.get(), &tv_zero)
                : nullptr),
      thread_type_(thread_type),
      thread_name_(thread_name),
      thread_(pthread_self()), // placeholder serving as "invalid" value
      running_(false),
      shutting_down_(false) {
  int rv;
  pthread_attr_t attr;

  if (!base_) {
    // logdevice::err was set by createEventBase()
    throw ConstructorFailed();
  }

  ld_check(zero_timeout_);

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
  if (running_) {
    // start() was already invoked, it's EventLoopHandle's responsibility to
    // destroy the object now (by stopping the event loop)
    return;
  }

  // force thread_ to exit
  shutting_down_ = true;
  start_sem_.post();

  pthread_join(thread_, nullptr);
}

void EventLoop::start() {
  start_sem_.post();
}

void* EventLoop::enter(void* self) {
  reinterpret_cast<EventLoop*>(self)->run();
  return nullptr;
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
  onThreadStarted();

  ld_check(base_);
  // this runs until our EventLoopHandle closes its end of the pipe or there
  // is a fatal error
  rv = LD_EV(event_base_loop)(base_.get(), 0);
  if (rv != 0) {
    ld_error("event_base_loop() exited abnormally with return value %d.", rv);
  }
  ld_check_ge(rv, 0);

  delete this;

  // the thread on which this EventLoop ran terminates here
}

}} // namespace facebook::logdevice
