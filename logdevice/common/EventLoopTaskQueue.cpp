/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EventLoopTaskQueue.h"

#include <event2/event.h>
#include <folly/Function.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

constexpr size_t EventLoopTaskQueue::kNumberOfPriorities;

EventLoopTaskQueue::EventLoopTaskQueue(struct event_base* base,
                                       size_t capacity,
                                       int dequeues_per_iteration)
    : num_hi_pri_dequeues_per_iteration_(dequeues_per_iteration),
      num_mid_pri_dequeues_per_iteration_(0),
      num_lo_pri_dequeues_per_iteration_(0),
      capacity_(capacity) {
  if (!base) {
    // err shoud be set by the function that tried to create base
    throw ConstructorFailed();
  }

  sem_waiter_ = sem_.beginAsyncWait();
  tasks_pending_event_ = LD_EV(event_new)(base,
                                          sem_waiter_->fd(),
                                          EV_READ | EV_PERSIST,
                                          EventHandler<haveTasksEventHandler>,
                                          this);

  if (!tasks_pending_event_) { // unlikely
    ld_error("Failed to create 'task pipe is readable' event for "
             "an event loop");
    err = E::NOMEM;
    throw ConstructorFailed();
  }

#if LIBEVENT_VERSION_NUMBER >= 0x02010000
  ld_assert(LD_EV(event_get_priority)(tasks_pending_event_) ==
            EventLoop::PRIORITY_NORMAL);
#endif

  int rv = LD_EV(event_add)(tasks_pending_event_, nullptr);
  if (rv != 0) { // unlikely
    ld_error("Failed to add 'task pipe is readable' event to event base");
    ld_check(false);
    LD_EV(event_free)(tasks_pending_event_);
    err = E::INTERNAL;
    throw ConstructorFailed();
  }
}

EventLoopTaskQueue::~EventLoopTaskQueue() {
  if (close_event_loop_on_shutdown_ && shutdown_signaled_) {
    // If this is responsible for shutting down the event loop and
    // shutdown() was called, then to be in the destructor we must have
    // already gone through the shutdown sequence (since the event loop
    // co-owns Taskqueue).
    ld_check(tasks_pending_event_ == nullptr);
  } else {
    // Otherwise, we may have gone through the async shutdown sequence or not.
    shutdown();
    if (tasks_pending_event_ != nullptr) {
      // If not, presumably the event loop shared ownership of the pump.  To
      // be in the destructor the event loop must have destructed itself so
      // it's safe to delete the event now.
      LD_EV(event_free)(tasks_pending_event_);
      tasks_pending_event_ = nullptr;
    }
  }
}

void EventLoopTaskQueue::setCloseEventLoopOnShutdown() {
  close_event_loop_on_shutdown_ = true;
}

void EventLoopTaskQueue::shutdown() {
  sem_.shutdown();
  shutdown_signaled_.store(true);
  // LifoEventSem::shutdown() makes the FD readable even if there are no
  // tasks pending.  `haveTasksEventHandler' will get called on the
  // EventLoop thread, notice the shutdown state and, if
  // `setCloseEventLoopOnShutdown()' was called, stop the event loop.
}

bool EventLoopTaskQueue::isFull() {
  if (sem_.valueGuess() >= capacity_) {
    return true;
  }

  return false;
}

int EventLoopTaskQueue::addWithPriority(Func func, int8_t priority) {
  if (UNLIKELY(sem_.isShutdown())) {
    err = E::SHUTDOWN;
    return -1;
  }
  // During dequeue, semaphore is decremented by a value followed by dequeue of
  // equal number of elements. Hence, enqueue here is done in order
  queues_[translatePriority(priority)].enqueue(std::move(func));
  sem_.post();
  return 0;
}

void EventLoopTaskQueue::haveTasksEventHandler(void* arg, short what) {
  EventLoopTaskQueue* self = static_cast<EventLoopTaskQueue*>(arg);
  if (!(what & EV_READ)) {
    ld_error("Got an unexpected event on task queue: what=%d", what);
    ld_check(false);
  }
  ld_check(self->sem_waiter_);
  try {
    auto cb = [self](size_t n) { self->executeTasks(n); };
    // processBatch() decrements the semaphore by some amount and calls our
    // callback with the amount.  We're guaranteed to have at least that many
    // items in the UMPSCQueue, because the producer pushes into the queue
    // first then increments the semaphore.
    self->sem_waiter_->processBatch(
        cb,
        self->num_hi_pri_dequeues_per_iteration_ +
            self->num_mid_pri_dequeues_per_iteration_ +
            self->num_lo_pri_dequeues_per_iteration_);
  } catch (const folly::ShutdownSemError&) {
    struct event_base* base = LD_EV(event_get_base)(self->tasks_pending_event_);
    // First delete the event since the fd is about to go away
    ld_check(self->tasks_pending_event_);
    LD_EV(event_free)(self->tasks_pending_event_);
    self->tasks_pending_event_ = nullptr;

    // Destroy the AsyncWaiter, which also closes the fd
    self->sem_waiter_.reset();

    // If requested, instruct the event loop to stop
    if (self->close_event_loop_on_shutdown_) {
      int rv = LD_EV(event_base_loopbreak)(base);
      if (UNLIKELY(rv != 0)) {
        ld_error("FATAL: event_base_loopbreak() failed");
        ld_check(false);
      }
    }
  }
}

void EventLoopTaskQueue::executeTasks(size_t tokens) {
  auto execute = [this, &tokens](const size_t times, size_t start_priority) {
    int to_execute = std::min(tokens, times);
    tokens -= to_execute;
    for (size_t i = 0; i < to_execute; ++i) {
      Func func;
      bool found = 0;

      for (int j = 0; j < kNumberOfPriorities; ++j) {
        if (queues_[start_priority].try_dequeue(func)) {
          found = 1;
          break;
        }
        ++start_priority;
        start_priority %= kNumberOfPriorities;
      }
      ld_assert(found);
      func();
    }
  };

  execute(num_hi_pri_dequeues_per_iteration_,
          translatePriority(folly::Executor::HI_PRI));

  execute(num_mid_pri_dequeues_per_iteration_,
          translatePriority(folly::Executor::MID_PRI));

  execute(num_lo_pri_dequeues_per_iteration_,
          translatePriority(folly::Executor::LO_PRI));
}

}} // namespace facebook::logdevice
