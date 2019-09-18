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

constexpr std::array<int8_t, EventLoopTaskQueue::kNumberOfPriorities>
    EventLoopTaskQueue::kLookupTable;

EventLoopTaskQueue::EventLoopTaskQueue(
    EvBase& base,
    size_t capacity,
    const std::array<uint32_t, kNumberOfPriorities>& dequeues_per_iteration)
    : base_(base), capacity_(capacity) {
  setDequeuesPerIteration(dequeues_per_iteration);

  sem_waiter_ = sem_.beginAsyncWait();
  try {
    tasks_pending_event_ =
        std::make_unique<Event>([this]() { haveTasksEventHandler(); },
                                Event::Events::READ_PERSIST,
                                sem_waiter_->fd(),
                                &base);
  } catch (const std::exception& e) {
    ld_error("Failed to create 'task pipe is readable' event for "
             "an event loop: %s",
             e.what());
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
    tasks_pending_event_.reset();
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
  ld_check(func);
  // During dequeue, semaphore is decremented by a value followed by dequeue of
  // equal number of elements. Hence, enqueue here is done in order
  Task t(std::move(func), folly::RequestContext::saveContext());
  queues_[translatePriority(priority)].enqueue(std::move(t));
  sem_.post();
  return 0;
}

void EventLoopTaskQueue::haveTasksEventHandler() {
  ld_check(sem_waiter_);
  try {
    auto cb = [this](uint32_t n) { executeTasks(n); };
    // processBatch() decrements the semaphore by some amount and calls our
    // callback with the amount.  We're guaranteed to have at least that many
    // items in the UMPSCQueue, because the producer pushes into the queue
    // first then increments the semaphore.
    sem_waiter_->processBatch(cb, total_dequeues_per_iteration_);
  } catch (const folly::ShutdownSemError&) {
    // First delete the event since the fd is about to go away
    ld_check(tasks_pending_event_);

    tasks_pending_event_.reset();
    // Destroy the AsyncWaiter, which also closes the fd
    sem_waiter_.reset();

    // If requested, instruct the event loop to stop
    if (close_event_loop_on_shutdown_) {
      auto status = base_.terminateLoop();
      if (UNLIKELY(status != EvBase::Status::OK)) {
        ld_error("FATAL: EvBase::terminateLoop() failed");
        ld_check(false);
      }
    }
  }
}

void EventLoopTaskQueue::executeTasks(uint32_t tokens) {
  std::array<uint32_t, kNumberOfPriorities> dequeues_to_execute{0};
  std::array<uint32_t, kNumberOfPriorities> tasks_available{0};

  // Assign just the required slots first.
  for (uint32_t i = 0; tokens > 0 && i < dequeues_to_execute.size(); ++i) {
    tasks_available[i] = queues_[i].size();
    dequeues_to_execute[i] = std::min(
        std::min(tasks_available[i], dequeues_per_iteration_[i]), tokens);
    tokens -= dequeues_to_execute[i];
  }

  // Then, do a final pass to assign all remaining tokens by order of priority.
  for (uint32_t i = 0; tokens > 0; ++i) {
    ld_assert(i < dequeues_to_execute.size());
    auto tasks_remaining = tasks_available[i] - dequeues_to_execute[i];
    auto dequeues = std::min(tokens, tasks_remaining);
    tokens -= dequeues;
    dequeues_to_execute[i] += dequeues;
  }

  for (size_t i = 0; i < dequeues_to_execute.size(); ++i) {
    while (dequeues_to_execute[i]--) {
      auto t = queues_[i].dequeue();
      if (UNLIKELY(!t.function)) {
        continue;
      }
      folly::RequestContextScopeGuard guard(std::move(t.context));
      t.function();
    }
  }
}

}} // namespace facebook::logdevice
