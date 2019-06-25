/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <numeric>

#include <folly/Executor.h>
#include <folly/Function.h>
#include <folly/concurrency/UnboundedQueue.h>
#include <folly/io/async/Request.h>

#include "logdevice/common/LifoEventSem.h"

struct event;
struct event_base;

namespace facebook { namespace logdevice {
using Func = folly::Function<void()>;
struct EventLoopTaskQueueImpl;

/**
 * @file This is a specialized bufferevent-type state machine that allows work
 * items to be passed to an EventLoop instance in the form of folly Functions.
 *
 * On the producer side, this provides a thread-safe facility to post functions
 * to be executed on EventLoop thread.
 *
 * On the consumer/EventLoop side, this manages a libevent event that hooks
 * into the EventLoop's event base.  The event fires when there are pending
 * tasks in the queue.  The event callback dequeues the function from the
 * queue and invokes it. The class returns control to the libevent loop after
 * processing a few tasks (Settings::requests_per_iteration) to avoid
 * hogging the event loop.
 *
 * The current implementation of multiple priorities based on three queues.
 * You can specify how many request per iteration from each queue we should try
 * to process, but if there is no enough requests to satisfy requirement we
 * will try to dequeue queue with lower priority, if there is no requests with
 * lower priority we will try to search in higher priorities from highest to
 * lowest.
 *
 * The expected pattern is for producers and the consumer to share a
 * std::shared_ptr<EventLoopTaskQueue> for shutdown safety.
 */
class EventLoopTaskQueue {
 public:
  constexpr static size_t kNumberOfPriorities = 3;

  /**
   * Registers the event.
   *
   * @param base     event base to use
   * @param capacity soft limit on the size, respected by tryPost()
   *
   * @throws ConstructorFailed on error
   */
  EventLoopTaskQueue(
      struct event_base* base,
      size_t capacity,
      const std::array<uint32_t, kNumberOfPriorities>& dequeues_per_iteration);

  virtual ~EventLoopTaskQueue();

  /**
   * May be called on any thread.  After this returns, RequestPump will not
   * accept new Requests.  Additionally, if setCloseEventLoopOnShutdown() was
   * called, this signals to the EventLoop to shut down soon.
   */
  virtual void shutdown();
  /**
   * @see shutdown()
   */
  virtual void setCloseEventLoopOnShutdown();

  /**
   * Add a function to be executed on the eventloop thread this TaskQueue is
   * associated with.
   *
   * Can be invoked from any thread.
   */
  virtual int addWithPriority(Func func, int8_t priority);

  virtual int add(Func func) {
    return addWithPriority(std::move(func), folly::Executor::LO_PRI);
  }

  /*
   * Checks if the queue is filled up to the soft capacity limit.
   */
  bool isFull();

  /**
   * @return request pump shutdown was initiated.
   */
  bool isShutdown() {
    return shutdown_signaled_.load();
  }

  static constexpr size_t translatePriority(const int8_t priority) {
    for (int i = 0; i < kNumberOfPriorities; ++i) {
      if (kLookupTable[i] == priority) {
        return i;
      }
    }
    return kNumberOfPriorities - 1;
  }

  void setDequeuesPerIteration(
      const std::array<uint32_t, kNumberOfPriorities>& dequeues_per_iteration) {
    dequeues_per_iteration_ = dequeues_per_iteration;
    total_dequeues_per_iteration_ =
        std::accumulate(dequeues_per_iteration_.begin(),
                        dequeues_per_iteration_.end(),
                        uint32_t(0));
  }

  void setDequeuesPerIterationForPriority(uint32_t num_dequeues,
                                          int8_t priority) {
    dequeues_per_iteration_[translatePriority(priority)] = num_dequeues;
    total_dequeues_per_iteration_ =
        std::accumulate(dequeues_per_iteration_.begin(),
                        dequeues_per_iteration_.end(),
                        uint32_t(0));
  }

 private:
  class Task {
   public:
    explicit Task(Func func, std::shared_ptr<folly::RequestContext> ctx)
        : function(std::move(func)), context(std::move(ctx)) {}
    Func function;
    std::shared_ptr<folly::RequestContext> context;
  };
  using Queue = folly::UMPSCQueue<Task, false /* MayBlock */, 9>;

  constexpr static std::array<int8_t, kNumberOfPriorities> kLookupTable = {
      {folly::Executor::HI_PRI,
       folly::Executor::MID_PRI,
       folly::Executor::LO_PRI}};

  // Execution probability distribution of different tasks. Hi Priority tasks
  // are called such because they have a higher chance of getting executed.
  std::array<uint32_t, kNumberOfPriorities> dequeues_per_iteration_;
  uint32_t total_dequeues_per_iteration_;

  // The data structures of choice for queue is an UnboundedQueue paired with a
  // LifoEventSem. The posting codepath writes into the queue, then posts to
  // the semaphore. LifoEventSem ensures that the FD hooked up to the event
  // "lights up", libevent invokes the handler and we read the tasks from
  // the queue.  The semaphore also acts as a ticket dispenser for items on
  // the queue: the consumer first consumes some portion of the semaphore's
  // value then pops that many tasks off of the queue (details hidden
  // behind LifoEventSem::processBatch()).
  std::array<Queue, kNumberOfPriorities> queues_;

  LifoEventSem sem_;
  std::unique_ptr<LifoEventSem::AsyncWaiter,
                  folly::DelayedDestruction::Destructor>
      sem_waiter_;

  // Max size of the queue.
  size_t capacity_;

  // Triggers when `queue' is not empty
  struct event* tasks_pending_event_;

  bool close_event_loop_on_shutdown_{false};
  // Indicates shutdown() was called (EventLoop is asynchronously
  // processing the shutdown)
  std::atomic<bool> shutdown_signaled_{false};

  // Callback registered with event base. Indicates pending events on the queue.
  static void haveTasksEventHandler(void* self, short what);

  // Invoked by haveTasksEventHandle to dequeue tasks from the queue.
  void executeTasks(uint32_t num_tasks_to_dequeue);
};

}} // namespace facebook::logdevice
