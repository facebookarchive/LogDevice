/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <memory>

#include <folly/Function.h>

#include "folly/concurrency/UnboundedQueue.h"
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
 * The expected pattern is for producers and the consumer to share a
 * std::shared_ptr<EventLoopTaskQueue> for shutdown safety.
 */
class EventLoopTaskQueue {
 public:
  /**
   * Registers the event.
   *
   * @param base     event base to use
   * @param capacity soft limit on the size, respected by tryPost()
   *
   * @throws ConstructorFailed on error
   */
  EventLoopTaskQueue(struct event_base* base,
                     size_t capacity,
                     int dequeues_per_iteration);

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
  virtual int add(Func func);

  virtual void setNumDequeuesPerIteration(int num) {
    num_dequeues_per_iteration_ = num;
  }

  /*
   * Checks if the queue is filled up to the soft capacity limit.
   */
  bool isFull();

 private:
  // The data structures of choice for queue is an UnboundedQueue paired with a
  // LifoEventSem. The posting codepath writes into the queue, then posts to
  // the semaphore. LifoEventSem ensures that the FD hooked up to the event
  // "lights up", libevent invokes the handler and we read the tasks from
  // the queue.  The semaphore also acts as a ticket dispenser for items on
  // the queue: the consumer first consumes some portion of the semaphore's
  // value then pops that many tasks off of the queue (details hidden
  // behind LifoEventSem::processBatch()).
  folly::UMPSCQueue<Func, false /* MayBlock */, 9> queue_;
  LifoEventSem sem_;
  std::unique_ptr<LifoEventSem::AsyncWaiter> sem_waiter_;

  // Max size of the queue.
  size_t capacity_;

  // Triggers when `queue' is not empty
  struct event* tasks_pending_event_;

  bool close_event_loop_on_shutdown_{false};
  // Indicates shutdown() was called (EventLoop is asynchronously
  // processing the shutdown)
  std::atomic<bool> shutdown_signaled_{false};

  // Try to dequeue this many elements at time from the queue. Actual number
  // depends on how many elements are present in the queue.
  int num_dequeues_per_iteration_;

  // Callback registered with event base. Indicates pending events on the queue.
  static void haveTasksEventHandler(void* self, short what);

  // Invoked by haveTasksEventHandle to dequeue tasks from the queue.
  void executeTasks(size_t num_tasks_to_dequeue);
};
}} // namespace facebook::logdevice
