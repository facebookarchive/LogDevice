/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <errno.h>
#include <memory>
#include <unistd.h>
#include <utility>

#include <folly/ScopeGuard.h>
#include <folly/container/Array.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  EventLoopHandle is how other threads create, control, and schedule
 *        requests to run on logdevice::EventLoops.
 */

class EventLoopHandle {
 public:
  /**
   * Creates the EventLoop's thread and a pipe to send requests to the loop.
   *
   * Takes ownership of the EventLoop.
   */
  explicit EventLoopHandle(EventLoop* loop,
                           size_t request_pump_capacity = 1024,
                           int requests_per_iteration = 16)
      : event_loop_(loop) {
    auto task_queue = std::make_shared<EventLoopTaskQueue>(
        event_loop_->getEventBase(),
        request_pump_capacity,
        folly::make_array<size_t>(requests_per_iteration, 0, 0));
    task_queue->setCloseEventLoopOnShutdown();
    event_loop_->setTaskQueue(task_queue);
    event_loop_->start();
  }

  EventLoopHandle(const EventLoopHandle&) = delete;
  EventLoopHandle(EventLoopHandle&&) = delete;
  EventLoopHandle& operator=(const EventLoopHandle&) = delete;
  EventLoopHandle& operator=(EventLoopHandle&&) = delete;

  /**
   * The destructor signals to the EventLoop that it must exit and free
   * resources. The loop may still be running for some time after this
   * destructor returns control.
   */
  ~EventLoopHandle() {
    pthread_t thread_id = event_loop_->getThread();
    // We just shutdown here explicitly, join the thread and delete
    // the eventloop instance.
    if (!event_loop_->getTaskQueue().isShutdown()) {
      // Tell EventLoop on the other end to destroy itself and terminate the
      // thread
      event_loop_->getTaskQueue().shutdown();
      pthread_join(thread_id, nullptr);
    }

    delete event_loop_;
  }

  /**
   * @return controlled EventLoop object
   */
  EventLoop* get() const {
    return event_loop_;
  }

  EventLoop* operator->() {
    return get();
  }

  EventLoop& operator*() {
    return *get();
  }

 private:
  // EventLoop object wrapped by this handle
  EventLoop* event_loop_;
};
}} // namespace facebook::logdevice
