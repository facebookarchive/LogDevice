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

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestPump.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  EventLoopHandle is how other threads create, control, and schedule
 *        requests to run on logdevice::EventLoops.
 */

class Processor;
class LocalLogStore;

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
      : event_loop_(loop), event_loop_thread_(event_loop_->getThread()) {
    request_pump_ = std::make_shared<RequestPump>(event_loop_->getEventBase(),
                                                  request_pump_capacity,
                                                  requests_per_iteration);
    request_pump_->setCloseEventLoopOnShutdown();
    event_loop_->setRequestPump(request_pump_);
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
    pthread_t thread_id = getThread();

    // Different scenarios in which handle gets deleted.
    // 1. Constructor failed and we never got a chance to run the thread.
    //    Just delete EventLoop instance here. No need to shutdown request pump
    //    or join thread.
    // 2. EventLoopHandle shutdown for workers in Processor::shutdown.
    //    Destruction order in this case is, workers have to be deleted before
    //    their corresponding executor and, EventLoopHandle and consequently
    //    EventLoop, is deleted after Workers go away. In this scenario,
    //    Processor::shutdown invokes request pump shutdown and joins EventLoop
    //    thread. As part of handle destructor we just delete the EventLoop
    //    instance. CommandListener is the another handle that is destructed in
    //    this manner because it takes a long time to join its thread.
    // 3. EventLoopHandles associated with others entities like various
    //    connection listeners. These do not invoke shutdown
    //    before this destructor is invoked. For such handles, we just shutdown
    //    here explicitly, join the thread and delete the eventloop instance.
    if (started_ && !request_pump_->isShutdown()) {
      // Tell EventLoop on the other end to destroy itself and terminate the
      // thread
      shutdown();
      pthread_join(thread_id, nullptr);
    }

    delete event_loop_;
  }

  /**
   * Start processing events on EventLoop's thread.
   */
  void start() {
    event_loop_->start();
    started_ = true;
  }

  /**
   * @return controlled EventLoop object
   */
  EventLoop* get() const {
    return event_loop_;
  }

  EventLoop* operator->() {
    return event_loop_;
  }

  EventLoop& operator*() {
    return *event_loop_;
  }

  struct event_base* getEventBase() {
    return event_loop_->getEventBase();
  }

  /**
   * A convenience wrapper around postRequest()/forcePostRequest().
   * Enqueues the function to run on the next event loop iteration.
   * If force = true, requests posting can only fail during shutdown.
   * Returns 0 on success, -1 on error with err set to:
   *       NOBUFS    if force = false, and too many requests are pending on the
   *                 event loop
   *       SHUTDOWN  if the EventLoop is shutting down
   */
  int runInEventThreadNonBlocking(std::function<void()> func,
                                  bool force = true);

  /**
   * Like runInEventThreadNonBlocking() but blocks until the request is done.
   * Must not be called from the event loop thread.
   */
  int runInEventThreadBlocking(std::function<void()> func, bool force = true);

  /**
   * This function signals the underlying EventLoop to shut down and terminate
   * its thread and marks this handle shut down so that all subsequent calls
   * to postRequest() etc on it will fail with E::SHUTDOWN.
   *
   */
  void shutdown() {
    request_pump_->shutdown();
  }

  /**
   * @return the pthread handle of the thread controlled by this
   *         EventLoopHandle
   */
  pthread_t getThread() {
    ld_check(!pthread_equal(event_loop_thread_, pthread_self()));
    return event_loop_thread_;
  }

  RequestPump& getRequestPump() {
    return *request_pump_;
  }

 private:
  // EventLoop object wrapped by this handle
  EventLoop* event_loop_;

  // Was start() called?
  bool started_ = false;

  std::shared_ptr<RequestPump> request_pump_;

  // pthread id of the event loop thread
  pthread_t event_loop_thread_;
};
}} // namespace facebook::logdevice
