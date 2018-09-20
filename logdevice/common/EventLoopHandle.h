/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <errno.h>
#include <unistd.h>

#include <memory>
#include <utility>
#include <atomic>

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
      : event_loop_(loop),
        wait_on_destruct_(true),
        event_loop_thread_(event_loop_->getThread()) {
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

    if (started_) {
      // Tell EventLoop on the other end to destroy itself and terminate the
      // thread
      shutdown();
      if (wait_on_destruct_) {
        pthread_join(thread_id, nullptr);
      }
    } else {
      // start() was never called, we still own the event loop and need to
      // delete it
      delete event_loop_;
    }
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

  struct event_base* getEventBase() {
    return event_loop_->getEventBase();
  }

  /**
   * By default, EventLoopHandle destructor waits for the EventLoop thread to
   * finish before returning.  If this method is called, the destructor will
   * only signal to the EventLoop to shut down but not wait for it.  This can
   * make it faster to shut down a pool of threads, by signalling to them all
   * to stop then manually joining the threads.
   */
  void dontWaitOnDestruct() {
    wait_on_destruct_ = false;
  }

  /**
   * Schedule a Request to run on the EventLoop controlled by this Handle
   * This method is thread safe.
   *
   * @param rq  request to execute. Must not be nullptr. On success the
   *            function takes ownership of the object (and passes it on
   *            to the EventLoop). On failure ownership remains with the
   *            caller.
   *
   * @return 0 if request was successfully passed to the EventLoop thread for
   *           execution, -1 on failure. err is set to
   *     INVALID_PARAM  if rq is invalid
   *     NOBUFS         if too many requests are pending to be delivered to
   *                    the event loop
   *     SHUTDOWN       the EventLoop is shutting down (a shutdown() method
   *                    has been executed on this handle object)
   */
  int postRequest(std::unique_ptr<Request>& rq) {
    if (!rq) {
      err = E::INVALID_PARAM;
      return -1;
    }
    return request_pump_->tryPost(rq);
  }

  /**
   * Like postRequest() above but posts even if the request pump is over
   * capacity.  (It can still fail with E::SHUTDOWN etc.)
   */
  int forcePostRequest(std::unique_ptr<Request>& rq) {
    if (!rq) {
      err = E::INVALID_PARAM;
      return -1;
    }
    return request_pump_->forcePost(rq);
  }

  /**
   * Runs a Request on the EventLoop, waiting for it to finish.
   *
   * For parameters and return values, see postRequest().
   */
  int blockingRequest(std::unique_ptr<Request>& rq) {
    Semaphore sem;
    rq->setClientBlockedSemaphore(&sem);

    int rv = postRequest(rq);
    if (rv != 0) {
      // Request destructor posts to the semaphore so destroy it first while
      // the semaphore still exists
      rq.reset();
      return rv;
    }

    // Block until the Request has completed
    sem.wait();
    return 0;
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

  // should we join the EventLoop thread in the destructor
  bool wait_on_destruct_;

  // pthread id of the event loop thread
  pthread_t event_loop_thread_;
};

}} // namespace facebook::logdevice
