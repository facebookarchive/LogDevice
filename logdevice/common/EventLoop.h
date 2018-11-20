/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <pthread.h>
#include <semaphore.h>
#include <unordered_map>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/ThreadID.h"

struct event_base;

namespace facebook { namespace logdevice {

/**
 * @file   an EventLoop is a LogDevice internal thread running a libevent 2.x
 *         event_base. All LogDevice requests are executed on EventLoop
 *         threads. EventLoop objects directly receive and execute requests
 *         from client threads, the listener thread, and the command port
 *         thread. Those other threads use EventLoopHandle objects to pass
 *         the requests to an EventLoop.
 */

class RequestPump;

class EventLoop {
 public:
  /**
   * Creates and starts the EventLoop's thread.  The thread does not
   * immediately start running the event loop, that only happens after start()
   * is called.
   *
   * @throws ConstructorFailed on error, sets err to
   *   NOMEM    if a libevent call failed because malloc() failed
   *   INTERNAL if a libevent call fails unexpectedly
   *   SYSLIMIT if system limits on thread stack sizes or total number of
   *            threads (if any) are reached
   */
  explicit EventLoop(
      std::string thread_name = "",
      ThreadID::Type thread_type = ThreadID::Type::UNKNOWN_EVENT_LOOP);

  // destructor has to be virtual because it is invoked by EventLoop::run()
  // as "delete this"
  virtual ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop(EventLoop&&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;
  EventLoop& operator=(EventLoop&&) = delete;

  struct event_base* getEventBase() {
    return base_.get();
  }

  /**
   * Provides shared ownership of the pump that the EventLoop will get
   * `Request' instances through.  Must be called before `start()'.
   */
  void setRequestPump(std::shared_ptr<RequestPump> pump) {
    request_pump_ = std::move(pump);
  }

  /**
   * Signal to the worker thread to start running the libevent loop.
   * Called by EventLoopHandle.
   */
  void start();

  /**
   * Get the thread handle of this EventLoop.
   *
   * @return the pthread handle. This function should never fail.
   */
  pthread_t getThread() const {
    return thread_;
  }

  int getThreadId() const {
    return tid_;
  }

  std::shared_ptr<RequestPump> getRequestPump() {
    return request_pump_;
  }

  /**
   * @return   a pointer to the EventLoop object running on this thread, or
   *           nullptr if this thread is not running a EventLoop.
   */
  static EventLoop* onThisThread() {
    return EventLoop::thisThreadLoop;
  }

  static const int PRIORITY_LOW = 2;    // lowest priority
  static const int PRIORITY_NORMAL = 1; // default libevent priority
  static const int PRIORITY_HIGH = 0;   // elevated priority (numerically lower)

  static const int NUM_PRIORITIES = PRIORITY_LOW + 1;

 private:
  // libevent 2.x event_base that runs the loop. We use a unique_ptr with a
  // deleter and make base_ first data member of this class to make sure it
  // is deleted after all of the events in it.
  std::unique_ptr<event_base, std::function<void(event_base*)>> base_;

 public:
  // a pointer to a fake struct timeval that event_base_init_common_timeout()
  // returned for this event_base and 0 duration. We use this to minimize the
  // overhead of adding and deleting common zero-timeout timers. See
  // event_base_init_common_timeout() for details.
  //
  // This has to be defined below base_ because of initialization order in
  // the constructor. Socket needs this constant as well, so I made it public.
  const struct timeval* const zero_timeout_;

  // total number of event handlers that libevent has called so far
  std::atomic<size_t> event_handlers_called_{0};
  // total number of event handlerss that finished processing and returned
  // control to libevent
  std::atomic<size_t> event_handlers_completed_{0};

 protected:
  // called on this EventLoop's thread before starting the event loop
  virtual void onThreadStarted() {}

 private:
  ThreadID::Type thread_type_;
  std::string thread_name_;

  pthread_t thread_; // thread on which this loop runs

  // pid of thread_
  int tid_{-1};

  // Main request pump; ownership shared to ensure safe shutdown (shutting
  // down this RequestPump stops the event loop)
  std::shared_ptr<RequestPump> request_pump_;

  std::atomic<bool> running_;
  std::atomic<bool> shutting_down_;

  // entry point of the loop's thread
  static void* enter(void* self);

  // called by enter() to run this event loop on .thread_
  void run();

  // this is how a thread finds if it's running an EventLoop, and which one
  static __thread EventLoop* thisThreadLoop;

  // stack size of this loop's thread (pthread defaults are low)
  static const size_t STACK_SIZE = (1024 * 1024);

  // Semaphore that coordinates initialization and starting of the event loop.
  // Sequence of events ("main" and "worker" denote threads):
  // (main   1) Constructor initializes semaphore, starts worker thread
  // (worker 1) run() waits on sempahore
  // (main   2) EventLoop subclass initializes
  // (main   3) EventLoopHandle posts to the semaphore
  // (worker 2) run() continues, starts libevent loop
  // The reason for this dance is so that the EventLoop *subclass* can modify
  // the event base in its constructor without worrying that the worker thread
  // is concurrently using it.
  Semaphore start_sem_;
};

}} // namespace facebook::logdevice
