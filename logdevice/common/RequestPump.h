/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include "logdevice/common/EventLoopTaskQueue.h"

struct event_base;

namespace facebook { namespace logdevice {

/**
 * @file This is a specialized EventLoopTaskQueue that accepts Request
 * instances.
 *
 * On the producer side, this provides a thread-safe facility to post Request
 * instances. This class will wrap Request::execute into a folly function adds
 * it to EventLoopTaskQueue.
 *
 * On the consumer side, EventLoopTaskQueue picks up a function to be executed
 * and will call execute method on the wrapped object.
 */

class Request;
struct RequestPumpImpl;

class RequestPump : public EventLoopTaskQueue {
 public:
  /**
   * Registers the event.
   *
   * @param base     event base to use
   * @param capacity soft limit on the size, respected by tryPost()
   *
   * @throws ConstructorFailed on error
   */
  RequestPump(
      struct event_base* base,
      size_t capacity,
      const std::array<uint32_t, kNumberOfPriorities>& requests_per_iteration);

  virtual ~RequestPump();

  /**
   * Attempts to post a Request for the EventLoop to process.  Respects the
   * soft capacity limit passed to the constructor; if too many requests are
   * already queued, fails with NOBUFS.
   *
   * Can be called from any thread.
   *
   * @return 0 on success, -1 on failures setting `err' to one of:
   *     INVALID_PARAM  req is nullptr
   *     NOBUFS         too many requests are already queued
   *     SHUTDOWN       shutdown() was already called
   */
  int tryPost(std::unique_ptr<Request>& req);

  /**
   * Similar to tryPost() above but forces the request to be queued even if
   * we're already over capacity.  Used for important requests where the
   * alternative would be to somehow retry until the post succeeds, so just
   * stuffing it into the queue is usually more efficient.
   *
   * @return 0 on success, -1 on failures setting `err' to one of:
   *     INVALID_PARAM  req is nullptr
   *     SHUTDOWN   shutdown() was already called
   */
  int forcePost(std::unique_ptr<Request>& req);

  /**
   * Runs a Request on the EventLoop, waiting for it to finish.
   *
   * For parameters and return values, see tryPost().
   */
  int blockingRequest(std::unique_ptr<Request>& req);

  /**
   * Change number of requests to process per event loop iteration.
   * Can be updated on the fly, but only from the thread on which the
   * RequestPump runs.
   */
  void setNumRequestsPerIteration(
      const std::array<uint32_t, kNumberOfPriorities>& dequeues_per_iteration) {
    setDequeuesPerIteration(dequeues_per_iteration);
  }

 private:
  bool check(const std::unique_ptr<Request>& req, bool force);
  int post(std::unique_ptr<Request>& req);
  /**
   * Called by enqueued folly function on this instance to process a request.
   */
  static void processRequest(std::unique_ptr<Request>& req);
};

}} // namespace facebook::logdevice
