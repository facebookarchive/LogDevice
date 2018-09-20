/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>

struct event_base;

namespace facebook { namespace logdevice {

/**
 * @file This is a specialized bufferevent-type state machine that allows work
 * items to be passed to an EventLoop instance in the form of Request instances.
 *
 * On the producer side, this provides a thread-safe facility to post Request
 * instances.
 *
 * On the consumer/EventLoop side, this manages a libevent event that hooks
 * into the EventLoop's event base.  The event fires when there are pending
 * requests in the queue.  The event callback invokes Request::execute() on
 * those Requests.  The class returns control to the libevent loop after
 * processing a few requests (Settings::requests_per_iteration) to avoid
 * hogging the event loop.
 *
 * The expected pattern is for producers and the consumer to share a
 * std::shared_ptr<RequestPump> for shutdown safety.
 */

class Request;
struct RequestPumpImpl;

class RequestPump {
 public:
  /**
   * Registers the event.
   *
   * @param base     event base to use
   * @param capacity soft limit on the size, respected by tryPost()
   *
   * @throws ConstructorFailed on error
   */
  RequestPump(struct event_base* base,
              size_t capacity,
              int requests_per_iteration);

  ~RequestPump();

  /**
   * May be called on any thread.  After this returns, RequestPump will not
   * accept new Requests.  Additionally, if setCloseEventLoopOnShutdown() was
   * called, this signals to the EventLoop to shut down soon.
   */
  void shutdown();

  /**
   * @see shutdown()
   */
  void setCloseEventLoopOnShutdown();

  /**
   * Attempts to post a Request for the EventLoop to process.  Respects the
   * soft capacity limit passed to the constructor; if too many requests are
   * already queued, fails with NOBUFS.
   *
   * May be called from any thread.
   *
   * @return 0 on success, -1 on failures setting `err' to one of:
   *     NOBUFS     too many requests are already queued
   *     SHUTDOWN   shutdown() was already called
   */
  int tryPost(std::unique_ptr<Request>& req);

  /**
   * Similar to tryPost() above but forces the request to be queued even if
   * we're already over capacity.  Used for important requests where the
   * alternative would be to somehow retry until the post succeeds, so just
   * stuffing it into the queue is usually more efficient.
   *
   * @return 0 on success, -1 on failures setting `err' to one of:
   *     SHUTDOWN   shutdown() was already called
   */
  int forcePost(std::unique_ptr<Request>& req);

  /**
   * Change number of requests to process per event loop iteration.
   * Can be updated on the fly, but only from the thread on which the
   * RequestPump runs.
   */
  void setNumRequestsPerIteration(int requestsPerIteration) {
    requestsPerIteration_ = requestsPerIteration;
  }

 private:
  std::unique_ptr<RequestPumpImpl> impl_;

  int requestsPerIteration_;

  static void haveRequestsEventHandler(void* self, short what);

  // called by haveRequestsCallback() above to do work
  void onRequestsPending(size_t n);
};

}} // namespace facebook::logdevice
