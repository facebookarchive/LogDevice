/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RequestPump.h"

#include <fcntl.h>
#include <unistd.h>
#include <utility>

#include <folly/Likely.h>
#include <folly/container/Array.h>

#include "logdevice/common/Request.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace std::literals;

RequestPump::RequestPump(
    struct event_base* base,
    size_t capacity,
    const std::array<uint32_t, kNumberOfPriorities>& dequeues_per_iteration)
    : EventLoopTaskQueue(base, capacity, dequeues_per_iteration) {}

RequestPump::~RequestPump() {}

int RequestPump::tryPost(std::unique_ptr<Request>& req) {
  if (!check(req, false)) {
    return -1;
  }
  return post(req);
}

int RequestPump::forcePost(std::unique_ptr<Request>& req) {
  if (!check(req, true)) {
    return -1;
  }
  return post(req);
}

int RequestPump::blockingRequest(std::unique_ptr<Request>& req) {
  if (!check(req, false)) {
    return -1;
  }
  Semaphore sem;
  req->setClientBlockedSemaphore(&sem);

  int rv = post(req);
  if (rv != 0) {
    req->setClientBlockedSemaphore(nullptr);
    return rv;
  }
  // Block until the Request has completed
  sem.wait();
  return 0;
}

bool RequestPump::check(const std::unique_ptr<Request>& req, bool force) {
  if (!req) {
    err = E::INVALID_PARAM;
    return false;
  }
  if (!force && isFull()) {
    err = E::NOBUFS;
    return false;
  }
  return true;
}

int RequestPump::post(std::unique_ptr<Request>& req) {
  req->enqueue_time_ = std::chrono::steady_clock::now();
  // Convert to folly function so that the request can be added
  // EventLoopTaskQueue.
  auto priority = req->getExecutorPriority();
  Func func = [req = std::move(req)]() mutable {
    RequestPump::processRequest(req);
  };
  int rv = addWithPriority(std::move(func), priority);
  ld_check(!req);
  return rv;
}

void RequestPump::processRequest(std::unique_ptr<Request>& req) {
  using namespace std::chrono;

  bool on_worker_thread = ThreadID::isWorker();

  ld_check(req); // otherwise why did the semaphore wake us?
  if (UNLIKELY(!req)) {
    RATELIMIT_CRITICAL(
        1s, 1, "INTERNAL ERROR: got a NULL request pointer from RequestPump");
    return;
  }

  RunContext run_context = req->getRunContext();

  if (on_worker_thread) {
    auto queue_time{
        duration_cast<microseconds>(steady_clock::now() - req->enqueue_time_)};
    HISTOGRAM_ADD(Worker::stats(), requests_queue_latency, queue_time.count());
    if (queue_time > 20ms) {
      RATELIMIT_WARNING(5s,
                        10,
                        "Request queued for %g msec: %s (id: %lu)",
                        queue_time.count() / 1000.0,
                        req->describe().c_str(),
                        req->id_.val());
    }

    Worker::onStartedRunning(run_context);
  }

  // req should not be accessed after execute, as it may have been deleted.
  Request::Execution status = req->execute();

  switch (status) {
    case Request::Execution::COMPLETE:
      // Count destructor towards request's execution time.
      req.reset();
      break;
    case Request::Execution::CONTINUE:
      req.release();
      break;
  }

  if (on_worker_thread) {
    Worker::onStoppedRunning(run_context);
    WORKER_STAT_INCR(worker_requests_executed);
  }
}

}} // namespace facebook::logdevice
