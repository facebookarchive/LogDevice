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

#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace std::literals;

RequestPump::RequestPump(struct event_base* base,
                         size_t capacity,
                         int requests_per_iteration)
    : EventLoopTaskQueue(base, capacity, requests_per_iteration) {}

RequestPump::~RequestPump() {}

int RequestPump::tryPost(std::unique_ptr<Request>& req) {
  ld_check(req);
  if (isFull()) {
    err = E::NOBUFS;
    return -1;
  }
  return forcePost(req);
}

int RequestPump::forcePost(std::unique_ptr<Request>& req) {
  ld_check(req);
  req->enqueue_time_ = std::chrono::steady_clock::now();
  // Convert to folly function so that the request can be added
  // EventLoopTaskQueue.
  Func func = [rq = std::move(req)]() mutable {
    RequestPump::processRequest(rq);
  };
  int rv = add(std::move(func));
  ld_check(!req);
  return rv;
}

void RequestPump::processRequest(std::unique_ptr<Request>& rq) {
  using namespace std::chrono;

  bool on_worker_thread = ThreadID::isWorker();

  ld_check(rq); // otherwise why did the semaphore wake us?
  if (UNLIKELY(!rq)) {
    RATELIMIT_CRITICAL(
        1s, 1, "INTERNAL ERROR: got a NULL request pointer from RequestPump");
    return;
  }

  RunContext run_context = rq->getRunContext();

  if (on_worker_thread) {
    auto queue_time{
        duration_cast<microseconds>(steady_clock::now() - rq->enqueue_time_)};
    HISTOGRAM_ADD(Worker::stats(), requests_queue_latency, queue_time.count());
    if (queue_time > 20ms) {
      RATELIMIT_WARNING(5s,
                        10,
                        "Request queued for %g msec: %s (id: %lu)",
                        queue_time.count() / 1000.0,
                        rq->describe().c_str(),
                        rq->id_.val());
    }

    Worker::onStartedRunning(run_context);
  }

  // rq should not be accessed after execute, as it may have been deleted.
  Request::Execution status = rq->execute();

  switch (status) {
    case Request::Execution::COMPLETE:
      // Count destructor towards request's execution time.
      rq.reset();
      break;
    case Request::Execution::CONTINUE:
      rq.release();
      break;
  }

  if (on_worker_thread) {
    Worker::onStoppedRunning(run_context);
    WORKER_STAT_INCR(worker_requests_executed);
  }
}

}} // namespace facebook::logdevice
