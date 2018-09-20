/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "RequestPump.h"

#include <fcntl.h>
#include <unistd.h>

#include <utility>

#include <folly/Likely.h>

#include "event2/event.h"

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/LifoEventSem.h"
#include "logdevice/common/MPSCQueue.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace std::literals;

struct RequestPumpImpl {
  // The data structures of choice are an MPSCQueue paired with a
  // LifoEventSem.  The posting codepath writes into the queue, then posts to
  // the semaphore.  LifoEventSem ensures that the FD hooked up to the event
  // "lights up", libevent invokes the handler and we read the requests from
  // the queue.  The semaphore also acts as a ticket dispenser for items on
  // the queue: the consumer first consumes some portion of the semaphore's
  // value then pops that many requests off of the queue (details hidden
  // behind LifoEventSem::processBatch()).
  MPSCQueue<Request, &Request::request_pump_hook_> queue;
  LifoEventSem sem;
  std::unique_ptr<LifoEventSem::AsyncWaiter> sem_waiter;

  size_t capacity;
  // Triggers when `queue' is not empty
  struct event* requests_pending_event;

  bool close_event_loop_on_shutdown = false;
  // Indicates shutdown() was called (EventLoop is asynchronously
  // processing the shutdown)
  std::atomic<bool> shutdown_signalled{false};
};

RequestPump::RequestPump(struct event_base* base,
                         size_t capacity,
                         int requests_per_iteration)
    : impl_(new RequestPumpImpl),
      requestsPerIteration_(requests_per_iteration) {
  impl_->capacity = capacity;

  if (!base) {
    // err shoud be set by the function that tried to create base
    throw ConstructorFailed();
  }

  impl_->sem_waiter = impl_->sem.beginAsyncWait();
  impl_->requests_pending_event =
      LD_EV(event_new)(base,
                       impl_->sem_waiter->fd(),
                       EV_READ | EV_PERSIST,
                       EventHandler<haveRequestsEventHandler>,
                       this);

  if (!impl_->requests_pending_event) { // unlikely
    ld_error("Failed to create 'request pipe is readable' event for "
             "an event loop");
    err = E::NOMEM;
    throw ConstructorFailed();
  }

#if LIBEVENT_VERSION_NUMBER >= 0x02010000
  ld_assert(LD_EV(event_get_priority)(impl_->requests_pending_event) ==
            EventLoop::PRIORITY_NORMAL);
#endif

  int rv = LD_EV(event_add)(impl_->requests_pending_event, nullptr);
  if (rv != 0) { // unlikely
    ld_error("Failed to add 'request pipe is readable' event to event base");
    ld_check(false);
    LD_EV(event_free)(impl_->requests_pending_event);
    err = E::INTERNAL;
    throw ConstructorFailed();
  }
}

RequestPump::~RequestPump() {
  if (impl_->close_event_loop_on_shutdown && impl_->shutdown_signalled) {
    // If this pump is responsible for shutting down the event loop and
    // shutdown() was called, then to be in the destructor we must have
    // already gone through the shutdown sequence (since the event loop
    // co-owns the request pump).
    ld_check(impl_->requests_pending_event == nullptr);
  } else {
    // Otherwise, we may have gone through the async shutdown sequence or not.
    shutdown();
    if (impl_->requests_pending_event != nullptr) {
      // If not, presumably the event loop shared ownership of the pump.  To
      // be in the destructor the event loop must have destructed itself so
      // it's safe to delete the event now.
      LD_EV(event_free)(impl_->requests_pending_event);
      impl_->requests_pending_event = nullptr;
    }
  }
}

void RequestPump::setCloseEventLoopOnShutdown() {
  impl_->close_event_loop_on_shutdown = true;
}

void RequestPump::shutdown() {
  impl_->sem.shutdown();
  impl_->shutdown_signalled.store(true);
  // LifoEventSem::shutdown() makes the FD readable even if there are no
  // requests pending.  `haveRequestsEventHandler' will get called on the
  // EventLoop thread, notice the shutdown state and, if
  // `setCloseEventLoopOnShutdown()' was called, stop the event loop.
}

int RequestPump::tryPost(std::unique_ptr<Request>& req) {
  ld_check(req);
  if (impl_->sem.valueGuess() >= impl_->capacity) {
    err = E::NOBUFS;
    return -1;
  }
  return forcePost(req);
}

int RequestPump::forcePost(std::unique_ptr<Request>& req) {
  ld_check(req);
  if (UNLIKELY(impl_->sem.isShutdown())) {
    err = E::SHUTDOWN;
    return -1;
  }
  req->enqueue_time_ = std::chrono::steady_clock::now();
  impl_->queue.push(std::move(req));
  ld_check(!req);
  impl_->sem.post();
  return 0;
}

void RequestPump::haveRequestsEventHandler(void* arg, short what) {
  RequestPump* self = static_cast<RequestPump*>(arg);
  if (!(what & EV_READ)) {
    ld_error("Got an unexpected event on request pipe: what=%d", what);
    ld_check(false);
  }
  ld_check(self->impl_->sem_waiter);
  try {
    auto cb = [self](size_t n) { self->onRequestsPending(n); };
    // processBatch() decrements the semaphore by some amount and calls our
    // callback with the amount.  We're guaranteed to have at least that many
    // items in the MPSCQueue, because the producer pushes into the queue
    // first then increments the semaphore.
    self->impl_->sem_waiter->processBatch(cb, self->requestsPerIteration_);
  } catch (const folly::ShutdownSemError&) {
    struct event_base* base =
        LD_EV(event_get_base)(self->impl_->requests_pending_event);
    // First delete the event since the fd is about to go away
    ld_check(self->impl_->requests_pending_event);
    LD_EV(event_free)(self->impl_->requests_pending_event);
    self->impl_->requests_pending_event = nullptr;

    // Destroy the AsyncWaiter, which also closes the fd
    self->impl_->sem_waiter.reset();

    // If requested, instruct the event loop to stop
    if (self->impl_->close_event_loop_on_shutdown) {
      int rv = LD_EV(event_base_loopbreak)(base);
      if (UNLIKELY(rv != 0)) {
        ld_error("FATAL: event_base_loopbreak() failed");
        ld_check(false);
      }
    }
  }
}

void RequestPump::onRequestsPending(size_t nrequests) {
  using namespace std::chrono;

  bool on_worker_thread = ThreadID::isWorker();

  for (size_t i = 0; i < nrequests; i++) {
    std::unique_ptr<Request> rq = impl_->queue.pop();
    ld_check(rq); // otherwise why did the semaphore wake us?
    if (UNLIKELY(!rq)) {
      RATELIMIT_CRITICAL(
          1s, 1, "INTERNAL ERROR: got a NULL request pointer from RequestPump");
      continue;
    }

    auto rqtype = rq->type_;
    RunState run_state(rqtype);

    if (on_worker_thread) {
      auto queue_time{
          duration_cast<microseconds>(steady_clock::now() - rq->enqueue_time_)};
      HISTOGRAM_ADD(
          Worker::stats(), requests_queue_latency, queue_time.count());
      if (queue_time > 20ms) {
        RATELIMIT_WARNING(5s,
                          10,
                          "Request queued for %g msec: %s (id: %lu)",
                          queue_time.count() / 1000.0,
                          rq->describe().c_str(),
                          rq->id_.val());
      }

      Worker::onStartedRunning(run_state);
    }

    // rq should not be accessed after execute, as it may have been deleted.
    Request::Execution status = rq->execute();

    if (on_worker_thread) {
      Worker::onStoppedRunning(run_state);
      Request::bumpStatsWhenExecuted();
    }

    switch (status) {
      case Request::Execution::COMPLETE:
        break;
      case Request::Execution::CONTINUE:
        rq.release();
        break;
    }
  }
  impl_->queue.compact();
}

}} // namespace facebook::logdevice
