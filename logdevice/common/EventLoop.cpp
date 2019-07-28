/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/EventLoop.h"

#include <errno.h>
#include <unistd.h>

#include <event2/event.h>
#include <folly/Memory.h>
#include <folly/container/Array.h>
#include <folly/io/async/Request.h>
#include <sys/syscall.h>
#include <sys/types.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/EventHandler.h"
#include "logdevice/common/EventLoopTaskQueue.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

thread_local EventLoop* EventLoop::thisThreadLoop_{nullptr};

static std::unique_ptr<EvBase> createEventBase() {
  std::unique_ptr<EvBase> result;
  auto base = std::make_unique<EvBase>();
  auto rv = base->init();
  switch (rv) {
    case EvBase::Status::NO_MEM:
      ld_error("Failed to create an event base for an EventLoop thread");
      err = E::NOMEM;
      break;
    case EvBase::Status::INVALID_PRIORITY:
      ld_error("failed to initialize eventbase priorities");
      err = E::SYSLIMIT;
      break;
    case EvBase::Status::OK:
      result = std::move(base);
      break;
    default:
      ld_error("Internal error when initializing EvBase");
      err = E::INTERNAL;
      break;
  }
  return result;
}

EventLoop::EventLoop(
    std::string thread_name,
    ThreadID::Type thread_type,
    size_t request_pump_capacity,
    bool enable_priority_queues,
    const std::array<uint32_t, EventLoopTaskQueue::kNumberOfPriorities>&
        requests_per_iteration)
    : thread_type_(thread_type),
      thread_name_(thread_name),
      disposer_(this),
      priority_queues_enabled_(enable_priority_queues) {
  Semaphore initialized;
  Status init_result{E::INTERNAL};
  thread_ = std::thread([request_pump_capacity,
                         &requests_per_iteration,
                         &init_result,
                         &initialized,
                         this]() {
    auto res = init_result =
        init(request_pump_capacity, requests_per_iteration);
    initialized.post();
    if (res == E::OK) {
      run();
    }
  });
  initialized.wait();
  if (init_result != E::OK) {
    err = init_result;
    thread_.join();
    throw ConstructorFailed();
  }
}

EventLoop::~EventLoop() {
  // Shutdown drains all the work contexts before invoking this destructor.
  ld_check(num_references_.load() == 0);
  if (!thread_.joinable()) {
    return;
  }
  // We just shutdown here explicitly, join the thread and delete
  // the eventloop instance.
  // Tell EventLoop on the other end to destroy itself and terminate the
  // thread
  task_queue_->shutdown();
  thread_.join();
}

void EventLoop::add(folly::Function<void()> func) {
  addWithPriority(std::move(func), folly::Executor::LO_PRI);
}

void EventLoop::addWithPriority(folly::Function<void()> func, int8_t priority) {
  task_queue_->addWithPriority(
      std::move(func),
      priority_queues_enabled_ ? priority : folly::Executor::HI_PRI);
}

void EventLoop::delayCheckCallback(void* arg, short) {
  EventLoop* self = (EventLoop*)arg;
  using namespace std::chrono;
  using namespace std::chrono_literals;
  auto now = steady_clock::now();
  if (self->scheduled_event_start_time_ != steady_clock::time_point::min()) {
    evtimer_add(self->scheduled_event_, self->getCommonTimeout(1s));
    if (now > self->scheduled_event_start_time_) {
      auto diff = now - self->scheduled_event_start_time_;
      uint64_t cur_delay = duration_cast<microseconds>(diff).count();
      self->delay_us_.fetch_add(cur_delay, std::memory_order_relaxed);
    }
    self->scheduled_event_start_time_ = steady_clock::time_point::min();
  } else {
    evtimer_add(self->scheduled_event_, self->getZeroTimeout());
    self->scheduled_event_start_time_ = now;
  }
}

E EventLoop::init(
    size_t request_pump_capacity,
    const std::array<uint32_t, EventLoopTaskQueue::kNumberOfPriorities>&
        requests_per_iteration) {
  tid_ = syscall(__NR_gettid);
  ThreadID::set(thread_type_, thread_name_);

  base_ = std::unique_ptr<EvBase>(createEventBase());
  if (!base_) {
    return err;
  }
  scheduled_event_ = LD_EV(event_new)(
      getEventBase(), -1, 0, EventHandler<EventLoop::delayCheckCallback>, this);
  if (!scheduled_event_) {
    return E::INTERNAL;
  }
  common_timeouts_ =
      std::make_unique<TimeoutMap>(getEventBase(), kMaxFastTimeouts);
  task_queue_ = std::make_unique<EventLoopTaskQueue>(
      getEventBase(), request_pump_capacity, requests_per_iteration);
  task_queue_->setCloseEventLoopOnShutdown();
  return E::OK;
}
void EventLoop::run() {
  EventLoop::thisThreadLoop_ = this; // save in a thread-local

  // Initiate runs to detect eventloop delays.
  using namespace std::chrono_literals;
  delay_us_.store(0);
  scheduled_event_start_time_ = std::chrono::steady_clock::time_point::min();
  evtimer_add(scheduled_event_, getCommonTimeout(1s));

  // this runs until we get destroyed or shutdown is called on
  // EventLoopTaskQueue
  auto status = base_->loop();
  if (status != EvBase::Status::OK) {
    ld_error("EvBase::loop() exited abnormally");
  }
  LD_EV(event_free)(scheduled_event_);
  // the thread on which this EventLoop ran terminates here
}

void EventLoop::dispose(ZeroCopyPayload* payload) {
  disposer_.dispose(payload);
}
}} // namespace facebook::logdevice
