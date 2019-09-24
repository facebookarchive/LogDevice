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

static std::unique_ptr<EvBase> createEventBase(EvBase::EvBaseType base_type) {
  std::unique_ptr<EvBase> result;
  auto base = std::make_unique<EvBase>();
  base->selectEvBase(base_type);
  auto rv = base->init();
  switch (rv) {
    case EvBase::Status::NO_MEM:
      ld_error("Failed to create an event base for an EventLoop thread");
      err = Status::NOMEM;
      break;
    case EvBase::Status::INVALID_PRIORITY:
      ld_error("failed to initialize eventbase priorities");
      err = Status::SYSLIMIT;
      break;
    case EvBase::Status::OK:
      result = std::move(base);
      break;
    default:
      ld_error("Internal error when initializing EvBase");
      err = Status::INTERNAL;
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
        requests_per_iteration,
    EvBase::EvBaseType base_type)
    : thread_type_(thread_type),
      thread_name_(thread_name),
      priority_queues_enabled_(enable_priority_queues) {
  Semaphore initialized;
  Status init_result{Status::INTERNAL};
  thread_ = std::thread([request_pump_capacity,
                         &requests_per_iteration,
                         &init_result,
                         &initialized,
                         &base_type,
                         this]() {
    auto res = init_result =
        init(base_type, request_pump_capacity, requests_per_iteration);
    initialized.post();
    if (res == Status::OK) {
      run();
    }
  });
  initialized.wait();
  if (init_result != Status::OK) {
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

Status EventLoop::init(
    EvBase::EvBaseType base_type,
    size_t request_pump_capacity,
    const std::array<uint32_t, EventLoopTaskQueue::kNumberOfPriorities>&
        requests_per_iteration) {
  tid_ = syscall(__NR_gettid);
  ThreadID::set(thread_type_, thread_name_);

  base_ = std::unique_ptr<EvBase>(createEventBase(base_type));
  if (!base_) {
    return err;
  }

  task_queue_ = std::make_unique<EventLoopTaskQueue>(
      *base_, request_pump_capacity, requests_per_iteration);
  task_queue_->setCloseEventLoopOnShutdown();

  return Status::OK;
}

void EventLoop::run() {
  EventLoop::thisThreadLoop_ = this; // save in a thread-local
  // this runs until we get destroyed or shutdown is called on
  // EventLoopTaskQueue
  auto status = base_->loop();
  if (status != EvBase::Status::OK) {
    ld_error("EvBase::loop() exited abnormally");
  }
  // the thread on which this EventLoop ran terminates here
}

}} // namespace facebook::logdevice
