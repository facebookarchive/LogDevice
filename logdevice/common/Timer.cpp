/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Timer.h"

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RunContext.h"
#include "logdevice/common/WheelTimer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

using namespace std::chrono;

namespace {
class LibEventTimerImpl : public TimerInterface, public LibeventTimer {
 public:
  LibEventTimerImpl();

  void assign(std::function<void()> callback) override;

  void activate(std::chrono::microseconds delay,
                TimeoutMap* timeout_map = nullptr) override {
    LibeventTimer::activate(delay, timeout_map);
  }

  void cancel() override {
    LibeventTimer::cancel();
  }

  bool isActive() const override {
    return LibeventTimer::isActive();
  }

  void setCallback(std::function<void()> callback) override {
    if (isAssigned()) {
      LibeventTimer::setCallback(callback);
    } else {
      auto ev_loop = EventLoop::onThisThread();
      ld_check(ev_loop);
      LibeventTimer::assign(ev_loop->getEventBase(), callback);
    }
  }

  bool isAssigned() const override {
    return LibeventTimer::isAssigned();
  }

 private:
  LibEventTimerImpl(const LibEventTimerImpl&) = delete;
  LibEventTimerImpl(LibEventTimerImpl&&) = delete;
  LibEventTimerImpl& operator=(const LibEventTimerImpl&) = delete;
  LibEventTimerImpl& operator=(LibEventTimerImpl&&) = delete;

  using LibeventTimer::assign;
};

class WheelTimerDispatchImpl : public TimerInterface {
 public:
  WheelTimerDispatchImpl();

  explicit WheelTimerDispatchImpl(std::function<void()> callback);

  void activate(std::chrono::microseconds delay,
                TimeoutMap* timeout_map = nullptr) override;

  void cancel() override;

  bool isActive() const override;

  void setCallback(std::function<void()> callback) override;

  void assign(std::function<void()> callback) override {
    setCallback(callback);
  }

  ~WheelTimerDispatchImpl() override;

  bool isAssigned() const override {
    return !!callback_;
  }

 private:
  WheelTimerDispatchImpl(const WheelTimerDispatchImpl&) = delete;
  WheelTimerDispatchImpl(WheelTimerDispatchImpl&&) = delete;
  WheelTimerDispatchImpl& operator=(WheelTimerDispatchImpl&&) = delete;
  WheelTimerDispatchImpl& operator=(const WheelTimerDispatchImpl&) = delete;

  decltype(auto) makeWheelTimerInternalExecutor(Worker* worker);

  RunContext workerRunContext_;
  // it always will be called on a timer creator thread, which should exist
  std::function<void()> callback_;
  std::shared_ptr<std::atomic<bool>> is_canceled_;
  bool is_activated_{false};
};

decltype(auto)
WheelTimerDispatchImpl::makeWheelTimerInternalExecutor(Worker* worker) {
  return [timer = this, canceled = is_canceled_, worker]() mutable {
    if (!*canceled) {
      auto start_time = steady_clock::now();
      worker->addWithPriority(
          [canceled = std::move(canceled), timer, start_time]() {
            if (!*canceled && timer->callback_) {
              auto run_context = timer->workerRunContext_;
              auto diff =
                  duration_cast<milliseconds>(steady_clock::now() - start_time);
              WORKER_STAT_ADD(wh_timer_sched_delay, diff.count());
              Worker::onStartedRunning(run_context);
              // Make a local copy of callback to make sure it's not destroyed
              // while it's running, in particular if it calls setCallback().
              {
                std::function<void()> cb = timer->callback_;

                cb();
                // `timer` might have been destroyed.
              }
              Worker::onStoppedRunning(run_context);
            }
          },
          folly::Executor::MID_PRI);
    }
  };
}

} // namespace

// Sometimes the worker is unavailable i.e. in tests and we cannot assign.
LibEventTimerImpl::LibEventTimerImpl() : LibeventTimer() {}

void LibEventTimerImpl::assign(std::function<void()> callback) {
  assign(EventLoop::onThisThread()->getEventBase(), callback);
}

WheelTimerDispatchImpl::WheelTimerDispatchImpl() {}

WheelTimerDispatchImpl::WheelTimerDispatchImpl(std::function<void()> callback) {
  setCallback(std::move(callback));
}

void WheelTimerDispatchImpl::setCallback(std::function<void()> callback) {
  callback_ = [this, callback = std::move(callback)] {
    is_activated_ = false;
    callback();
  };
}

bool WheelTimerDispatchImpl::isActive() const {
  return is_activated_;
}

void WheelTimerDispatchImpl::cancel() {
  if (is_activated_) {
    is_canceled_->store(true);
    is_activated_ = false;
  }
}

void WheelTimerDispatchImpl::activate(microseconds delay, TimeoutMap*) {
  // reactivation: should cancel the old one
  if (is_activated_) {
    // it seems here should be a race condition
    // but we had it before otherwise how is possible to reactivate old timer?
    is_canceled_->store(true);
  }

  is_canceled_ = std::make_shared<std::atomic<bool>>(false);
  auto worker = Worker::onThisThread();

  worker->processor_->getWheelTimer().createTimer(
      makeWheelTimerInternalExecutor(worker),
      duration_cast<milliseconds>(delay));

  workerRunContext_ = worker->currentlyRunning_;
  is_activated_ = true;
}

WheelTimerDispatchImpl::~WheelTimerDispatchImpl() {
  cancel();
}

Timer::Timer() {}

Timer::Timer(std::function<void()> callback) {
  getTimerImpl().assign(callback);
}

TimerInterface& Timer::getTimerImpl() const {
  if (!impl_) {
    // This is called from tests and ldbench workers. Caller cannot assume
    // Worker interface to be available in those cases.
    auto worker = Worker::onThisThread(false /* enforce_worker */);
    if (worker && worker->updateable_settings_->enable_hh_wheel_backed_timers) {
      impl_ = std::make_unique<WheelTimerDispatchImpl>();
    } else {
      impl_ = std::make_unique<LibEventTimerImpl>();
    }
  }
  return *impl_;
}

}} // namespace facebook::logdevice
