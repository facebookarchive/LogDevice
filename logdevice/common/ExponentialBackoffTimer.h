/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include <boost/noncopyable.hpp>
#include <folly/IntrusiveList.h>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/util.h"

/**
 * @file Implementation of BackoffTimer that wraps timers and uses an
 *       exponential delay between firing.
 *
 *       This class is not thread-safe.  All operations (including
 *       construction and destruction) should be made on a single thread.
 */

namespace facebook { namespace logdevice {

class TimeoutMap;
class Worker;

class ExponentialBackoffTimer : public BackoffTimer, boost::noncopyable {
 public:
  using BackoffTimer::Duration;

  ExponentialBackoffTimer() = default;

  ExponentialBackoffTimer(
      std::function<void()> callback,
      Duration initial_delay,
      Duration max_delay,
      Duration::rep multiplier =
          chrono_expbackoff_t<Duration>::DEFAULT_MULTIPLIER) {
    assign(std::move(callback), initial_delay, max_delay, multiplier);
  }

  ExponentialBackoffTimer(std::function<void()> callback,
                          const chrono_expbackoff_t<Duration>& settings) {
    assign(std::move(callback), settings);
  }

  /**
   * Deactivates any pending timers.
   */
  ~ExponentialBackoffTimer() override;

  void assign(std::function<void()> callback,
              Duration initial_delay,
              Duration max_delay,
              Duration::rep multiplier =
                  chrono_expbackoff_t<Duration>::DEFAULT_MULTIPLIER) {
    assign(std::move(callback),
           chrono_expbackoff_t<Duration>(initial_delay, max_delay, multiplier));
  }

  void assign(std::function<void()> callback,
              const chrono_expbackoff_t<Duration>& settings);

  void updateSettings(const chrono_expbackoff_t<Duration>& settings);

  /**
   * Has this timer been assigned to an event ?
   */
  bool isAssigned() const {
    return timer_.isAssigned();
  }

  void setCallback(std::function<void()> callback) override;

  void activate() override;

  /**
   * Forces the timer to fire and execute the associated callback in the next
   * iteration of the event loop.
   * Also resets the delay counter to its inital value.
   */
  void fire();

  void reset() override;

  bool isActive() const override;

  // Cancels the timer (if active) without resetting the delay.
  void cancel() override;

  /**
   * Sets the TimeoutMap instance to use when adding libevent timers.  This is
   * optional but can reduce the amount of processing in libevent if there are
   * many timers with the same timeout.
   *
   * The TimeoutMap instance must live at least as long as activate() calls
   * happen.
   */
  void setTimeoutMap(TimeoutMap* timeout_map) {
    ld_check(randomization_ == 0);
    timeout_map_ = timeout_map;
  }

  /**
   * Add some randomness to the delay period when scheduling the timer.  The
   * purpose is to avoid a bunch of timers firing around the same time,
   * instead diffusing them over time.
   *
   * The actual delay every time the timer is activated will be `expected_delay'
   * * [1 - factor, 1 + factor], where `expected_delay' grows exponentially.
   *
   * NOTE: because this generates many different delays, it must not be used
   * in conjunction with setTimeoutMap().
   */
  void randomize(double factor = 0.5) {
    ld_check(timeout_map_ == nullptr);
    ld_check(factor > 0 && factor <= 1.0);
    randomization_ = factor;
  }

  Duration getCurrentDelay() const {
    return current_delay_;
  }

  /**
   * Get the next actual delay to be used when next activate() is called.
   */
  Duration getNextDelay() const override {
    return next_effective_delay_;
  }

 private:
  Timer timer_;

  // User-defined settings.
  chrono_expbackoff_t<Duration> settings_;

  // Delay that is in use for most recent invocation of activate()
  Duration current_delay_;

  // Theoretical delay to use next time activate() is called.
  // Equal to `initial_delay_` * 2^(num_attempts-1), capped by `max_delay_`.
  Duration next_delay_;

  // Actual delay to use next time activate() is called. This will either be
  // exactly equal to `next_delay_`, or fuzzy if randomize() was called.
  Duration next_effective_delay_;

  double randomization_ = 0;

  TimeoutMap* timeout_map_ = nullptr; // unowned

  /**
   * Calculate the next actual delay which is being used in the following
   * activate() call.
   */
  void calculateNextEffectiveDelay();
};

// Wrapper around ExponentialBackoffTimer that can be placed inside an
// intrusive_list.
struct ExponentialBackoffTimerNode {
  explicit ExponentialBackoffTimerNode(
      std::unique_ptr<ExponentialBackoffTimer> timer)
      : timer(std::move(timer)) {}

  std::unique_ptr<ExponentialBackoffTimer> timer;
  folly::IntrusiveListHook list_hook;
};

}} // namespace facebook::logdevice
