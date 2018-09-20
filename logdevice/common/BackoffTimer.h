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

/**
 * @file Interface for backoff timers such as ExponentialBackoffTimer.
 *
 *       The API is tailored for timers that deal with error conditions.
 *
 *       The timer can be active or inactive.  If it is active, the callback
 *       will get called after some time.  The timer is initially inactive.
 */

namespace facebook { namespace logdevice {

class BackoffTimer {
 public:
  // Timers run at millisecond precision.
  typedef std::chrono::milliseconds Duration;

  virtual ~BackoffTimer() = default;

  /**
   * Sets the callback to be called when the timer fires.
   *
   * A typical pattern is for the BackoffTimer object to be a member in a
   * class and for the callback to want to call a method of the owning class.
   * This is safe; as long as the timer object is destroyed with the class,
   * there is no risk of the callback getting invoked when the parent class
   * has already been destroyed.
   */
  virtual void setCallback(std::function<void()> callback) = 0;

  /**
   * If the timer is inactive, activates the timer to fire once.  If the timer
   * is already active, does nothing.
   *
   * Assuming the timer is used to handle an error condition, this is called
   * when the error was observed, in order to retry the action later.
   *
   * After the timer fires, it will not fire again unless activate() is called
   * again.  Typically, the callback retries whatever action caused the timer
   * to get activated, then calls activate() if the action fails again or
   * reset() if it succeeds.
   *
   * The delay depends on how many times the timer was previously activated
   * and fired without calling cancel().
   */
  virtual void activate() = 0;

  /**
   * Cancels the timer (if active) and resets the delay.
   *
   * Assuming the timer is used to handle an error condition, this is called
   * when the error has gone away.
   */
  virtual void reset() = 0;

  /**
   * Cancels the timer (if active) without resetting the delay.
   */
  virtual void cancel() = 0;

  virtual bool isActive() const = 0;

  virtual Duration getNextDelay() const = 0;
};

}} // namespace facebook::logdevice
