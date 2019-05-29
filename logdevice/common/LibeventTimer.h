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

#include <boost/noncopyable.hpp>
#include <folly/IntrusiveList.h>
#include <folly/io/async/Request.h>

#include "event2/event_struct.h"
#include "logdevice/common/RunContext.h"

/**
 * @file
 * Convenience class for a timer that runs on libevent.
 *
 * This class is not thread-safe in general.  All operations (including
 * construction, and destruction if the timer is active) should be mode
 * on a single thread.
 */

namespace facebook { namespace logdevice {

class TimeoutMap;
class Worker;

class LibeventTimer : boost::noncopyable {
 public:
  LibeventTimer();

  /**
   * Creates an initially inactive timer.
   *
   * A typical pattern is for the object to be a member in a class and for the
   * callback to want to call a method of the owning class.  This is safe; as
   * long as the timer object is destroyed with the class, there is no risk of
   * the callback getting invoked when the parent class has already been
   * destroyed. It's also safe to destroy the owning LibeventTimer object within
   * the callback.
   *
   * @param base  event base to attach events to throughout the lifetime of the
   *              timer object
   * @param callback  to be called (with this LibeventTimer object as argument)
   *                  when timer fires, must be copyable
   */
  LibeventTimer(struct event_base* base, std::function<void()> callback);

  // Uses nullptr callback. setCallback() must be called before the timer is
  // activated.
  LibeventTimer(struct event_base* base);

  /**
   * Activates the timer to fire once after the specified delay.  Microseconds
   * are the granularity which libevent offers.  Larger std::chrono::duration
   * classes implicitly convert to smaller ones so you can call this with
   * seconds or milliseconds too.
   *
   * If a TimeoutMap object is provided, it will be used to convert the given
   * delay to struct timeval (possibly using libevent's common timeout
   * optimization).
   *
   * If activate() is called while the timer is already active, it effectively
   * cancels the previous timer.
   */
  virtual void activate(std::chrono::microseconds delay,
                        TimeoutMap* timeout_map = nullptr);

  /**
   * Cancels the timer if active.
   */
  virtual void cancel();

  /**
   * Is the timer active?
   */
  virtual bool isActive() const;

  /**
   * Has this timer been assigned to an event base?
   */
  virtual bool isAssigned() const;

  /**
   * Changes the callback.
   */
  virtual void setCallback(std::function<void()> callback) {
    callback_ = callback;
  }

  virtual ~LibeventTimer();

  // Methods are virtual to allow MockLibeventTimer to override in tests

  virtual void assign(struct event_base* base, std::function<void()> callback);

 private:
  /**
   * Variant of activate() that takes a struct timeval.
   */
  virtual void activate(const struct timeval* delay);

  // Called by libevent when the timer goes off.  Invokes the supplied
  // callback.
  static void libeventCallback(void* instance, short);

  bool initialized_{false};

  struct event timer_;

  // Use to save off worker context and used in LibeventTimer::libeventCallback
  // when timeout happens.
  Worker* worker_;

  std::function<void()> callback_;
  bool active_ = false;

  // The worker run context that this timer was activated in. Will be propagated
  // with all callbacks.
  RunContext workerRunContext_;
};

}} // namespace facebook::logdevice
