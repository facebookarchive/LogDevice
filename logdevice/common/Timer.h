/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/LibeventTimer.h"

/**
 * @file
 * Convenience class for a timer that runs on libevent.
 *
 * This class is not thread-safe in general.  All operations (including
 * construction, and destruction if the timer is active) should be mode
 * on a single thread.
 */

namespace facebook { namespace logdevice {

class Worker;

class Timer : public LibeventTimer {
 public:
  Timer();

  explicit Timer(std::function<void()> callback);

  virtual void assign(std::function<void()> callback);

 private:
  using LibeventTimer::assign;
};

}} // namespace facebook::logdevice
