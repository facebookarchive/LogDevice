/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Timer.h"

#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

// Sometimes the worker is unavailable i.e. in tests and we cannot assign.
Timer::Timer() : LibeventTimer() {}

Timer::Timer(std::function<void()> callback)
    : LibeventTimer(Worker::onThisThread()->getEventBase(), callback) {}

void Timer::assign(std::function<void()> callback) {
  assign(Worker::onThisThread()->getEventBase(), callback);
}

}} // namespace facebook::logdevice
