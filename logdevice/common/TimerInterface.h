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

namespace facebook { namespace logdevice {

class TimeoutMap;

class TimerInterface {
 public:
  virtual void activate(std::chrono::microseconds delay,
                        TimeoutMap* timeout_map = nullptr) = 0;
  virtual void cancel() = 0;
  virtual bool isActive() const = 0;
  virtual void setCallback(std::function<void()> callback) = 0;
  virtual void assign(std::function<void()> callback) = 0;
  virtual bool isAssigned() const = 0;
  virtual ~TimerInterface() = default;
};

}} // namespace facebook::logdevice
