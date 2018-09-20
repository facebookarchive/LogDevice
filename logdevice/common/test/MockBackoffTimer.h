/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <utility>

#include "logdevice/common/BackoffTimer.h"

namespace facebook { namespace logdevice {

/**
 * @file No-op implementation of BackoffTimer interface.
 */

class MockBackoffTimer : public BackoffTimer {
 public:
  using BackoffTimer::Duration;
  MockBackoffTimer() noexcept = default;
  void setCallback(std::function<void()> callback) override {
    callback_ = std::move(callback);
  }
  void activate() override {
    active_ = true;
  }
  void reset() override {
    active_ = false;
  }
  void cancel() override {
    active_ = false;
  }
  bool isActive() const override {
    return active_;
  }
  Duration getNextDelay() const override {
    return Duration(123);
  }
  const std::function<void()>& getCallback() const {
    return callback_;
  }

  void trigger() {
    ld_check(isActive());
    ld_check(callback_);
    reset();
    callback_();
  }

 private:
  std::function<void()> callback_;
  bool active_ = false;
};

}} // namespace facebook::logdevice
