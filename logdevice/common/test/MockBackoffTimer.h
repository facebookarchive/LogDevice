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
 * @file Mock implementation of BackoffTimer interface. If trigger_on_activate_
 * is true, callback is invoked directly inside activate(). Else, activate() is
 * a No-op. Be careful when using this since callback is invoked synchronously
 * inside activate(). For scenarios that require to mock async firing, use the
 * version with trigger_on_activate = false and mock by explicitly calling
 * trigger().
 */

class MockBackoffTimer : public BackoffTimer {
 public:
  using BackoffTimer::Duration;
  MockBackoffTimer(bool trigger_on_activate = false)
      : trigger_on_activate_(trigger_on_activate) {}
  void setCallback(std::function<void()> callback) override {
    callback_ = std::move(callback);
  }
  void activate() override {
    active_ = true;
    if (trigger_on_activate_) {
      callback_();
    }
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
  const bool trigger_on_activate_;
};

}} // namespace facebook::logdevice
