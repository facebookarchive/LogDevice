/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Function.h>
#include <folly/io/async/TimeoutManager.h>

struct timeval;
struct event;
namespace facebook { namespace logdevice {
class EvBaseLegacy;
class EvTimerLegacy {
 public:
  using Callback = folly::Function<void()>;
  explicit EvTimerLegacy(EvBaseLegacy* base);
  static const timeval* getCommonTimeout(std::chrono::microseconds timeout);
  void attachTimeoutManager(folly::TimeoutManager* timeoutManager);
  void attachCallback(Callback callback) {
    callback_ = std::move(callback);
  }

  void timeoutExpired() noexcept {
    callback_();
  }

  event* getEvent() const {
    return event_;
  }

  bool scheduleTimeout(uint32_t milliseconds);
  bool scheduleTimeout(folly::TimeoutManager::timeout_type timeout);
  void cancelTimeout();
  bool isScheduled() const;

 private:
  bool isInTimeoutManagerThread();
  static void libeventCallback(int fd, short events, void* arg);
  event* event_;
  Callback callback_;
  EvBaseLegacy* timeout_manager_{nullptr};
};
}} // namespace facebook::logdevice
