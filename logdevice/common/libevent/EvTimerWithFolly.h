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
#include <folly/io/async/AsyncTimeout.h>

#include "logdevice/common/libevent/EvBaseWithFolly.h"

struct timeval;
namespace facebook { namespace logdevice {

class EvTimerWithFolly : public folly::AsyncTimeout {
 public:
  EvTimerWithFolly(IEvBase* base) : folly::AsyncTimeout(base) {}
  void attachCallback(IEvBase::EventCallback callback) {
    callback_ = std::move(callback);
  }
  void timeoutExpired() noexcept override {
    callback_();
  }
  /**
   * This is unsupported for folly evenbase, you need to use AsyncTimeout.
   */
  static const timeval* getCommonTimeout(std::chrono::microseconds timeout) {
    return nullptr;
  }

  int setPriority(int pri);

  void activate(int res, short ncalls);

 private:
  IEvBase::EventCallback callback_;
};

}} // namespace facebook::logdevice
