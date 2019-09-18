/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

struct timeval;
namespace facebook { namespace logdevice {

class EvTimerWithFolly {
 public:
  /**
   * This is unsupported for folly evenbase, you need to use AsyncTimeout.
   */
  static const timeval* getCommonTimeout(std::chrono::microseconds timeout) {
    return nullptr;
  }
};

}} // namespace facebook::logdevice
