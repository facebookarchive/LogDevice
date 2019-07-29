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
class EvTimer {
 public:
  static const timeval* getCommonTimeout(std::chrono::microseconds timeout);
};
}} // namespace facebook::logdevice
