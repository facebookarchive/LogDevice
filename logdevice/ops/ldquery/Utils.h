/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

#include <folly/Conv.h>
#include <folly/Optional.h>

namespace facebook { namespace logdevice { namespace ldquery {

/**
 * A few helper functions for converting a value to std::string.
 */

template <class T>
std::string s(const T& val) {
  return folly::to<std::string>(val);
}

std::string s(const folly::Optional<int>& val);

std::string s(const bool& val);

std::string s(const folly::Optional<std::chrono::seconds>& val);

std::string s(const folly::Optional<std::chrono::milliseconds>& val);

}}} // namespace facebook::logdevice::ldquery
