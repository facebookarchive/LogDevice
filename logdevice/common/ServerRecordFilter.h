/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <chrono>
#include <string>

#include <folly/Range.h>

namespace facebook { namespace logdevice {

/**
 * @file This class serves as an interface for server-side filter classes.
 *       There are two implementations right now. Experimental feature:
 *       Use with caution.
 */

/**
 * Currently only two types.
 * 1) EQUALITY means exact match. It describes string equality filter based for
 *    now.
 * 2) RANGE means filter by upper and lower bounds. It describes string
 *    based range filter for now.
 */

enum class ServerRecordFilterType : uint8_t {
  NOFILTER = 0,
  EQUALITY = 1,
  RANGE = 2,
  MAX
};

class ServerRecordFilter {
 public:
  virtual bool operator()(folly::StringPiece key) = 0;
  virtual std::string toString() const = 0;
  virtual ~ServerRecordFilter() {}
};
}} // namespace facebook::logdevice
