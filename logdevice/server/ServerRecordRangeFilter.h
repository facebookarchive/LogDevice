/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <chrono>
#include <sstream>
#include <string>

#include "logdevice/common/ServerRecordFilter.h"

namespace facebook { namespace logdevice {

/**
 * @file ServerRecordRangeFilter is an implementation of range-based filter
 *       with uppper and lower limits. It is used to filter record by key
 *       in ServerReadStream. Experimental feature: Use with caution.
 */

class ServerRecordRangeFilter final : public ServerRecordFilter {
 public:
  /**
   * @param lo   low limit for range(inclusive)
   * @param hi   high limit for range(inclusive)
   */
  explicit ServerRecordRangeFilter(folly::StringPiece lo, folly::StringPiece hi)
      : low_limit_(lo.str()), high_limit_(hi.str()) {}

  /**
   * @param record_key   key of record or string you wish to be filtered
   * @return             whether input string will be filtered out
   */
  bool operator()(folly::StringPiece record_key) override {
    return record_key >= low_limit_ && record_key <= high_limit_;
  }

  /**
   *  @return             A human-readable string which describes this
   *                      server-side filter.
   */
  std::string toString() const override {
    std::stringstream ss;
    ss << "Server-side filter type: RANGE, lo: " << low_limit_
       << ", hi: " << high_limit_;
    return ss.str();
  }

 private:
  const std::string low_limit_;
  const std::string high_limit_;
};
}} // namespace facebook::logdevice
