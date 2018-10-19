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

#include <folly/Range.h>

#include "logdevice/common/ServerRecordFilter.h"

namespace facebook { namespace logdevice {

/**
 *  @file ServerRecordFilter is used by ServerReadStream to filter out
 *  records when reading from LocalLogStore before sending to client. It will
 *  be created by ServerRecordFilterFactory and owned by ServerReadStream.
 *  Experimental Feature: Use with caution.
 */

class ServerRecordEqualityFilter final : public ServerRecordFilter {
 public:
  /**
   *  @param filter_key   A string which could be used to filter out records
   *         type         Filter type which is defined in ServerRecordFilter.h
   */
  explicit ServerRecordEqualityFilter(folly::StringPiece key)
      : filter_key_(key.str()) {}

  /**
   *  @param filter_key   key of record or string you wish to be filtered
   *  @return bool        whether input string will be filtered out
   */
  bool operator()(folly::StringPiece record_key) override {
    return record_key == filter_key_;
  }

  /**
   *  @return             A human-readable string which describes this
   *                      server-side filter.
   */
  std::string toString() const override {
    std::stringstream ss;
    ss << "Server-side filter type: EQUALITY, filter key: " << filter_key_;
    return ss.str();
  }

 private:
  const std::string filter_key_;
};
}} // namespace facebook::logdevice
