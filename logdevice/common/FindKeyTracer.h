/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/protocol/FINDKEY_Message.h"

namespace facebook { namespace logdevice {

class TraceLogger;
struct NodeID;

constexpr auto FINDKEY_TRACER = "findkey";

class FindKeyTracer : public SampledTracer {
 public:
  explicit FindKeyTracer(std::shared_ptr<TraceLogger> logger,
                         const Sockaddr& from,
                         const FINDKEY_Header& header)
      : SampledTracer(std::move(logger)), from_(from), header_(header) {
    start_time_ = std::chrono::steady_clock::now();
  }

  FindKeyTracer() : SampledTracer(nullptr) {}

  void trace(Status st, lsn_t result_lo, lsn_t result_hi);
  void setApproximateEnforced(bool enforced) {
    is_approximate_enforced_ = enforced;
  }
  void setTimestamp(std::chrono::milliseconds timestamp) {
    timestamp_ = timestamp;
  }
  void setKey(const std::string& key) {
    key_ = key;
  }

 private:
  Sockaddr from_;
  FINDKEY_Header header_;
  // If approximate is enforced and flags have been overridden we want to know
  // that
  bool is_approximate_enforced_ = false;
  folly::Optional<std::chrono::milliseconds> timestamp_;
  folly::Optional<std::string> key_;
  // The time when we received this message. This is used to measure how long
  // did it take us to respond to this request.
  std::chrono::steady_clock::time_point start_time_;
};
}} // namespace facebook::logdevice
