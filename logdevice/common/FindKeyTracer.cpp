/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FindKeyTracer.h"

#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {
void FindKeyTracer::trace(Status st, lsn_t result_lo, lsn_t result_hi) {
  auto latency_us = usec_since(start_time_);

  auto sample_builder = [&]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addIntValue("latency_us", latency_us);
    sample->addNormalValue("status", error_name(st));
    sample->addIntValue(
        "is_approximate_enforced", (uint8_t)is_approximate_enforced_);
    sample->addNormalValue(
        "type",
        (header_.flags & FINDKEY_Header::USER_KEY) ? "FIND_KEY" : "FIND_TIME");
    sample->addNormalValue("accuracy",
                           (header_.flags & FINDKEY_Header::APPROXIMATE)
                               ? "APPROXIMATE"
                               : "EXACT");
    // Who (Client Address)
    std::string client_sock_str = from_.toStringNoPort();
    sample->addNormalValue("client_sock_addr", client_sock_str);
    // Which log (LogID)
    sample->addIntValue("log_id", header_.log_id.val());
    // Log group path
    auto config = logger_->getConfiguration();
    sample->addNormalValue(
        "log_group_path",
        config->getLogGroupPath(header_.log_id).value_or("UNKNOWN"));
    if (key_) {
      sample->addNormalValue("query_key", key_.value());
    }
    if (timestamp_) {
      sample->addIntValue("query_timestamp", timestamp_.value().count());
    }
    // result-lo , result-hi
    sample->addIntValue("result_lo", result_lo);
    sample->addIntValue("result_hi", result_hi);
    return sample;
  };

  // logger is nullptr in case we created an empty tracer FindKeyTracer().
  // This is used in test cases.
  if (logger_) {
    publish(FINDKEY_TRACER, sample_builder);
  }
}
}} // namespace facebook::logdevice
