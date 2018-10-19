/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/TraceLogger.h"

namespace facebook { namespace logdevice {

class TraceSample;

// Limits the rate of samples: pushes at most `limit` samples per
// `duration` seconds per ThrottledTracer instance.
// If some samples were skipped, it'll add the number of skipped samples to
// sample_rate of the next non-skipped sample. Be careful interpreting these
// numbers in the dataset: if skipped samples weren't followed by non-skipped
// ones, they won'be recorded in any sample_rate field. In particular,
// for a short burst of samples (shorter than `duration`) the first `limit`
// ones will be pushed and the rest will be silently discarded.
//
// Not thread safe.

// The rate limiter.
class TraceThrottler {
 public:
  TraceThrottler(std::string table, std::chrono::seconds duration, size_t limit)
      : table_(table), rate_limiter_(rate_limit_t((limit), duration)) {}

  bool publish(std::function<std::unique_ptr<TraceSample>()> builder,
               TraceLogger* logger) {
    if (logger) {
      bool isLogging = rate_limiter_.isAllowed();
      accum_not_allowed_++;
      if (isLogging) {
        auto sample = builder();
        logger->pushSample(table_, accum_not_allowed_, std::move(sample));
        accum_not_allowed_ = 0;
      }
      return isLogging;
    }
    return false;
  }

 private:
  std::string table_;
  RateLimiter rate_limiter_;
  int32_t accum_not_allowed_ = 0;
};

// A base class for a tracer that contains a rate limiter and a shared_ptr to
// TraceLogger.
class ThrottledTracer {
 public:
  ThrottledTracer(const std::shared_ptr<TraceLogger> logger,
                  std::string table,
                  std::chrono::seconds duration,
                  size_t limit)
      : logger_(logger), throttler_(table, duration, limit) {}

  const std::shared_ptr<TraceLogger> logger_;

 protected:
  bool publish(std::function<std::unique_ptr<TraceSample>()> builder) {
    return throttler_.publish(builder, logger_.get());
  }

  TraceThrottler throttler_;
};
}} // namespace facebook::logdevice
