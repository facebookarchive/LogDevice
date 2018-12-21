/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/SharedMutex.h>

#include "logdevice/common/Timestamp.h"

namespace facebook { namespace logdevice {

// A data structure for estimating rate of events in a moving window,
// e.g. average append rate in bytes/s over the last 10 minutes.
//
// Somewhat similar to folly::BucketedTimeSeries. Differences are:
//  * Thread safe. Uses a folly::SharedMutex that is rarely locked exclusively.
//  * Window size can be changed on the fly.
//  * Number of buckets is hard-coded to 2.
//  * Smaller: 48 bytes vs 88 bytes. (But SharedMutex might have some
//    core-local memory footprint behind the scenes.)
//  * Doesn't provide the features for which these 88 bytes are needed:
//    totals, counts, averages.
class RateEstimator {
 public:
  // `now` represents the time when observation started.
  // Duration returned by getRate() will never extend past that time point.
  explicit RateEstimator(SteadyTimestamp now = SteadyTimestamp::now());

  void addValue(int64_t val,
                std::chrono::milliseconds window,
                SteadyTimestamp now = SteadyTimestamp::now());

  // Returns n and the total value during the last n milliseconds.
  // n is typically between `window` and `window * 2`.
  // Can be bigger if window size was recently decreased.
  // Can be smaller if window size was recently increased or if
  // the RateEstimator was created less than `window` ago.
  std::pair<int64_t, std::chrono::milliseconds>
  getRate(std::chrono::milliseconds window,
          SteadyTimestamp now = SteadyTimestamp::now()) const;

  // Reset everything into a state as if we never received any values.
  // `now` has the same meaning as in constructor.
  void clear(SteadyTimestamp now = SteadyTimestamp::now());

 private:
  // prevBucketSum_ is sum of values in [prevBucketStart_, currentBucketStart_).
  // currentBucketSum_ - in [currentBucketStart_, lastSeen_).
  // The atomic fields can be updated with mutex_ locked in shared mode.
  // The non-atomic fields require exclusive lock to write, shared lock to read.
  SteadyTimestamp prevBucketStart_ = SteadyTimestamp::min();
  SteadyTimestamp currentBucketStart_;
  int64_t prevBucketSum_ = 0;
  std::atomic<int64_t> currentBucketSum_{0};
  AtomicSteadyTimestamp lastSeen_;

  folly::SharedMutex mutex_;
};
}} // namespace facebook::logdevice
