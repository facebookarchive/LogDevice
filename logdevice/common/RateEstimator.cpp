/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RateEstimator.h"

namespace facebook { namespace logdevice {

RateEstimator::RateEstimator(SteadyTimestamp now)
    : currentBucketStart_(now), lastSeen_(now) {}

void RateEstimator::addValue(int64_t val,
                             std::chrono::milliseconds window,
                             SteadyTimestamp now) {
  ld_check(window.count() > 0);

  auto add_to_current_bucket = [&] {
    currentBucketSum_.fetch_add(val, std::memory_order_relaxed);
    // This can race with another call to addValue(), but the timestamps
    // would be very close, so it doesn't matter. We could do storeMax(),
    // but it doesn't seem worth the added cost of a compare-exchange.
    // Note: AtomicTimestamp::operator=() uses memory_order_relaxed.
    lastSeen_ = now;
  };

  {
    folly::SharedMutex::ReadHolder read_lock(mutex_);

    SteadyTimestamp current_bucket_end = currentBucketStart_ + window;
    ld_check(current_bucket_end > currentBucketStart_); // didn't overflow

    if (now < current_bucket_end) {
      // Fast path - no need to switch buckets.
      add_to_current_bucket();
      return;
    }
  }

  // Need to switch buckets. Grab an exclusive lock.
  folly::SharedMutex::WriteHolder write_lock(mutex_);

  SteadyTimestamp current_bucket_end = currentBucketStart_ + window;
  ld_check(current_bucket_end > currentBucketStart_); // didn't overflow

  if (now < current_bucket_end) {
    // Another thread beat us to switching buckets.
    add_to_current_bucket();
    return;
  }

  // Switch buckets.

  SteadyTimestamp last_seen = lastSeen_;

  if (last_seen >= current_bucket_end) {
    // Current bucket is already bigger than window, presumably because window
    // has just been decreased.
    // Leave the big bucket as is and start a new one just above it.
    // Note: instead, we could shrink the current bucket and linearly
    // interpolate the total value in it, but it seems cleaner to not do any
    // interpolation in this class and let the caller of getRate() decide what
    // to do if the reported window is much bigger than requested.
    current_bucket_end = last_seen;
  }

  if (now - current_bucket_end < window) {
    // Start a new bucket right after the current one.
    prevBucketStart_ = currentBucketStart_;
    prevBucketSum_ = currentBucketSum_.load();
    currentBucketStart_ = current_bucket_end;
    currentBucketSum_.store(val);
    lastSeen_.store(now);
    return;
  }

  // We didn't get any values for at least one full window. Fast-forward
  // through all the empty windows between the lastSeen_ and now.
  // Note: instead of fast-forwarding by an integer number of windows, we
  // could just set currentBucketStart_ = now - window. But it doesn't feel
  // right to let window boundaries depend on data. It may bias the estimate
  // in some obscure way. The same concern applies to the
  // `last_seen >= current_bucket_end` case above, and to the empty
  // window case in getRate(), but those seem acceptable.
  int64_t num_windows_skipped = (now - current_bucket_end) / window;
  currentBucketStart_ = current_bucket_end + window * num_windows_skipped;
  prevBucketStart_ = currentBucketStart_ - window;
  prevBucketSum_ = 0;
  currentBucketSum_.store(val);
  lastSeen_.store(now);

  ld_check(num_windows_skipped > 0);
  ld_check(currentBucketStart_ > current_bucket_end);
  ld_check(currentBucketStart_ <= now);
  ld_check(currentBucketStart_ + window > now);
  ld_check(prevBucketStart_ >= last_seen);
}

std::pair<int64_t, std::chrono::milliseconds>
RateEstimator::getRate(std::chrono::milliseconds window,
                       SteadyTimestamp now) const {
  ld_check(window.count() > 0);

  folly::SharedMutex::ReadHolder read_lock(mutex_);

  if (now - lastSeen_ >= window) {
    // We didn't get any values for a full window.
    return std::make_pair(0ul, window);
  }

  auto current_bucket_duration =
      std ::chrono::duration_cast<std::chrono::milliseconds>(
          now - currentBucketStart_);
  if (current_bucket_duration >= window ||
      prevBucketStart_ == SteadyTimestamp::min()) {
    // Current bucket is long enough or is the only one.
    // No need to look at previous bucket.
    return std::make_pair(currentBucketSum_.load(), current_bucket_duration);
  }

  // Use both buckets.
  return std::make_pair(prevBucketSum_ + currentBucketSum_.load(),
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            now - prevBucketStart_));
}

void RateEstimator::clear(SteadyTimestamp now) {
  folly::SharedMutex::WriteHolder write_lock(mutex_);
  prevBucketStart_ = SteadyTimestamp::min();
  currentBucketStart_ = now;
  prevBucketSum_ = 0;
  currentBucketSum_.store(0);
  lastSeen_.store(now);
}

}} // namespace facebook::logdevice
