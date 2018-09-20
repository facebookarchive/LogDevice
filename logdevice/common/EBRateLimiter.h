/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace facebook { namespace logdevice {

/**
 * EBRateLimiter (Exponential Backoff Rate Limiter) is used to rate limit
 * operations such that a burst of operations is allowed every time the number
 * of skipped operations reaches a new order of magnitude during a certain
 * period of time.
 *
 * For example, for a limit of 2, sequential calls to ::isAllowed() will return
 * `true` 2 times, `false` 10 times, `true` 2 times, `false` 100 times and so
 * on and so forth.
 *
 * As an example, the following code should produce the output bellow.
 *
 * \code{.cpp}
 * EBRateLimiter limiter(2, std::chrono::seconds(5));
 *
 * for (size_t i = 0; i < 1117; i++) {
 *   size_t skipped;
 *   if (limiter.isAllowed(skipped)) {
 *     if (skipped) {
 *       printf("Skipped: %zu\n", skipped);
 *     }
 *     printf("MESSAGE\n");
 *   }
 * }
 * \endcode
 *
 * \verbatim
 * MESSAGE
 * MESSAGE
 * Skipped:10
 * MESSAGE
 * MESSAGE
 * Skipped: 100
 * MESSAGE
 * MESSAGE
 * Skipped: 1000
 * MESSAGE
 * \endverbatim
 *
 * The exponentially growing interval is reset after the time interval defined
 * when the object is instantiated. This interval starts to count on the first
 * call to ::isAllowed() after the object was created or it expired the last
 * time.
 */
class EBRateLimiter {
 public:
  /**
   * \param limit Maximum number of operations allowed per burst.
   * \param period Time interval in milliseconds.
   */
  EBRateLimiter(size_t limit, std::chrono::milliseconds period)
      : burst_limit_(limit),
        period_(period),
        burst_(0),
        skipped_(0),
        skipped_limit_(0),
        start_(std::chrono::steady_clock::time_point::min()) {
    /* Because `start_` is being initialized to `min()` the first time
     * ::isAllowed() is called it will be expired and ::reset() is going to be
     * called initializing the remaining members.
     */
  }

  /**
   * Returns `true` if the current time is higher than the sum of the start
   * time and the period passed as an argument to the constructor. The start
   * time is set on the first call to ::isAllowed() after the object is created
   * or it expired the last time.
   */
  bool isExpired() const {
    return std::chrono::steady_clock::now() >= (start_ + period_);
  }

  /**
   * Returns `true` if the operation should be allowed as described above or
   * `false` otherwise.
   *
   * \param skipped Output parameter returning the number of skipped events
   *        that ocurred up until this call.
   */
  bool isAllowed(size_t& skipped) {
    skipped = skipped_;

    if (isExpired()) {
      reset();
    }

    if (burst_ < burst_limit_) {
      burst_++;
      return true;
    } else if (skipped_ < skipped_limit_) {
      skipped_++;
      return false;
    } else {
      burst_ = 1;
      skipped_ = 0;
      skipped_limit_ *= 10;
      return true;
    }
  }

 private:
  void reset() {
    burst_ = 0;
    skipped_ = 0;
    skipped_limit_ = 10;
    while (skipped_limit_ <= burst_limit_) {
      skipped_limit_ *= 10;
    }
    start_ = std::chrono::steady_clock::now();
  }

 private:
  const size_t burst_limit_;
  const std::chrono::milliseconds period_;

  size_t burst_;
  size_t skipped_;
  size_t skipped_limit_;
  std::chrono::steady_clock::time_point start_;
};

}} // namespace facebook::logdevice
