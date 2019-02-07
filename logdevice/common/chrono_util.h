/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>

/**
 * @file   Utilities related to time. Mostly compensating for missing features
 *         and annoying verbosity of std::chrono.
 */

namespace facebook { namespace logdevice {

/* Shorter names for some duration conversions */

template <typename T>
std::chrono::microseconds to_usec(const T& value) {
  return std::chrono::duration_cast<std::chrono::microseconds>(value);
}

template <typename T>
std::chrono::milliseconds to_msec(const T& value) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(value);
}

template <typename T>
std::chrono::seconds to_sec(const T& value) {
  return std::chrono::duration_cast<std::chrono::seconds>(value);
}

template <typename T>
double to_sec_double(const T& value) {
  // The default unit of chrono::duration is seconds.
  return std::chrono::duration_cast<std::chrono::duration<double>>(value)
      .count();
}

template <typename TimePoint>
int64_t usec_since(const TimePoint& start) {
  return to_usec(TimePoint::clock::now() - start).count();
}

template <typename TimePoint>
int64_t msec_since(const TimePoint& start) {
  return to_msec(TimePoint::clock::now() - start).count();
}

template <typename TimePoint>
int64_t sec_since(const TimePoint& start) {
  return to_sec(TimePoint::clock::now() - start).count();
}

// Add a given duration to a given time point. If the addition would overflow,
// return TimePoint::max(). See the asserts for limitations.
//
// Example:
//   void f(std::chrono::milliseconds timeout) {
//     // This will overflow if timeout is too big, e.g. milliseconds::max().
//     //auto deadline = std::chrono::stady_clock::now() + timeout;
//     // Use this instead.
//     auto deadline = truncated_add(std::chrono::stady_clock::now(), timeout);
//     do_someting(deadline);
//   }
template <typename TimePoint, typename Duration>
TimePoint truncated_add(TimePoint t, Duration d) {
  // If these are failing on your compiler, please add some `if`s and implement
  // the missing cases.
  static_assert(
      std::is_same<typename TimePoint::rep, typename Duration::rep>::value,
      "time point and duration must have the same underlying type");
  static_assert(std::ratio_less_equal<typename TimePoint::period,
                                      typename Duration::period>::value,
                "duration must be coraser than time point");
  ld_check(d.count() >= 0);

  if (d > std::chrono::duration_cast<Duration>(TimePoint::max() - t)) {
    return TimePoint::max();
  }
  return t + d;
}

}} // namespace facebook::logdevice
