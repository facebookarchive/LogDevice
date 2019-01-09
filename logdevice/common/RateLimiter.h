/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file RateLimiter is a helper class used to check if the number (or total
 *       weight) of executed operations per unit of time exceeded the limit.
 *
 *       Thread safe (except for assignment operator).
 *
 *       There are 4 main ways to use it:
 *        1. Whenever you want to do an operation, call isAllowed(). If it
 *           returns true, go ahead and do it, otherwise try again later.
 *           RateLimiter can provide a hint for how much later to try if you
 *           pass non-null time_until_allowed and zero max_delay - but beware
 *           of thundering herd; strongly not recommended when the RateLimiter
 *           is shared among multiple state machines.
 *        2. Whenever you want to do an operation, call waitUntilAllowed(), then
 *           do the operation. waitUntilAllowed() will block the current thread,
 *           so it only makes sense if the thread doesn't have anything
 *           else to do.
 *        3. Whenever you want to do an operation, call isAllowed() with
 *           non-null time_until_allowed, wait for the returned amount of time,
 *           then do the operation. This is the most flexible mode.
 *        4. If you only know the cost of each operation after the operation
 *           completes, you can separate rate limiting from reporting the cost:
 *           call isAllowed() with zero cost (or some prior estimate), use
 *           any of the ways 1-3 above to wait until the operation is allowed,
 *           do the operation, then report the cost by calling
 *           isAllowed(cost, &unused) and ignoring its return value.
 *           Alternatively, you can combine the cost-reporting isAllowed() call
 *           with the next permission-requesting isAllowed() by just remembering
 *           the cost of previous operation and passing it to isAllowed() for
 *           the next operation.
 *
 *       The main limitations are:
 *        a. You need to know the cost of the operation in advance.
 *        b. The reservation is made at time of call to isAllowed().
 *           If, before that time elapses, you change your mind about doing the
 *           operation, the reserved budget is wasted. (Maybe this can be made a
 *           little better by adding a method to "unreserve" a given cost,
 *           but that has its own problems and doesn't seem like a good idea.)
 *       To solve these problems, one would need a different kind of rate
 *       limiter, having a queue of parties (callbacks?) waiting for their turn.
 *
 *       RateLimiters are used as static objects in some places.
 *       Because LogDevice may not
 *       always terminate cleanly (unit tests, abnormal termination), some code
 *       may call into static RateLimiter objects after they have been
 *       destroyed. This is undefined behavior, but does not cause issues
 *       currently, because this class has no virtual functions and only a small
 *       number of primitive data members. Adding data members or virtual
 *       functions may cause crashes however.
 */

// Timing functions used by RateLimiter. Can be mocked in tests.
struct RateLimiterDependencies {
  static std::chrono::steady_clock::time_point currentTime() {
    return std::chrono::steady_clock::now();
  }

  static void sleepFor(std::chrono::steady_clock::duration duration) {
    std::this_thread::sleep_for(duration);
  }
};

// Imagine the rate limiter as a bucket, continuously filling with water at
// a rate limit_per_second units per second. The size of the bucket is so
// that it takes `burst_limit` time to fill from zero to full. An operation
// instantaneously consumes `cost` units of water from the bucket.
// The water level is allowed to go negative. An operation is allowed
// whenever the water level is nonnegative.
//
// It turns out that to simulate such a bucket it's enough to maintain a
// single number: the time at which water level did or will hit zero.
// Which is nice because it allows making everything lock-free by making this
// number atomic and doing everything in CAS loops.
//
// (Another way would be to maintain the level at time 0. But that would be less
//  robust because then we'd have to do everything in floating-point numbers
//  and often subtract numbers that are close to each other, and 64-bit numbers
//  would barely have enough precision for that.)
template <typename Deps>
class RateLimiterBase {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;
  using Duration = std::chrono::steady_clock::duration;

  // Allows at most `limit.first` units of cost per `limit.second` units
  // of time.
  RateLimiterBase(rate_limit_t limit)
      : RateLimiterBase(
            limit.second.count() <= 0 ? -1.
                                      : limit.first /
                    std::chrono::duration_cast<std::chrono::duration<double>>(
                        limit.second)
                        .count(),
            limit.second) {}

  // Allows at most `limit_per_second` units of cost per second on average,
  // with quick bursts of up to `burst_limit` units of cost.
  // limit_per_second == -1 means unlimited.
  RateLimiterBase(double limit_per_second, double burst_limit)
      : RateLimiterBase(limit_per_second,
                        limit_per_second <= 0.
                            ? Duration(0)
                            : std::chrono::duration_cast<Duration>(
                                  std::chrono::duration<double>(
                                      burst_limit / limit_per_second))) {}

  RateLimiterBase() : RateLimiterBase(-1, 0) {}
  RateLimiterBase(const RateLimiterBase& rhs)
      : limit_per_second(rhs.limit_per_second),
        burst_limit(rhs.burst_limit),
        next_allowed_time(rhs.next_allowed_time.load()) {}
  // Not thread safe.
  RateLimiterBase& operator=(const RateLimiterBase& rhs) {
    limit_per_second = rhs.limit_per_second;
    burst_limit = rhs.burst_limit;
    next_allowed_time.store(rhs.next_allowed_time.load());
    return *this;
  }

  // Updates the rate limit and burst limit. Not thread safe: can't be called
  // concurrently with another update(), isAllowed() or other methods.
  // Unlike operator=(), update() doesn't reset the state (doesn't replenish
  // the water level).
  void update(const RateLimiterBase& rhs) {
    if (limit_per_second != rhs.limit_per_second) {
      // The rate of flow of water into our bucket has changed.
      // If water level is negative, adjust next_allowed_time to reflect the
      // change.
      const auto now = Deps::currentTime();
      auto next = TimePoint(next_allowed_time.load());
      if (next > now) {
        if (limit_per_second <= 0. || rhs.limit_per_second <= 0.) {
          next_allowed_time.store(now.time_since_epoch());
        } else {
          auto remaining = next - now;
          remaining = decltype(remaining)(
              static_cast<int64_t>(1. * limit_per_second /
                                   rhs.limit_per_second * remaining.count()));
          next_allowed_time.store((now + remaining).time_since_epoch());
        }
      }
    }
    limit_per_second = rhs.limit_per_second;
    burst_limit = rhs.burst_limit;
  }
  void update(rate_limit_t limit) {
    update(RateLimiterBase(limit));
  }

  // If the operation of the given cost is allowed right now, pays the cost
  // and returns true. Otherwise:
  //  * If `time_until_allowed` is nullptr, returns false and doesn't change
  //    state.
  //  * If `time_until_allowed` is not nullptr, calculates how long we'd have
  //    to wait until the operation becomes allowed. If the result is greater
  //    than max_delay, returns false and doesn't change state. Otherwise
  //    *pays the cost right away* and returns true; in this case you should
  //    wait for `*time_until_allowed`, then do the operation WITHOUT calling
  //    isAllowed() again. *time_until_allowed gets assigned in either case.
  // @return  Whether the reservation was made. If true, the operation of cost
  //          `cost` is allowed to proceed, either immediately
  //          (if `time_until_allowed` is nullptr) or after
  //          a delay of `*time_until_allowed`. Note that in this case you
  //          DON'T need to call isAllowed() again after the delay.
  //          If false, the RateLimiter's state is left unchanged.
  bool isAllowed(size_t cost = 1,
                 Duration* time_until_allowed = nullptr,
                 Duration max_delay = Duration::max()) {
    if (limit_per_second == -1.) {
      // Unlimited.
      if (time_until_allowed != nullptr) {
        *time_until_allowed = Duration::zero();
      }
      return true;
    }
    if (limit_per_second == 0.) {
      // Stalled.
      if (time_until_allowed) {
        *time_until_allowed = Duration::max();
      }
      return false;
    }

    const auto now = Deps::currentTime();
    // Convert from units of cost into units of time, i.e. how long we need
    // to stay idle to "pay" for the operation of this cost.
    const Duration time_cost = std::chrono::duration_cast<Duration>(
        std::chrono::duration<double>(cost / limit_per_second));

    auto next = next_allowed_time.load();
    TimePoint new_next;
    Duration res;
    do {
      res = TimePoint(next) - now; // how long to wait
      if (res.count() <= 0) {
        // No waiting needed.
        res = Duration(0);
      } else if (time_until_allowed == nullptr || res > max_delay) {
        // Waiting is needed, but we're not allowed to wait for this long.
        if (time_until_allowed != nullptr) {
          *time_until_allowed = res;
        }
        return false;
      }
      new_next = std::max(TimePoint(next), now - burst_limit) + time_cost;
    } while (!next_allowed_time.compare_exchange_weak(
        next, new_next.time_since_epoch()));

    if (time_until_allowed) {
      *time_until_allowed = res;
    } else {
      ld_check_eq(0, res.count());
    }

    return true;
  }

  void waitUntilAllowed(size_t cost = 1) {
    Duration wait;
    if (!isAllowed(cost, &wait)) {
      Deps::sleepFor(wait);
    }
  }

  bool isUnlimited() const {
    return limit_per_second == -1;
  }

 private:
  // The non-atomic fields are immutable, expcept by operator=().
  double limit_per_second; // -1 means unlimited
  Duration burst_limit;

  // The "Duration" is actually a time point.
  // atomic<TimePoint> doesn't work because C++.
  std::atomic<Duration> next_allowed_time;

  RateLimiterBase(double limit_per_second, Duration burst_limit)
      : limit_per_second(limit_per_second),
        burst_limit(burst_limit),
        // Subtracting burst_limit allows a full burst right after creating the
        // RateLimiter.
        next_allowed_time(
            (Deps::currentTime() - burst_limit).time_since_epoch()) {
    ld_check(limit_per_second == -1 || limit_per_second >= 0);
    ld_check(burst_limit.count() >= 0);
  }
};

extern template class RateLimiterBase<RateLimiterDependencies>;
using RateLimiter = RateLimiterBase<RateLimiterDependencies>;

}} // namespace facebook::logdevice
