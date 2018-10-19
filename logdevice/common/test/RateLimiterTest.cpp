/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RateLimiter.h"

#include <thread>

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

using TimePoint = RateLimiter::TimePoint;
using Duration = RateLimiter::Duration;

const TimePoint BASE_TIME = TimePoint(std::chrono::seconds(1ll << 29));
const std::chrono::milliseconds SECOND = std::chrono::seconds(1);

namespace {

struct Deps {
  static TimePoint now;
  static folly::Optional<Duration> slept_for;

  static std::chrono::steady_clock::time_point currentTime() {
    return now;
  }

  static void sleepFor(std::chrono::steady_clock::duration duration) {
    ASSERT_FALSE(slept_for.hasValue());
    slept_for = duration;
  }
};

TimePoint Deps::now;
folly::Optional<Duration> Deps::slept_for;

} // namespace

static uint64_t to_ms(Duration d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

class MockRateLimiter : public RateLimiterBase<Deps> {
 public:
  using RateLimiterBase::RateLimiterBase;
};

class RateLimiterTest : public ::testing::Test {
 public:
  RateLimiterTest() {
    Deps::now = BASE_TIME;
    Deps::slept_for = folly::none;
  }
};

TEST_F(RateLimiterTest, Basic) {
  MockRateLimiter limiter(rate_limit_t(1, SECOND));

  // first two attempts should go through
  EXPECT_TRUE(limiter.isAllowed());
  EXPECT_TRUE(limiter.isAllowed());
  EXPECT_FALSE(limiter.isAllowed());

  // too soon
  Deps::now += SECOND * 2 / 3;
  EXPECT_FALSE(limiter.isAllowed());

  // good to go again
  Deps::now += SECOND * 2 / 3;
  EXPECT_TRUE(limiter.isAllowed());
  EXPECT_FALSE(limiter.isAllowed());
}

TEST_F(RateLimiterTest, Reservations) {
  // 1 per second, burst up to 2.
  MockRateLimiter limiter(rate_limit_t(2, SECOND * 2));
  Duration to_wait;

  // t=0. Consume 6 units. Can go again in 4 second.
  EXPECT_TRUE(limiter.isAllowed());
  EXPECT_TRUE(limiter.isAllowed(5));
  EXPECT_FALSE(limiter.isAllowed());

  // t=3. Reserve 2 units.
  Deps::now += SECOND * 3;
  EXPECT_TRUE(limiter.isAllowed(2, &to_wait));
  EXPECT_EQ(1000, to_ms(to_wait));
  // Now booked up to t=6.

  // t=3. Reserve 1 unit.
  EXPECT_FALSE(limiter.isAllowed(1, &to_wait, SECOND * 2));
  EXPECT_TRUE(limiter.isAllowed(1, &to_wait));
  EXPECT_EQ(3000, to_ms(to_wait));
  // Now booked up to t=5.

  // t=4+2/3. Still can't proceed.
  Deps::now += SECOND * 3 + SECOND * 2 / 3;
  EXPECT_FALSE(limiter.isAllowed(10000, &to_wait, SECOND * 0));

  // t=5+1/3. Consume 1 unit.
  Deps::now += SECOND * 2 / 3;
  EXPECT_TRUE(limiter.isAllowed(1, &to_wait, SECOND));
  EXPECT_EQ(0, to_ms(to_wait));
  EXPECT_FALSE(limiter.isAllowed(1));
  // Now booked up to t=6.

  // The weird sequence of operations is to get the right rounding.
  // t=10. Consume 3 units.
  Deps::now += SECOND * 6 - SECOND * 2 / 3 * 2;
  EXPECT_TRUE(limiter.isAllowed(1));
  EXPECT_TRUE(limiter.isAllowed(1));
  EXPECT_TRUE(limiter.isAllowed(1));
  EXPECT_FALSE(limiter.isAllowed(1));
}

TEST_F(RateLimiterTest, ZeroAndInfiniteLimits) {
  MockRateLimiter limiter(rate_limit_t(0, SECOND));
  Duration to_wait;

  EXPECT_FALSE(limiter.isAllowed());
  EXPECT_FALSE(limiter.isAllowed(42));
  EXPECT_FALSE(limiter.isAllowed(42, &to_wait));
  EXPECT_FALSE(limiter.isAllowed(42, &to_wait, SECOND));

  limiter = MockRateLimiter(rate_limit_t(1, SECOND * 0));
  EXPECT_TRUE(limiter.isAllowed());
  EXPECT_TRUE(limiter.isAllowed(42));
  EXPECT_TRUE(limiter.isAllowed(42, &to_wait));
  EXPECT_EQ(0, to_ms(to_wait));
  EXPECT_TRUE(limiter.isAllowed(42, &to_wait, SECOND));
  EXPECT_EQ(0, to_ms(to_wait));
}

TEST(EBRateLimiterTestWeight, Basic) {
  size_t skipped = 0;
  EBRateLimiter limiter(2, std::chrono::seconds(10));

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  for (size_t i = 0; i < 10; i++) {
    EXPECT_FALSE(limiter.isAllowed(skipped));
    EXPECT_EQ(skipped, i);
  }

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 10);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  for (size_t i = 0; i < 100; i++) {
    EXPECT_FALSE(limiter.isAllowed(skipped));
    EXPECT_EQ(skipped, i);
  }

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 100);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  for (size_t i = 0; i < 1000; i++) {
    EXPECT_FALSE(limiter.isAllowed(skipped));
    EXPECT_EQ(skipped, i);
  }

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 1000);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  EXPECT_FALSE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
}

TEST(EBRateLimiterTestTime, Basic) {
  size_t skipped = 0;
  EBRateLimiter limiter(2, std::chrono::seconds(1));

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  EXPECT_FALSE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 1);
  EXPECT_TRUE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
  EXPECT_FALSE(limiter.isAllowed(skipped));
  EXPECT_EQ(skipped, 0);
}

}} // namespace facebook::logdevice
