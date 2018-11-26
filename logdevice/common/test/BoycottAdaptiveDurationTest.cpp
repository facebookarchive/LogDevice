// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/common/sequencer_boycotting/BoycottAdaptiveDuration.h"

#include <chrono>

#include <gtest/gtest.h>

using namespace ::testing;
using namespace std::literals::chrono_literals;
using facebook::logdevice::BoycottAdaptiveDuration;

TEST(BoycottAdaptiveDurationTest, testGetEffectiveValue) {
  auto now = std::chrono::system_clock::now();
  auto dur =
      BoycottAdaptiveDuration(1, 30min, 2h, 1min, 30s, 2, 60min, now, 30s);

  // We start by a value of 60mins, so getting it directly should be still
  // the same.
  EXPECT_EQ(60min, dur.getEffectiveDuration(now));

  // After 30s which is the last boycott duration, it should still be the same.
  EXPECT_EQ(60min, dur.getEffectiveDuration(now + 30s));

  // Let's move in time after the boycott duration is done.
  now += 30s;

  // The decrease step is 30s. So after 10secs it should still be the same.
  EXPECT_EQ(60min, dur.getEffectiveDuration(now + 10s));

  // After 30secs it should still be decreased one step.
  EXPECT_EQ(59min, dur.getEffectiveDuration(now + 30s));

  EXPECT_EQ(58min, dur.getEffectiveDuration(now + 60s));

  EXPECT_EQ(30min, dur.getEffectiveDuration(now + 30s * 30));

  // Should never go below the minimum
  EXPECT_EQ(30min, dur.getEffectiveDuration(now + 30s * 40));
}

TEST(BoycottAdaptiveDurationTest, testNegativeFeedback) {
  auto now = std::chrono::system_clock::now();
  auto dur = BoycottAdaptiveDuration(1, 30min, 2h, 1min, 30s, 2, 30min, now);

  dur.negativeFeedback(30min, now);
  EXPECT_EQ(60min, dur.getEffectiveDuration(now));

  // Test don't go over max duration
  dur.negativeFeedback(60min, now);
  EXPECT_EQ(2h, dur.getEffectiveDuration(now));
  dur.negativeFeedback(120min, now);
  EXPECT_EQ(2h, dur.getEffectiveDuration(now));
}

TEST(BoycottAdaptiveDurationTest, testNegativeFeedbackUsesEffectiveTime) {
  auto now = std::chrono::system_clock::now();
  auto dur = BoycottAdaptiveDuration(1, 30min, 2h, 1min, 30s, 2, 60min, now);

  EXPECT_EQ(50min, dur.getEffectiveDuration(now + 300s));
  dur.negativeFeedback(50min, now + 300s);
  EXPECT_EQ(100min, dur.getEffectiveDuration(now + 300s));

  // Make sure that the new timestamp and the last duration are persisted.
  EXPECT_EQ(99min, dur.getEffectiveDuration(now + 300s + 50min + 30s));
}

TEST(BoycottAdaptiveDurationTest, testResetIssued) {
  auto now = std::chrono::system_clock::now();
  auto dur =
      BoycottAdaptiveDuration(1, 30min, 2h, 1min, 30s, 2, 60min, now, 30min);

  // We start by a value of 60mins, so getting it directly should be still
  // the same.
  EXPECT_EQ(60min, dur.getEffectiveDuration(now));

  // After 10mins it should still be the same because we are still under the
  // last boycott duration
  EXPECT_EQ(60min, dur.getEffectiveDuration(now + 10min));

  // Issue a reset for this node
  dur.resetIssued(60min, now + 10min);

  // The duration should start decreasing immediately after the reset
  EXPECT_EQ(59min, dur.getEffectiveDuration(now + 10min + 30s));
}

TEST(BoycottAdaptiveDurationTest, testIsDefault) {
  auto now = std::chrono::system_clock::now();
  auto dur = BoycottAdaptiveDuration(1, 30min, 2h, 1min, 30s, 2, 30min, now);

  EXPECT_TRUE(dur.isDefault(now));

  dur.negativeFeedback(30min, now);
  EXPECT_FALSE(dur.isDefault(now));

  EXPECT_TRUE(dur.isDefault(now + 30min + 30s * 30));
}
