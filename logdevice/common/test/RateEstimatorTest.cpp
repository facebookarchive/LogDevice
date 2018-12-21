/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RateEstimator.h"

#include <gtest/gtest.h>

#include "logdevice/common/debug.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;

namespace std {
// Tell gtest how to print our values.
// This has to be in namespace std for C++ reasons.
void PrintTo(const std::pair<int64_t, std::chrono::milliseconds>& p,
             std::ostream* o) {
  (*o) << "(" << p.first << ", " << p.second.count() << "ms)";
}
} // namespace std

TEST(RateEstimatorTest, Basic) {
  const SteadyTimestamp TS0 = SteadyTimestamp(10min);
  RateEstimator r(TS0);

  EXPECT_EQ(std::make_pair(0l, 100ms), r.getRate(10s, TS0 + 100ms));
  EXPECT_EQ(std::make_pair(0l, 10s + 0ms), r.getRate(10s, TS0 + 12s));

  r.addValue(42, 10s, TS0 + 12s + 10ms);
  EXPECT_EQ(std::make_pair(42l, 12s + 20ms), r.getRate(10s, TS0 + 12s + 20ms));

  r.addValue(100, 10s, TS0 + 19s);
  EXPECT_EQ(
      std::make_pair(142l, 19s + 900ms), r.getRate(10s, TS0 + 19s + 900ms));
  EXPECT_EQ(std::make_pair(142l, 13s + 0ms), r.getRate(10s, TS0 + 23s));

  r.addValue(10, 10s, TS0 + 23s);
  EXPECT_EQ(std::make_pair(152l, 13s + 0ms), r.getRate(10s, TS0 + 23s));
  EXPECT_EQ(std::make_pair(152l, 19s + 0ms), r.getRate(10s, TS0 + 29s));
  EXPECT_EQ(std::make_pair(10l, 12s + 0ms), r.getRate(10s, TS0 + 32s));

  std::chrono::milliseconds alot = 1000000h;
  r.addValue(1337, 10s, TS0 + alot + 10ms);
  EXPECT_EQ(std::make_pair(1337l, 11s + 0ms), r.getRate(10s, TS0 + alot + 1s));
  EXPECT_EQ(std::make_pair(0l, 10s + 0ms), r.getRate(10s, TS0 + alot + 111s));

  r.clear(TS0 + alot + 115s);
  EXPECT_EQ(std::make_pair(0l, 0ms), r.getRate(10s, TS0 + alot + 115s));

  r.addValue(1, 10s, TS0 + alot + 115s + 10ms);
  EXPECT_EQ(std::make_pair(1l, 20ms), r.getRate(10s, TS0 + alot + 115s + 20ms));
}

TEST(RateEstimatorTest, NegativeAndZeroTimestamps) {
  const SteadyTimestamp TS0 = SteadyTimestamp(-10min);
  RateEstimator r(TS0);

  EXPECT_EQ(std::make_pair(0l, 100ms), r.getRate(10s, TS0 + 100ms));

  r.addValue(42, 10s, TS0 + 3s + 10ms);
  EXPECT_EQ(std::make_pair(42l, 12s + 0ms), r.getRate(10s, TS0 + 12s));

  r.addValue(13, 10s, TS0 + 10min - 1ms);
  EXPECT_EQ(std::make_pair(13l, 20s - 1ms), r.getRate(10s, TS0 + 10min - 1ms));
  EXPECT_EQ(std::make_pair(13l, 10s + 0ms), r.getRate(10s, TS0 + 10min));

  r.addValue(10, 10s, TS0 + 10min);
  EXPECT_EQ(std::make_pair(23l, 10s + 0ms), r.getRate(10s, TS0 + 10min));

  r.addValue(20, 10s, TS0 + 10min + 10ms);
  EXPECT_EQ(
      std::make_pair(43l, 10s + 15ms), r.getRate(10s, TS0 + 10min + 15ms));
}

TEST(RateEstimatorTest, ChangingWinow) {
  const SteadyTimestamp TS0 = SteadyTimestamp(10h);
  RateEstimator r(TS0);

  r.addValue(42, 10s, TS0 + 5s);
  EXPECT_EQ(std::make_pair(42l, 6s + 0ms), r.getRate(10s, TS0 + 6s));
  EXPECT_EQ(std::make_pair(42l, 6s + 0ms), r.getRate(100s, TS0 + 6s));

  r.addValue(100, 3s, TS0 + 7s);
  EXPECT_EQ(std::make_pair(142l, 7s + 0ms), r.getRate(3s, TS0 + 7s));
  EXPECT_EQ(std::make_pair(100l, 3s + 0ms), r.getRate(3s, TS0 + 8s));

  r.addValue(200, 3s, TS0 + 21s);
  EXPECT_EQ(std::make_pair(200l, 5s + 0ms), r.getRate(3s, TS0 + 22s));
  EXPECT_EQ(std::make_pair(200l, 5s + 0ms), r.getRate(300s, TS0 + 22s));

  r.addValue(400, 300s, TS0 + 40s);
  EXPECT_EQ(std::make_pair(600l, 28s + 0ms), r.getRate(300s, TS0 + 45s));
}
