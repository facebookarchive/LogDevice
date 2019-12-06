/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/debug.h"
#include "logdevice/test/ldbench/worker/util.h"

using namespace facebook::logdevice::ldbench;

class WorkerUtilTest : public ::testing::Test {};

TEST_F(WorkerUtilTest, Log2Histogram) {
  Log2Histogram h;
  EXPECT_EQ(1, h.sample());
  EXPECT_EQ(1, h.sample(0.5));
  EXPECT_EQ(1, h.getMean());

  EXPECT_FALSE(h.parse("hello world"));
  EXPECT_FALSE(h.parse("[1,2,3]"));
  EXPECT_FALSE(h.parse("1,"));
  EXPECT_FALSE(h.parse("1,2,"));
  EXPECT_FALSE(h.parse("0.1.2"));
  EXPECT_FALSE(h.parse("12s"));
  EXPECT_FALSE(h.parse(""));
  EXPECT_FALSE(h.parse(
      "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,"
      "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1"));

  ASSERT_TRUE(h.parse("constant"));
  EXPECT_EQ(std::vector<double>({}), h.p);

  EXPECT_TRUE(h.parse("42, 42"));
  EXPECT_EQ(std::vector<double>({.5, 1}), h.p);

  ASSERT_TRUE(h.parse("42"));
  EXPECT_EQ(std::vector<double>({1}), h.p);
  EXPECT_DOUBLE_EQ(h.getMean(), 1.5);

  ASSERT_TRUE(h.parse("0.42,0.42"));
  EXPECT_EQ(std::vector<double>({.5, 1}), h.p);
  EXPECT_DOUBLE_EQ(h.getMean(), 2.25);

  ASSERT_TRUE(h.parse("1,2,3,4"));
  EXPECT_EQ(h.sample(0.42), h.sample(0.42));
  ASSERT_EQ(4, h.p.size());
  EXPECT_NEAR(.1, h.p[0], 1e-12);
  EXPECT_NEAR(.3, h.p[1], 1e-12);
  EXPECT_NEAR(.6, h.p[2], 1e-12);
  EXPECT_NEAR(1., h.p[3], 1e-12);
  const int iters = 1000000;
  double sum_float = 0;
  uint64_t sum_int = 0;
  for (int i = 0; i < iters; ++i) {
    double x = h.sample();
    EXPECT_GE(x, 1 - 1e-12);
    EXPECT_LE(x, 16 + 1e-12);
    sum_float += x;
    sum_int += (uint64_t)x;
  }
  EXPECT_NEAR(sum_float / iters, h.getMean(), 1e-1);
}

TEST_F(WorkerUtilTest, RoughProbabilityDistribution) {
  RoughProbabilityDistribution d;
  EXPECT_EQ(0, d.sampleFloat());
  EXPECT_EQ(0, d.sampleInteger());

  Log2Histogram h;
  ASSERT_TRUE(h.parse("1,2,3"));
  const double target_mean = 42;
  d = RoughProbabilityDistribution(target_mean, h);

  EXPECT_EQ(d.sampleFloat(0.42), d.sampleFloat(0.42));
  EXPECT_EQ(d.sampleInteger(0.72, 0.29), d.sampleInteger(0.72, 0.29));

  const int iters = 1000000;
  double sum_float = 0, min_float = 1e200, max_float = -1e200;
  uint64_t sum_int = 0, min_int = (uint64_t)1e15, max_int = 0;
  for (int i = 0; i < iters; ++i) {
    double x = d.sampleFloat();
    sum_float += x;
    min_float = std::min(min_float, x);
    max_float = std::max(max_float, x);

    uint64_t z = d.sampleInteger();
    sum_int += z;
    min_int = std::min(min_int, z);
    max_int = std::max(max_int, z);
  }
  EXPECT_NEAR(sum_float / iters, target_mean, 1e-1);
  EXPECT_NEAR(sum_int * 1. / iters, target_mean, 1e-1);
  EXPECT_NEAR(min_float, 1 / h.getMean() * target_mean, 1e-2);
  EXPECT_NEAR(max_float, pow(2., h.p.size()) / h.getMean() * target_mean, 1e-2);
  EXPECT_NEAR(min_int, min_float, 2);
  EXPECT_NEAR(max_int, max_float, 2);
}

TEST_F(WorkerUtilTest, Spikiness) {
  Spikiness s;
  EXPECT_EQ(12.345, s.transform(12.345));
  EXPECT_EQ(54.321, s.inverseTransform(54.321));

  EXPECT_FALSE(s.parse(""));
  EXPECT_FALSE(s.parse("hai"));
  EXPECT_FALSE(s.parse("/"));
  EXPECT_FALSE(s.parse("//"));
  EXPECT_FALSE(s.parse("///"));
  EXPECT_FALSE(s.parse("2/1/3s"));
  EXPECT_FALSE(s.parse("2/1/3s/aligned"));
  EXPECT_FALSE(s.parse("2/1%/3s/aligned"));
  EXPECT_FALSE(s.parse("2%/1/3s/aligned"));
  EXPECT_FALSE(s.parse("2%/1%/3/aligned"));
  EXPECT_FALSE(s.parse("2%/1%/3sqwe/aligned"));
  EXPECT_FALSE(s.parse("2%/1%qwe/3s/aligned"));
  EXPECT_FALSE(s.parse("2%/1%/3s/aligned/meow"));
  EXPECT_FALSE(s.parse("1%/2%/3s/aligned"));
  EXPECT_FALSE(s.parse("1%/2%/3s"));
  EXPECT_FALSE(s.parse("2%/1%/0s"));

  std::string er;
  EXPECT_TRUE(s.parse("2%/1%/3s/aligned", &er)) << er;
  EXPECT_TRUE(s.aligned);
  EXPECT_TRUE(s.parse("2%/1%/3s", &er)) << er;
  EXPECT_FALSE(s.aligned);
  EXPECT_TRUE(s.parse("none", &er)) << er;
  EXPECT_EQ(0, s.spike_time_fraction);
  EXPECT_EQ(0, s.spike_load_fraction);
  EXPECT_TRUE(s.parse("0%/0%/1s", &er)) << er;

  auto test_inverse = [&] {
    for (int i = 0; i < 100; ++i) {
      double t = folly::Random::randDouble01() * 100;
      double x = s.inverseTransform(t);
      EXPECT_NEAR(t, s.transform(x), 1e-12);
    }
  };

  ASSERT_TRUE(s.parse("50%/0%/3s", &er)) << er;
  EXPECT_DOUBLE_EQ(90, s.transform(90));
  EXPECT_DOUBLE_EQ(90, s.transform(90.5));
  EXPECT_DOUBLE_EQ(90, s.transform(91));
  EXPECT_DOUBLE_EQ(90, s.transform(91.5));
  EXPECT_DOUBLE_EQ(91.5, s.transform(92.25));
  EXPECT_DOUBLE_EQ(93 - 2e-3, s.transform(93 - 1e-3));
  EXPECT_DOUBLE_EQ(93, s.transform(93 + 1e-3));
  {
    SCOPED_TRACE("");
    test_inverse();
  }

  ASSERT_TRUE(s.parse("100%/10%/3s", &er)) << er;
  EXPECT_DOUBLE_EQ(90 + 1e-4, s.transform(90 + 1e-3));
  EXPECT_DOUBLE_EQ(90.1, s.transform(91));
  EXPECT_DOUBLE_EQ(90.3 - 1e-4, s.transform(93 - 1e-3));

  EXPECT_DOUBLE_EQ(90 + 1e-3, s.inverseTransform(90 + 1e-4));
  EXPECT_DOUBLE_EQ(91, s.inverseTransform(90.1));
  EXPECT_NEAR(93 - 1e-3, s.inverseTransform(90.3 - 1e-4), 1e-12);
  EXPECT_DOUBLE_EQ(93, s.inverseTransform(91));

  ASSERT_TRUE(s.parse("100%/0%/3s", &er)) << er;
  EXPECT_DOUBLE_EQ(90, s.transform(90 + 1e-3));
  EXPECT_DOUBLE_EQ(90, s.transform(91));
  EXPECT_DOUBLE_EQ(90, s.transform(92));
  EXPECT_DOUBLE_EQ(90, s.transform(93 - 1e-3));

  EXPECT_DOUBLE_EQ(93, s.inverseTransform(90 + 1e-3));
  EXPECT_DOUBLE_EQ(93, s.inverseTransform(91));
  EXPECT_DOUBLE_EQ(93, s.inverseTransform(92));
  EXPECT_DOUBLE_EQ(93, s.inverseTransform(93 - 1e-3));

  ASSERT_TRUE(s.parse("40%/20%/5s/aligned", &er)) << er;
  EXPECT_DOUBLE_EQ(50, s.transform(50));
  EXPECT_DOUBLE_EQ(50.5, s.transform(51));
  EXPECT_DOUBLE_EQ(51 - 1e-3, s.transform(52 - 2e-3));
  EXPECT_DOUBLE_EQ(51, s.transform(52));
  EXPECT_DOUBLE_EQ(51 + 1e-3, s.transform(52 + .75e-3));
  EXPECT_DOUBLE_EQ(51 + 4. / 3, s.transform(53));
  EXPECT_DOUBLE_EQ(55 - 1e-3, s.transform(55 - .75e-3));
  EXPECT_DOUBLE_EQ(55, s.transform(55));
  EXPECT_DOUBLE_EQ(55 + 1e-3, s.transform(55 + 2e-3));
  {
    SCOPED_TRACE("");
    test_inverse();
  }
}

TEST_F(WorkerUtilTest, RandomEventSequence) {
  RandomEventSequence e;
  auto s = e.newState(0, 100);
  EXPECT_GT(e.nextEvent(s), 1e8);

  s = e.newState(2, 100);
  uint64_t cnt = 0;
  double prev = 100;
  for (; prev < 100 + 5e3 && cnt < (int)1e7; ++cnt) {
    double x = e.nextEvent(s);
    ASSERT_GE(x, prev);
    ASSERT_LT(cnt, (int)1e7);
    prev = x;
  }
  EXPECT_GE(prev, 100 + 5e3);
  EXPECT_NEAR(cnt, 1e4, 1e3);

  ASSERT_TRUE(e.spikiness.parse("50%/10%/1s/aligned"));
  s = e.newState(2, 100);
  cnt = 0;
  prev = 100;
  uint64_t cnt_hi = 0, cnt_lo = 0;
  for (; prev < 100 + 5e3 && cnt < (int)1e7; ++cnt) {
    double x = e.nextEvent(s);
    ASSERT_GE(x, prev);
    ASSERT_LT(cnt, (int)1e7);
    ++(x - floor(x) < .1 ? cnt_hi : cnt_lo);
    prev = x;
  }
  EXPECT_GE(prev, 100 + 1e3);
  EXPECT_NEAR(cnt, 1e4, 1e3);
  EXPECT_NEAR(cnt_hi, 5e3, 1e3);
  EXPECT_NEAR(cnt_lo, 5e3, 1e3);
}
