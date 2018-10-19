/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>

#include <boost/any.hpp>
#include <boost/program_options.hpp>
#include <gtest/gtest.h>

#include "logdevice/common/commandline_util_chrono.h"

namespace facebook { namespace logdevice {

typedef chrono_interval_t<std::chrono::milliseconds> MsInterval;

template <class DurationType>
static DurationType parse(const std::string& str) {
  boost::any any;
  validate(any, std::vector<std::string>{str}, (DurationType*)nullptr, 0);
  return boost::any_cast<DurationType>(any);
}

// "External constructor" for intervals. Needed because EXPECT_EQ macro does
// not mix with universal initialization syntax.
template <typename D1, typename D2>
static inline MsInterval makeMsInterval(D1 lo, D2 hi) noexcept {
  return MsInterval{lo, hi};
}

// Equality operator for use by EXPECT_EQ
template <typename D1, typename D2>
static inline bool operator==(const chrono_interval_t<D1>& left,
                              const chrono_interval_t<D2>& right) noexcept {
  return (left.lo == right.lo) && (left.hi == right.hi);
}

// Equality operator for use by EXPECT_EQ
template <typename D1, typename D2>
static inline bool operator==(const chrono_expbackoff_t<D1>& left,
                              const chrono_expbackoff_t<D2>& right) noexcept {
  return (left.initial_delay == right.initial_delay) &&
      (left.max_delay == right.max_delay) &&
      (left.multiplier == right.multiplier);
}

TEST(CommandLineUtilTest, ValidDurations) {
  using namespace std::chrono;

  // Test all the basic types
  EXPECT_EQ(nanoseconds(1), parse<nanoseconds>("1ns"));
  EXPECT_EQ(microseconds(2), parse<microseconds>("2us"));
  EXPECT_EQ(milliseconds(3), parse<milliseconds>("3ms"));
  EXPECT_EQ(seconds(4), parse<seconds>("4s"));
  EXPECT_EQ(minutes(5), parse<minutes>("5min"));
  EXPECT_EQ(hours(6), parse<seconds>("6h"));

  // Conversions
  EXPECT_EQ(milliseconds(1000), parse<milliseconds>("1s"));
  EXPECT_EQ(seconds(1), parse<seconds>("1000ms"));

  // Days don't have a type
  EXPECT_EQ(hours(24), parse<hours>("1day"));
  EXPECT_EQ(hours(48), parse<hours>("2days"));

  // Fractionals
  EXPECT_EQ(milliseconds(100), parse<milliseconds>("0.1s"));
  EXPECT_EQ(milliseconds(1500), parse<milliseconds>("1.5s"));
  EXPECT_EQ(microseconds(100), parse<microseconds>("0.1ms"));
  EXPECT_EQ(hours(36), parse<hours>("1.5days"));

  // Spaces are fine
  EXPECT_EQ(milliseconds(1), parse<milliseconds>("1  ms"));
  EXPECT_EQ(hours(48), parse<hours>("2 days"));

  // Zero should parse without a suffix
  EXPECT_EQ(seconds(0), parse<seconds>("0"));
}

TEST(CommandLineUtilTest, InvalidDurations) {
  using namespace std::chrono;
  using namespace boost::program_options;
  ASSERT_THROW(parse<seconds>(""), validation_error);
  ASSERT_THROW(parse<seconds>(" "), validation_error);
  ASSERT_THROW(parse<seconds>("a"), validation_error);
  ASSERT_THROW(parse<seconds>("a "), validation_error);
  ASSERT_THROW(parse<seconds>("1"), validation_error);
  ASSERT_THROW(parse<seconds>("0.1"), validation_error);
  ASSERT_THROW(parse<seconds>("0.1a"), validation_error);
  ASSERT_THROW(parse<seconds>("ms"), validation_error);
  ASSERT_THROW(parse<seconds>(" ms"), validation_error);
  ASSERT_THROW(parse<seconds>("- ms"), validation_error);
  ASSERT_THROW(parse<seconds>("1 msabcdefghijklmnopqrstuv"), validation_error);

  // This is the tricky case where "100 ms" gets truncated to zero seconds.
  ASSERT_THROW(parse<seconds>("100 ms"), error);
}

TEST(CommandLineUtilTest, Intervals) {
  using namespace std::chrono;
  using namespace boost::program_options;

  // Basic
  EXPECT_EQ(makeMsInterval(milliseconds(100), milliseconds(200)),
            parse<MsInterval>("100ms..200ms"));
  EXPECT_EQ(makeMsInterval(hours(1), hours(72)), parse<MsInterval>("1h..3d"));

  // Fractionals
  EXPECT_EQ(makeMsInterval(milliseconds(100), milliseconds(1500)),
            parse<MsInterval>("0.1s..1.5s"));

  // Spaces are fine
  EXPECT_EQ(makeMsInterval(milliseconds(1), hours(48)),
            parse<MsInterval>("1  ms..2 days"));

  // Invalid
  ASSERT_THROW(parse<MsInterval>(""), validation_error);
  ASSERT_THROW(parse<MsInterval>(" "), validation_error);
  ASSERT_THROW(parse<MsInterval>(".."), validation_error);
  ASSERT_THROW(parse<MsInterval>("1ms.."), validation_error);
  ASSERT_THROW(parse<MsInterval>("..1ms"), validation_error);
  ASSERT_THROW(parse<MsInterval>("1ms..x"), validation_error);
  ASSERT_THROW(parse<MsInterval>("x..1ms"), validation_error);
  ASSERT_THROW(parse<MsInterval>("1ms..100ns"), error);
}

TEST(CommandLineUtilTest, ExpBackoffs) {
  using namespace std::chrono;
  using namespace boost::program_options;
  typedef chrono_expbackoff_t<milliseconds> ExpBackoff;

  // Basic
  EXPECT_EQ(ExpBackoff(milliseconds(100), milliseconds(200)),
            parse<ExpBackoff>("100ms..200ms"));
  EXPECT_EQ(ExpBackoff(hours(1), hours(72)), parse<ExpBackoff>("1h..3d"));
  EXPECT_EQ(ExpBackoff(milliseconds(100), milliseconds(200), 3),
            parse<ExpBackoff>("100ms..200ms-3x"));

  // Spaces are fine
  EXPECT_EQ(ExpBackoff(milliseconds(1), hours(48)),
            parse<ExpBackoff>("1  ms..2 days"));
  EXPECT_EQ(ExpBackoff(milliseconds(1), hours(48), 3),
            parse<ExpBackoff>("1  ms..2 days-3x"));

  // Invalid
  ASSERT_THROW(parse<ExpBackoff>(""), validation_error);
  ASSERT_THROW(parse<ExpBackoff>(" "), validation_error);
  ASSERT_THROW(parse<ExpBackoff>(".."), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms.."), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("..1ms"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..x"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("x..1ms"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("x..1ms"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("-"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("..-"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..-3x"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("..1ms-3x"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms-"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms-3"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms-3y"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms-3xy"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms-ax"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..2ms--3x"), validation_error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..100ns"), error);
  ASSERT_THROW(parse<ExpBackoff>("1ms..100ns-3x"), error);
}

}} // namespace facebook::logdevice
