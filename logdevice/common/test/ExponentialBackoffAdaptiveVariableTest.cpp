/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ExponentialBackoffAdaptiveVariable.h"

#include <gtest/gtest.h>

namespace facebook { namespace logdevice {

using namespace std::chrono_literals;
using TS = ExponentialBackoffAdaptiveVariable::TS;

TEST(ExponentialBackoffAdaptiveVariableTest, Test1) {
  // A variable initialized to 20, that doubles on negative feedback and
  // decreases at a rate of 1/s on positive feedback.
  ExponentialBackoffAdaptiveVariable var(/*min=*/4,
                                         /*initial=*/20,
                                         /*max=*/100,
                                         /*multiplier=*/2,
                                         /*decrease_rate=*/1,
                                         /*fuzz_factor=*/0);

  // Negative feedback until we reach the cap at 100.
  ASSERT_EQ(20, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(40, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(80, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(100, var.getCurrentValue());

  // Positive feedback until we go down to the minimum cap (4).
  auto now = TS::now();
  var.positiveFeedback(now);
  ASSERT_EQ(100, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(99.5, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(99.0, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(98.5, var.getCurrentValue());
  now += 5s;
  var.positiveFeedback(now);
  ASSERT_EQ(93.5, var.getCurrentValue());
  now += 40s;
  var.positiveFeedback(now);
  ASSERT_EQ(53.5, var.getCurrentValue());
  now += 1ms;
  var.positiveFeedback(now);
  ASSERT_EQ(53.499, var.getCurrentValue());
  now += 100s;
  var.positiveFeedback(now);
  ASSERT_EQ(4, var.getCurrentValue());

  // Negative feedback until we reach 32.
  var.negativeFeedback();
  ASSERT_EQ(8, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(16, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(32, var.getCurrentValue());
}

TEST(ExponentialBackoffAdaptiveVariableTest, TestChrono) {
  ChronoExponentialBackoffAdaptiveVariable<std::chrono::seconds> var(
      /*min=*/4s,
      /*initial=*/20s,
      /*max=*/100s,
      /*multiplier=*/2,
      /*decrease_rate=*/1,
      /*fuzz_factor=*/0);

  // Negative feedback until we reach the cap at 100s.
  ASSERT_EQ(20s, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(40s, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(80s, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(100s, var.getCurrentValue());

  // Positive feedback until we go down to the minimum cap (4s).
  // The chrono variable has a granularity of seconds, but the underlying
  // implementation uses floating point values, which means that we can call
  // `positiveFeedback` at intervals < 1s and the variable will decrease
  // according to the rate across successive calls.
  auto now = TS::now();
  var.positiveFeedback(now);
  ASSERT_EQ(100s, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(99s, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(99s, var.getCurrentValue());
  now += 500ms;
  var.positiveFeedback(now);
  ASSERT_EQ(98s, var.getCurrentValue());
  now += 5s;
  var.positiveFeedback(now);
  ASSERT_EQ(93s, var.getCurrentValue());
  now += 40s;
  var.positiveFeedback(now);
  ASSERT_EQ(53s, var.getCurrentValue());
  now += 1ms;
  var.positiveFeedback(now);
  ASSERT_EQ(53s, var.getCurrentValue());
  now += 100s;
  var.positiveFeedback(now);
  ASSERT_EQ(4s, var.getCurrentValue());

  // Negative feedback until we reach 32s.
  var.negativeFeedback();
  ASSERT_EQ(8s, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(16s, var.getCurrentValue());
  var.negativeFeedback();
  ASSERT_EQ(32s, var.getCurrentValue());
}

}} // namespace facebook::logdevice
