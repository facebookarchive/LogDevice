/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/test/FlowGroupTest.h"

using namespace facebook::logdevice;

namespace {

TEST(FlowMeterTest, FlowMeterFill) {
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  test_bucket.setCapacity(300);

  // With a deposit budget of 200, we should be able to fill up to
  // 200 bytes. This won't exceed the specified bucket capacity.
  size_t deposit_budget = 200;

  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  // We are limited by the deposit budget.
  ASSERT_EQ(test_bucket.fill(150, deposit_budget), 25);
  ASSERT_EQ(test_bucket.level(), 200);
  // And continue to be until it is reset
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 25);
  ASSERT_EQ(test_bucket.level(), 200);

  // If we give the meter more deposit budget, we can fill up to the
  // capacity limit.
  deposit_budget = 200;
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  // We are limited by capacity.
  ASSERT_EQ(test_bucket.fill(35, deposit_budget), 10);
  ASSERT_EQ(test_bucket.level(), 300);

  // Still out of capacity. We can't fill.
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 25);
  ASSERT_EQ(test_bucket.level(), 300);

  // But if we extend the capacity, we should again run until running
  // out of deposit budget.
  test_bucket.setCapacity(500);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  ASSERT_EQ(test_bucket.fill(25, deposit_budget), 0);
  // We are limited by the deposit budget.
  ASSERT_EQ(test_bucket.fill(40, deposit_budget), 15);
  ASSERT_EQ(test_bucket.level(), 400);

  // If we shrink the bucket, the deposit budget is increased by
  // the amount we can no longer hold. This is a kind of retroactive
  // overflow.
  test_bucket.setCapacity(100);
  ASSERT_EQ(test_bucket.fill(50, deposit_budget), 350);
  ASSERT_EQ(test_bucket.level(), 100);
  test_bucket.setCapacity(500);
  // Limited by deposit budget which is now 300.
  ASSERT_EQ(deposit_budget, 300);
  ASSERT_EQ(test_bucket.fill(350, deposit_budget), 50);
  ASSERT_EQ(test_bucket.level(), 400);

  // Test whether dropping the bucket capacity with max deposit_budget should
  // not lead to wrap around of deposit_budget.
  deposit_budget = INT64_MAX;
  test_bucket.setCapacity(300);
  ASSERT_EQ(test_bucket.fill(50, deposit_budget), 150);
  ASSERT_EQ(test_bucket.level(), 300);
  ASSERT_EQ(deposit_budget, INT64_MAX);
}

TEST(FlowMeterTest, FlowMeterPutUnutilizedCreditsOverFlowTest) {
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  const int64_t MAX_BUCKET_CAPACITY = INT64_MAX;
  const int64_t MAX_BUCKET_LEVEL = INT64_MAX;

  // test1: single overflow, since bucket and level caps have the same value
  test_bucket.setCapacity(MAX_BUCKET_CAPACITY);
  ASSERT_EQ(test_bucket.level(), 0);
  ASSERT_EQ(test_bucket.returnCredits(MAX_BUCKET_LEVEL - 1), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), MAX_BUCKET_LEVEL - 1);
  ASSERT_EQ(test_bucket.returnCredits(2), /*overflow*/ 1);
  ASSERT_EQ(test_bucket.level(), MAX_BUCKET_LEVEL);

  // test2: level_=MAX_BUCKET_LEVEL, shrink bucket from MAX_BUCKET_LEVEL to 20,
  //        attempt to return 10 credits to the bucket.
  //        EXPECTED OVERFLOW: MAX_BUCKET_LEVEL - new_bucket_capacity(20) +
  //                           added_credits(10)
  test_bucket.setCapacity(20);
  ASSERT_EQ(test_bucket.returnCredits(10), /*overflow*/ MAX_BUCKET_LEVEL - 10);
  ASSERT_EQ(test_bucket.level(), 20);

  // test3: Level is negative (bucket has debt), add MAX_BUCKET_LEVEL, but we
  //        get restricted by bucket capacity
  //        EXPECTED_OVERFLOW:
  ASSERT_EQ(test_bucket.drain(25), true);
  ASSERT_EQ(test_bucket.level(), -5);
  ASSERT_EQ(
      test_bucket.returnCredits(MAX_BUCKET_LEVEL),
      /*overflow*/ (/*debt*/ -5 + /*returned*/ INT64_MAX) - /*capacity*/ 20);
  ASSERT_EQ(test_bucket.level(), 20);

  // test4: Level starts negative, and remains negative even after returning
  //        credit.
  //        EXPECTED_OVERFLOW: 0
  ASSERT_EQ(test_bucket.drain(25), true);
  ASSERT_EQ(test_bucket.level(), -5);
  ASSERT_EQ(test_bucket.returnCredits(1), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), -4);

  // test5: Returning 0 credit is a no-op.
  ASSERT_EQ(test_bucket.returnCredits(0), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), -4);

  // test6: Test largest possible overflow: level_=MAX_BUCKET_LEVEL, shrink
  //        bucket size to 0, and return MAX_BUCKET_LEVEL credits.
  //        EXPECTED_OVERFLOW:
  ASSERT_EQ(test_bucket.returnCredits(4), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), 0);
  test_bucket.setCapacity(MAX_BUCKET_CAPACITY);
  ASSERT_EQ(test_bucket.returnCredits(MAX_BUCKET_LEVEL), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), MAX_BUCKET_LEVEL);
  test_bucket.setCapacity(0);
  size_t max_overflow = folly::constexpr_add_overflow_clamped(
      static_cast<size_t>(MAX_BUCKET_CAPACITY),
      static_cast<size_t>(MAX_BUCKET_LEVEL));
  ASSERT_EQ(test_bucket.returnCredits(MAX_BUCKET_LEVEL), max_overflow);
  ASSERT_EQ(test_bucket.consumeReturnedCreditOverflow(), SIZE_MAX);
  ASSERT_EQ(test_bucket.level(), 0);
}

TEST(FlowMeterTest, FlowMeterPutUnutilizedCredits) {
  size_t megabyte = 1000000;
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  size_t bucket_capacity = 400;
  test_bucket.setCapacity(bucket_capacity);

  // 1. Test returnCredits() when actual credits used were less
  //    than anticipated
  size_t deposit_budget = bucket_capacity / 2;
  ASSERT_EQ(test_bucket.fill(bucket_capacity, deposit_budget),
            /*overflow*/ bucket_capacity / 2);
  ASSERT_EQ(test_bucket.level(), bucket_capacity / 2);
  deposit_budget = bucket_capacity / 2;
  ASSERT_EQ(
      test_bucket.fill(bucket_capacity / 2, deposit_budget), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);

  // Drain 1MB
  ASSERT_TRUE(test_bucket.drain(megabyte, /*drain on negative level*/ true));
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Verify that another regular drain() fails at this point
  ASSERT_FALSE(test_bucket.drain(50));
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Caller realizes he needed 1MB-1000, returns unutilized(1000)
  ASSERT_EQ(test_bucket.returnCredits(1000), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte + 1000);

  // 2. Test returnCredits() when actual credits used were more than
  //    requested
  //
  // Reinitialize meter state - It takes some time to get back the level to >0,
  // let's simulate that by setting a temporary big reset deposit budget
  // , followed by a fill()
  deposit_budget = INT64_MAX;
  ASSERT_EQ(
      test_bucket.fill(bucket_capacity - test_bucket.level(), deposit_budget),
      0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);
  // Revert temporary deposit budget change
  deposit_budget = bucket_capacity / 2;
  // Drain 1MB
  ASSERT_EQ(
      test_bucket.drain(megabyte, /*drain on negative level*/ true), true);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Caller realizes it needed 1000 credits more than it anticipated
  // Regular drain won't allow us to debit the meter further
  ASSERT_EQ(test_bucket.drain(1000), /*drain on negative level*/ false);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte); // no level change
  // Therefore we need to allow drain on negative level
  ASSERT_EQ(test_bucket.drain(1000, /*drain on negative level*/ true), true);
  ASSERT_EQ(test_bucket.level(),
            bucket_capacity - (megabyte + 1000)); // level changes

  // 3. fill() v/s returnCredits()
  // If deposit budget is small, fill() will reject credits, but
  // returnCredits() won't
  //
  // Reset meter state
  deposit_budget = INT64_MAX;
  test_bucket.fill(2 * megabyte, deposit_budget);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);

  // Now change deposit budget to 0 so that we can compare the 2 APIs
  deposit_budget = 0;
  ASSERT_EQ(test_bucket.drain(50), true);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - 50);
  // verify level doesn't change with fill() because of 0 deposit budget
  ASSERT_EQ(test_bucket.fill(50, deposit_budget), 50);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - 50);
  // verify level does change with returnCredits() as it ignores deposit
  // budget, but only looks at capacity
  ASSERT_EQ(test_bucket.returnCredits(50), /*overflow*/ 0);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);
  // reached capacity, now we'll get overflow
  ASSERT_EQ(test_bucket.returnCredits(10), /*overflow*/ 10);
}

TEST(FlowMeterTest, FlowMeterTransferCredit) {
  FlowMeter test_meter;
  auto& dest_bucket = test_meter.entries[0];
  dest_bucket.setCapacity(200);
  // Limit dest bucket to receiving at most 50 bytes.
  size_t dest_deposit_budget = 50;
  // Filling or transferring credits into a bucket is limited by destination
  // bucket's deposit budget.
  auto& source_bucket = test_meter.entries[1];
  size_t source_deposit_budget = /*unlimited*/ INT64_MAX;
  source_bucket.setCapacity(1000);
  ASSERT_EQ(source_bucket.fill(100, source_deposit_budget), 0);
  ASSERT_EQ(source_bucket.level(), 100);
  source_bucket.transferCredit(
      dest_bucket, /*requested amount*/ 100, dest_deposit_budget);
  ASSERT_EQ(dest_bucket.level(), 50);
  ASSERT_EQ(source_bucket.level(), 50);

  // Transfer of credits will be limited by credits available in the source
  // bucket.
  dest_deposit_budget = 100;
  source_bucket.transferCredit(
      dest_bucket, /*requested_amount*/ 100, dest_deposit_budget);
  ASSERT_EQ(source_bucket.level(), 0);
  ASSERT_EQ(dest_bucket.level(), 100);
}

} // anonymous namespace
