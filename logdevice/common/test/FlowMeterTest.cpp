/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/test/SocketTest_fixtures.h"

using namespace facebook::logdevice;

namespace {

TEST_F(FlowGroupTest, FlowMeterFill) {
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  size_t bucket_capacity = 300;

  // With a deposit budget of 200, we should be able to fill up to
  // 200 bytes. This won't exceed the specified bucket capacity.
  test_bucket.resetDepositBudget(200);

  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  // We are limited by the deposit budget.
  ASSERT_EQ(test_bucket.fill(150, bucket_capacity), 25);
  ASSERT_EQ(test_bucket.level(), 200);
  // And continue to be until it is reset
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 25);
  ASSERT_EQ(test_bucket.level(), 200);

  // If we give the meter more deposit budget, we can fill up to the
  // capacity limit.
  test_bucket.resetDepositBudget(200);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  // We are limited by capacity.
  ASSERT_EQ(test_bucket.fill(35, bucket_capacity), 10);
  ASSERT_EQ(test_bucket.level(), 300);

  // Still out of capacity. We can't fill.
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 25);
  ASSERT_EQ(test_bucket.level(), 300);

  // But if we extend the capacity, we should again run until running
  // out of deposit budget.
  bucket_capacity = 500;
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  ASSERT_EQ(test_bucket.fill(25, bucket_capacity), 0);
  // We are limited by the deposit budget.
  ASSERT_EQ(test_bucket.fill(40, bucket_capacity), 15);
  ASSERT_EQ(test_bucket.level(), 400);

  // If we shrink the bucket, the deposit budget is increased by
  // the amount we can no longer hold. This is a kind of retroactive
  // overflow.
  bucket_capacity = 100;
  ASSERT_EQ(test_bucket.fill(50, bucket_capacity), 350);
  ASSERT_EQ(test_bucket.level(), 100);
  bucket_capacity = 500;
  // Limited by deposit budget which is now 300.
  ASSERT_EQ(test_bucket.fill(350, bucket_capacity), 50);
  ASSERT_EQ(test_bucket.level(), 400);

  // Test whether dropping the bucket capacity with max deposit_budget should
  // not lead to wrap around of deposit_budget.
  test_bucket.resetDepositBudget(INT64_MAX);
  bucket_capacity = 300;
  ASSERT_EQ(test_bucket.fill(50, bucket_capacity), 150);
  ASSERT_EQ(test_bucket.level(), 300);
  ASSERT_EQ(test_bucket.depositBudget(), INT64_MAX);
}

TEST_F(FlowGroupTest, FlowMeterPutUnutilizedCreditsOverFlowTest) {
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  int64_t bucket_capacity = INT64_MAX;
  test_bucket.setCapacity(bucket_capacity);

  // test1: single overflow, since bucket and level are same size
  ASSERT_EQ(test_bucket.level(), 0);
  ASSERT_EQ(test_bucket.returnCredits(INT64_MAX - 1), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), INT64_MAX - 1);
  ASSERT_EQ(test_bucket.returnCredits(2), 1 /* overflow */);
  ASSERT_EQ(test_bucket.level(), INT64_MAX);

  // test2: level_=INT64_MAX, amount=10, bucket=20, expect two overflows
  test_bucket.setCapacity(20);
  ASSERT_EQ(test_bucket.returnCredits(10), INT64_MAX - 10 /* overflow */);
  ASSERT_EQ(test_bucket.level(), 20);

  // test3: level is negative, add INT64_MAX, but we get restricted by
  // bucket size - expect only 1 overflow
  ASSERT_EQ(test_bucket.drain(25), true);
  ASSERT_EQ(test_bucket.level(), -5);
  ASSERT_EQ(test_bucket.returnCredits(INT64_MAX),
            (-5 + INT64_MAX) - 20 /* overflow */);
  ASSERT_EQ(test_bucket.level(), 20);

  // test4: level is negative, and remains negative even after adding amount,
  // i.e. no overflow
  ASSERT_EQ(test_bucket.drain(25), true);
  ASSERT_EQ(test_bucket.level(), -5);
  ASSERT_EQ(test_bucket.returnCredits(1), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), -4);

  // test5: amount=0
  ASSERT_EQ(test_bucket.returnCredits(0), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), -4);

  // test6: level_=INT64_MAX, amount=INT64_MAX, and bucket=0 to get the largest
  // possible overflow
  ASSERT_EQ(test_bucket.returnCredits(4), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), 0);
  test_bucket.setCapacity(INT64_MAX);
  ASSERT_EQ(test_bucket.returnCredits(INT64_MAX), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), INT64_MAX);
  test_bucket.setCapacity(0);
  size_t max_overflow = INT64_MAX;
  max_overflow += INT64_MAX;
  ASSERT_EQ(test_bucket.returnCredits(INT64_MAX), max_overflow /* overflow */);
  ASSERT_EQ(test_bucket.level(), 0);
}

TEST_F(FlowGroupTest, FlowMeterPutUnutilizedCredits) {
  size_t megabyte = 1000000;
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  size_t bucket_capacity = 400;
  test_bucket.setCapacity(bucket_capacity);

  // 1. Test returnCredits() when actual credits used were less
  //    than anticipated
  test_bucket.resetDepositBudget(bucket_capacity / 2);
  ASSERT_EQ(test_bucket.fill(bucket_capacity, bucket_capacity),
            bucket_capacity / 2 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity / 2);
  test_bucket.resetDepositBudget(bucket_capacity / 2);
  ASSERT_EQ(
      test_bucket.fill(bucket_capacity / 2, bucket_capacity), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);

  // Drain 1MB
  ASSERT_TRUE(test_bucket.drain(megabyte, true /* drain on negative level */));
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Verify that another regular drain() fails at this point
  ASSERT_FALSE(test_bucket.drain(50));
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Caller realizes he needed 1MB-1000, returns unutilized(1000)
  ASSERT_EQ(test_bucket.returnCredits(1000), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte + 1000);

  // 2. Test returnCredits() when actual credits used were more than
  //    requested
  //
  // Reinitialize meter state - It takes some time to get back the level to >0,
  // let's simulate that by setting a temporary big reset deposit budget
  // , followed by a fill()
  test_bucket.resetDepositBudget(INT64_MAX);
  ASSERT_EQ(
      test_bucket.fill(bucket_capacity - test_bucket.level(), bucket_capacity),
      0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);
  // Revert temporary deposit budget change
  test_bucket.resetDepositBudget(bucket_capacity / 2);
  // Drain 1MB
  ASSERT_EQ(
      test_bucket.drain(megabyte, true /* drain on negative level */), true);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte);
  // Caller realizes it needed 1000 credits more than it anticipated
  // Regular drain won't allow us to debit the meter further
  ASSERT_EQ(test_bucket.drain(1000), false);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - megabyte); // no level change
  // Therefore we need to allow drain on negative level
  ASSERT_EQ(test_bucket.drain(1000, true /* drain on negateive level */), true);
  ASSERT_EQ(test_bucket.level(),
            bucket_capacity - (megabyte + 1000)); // level changes

  // 3. fill() v/s returnCredits()
  // If deposit budget is small, fill() will reject credits, but
  // returnCredits() won't
  //
  // Reset meter state
  test_bucket.resetDepositBudget(INT64_MAX);
  test_bucket.fill(2 * megabyte, bucket_capacity);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);

  // Now change deposit budget to 0 so that we can compare the 2 APIs
  test_bucket.resetDepositBudget(0);
  ASSERT_EQ(test_bucket.drain(50), true);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - 50);
  // verify level doesn't change with fill() because of 0 deposit budget
  ASSERT_EQ(test_bucket.fill(50, bucket_capacity), 50);
  ASSERT_EQ(test_bucket.level(), bucket_capacity - 50);
  // verify level does change with returnCredits() as it ignores deposit
  // budget, but only looks at capacity
  ASSERT_EQ(test_bucket.returnCredits(50), 0 /* overflow */);
  ASSERT_EQ(test_bucket.level(), bucket_capacity);
  // reached capacity, now we'll get overflow
  ASSERT_EQ(test_bucket.returnCredits(10), 10 /* overflow */);
}

TEST_F(FlowGroupTest, FlowMeterTransferCredit) {
  FlowMeter test_meter;
  auto& test_bucket = test_meter.entries[0];
  auto bucket_capacity = 100;
  // Give test bucket an explicit deposit budget.
  // All other buckets have effectively no limit(INT64_MAX).
  test_bucket.resetDepositBudget(50);
  // Filling or transferring credits into a bucket is limited by destination
  // bucket's deposit budget.
  auto& backup_bucket = test_meter.entries[1];
  ASSERT_EQ(backup_bucket.depositBudget(), INT64_MAX);
  ASSERT_EQ(backup_bucket.fill(100, bucket_capacity), 0);
  ASSERT_EQ(backup_bucket.level(), 100);
  backup_bucket.transferCredit(test_bucket, bucket_capacity);
  ASSERT_EQ(test_bucket.level(), 50);
  ASSERT_EQ(backup_bucket.level(), 50);

  // Transfer of credits will be limited by credits available in the source
  // bucket.
  test_bucket.resetDepositBudget(100);
  backup_bucket.transferCredit(test_bucket, bucket_capacity);
  ASSERT_EQ(backup_bucket.level(), 0);
  ASSERT_EQ(test_bucket.level(), 100);
}

} // anonymous namespace
