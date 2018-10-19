/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/FindKeyStorageTask.h"

#include <deque>
#include <vector>

#include <gtest/gtest.h>

#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"

using namespace facebook::logdevice;

constexpr std::chrono::milliseconds
/* implicit */
operator"" _ms(unsigned long long val) {
  return std::chrono::milliseconds(val);
}

// Convenience wrapper for FindKeyStorageTask, asserts the result LSN and
// status
static void find_time(LocalLogStore& store,
                      logid_t log_id,
                      std::chrono::milliseconds target_timestamp,
                      lsn_t last_released_lsn,
                      lsn_t expected_lo,
                      lsn_t expected_hi,
                      lsn_t trim_point = 0,
                      std::chrono::steady_clock::time_point deadline =
                          std::chrono::steady_clock::time_point::max()) {
  FindKeyTracer tracer;
  FindKeyStorageTask task(
      ClientID(),
      request_id_t(1),
      log_id,
      target_timestamp,
      folly::none,
      last_released_lsn,
      trim_point,
      0,
      deadline,
      std::move(tracer) /* do not pass a tracer in tests */);
  task.executeImpl(store);
  ASSERT_EQ(expected_lo, task.result_lo_);
  ASSERT_EQ(expected_hi, task.result_hi_);
}

static void find_key(LocalLogStore& store,
                     logid_t log_id,
                     folly::Optional<std::string> target_key,
                     lsn_t expected_lo,
                     lsn_t expected_hi,
                     std::chrono::steady_clock::time_point deadline =
                         std::chrono::steady_clock::time_point::max()) {
  FindKeyTracer tracer;
  FindKeyStorageTask task(
      ClientID(),
      request_id_t(1),
      log_id,
      std::chrono::milliseconds(0),
      target_key,
      LSN_INVALID,
      LSN_INVALID,
      0,
      deadline,
      std::move(tracer) /* do not pass a tracer in tests */);
  task.executeImpl(store);
  ASSERT_EQ(expected_lo, task.result_lo_);
  ASSERT_EQ(expected_hi, task.result_hi_);
}

/**
 * Given the test data that was put in the local log store, this runs an
 * exhaustive test, trying all possible timestamps.
 */
static void exhaustive_test(LocalLogStore& store,
                            const std::vector<TestRecord>& data,
                            const lsn_t last_released_lsn) {
  for (const TestRecord& record : data) {
    ld_check(record.log_id_ == DEFAULT_LOG_ID);
  }

  std::chrono::milliseconds max_ts = data.back().timestamp_ + 1_ms;
  for (auto query_ts = 1_ms; query_ts <= max_ts; ++query_ts) {
    // Looking for the narrowest range (lo, hi] such that `lo` is before the
    // timestamp and `hi` as at or after.

    // First find the expected range by going through all records.
    lsn_t expected_lo = 0;
    lsn_t expected_hi = LSN_MAX;
    for (const TestRecord& record : data) {
      if (record.lsn_ > last_released_lsn) {
        // Must ignore
        continue;
      }

      if (record.timestamp_ < query_ts && record.lsn_ > expected_lo) {
        expected_lo = record.lsn_;
      }
      if (record.timestamp_ >= query_ts && record.lsn_ < expected_hi) {
        expected_hi = record.lsn_;
      }
    }

    // Now check that the production implementation yields the same result.
    find_time(store,
              DEFAULT_LOG_ID,
              query_ts,
              last_released_lsn,
              expected_lo,
              expected_hi);
  }
}

class FindKeyStorageTaskTest : public ::testing::TestWithParam<bool> {};

// Exhaustive test on dataset with consecutive LSNs. This test will be
// instantiated once with read_find_time_index=true, and once with false.
TEST_P(FindKeyStorageTaskTest, TimeSequentialLSNs) {
  std::vector<TestRecord> data{
      TestRecord(1, 10_ms),
      TestRecord(2, 20_ms),
      TestRecord(3, 30_ms),
      TestRecord(4, 40_ms),
      TestRecord(5, 50_ms),
      TestRecord(6, 60_ms),
  };

  TemporaryRocksDBStore store(/*read_find_time_index=*/GetParam());
  store_fill(store, data);

  exhaustive_test(store, data, lsn_t(99));
}

TEST(FindKeyStorageTaskTest, TimeTimedOut) {
  std::vector<TestRecord> data{
      TestRecord(1, 10_ms),
      TestRecord(2, 20_ms),
      TestRecord(3, 30_ms),
      TestRecord(4, 40_ms),
      TestRecord(5, 50_ms),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  // Deadline is current time.
  // There is no time to execute, so result should be invalid due to timeout.
  find_time(store,
            DEFAULT_LOG_ID,
            35_ms,
            lsn_t(100),
            LSN_INVALID,
            LSN_INVALID,
            0,
            std::chrono::steady_clock::now());

  // Giving one second to execute should allow to get correct result.
  find_time(store,
            DEFAULT_LOG_ID,
            35_ms,
            lsn_t(100),
            lsn_t(3),
            lsn_t(4),
            0,
            std::chrono::steady_clock::now() + std::chrono::milliseconds(1000));
}

// Exhaustive test when LSNs are not consecutive
TEST_P(FindKeyStorageTaskTest, TimeGapsBetweenLSNs) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(11, 200_ms),
      TestRecord(13, 300_ms),
      TestRecord(16, 400_ms),
      TestRecord(20, 500_ms),
      TestRecord(25, 600_ms),
      TestRecord(50, 700_ms),
  };

  TemporaryRocksDBStore store(/*read_find_time_index=*/GetParam());
  store_fill(store, data);
  exhaustive_test(store, data, lsn_t(100));
}

// Empty store
TEST_P(FindKeyStorageTaskTest, EmptyStore) {
  TemporaryRocksDBStore store(/*read_find_time_index=*/GetParam());

  // With an empty store, the best answer we can get is that the result is in
  // the range <0, LSN_MAX].
  find_time(store, DEFAULT_LOG_ID, 100_ms, lsn_t(50), lsn_t(0), LSN_MAX);
  find_time(store, DEFAULT_LOG_ID, 100_ms, lsn_t(99), lsn_t(0), LSN_MAX);
}

// Test that any records that have not yet been released are ignored
TEST(FindKeyStorageTaskTest, TimeLastReleasedRespected) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(20, 200_ms),
      TestRecord(30, 300_ms),
      TestRecord(40, 400_ms),
      TestRecord(50, 500_ms),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  // If last released LSN is 40, it's fine to find that record
  find_time(store, DEFAULT_LOG_ID, 450_ms, lsn_t(40), lsn_t(40), LSN_MAX);
  // But if it's say 35, we should ignore anything after it
  find_time(store, DEFAULT_LOG_ID, 450_ms, lsn_t(35), lsn_t(30), LSN_MAX);

  exhaustive_test(store, data, lsn_t(35));
  exhaustive_test(store, data, lsn_t(40));
  exhaustive_test(store, data, lsn_t(45));
  exhaustive_test(store, data, lsn_t(50));
  exhaustive_test(store, data, lsn_t(55));
}

// Result range should be constrained by trim_point on left side
TEST(FindKeyStorageTaskTest, TimeTrimPointRespected) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(20, 200_ms),
      TestRecord(30, 300_ms),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  find_time(store,
            DEFAULT_LOG_ID,
            200_ms,
            lsn_t(100), // last released
            lsn_t(15),  // expected_lo
            lsn_t(20),  // expected_hi
            lsn_t(15)); // trim point
}

TEST(FindKeyStorageTaskTest, TimeUseIndexBoundsIgnored) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(20, 200_ms),
      TestRecord(30, 300_ms),
      TestRecord(40, 400_ms),
      TestRecord(50, 500_ms),
  };

  TemporaryRocksDBStore store(/*read_find_time_index=*/true);
  store_fill(store, data);

  find_time(store,
            DEFAULT_LOG_ID,
            200_ms,
            lsn_t(100), // last released
            lsn_t(10),  // expected_lo
            lsn_t(20),  // expected_hi
            lsn_t(15)); // trim point

  find_time(store,
            DEFAULT_LOG_ID,
            350_ms,
            lsn_t(25),  // last released
            lsn_t(30),  // expected_lo
            lsn_t(40),  // expected_hi
            lsn_t(15)); // trim point
}

// Exhaustive test with duplicate timestamps
TEST_P(FindKeyStorageTaskTest, TimeDuplicateTimestamps) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(11, 200_ms),
      TestRecord(13, 200_ms),
      TestRecord(16, 200_ms),
      TestRecord(20, 500_ms),
      TestRecord(25, 500_ms),
      TestRecord(40, 600_ms),
      TestRecord(50, 700_ms),
  };

  TemporaryRocksDBStore store(/*read_find_time_index=*/GetParam());
  store_fill(store, data);

  // Manual test
  find_time(store, DEFAULT_LOG_ID, 201_ms, lsn_t(99), lsn_t(16), lsn_t(20));
  find_time(store, DEFAULT_LOG_ID, 500_ms, lsn_t(99), lsn_t(16), lsn_t(20));
  find_time(store, DEFAULT_LOG_ID, 501_ms, lsn_t(99), lsn_t(25), lsn_t(40));

  // Exhaustive test
  exhaustive_test(store, data, lsn_t(100));
}

// Exhaustive test with duplicate LSNs, which we can get in the local log
// store because of multiple waves and failed followup DELETEs
TEST_P(FindKeyStorageTaskTest, TimeDuplicateLSNs) {
  std::vector<TestRecord> data{
      TestRecord(10, 100_ms),
      TestRecord(15, 200_ms, 1),
      TestRecord(15, 200_ms, 2),
      TestRecord(20, 300_ms, 1),
      TestRecord(20, 300_ms, 2),
      TestRecord(20, 300_ms, 3),
      TestRecord(20, 300_ms, 4),
  };

  TemporaryRocksDBStore store(/*read_find_time_index=*/GetParam());
  store_fill(store, data);

  // Manual test
  find_time(store, DEFAULT_LOG_ID, 201_ms, lsn_t(99), lsn_t(15), lsn_t(20));
  find_time(store, DEFAULT_LOG_ID, 301_ms, lsn_t(99), lsn_t(20), LSN_MAX);

  // Exhaustive test
  exhaustive_test(store, data, lsn_t(100));
}

TEST(FindKeyStorageTaskTest, KeySimple) {
  logid_t log_id(1);
  std::vector<TestRecord> data{
      TestRecord(log_id, 10, std::string("10000000")),
      TestRecord(log_id, 20, std::string("10000001")),
      TestRecord(log_id, 30, std::string("10000002")),
      TestRecord(log_id, 40, std::string("10000003")),
      TestRecord(log_id, 50, std::string("10000005")),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  find_key(store, log_id, std::string("10000000"), LSN_INVALID, 10);
  find_key(store, log_id, std::string("10000001"), 10, 20);
  find_key(store, log_id, std::string("10000002"), 20, 30);
  find_key(store, log_id, std::string("10000003"), 30, 40);
  find_key(store, log_id, std::string("10000004"), 40, 50);
  find_key(store, log_id, std::string("10000005"), 40, 50);
}

TEST(FindKeyStorageTaskTest, KeyTimedOut) {
  logid_t log_id(1);
  std::vector<TestRecord> data{
      TestRecord(log_id, 10, std::string("10000000")),
      TestRecord(log_id, 20, std::string("10000001")),
      TestRecord(log_id, 30, std::string("10000002")),
      TestRecord(log_id, 40, std::string("10000003")),
      TestRecord(log_id, 50, std::string("10000005")),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  find_key(store,
           log_id,
           std::string("10000001"),
           LSN_INVALID,
           LSN_INVALID,
           std::chrono::steady_clock::now());
  find_key(store,
           log_id,
           std::string("10000001"),
           10,
           20,
           std::chrono::steady_clock::now() + std::chrono::milliseconds(1000));
}

TEST(FindKeyStorageTaskTest, KeyEmptyStore) {
  TemporaryRocksDBStore store;

  find_key(store, logid_t(1), std::string("12345678"), LSN_INVALID, LSN_MAX);
}

TEST(FindKeyStorageTaskTest, KeyDuplicateKeys) {
  logid_t log_id(1);
  std::vector<TestRecord> data{
      TestRecord(log_id, 10, std::string("10000000")),
      TestRecord(log_id, 20, std::string("10000001")),
      TestRecord(log_id, 30, std::string("10000002")),
      TestRecord(log_id, 40, std::string("10000003")),
      TestRecord(log_id, 50, std::string("10000003")),
      TestRecord(log_id, 60, std::string("10000004")),
  };

  TemporaryRocksDBStore store;
  store_fill(store, data);

  find_key(store, logid_t(1), std::string("10000001"), 10, 20);
  find_key(store, logid_t(1), std::string("10000003"), 30, 40);
  find_key(store, logid_t(1), std::string("10000004"), 50, 60);
}

// Run tests with findTime index disabled and then enabled
INSTANTIATE_TEST_CASE_P(FindKeyStorageTaskTest,
                        FindKeyStorageTaskTest,
                        ::testing::Values(false, true));
