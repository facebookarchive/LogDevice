/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"

/**
 * @file Utility that will, given a target timestamp and a rocksdb iterator,
 * look for the narrowest range of LSNs (lo, hi] such that the earliest record
 * with a timestamp >= target timestamp is surely in the range.  The range may
 * be empty (lo == hi) if we are at the end of the log.
 */

namespace facebook { namespace logdevice {

class IteratorSearch {
 public:
  IteratorSearch(const RocksDBLogStoreBase* store,
                 rocksdb::ColumnFamilyHandle* cf,
                 char index_type,
                 uint64_t target_timestamp,
                 std::string target_key,
                 logid_t log_id,
                 lsn_t lo,
                 lsn_t hi,
                 bool allow_blocking_io,
                 std::chrono::steady_clock::time_point deadline =
                     std::chrono::steady_clock::time_point::max());

  /**
   * Do the search.
   *
   * @param result_lo On success, the lower bound of the result range is written
   *                  at this memory location.
   * @param result_hi On success, the upper bound of the result range is written
   *                  at this memory location.
   * @return 0 on success, -1 on failure and err is set to:
   *         - E::WOULDBLOCK: request couldn't be satisfied from cache,
   *                          and allow_blocking_io is false.
   *         - E::FAILED: there was an error reading from the database.
   *         - E::TIMEDOUT: execution of binary search passed deadline
   */
  int execute(lsn_t* result_lo, lsn_t* result_hi);

 private:
  const RocksDBLogStoreBase* store_;

  rocksdb::ColumnFamilyHandle* cf_;

  char index_type_;

  uint64_t target_timestamp_;

  std::string target_key_;

  logid_t log_id_;

  // Upper bound of the range of LSNs in which to search for.
  // Typically this will be the value of last released lsn.
  const lsn_t hi_;
  // Lower bound of the range of LSNs in which to search for.
  // Typically this will be the value of the trim point.
  const lsn_t lo_;

  bool allow_blocking_io_;

  std::chrono::steady_clock::time_point deadline_;

  // Helper method to evaluate one record during the binary search, deciding
  // which bound of the search space (`lo` or `hi`) to move
  enum class Evaluation { ERROR, MOVE_LO, MOVE_HI };
  Evaluation
  evaluateDatabaseResult(const LocalLogStore::ReadIterator& it) const;

  int executeWithBinarySearch(lsn_t* result_lo, lsn_t* result_hi);

  int executeWithIndex(lsn_t* result_lo, lsn_t* result_hi);
};

}} // namespace facebook::logdevice
