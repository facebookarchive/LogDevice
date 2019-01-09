/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"

namespace facebook { namespace logdevice {

/**
 * @file Implementation of FindTime that leverages the LogsDB implementation to
 *       reduce the number of seeks.
 *       @see logdevice/common/FindTimeRequest.h for more information about
 *       FindTime.
 */

class PartitionedRocksDBStore::FindTime {
 public:
  FindTime(const PartitionedRocksDBStore& store,
           logid_t logid,
           std::chrono::milliseconds timestamp,
           lsn_t min_lo,
           lsn_t max_hi,
           bool approximate = false,
           bool allow_blocking_io = true,
           std::chrono::steady_clock::time_point deadline =
               std::chrono::steady_clock::time_point::max());

  /**
   * Look for the narrowest range of LSNs (lo, hi] such that the earliest record
   * with a timestamp >= timestamp_ is surely in the range.
   *
   * @param result_lo On success, the lower bound of the result range is written
   *                  at this memory location.
   * @param result_hi On success, the upper bound of the result range is written
   *                  at this memory location.
   * @return 0 on success, -1 on failure and err is set to:
   *         - E::FAILED:     there was an error reading from the database.
   *         - E::WOULDBLOCK: operation would block waiting for io but was not
   *                          allowed to do so
   */
  int execute(lsn_t* lo, lsn_t* hi);

 private:
  /**
   * Does a binary search in directory to find: (a) lower bound on lo_ and
   * upper bound on hi_, at partition granularity, and (b) out_partition -
   * the partition that both covers timestamp_ and contains some records for
   * this log.
   *
   * @param out_partition The partition inside which we need to do a further
   *                      search to improve the precision of lo_ and hi_.
   *                      nullptr if no further search is needed.
   * @param out_first_lsn first_lsn from the directory entry corresponding
   *                      to out_partition will be written here.
   * @return    0 on success, -1 on error and err is set to:
   *            - E::FAILED: there was an error reading from rocksdb
   *            - E::WOULDBLOCK: iterator got into incomplete state
   *            - E::TIMEDOUT: the clint request is timed out already and
   *               there is no reason to continue task execution.
   */
  int findPartition(PartitionPtr* out_partition, lsn_t* out_first_lsn) const;

  /**
   * Do a binary search or, if the findTime index is used, a seek on the given
   * column family, and update *lo_ and *hi_ with the result. Only one of *lo_
   * and *hi_ may be updated, or none of them if the search is unable to find
   * both a record stamped before `timestamp_` and a record stamped at or after.
   *
   * @param cf Column family on which to search.
   * @return 0 on success or -1 if there is an error reading from rocksdb.
   */
  int partitionSearch(rocksdb::ColumnFamilyHandle* cf) const;

  bool isTimedOut() const {
    return std::chrono::steady_clock::now() >= deadline_;
  }

  const PartitionedRocksDBStore& store_;
  const logid_t logid_;
  const RecordTimestamp timestamp_;

  lsn_t min_lo_;
  lsn_t max_hi_;

  lsn_t* lo_{nullptr};
  lsn_t* hi_{nullptr};

  bool approximate_;
  bool allow_blocking_io_;
  bool use_index_;
  std::chrono::steady_clock::time_point deadline_;
};

}} // namespace facebook::logdevice
