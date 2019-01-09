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
 * @file Implementation of FindKey.
 *       @see logdevice/include/Client.h for more information about FindKey.
 */

class PartitionedRocksDBStore::FindKey {
 public:
  FindKey(const PartitionedRocksDBStore& store,
          logid_t logid,
          std::string key,
          bool approximate = false,
          bool allow_blocking_io = true);

  /**
   * Look for the narrowest range of LSNs (lo, hi] such that the earliest record
   * with a key >= key_ is surely in the range, assuming keys non-decrease with
   * LSN.
   *
   * @param lo On success, the lower bound of the result range is written
   *           at this memory location.
   * @param hi On success, the upper bound of the result range is written
   *           at this memory location.
   * @return 0 on success, -1 on failure and err is set to:
   *         - E::FAILED:     there was an error reading from the database.
   *         - E::WOULDBLOCK: operation would block waiting for io but was not
   *                          allowed to do so
   */
  int execute(lsn_t* lo, lsn_t* hi);

 private:
  /**
   * This function does a binary search on the metadata column family in order
   * to find the partition which contains the record with the LSN lo. During the
   * search, this function updates the range (lo, hi].
   *
   * @param out_partition_lo On success, the PartitionPtr of the partition that
   *                         contains the lower bound is written at this memory
   *                         location.
   * @return 0 on success, -1 on error and err is set to:
   *         - E::FAILED: there was an error reading from rocksdb
   *         - E::WOULDBLOCK: iterator got into incomplete state.
   */
  int findPartitionLo(PartitionPtr* out_partition_lo);

  /**
   * Use the in-partition index to find the precise lower and upper bounds for
   * the findKey result.
   *
   * @param partition   The PartitionPtr corresponding to the partition that
   *                    contains the lower bound.
   * @return 0 on success, -1 on error and err is set to:
   *         - E::FAILED: there was an error reading form rocksdb
   *         - E::WOULDBLOCK: iterator got into incomplete state.
   */
  int findPreciseBound(PartitionPtr partition);

  const PartitionedRocksDBStore& store_;
  const logid_t logid_;
  const std::string key_;

  lsn_t* lo_{nullptr};
  lsn_t* hi_{nullptr};

  bool approximate_;
  bool allow_blocking_io_;
};

}} // namespace facebook::logdevice
