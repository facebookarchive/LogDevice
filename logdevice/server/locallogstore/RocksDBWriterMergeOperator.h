/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>
#include <string>

#include <rocksdb/merge_operator.h>
#include <rocksdb/version.h>

#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file Our implementation of rocksdb Merge operator.  Resolves all Merge
 * operations, with a dispatch based on the row type and/or the type of merge
 * (encoded in the Merge value).  Maintains no state, just logic.
 */

class RocksDBWriterMergeOperator final : public rocksdb::MergeOperator {
 public:
  explicit RocksDBWriterMergeOperator(shard_index_t this_shard)
      : thisShard_(this_shard) {}

  // First byte of the value for merge operands on data records
  static constexpr char DATA_MERGE_HEADER = 'd';

  bool FullMerge(const rocksdb::Slice& key,
                 const rocksdb::Slice* existing_value,
                 const std::deque<std::string>& operand_list,
                 std::string* new_value,
                 rocksdb::Logger* logger) const override;

  bool PartialMerge(const rocksdb::Slice& key,
                    const rocksdb::Slice& left_operand,
                    const rocksdb::Slice& right_operand,
                    std::string* new_value,
                    rocksdb::Logger* logger) const override;

  bool PartialMergeMulti(const rocksdb::Slice& key,
                         const std::deque<rocksdb::Slice>& operand_list,
                         std::string* new_value,
                         rocksdb::Logger* logger) const override;

#ifdef LOGDEVICED_ROCKSDB_HAS_FULL_MERGE_V2
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override;
#endif

  const char* Name() const override {
    return "logdevice::RocksDBWriterMergeOperator";
  }

 private:
  shard_index_t thisShard_;

  // Common implementation of FullMerge(), FullMergeV2() and PartialMergeMulti()
  template <typename OperandList>
  bool AnyWayMerge(bool full_merge,
                   const rocksdb::Slice& key,
                   const rocksdb::Slice* existing_value,
                   const OperandList& operand_list,
                   std::string& new_value,
                   rocksdb::Slice* existing_operand) const;
};

}} // namespace facebook::logdevice
