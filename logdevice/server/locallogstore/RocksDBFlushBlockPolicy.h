/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <rocksdb/flush_block_policy.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"

namespace facebook { namespace logdevice {

// RocksDB uses this class to decide when to start a new block when flushing
// an SST file.
class RocksDBFlushBlockPolicy : public rocksdb::FlushBlockPolicy {
 public:
  struct Options {
    size_t block_size;
    size_t min_block_size;
    bool flush_for_each_copyset;
    StatsHolder* stats;
  };

  explicit RocksDBFlushBlockPolicy(const Options& opts) : opts_(opts) {}

  bool Update(const rocksdb::Slice& key, const rocksdb::Slice& value) override {
    // RocksDB's default flush block policy uses BlockBuilder to get a better
    // estimate of the current block size. This class just adds up the key
    // and value sizes, which may be a little underestimated (because rocksdb
    // adds some overhead per key-value pair) or overestimated (because rocksdb
    // uses prefix compression for keys).

    bool ret = false;
    Group g = getGroup(key, value);

    // Cut a block between csi and data records even if the block will be
    // smaller than min_block_size. This keeps csi blocks small, so more of them
    // fit in block cache.
    if (cur_block_bytes_ >= opts_.block_size ||
        (g != cur_group_ && cur_block_bytes_ >= opts_.min_block_size) ||
        ((g.log == LOGID_INVALID) != (cur_group_.log == LOGID_INVALID) &&
         cur_block_bytes_ != 0)) {
      bumpStatsForCurBlock();
      cur_group_ = g;
      cur_block_bytes_ = 0;
      ret = true;
    }

    cur_block_bytes_ += std::max(1ul, key.size() + value.size());

    return ret;
  }

  ~RocksDBFlushBlockPolicy() override {
    // Bump stats for the final block, assuming that RocksDB destroys the
    // FlushBlockPolicy after writing each file.
    bumpStatsForCurBlock();
  }

 private:
  struct Group {
    // LOGID_INVALID means it's not a data record; e.g. metadata, csi,
    // findtime index (even if the metadata/index is associated with some log).
    logid_t log{LOGID_INVALID};
    size_t copyset_hash{0};

    bool operator!=(const Group& rhs) const {
      return std::tie(log, copyset_hash) != std::tie(rhs.log, rhs.copyset_hash);
    }
  };

  const Options opts_;

  size_t cur_block_bytes_ = 0;
  Group cur_group_;

  Group getGroup(const rocksdb::Slice& key, const rocksdb::Slice& value) {
    Group g;
    if (!RocksDBKeyFormat::DataKey::valid(key.data(), key.size())) {
      // Put all non-records in the same group.
      return g;
    }

    g.log = RocksDBKeyFormat::DataKey::getLogID(key.data());

    if (opts_.flush_for_each_copyset) {
      // Logdevice's value format is slightly different for merge operands vs
      // full values: merge operands have an extra 'd' prefix. (This doesn't
      // really serve any purpose and should probably be removed.)
      // RocksDB doesn't tell us whether the `value` is a merge operand or not,
      // so we have to guess. It usually is, so if the first character is 'd',
      // let's assume it's a merge operand.
      auto modified_value = value;
      if (!value.empty() &&
          value.data()[0] == RocksDBWriterMergeOperator::DATA_MERGE_HEADER) {
        modified_value.remove_prefix(1);
      }
      LocalLogStoreRecordFormat::getCopysetHash(
          Slice(modified_value.data(), modified_value.size()), &g.copyset_hash);
    }

    return g;
  }

  void bumpStatsForCurBlock() {
    if (cur_block_bytes_ == 0) {
      return;
    }

    STAT_INCR(opts_.stats, sst_blocks_written);
    STAT_ADD(opts_.stats, sst_blocks_bytes, cur_block_bytes_);
    if (cur_group_.log != LOGID_INVALID) {
      STAT_INCR(opts_.stats, sst_record_blocks_written);
      STAT_ADD(opts_.stats, sst_record_blocks_bytes, cur_block_bytes_);
    }
  }
};

class RocksDBFlushBlockPolicyFactory : public rocksdb::FlushBlockPolicyFactory {
 public:
  RocksDBFlushBlockPolicyFactory(size_t block_size,
                                 size_t min_block_size,
                                 bool flush_for_each_copyset,
                                 StatsHolder* stats)
      : opts_({block_size, min_block_size, flush_for_each_copyset, stats}) {}

  const char* Name() const override {
    return "facebook::logdevice::RocksDBFlushBlockPolicyFactory";
  }

  rocksdb::FlushBlockPolicy*
  NewFlushBlockPolicy(const rocksdb::BlockBasedTableOptions& /*table_options*/,
                      const rocksdb::BlockBuilder&) const override {
    return new RocksDBFlushBlockPolicy(opts_);
  }

 private:
  RocksDBFlushBlockPolicy::Options opts_;
};

}} // namespace facebook::logdevice
