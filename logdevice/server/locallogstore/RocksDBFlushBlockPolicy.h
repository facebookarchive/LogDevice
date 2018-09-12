/**
 * Copyright (c) 2017-present, Facebook, Inc.
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
    // and value sizes, which may underestimate a little.

    bool ret = false;
    Group g = getGroup(key, value);

    if (cur_block_bytes_ >= opts_.block_size ||
        (g != cur_group_ && cur_block_bytes_ >= opts_.min_block_size)) {
      STAT_INCR(opts_.stats, sst_blocks_written);
      STAT_ADD(opts_.stats, sst_blocks_bytes, cur_block_bytes_);
      cur_group_ = g;
      cur_block_bytes_ = 0;
      ret = true;
    }

    cur_block_bytes_ += key.size() + value.size();

    return ret;
  }

  ~RocksDBFlushBlockPolicy() override {
    // Bump stats for the final block, assuming that RocksDB destroys the
    // FlushBlockPolicy after writing each file.
    if (cur_block_bytes_ == 0) {
      return;
    }
    STAT_INCR(opts_.stats, sst_blocks_written);
    STAT_ADD(opts_.stats, sst_blocks_bytes, cur_block_bytes_);
  }

 private:
  struct Group {
    logid_t log{0};
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
      // ignore return value
      LocalLogStoreRecordFormat::getCopysetHash(
          Slice(value.data(), value.size()), &g.copyset_hash);
    }

    return g;
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
