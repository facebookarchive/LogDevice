/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <vector>

#include "../Context.h"
#include "AdminCommandTable.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class Partitions : public AdminCommandTable {
 public:
  explicit Partitions(std::shared_ptr<Context> ctx) : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "partitions";
  }
  std::string getDescription() override {
    return "List of LogsDB partitions that store records.  Each shard on each "
           "node has a sequence of partitions.  Each partition corresponds to "
           "a time range a few minutes or tens of minutes log.  Each partition "
           "is a RocksDB column family.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard",
         DataType::BIGINT,
         "Index of the local log store shard that this partitions belongs to."},
        {"id",
         DataType::BIGINT,
         "Sequence number of this partition. Partitions in each shard are "
         "numbered in chronological order without gaps."},
        {"start_time",
         DataType::TIME,
         "The beginning of the time range that this partition corresponds to. "
         "For most partitions it's equal to the time when partition was "
         "created. The exception is partitions created retroactively to "
         "accommodate rebuilt data."},
        {"min_time",
         DataType::TIME,
         "Approximate minimum timestamp of records stored in this partition."},
        {"max_time",
         DataType::TIME,
         "Approximate maximum timestamp of records stored in this partition."},
        {"min_durable_time",
         DataType::TIME,
         "Persisted (WAL sync has occurred since the value was written) "
         "version of 'min_time'."},
        {"max_durable_time",
         DataType::TIME,
         "Persisted (WAL sync has occurred since the value was written) "
         "version of 'max_time'."},
        {"last_compacted",
         DataType::TIME,
         "Last time when the partition was compacted."},
        {"approx_size",
         DataType::BIGINT,
         "Estimated size of the partition in bytes."},
        {"l0_files",
         DataType::BIGINT,
         "Number of sst (immutable table) files belonging to this partition."},
        {"immutable_memtables",
         DataType::BIGINT,
         "Number of inactive memtables (im-memory write buffers) that are "
         "still kept in memory. The most common cases are memtables in the "
         "process of being flushed to disk (memtable_flush_pending) and "
         "memtables pinned by iterators."},
        {"memtable_flush_pending",
         DataType::BIGINT,
         "Number of memtables (im-memory write buffers) that are in the "
         "process of being flushed to disk."},
        {"active_memtable_size",
         DataType::BIGINT,
         "Size in bytes of the active memtable."},
        {"all_not_flushed_memtables_size",
         DataType::BIGINT,
         "Size in bytes of all memtables that weren't flushed to disk yet. The "
         "difference all_memtables_size-all_not_flushed_memtables_size is "
         "usually equal to the total size of memtables pinned by iterators."},
        {"all_memtables_size",
         DataType::BIGINT,
         "Size in bytes of all memtables. Usually these are: active memtable, "
         "memtables that are being flushed and memtables pinned by iterators."},
        {"est_num_keys",
         DataType::BIGINT,
         "Estimated number of keys stored in the partition.  They're usually "
         "records and copyset index entries. The estimate tends to be poor "
         "when merge operator is used."},
        {"est_mem_by_readers",
         DataType::BIGINT,
         "Estimated memory used by rocksdb iterators in this partition, "
         "excluding block cache. This pretty much only includes SST file "
         "indexes loaded in memory."},
        {"live_versions",
         DataType::BIGINT,
         "Number of live \"versions\" of this column family in RocksDB.  One "
         "(current) version is always live. If this value is greater than one, "
         "it means that some iterators are pinning some memtables or sst "
         "files."},
        {"current_version", DataType::BIGINT, "The current live version"},
        {"append_dirtied_by",
         DataType::TEXT,
         "Nodes that have uncommitted append data in this partition."},
        {"rebuild_dirtied_by",
         DataType::TEXT,
         "Nodes that have uncommitted rebuild data in this partition."}};
  }
  std::string getCommandToSend(QueryContext& ctx) const override {
    std::string expr;
    if (columnHasEqualityConstraint(1, ctx, expr)) {
      return std::string("info partitions ") + expr.c_str() +
          " --spew --json\n";
    } else {
      return std::string("info partitions --spew --json\n");
    }
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
