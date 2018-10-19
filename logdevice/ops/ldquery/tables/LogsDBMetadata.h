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

class LogsDBMetadata : public AdminCommandTable {
 public:
  explicit LogsDBMetadata(std::shared_ptr<Context> ctx)
      : AdminCommandTable(ctx) {}
  static std::string getName() {
    return "logsdb_metadata";
  }
  std::string getDescription() override {
    return "List of auxiliary RocksDB column families used by LogsDB "
           "(partitioned local log store). \"metadata\" column family contains "
           "partition directory and various logdevice metadata, per-log and "
           "otherwise. \"unpartitioned\" column family contains records of "
           "metadata logs and event log.";
  }
  TableColumns getFetchableColumns() const override {
    return {
        {"shard",
         DataType::BIGINT,
         "Index of the local log store shard that this column family "
         "belongs to."},
        {"column_family", DataType::TEXT, "Name of the column family."},
        {"approx_size",
         DataType::BIGINT,
         "Estimated size of the column family in bytes."},
        {"l0_files",
         DataType::BIGINT,
         "Number of sst (immutable table) files belonging to this column "
         "family."},
        {"immutable_memtables",
         DataType::BIGINT,
         "Number of inactive memtables that are still kept in memory.  See "
         "'partitions' table for details."},
        {"memtable_flush_pending",
         DataType::BIGINT,
         "Number of memtables that are in the process of being flushed to "
         "disk."},
        {"active_memtable_size",
         DataType::BIGINT,
         "Size in bytes of the active memtable."},
        {"all_memtables_size",
         DataType::BIGINT,
         "Size in bytes of all memtables. See 'partitions' table for details."},
        {"est_num_keys",
         DataType::BIGINT,
         "Estimated number of keys stored in the column family.  "
         "The estimate tends to be poor because of merge operator."},
        {"est_mem_by_readers",
         DataType::BIGINT,
         "Estimated memory used by rocksdb iterators in this column family, "
         "excluding block cache. See 'partitions' table for details."},
        {"live_versions",
         DataType::BIGINT,
         "Number of live \"versions\" of this column family in RocksDB. See "
         "'partitions' table for details."},
    };
  }
  std::string getCommandToSend(QueryContext& /*ctx*/) const override {
    return std::string("info logsdb metadata --json\n");
  }
};

}}}} // namespace facebook::logdevice::ldquery::tables
