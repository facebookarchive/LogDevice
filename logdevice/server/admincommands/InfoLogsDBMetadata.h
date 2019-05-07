/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoLogsDBMetadata : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }
  std::string getUsage() override {
    return "info logsdb metadata [--json]";
  }

  void run() override {
    InfoLogsDBMetadataTable table(!json_,
                                  "Shard",
                                  "Column Family",
                                  "Approx. Size",
                                  "L0 files",
                                  "Immutable Memtables",
                                  "Memtable Flush Pending",
                                  "Active Memtable Size",
                                  "All Memtables Size",
                                  "Est Num Keys",
                                  "Est Mem by Readers",
                                  "Live Versions");

    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_store = server_->getShardedLocalLogStore();

      shard_size_t num_shards = sharded_store->numShards();
      for (shard_index_t shard_idx = 0; shard_idx < num_shards; ++shard_idx) {
        auto store = sharded_store->getByIndex(shard_idx);
        auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
        if (partitioned_store == nullptr) {
          if (!json_) {
            out_.printf("Not partitioned\r\n\r\n");
          }
          continue;
        }

        auto do_cf = [&](const char* cfName, rocksdb::ColumnFamilyHandle* cf) {
          table.next()
              .set<0>(shard_idx)
              .set<1>(cfName)
              .set<2>(partitioned_store->getApproximatePartitionSize(cf))
              .set<3>(partitioned_store->getNumL0Files(cf));

          auto prop = [&](const char* name) -> std::string {
            std::string val;
            bool ok = partitioned_store->getDB().GetProperty(
                cf, rocksdb::Slice(name, strlen(name)), &val);
            if (ok) {
              return val;
            } else {
              return "error";
            }
          };

          table.set<4>(prop("rocksdb.num-immutable-mem-table"))
              .set<5>(prop("rocksdb.mem-table-flush-pending"))
              .set<6>(prop("rocksdb.cur-size-active-mem-table"))
              .set<7>(prop("rocksdb.cur-size-all-mem-tables"))
              .set<8>(prop("rocksdb.estimate-num-keys"))
              .set<9>(prop("rocksdb.estimate-table-readers-mem"))
              .set<10>(prop("rocksdb.num-live-versions"));
        };

        do_cf("metadata", partitioned_store->getMetadataCFHandle());
        do_cf("unpartitioned", partitioned_store->getUnpartitionedCFHandle());
      }
    }

    if (json_) {
      table.printJson(out_);
    } else {
      table.print(out_);
    }
  }
};

}}} // namespace facebook::logdevice::commands
