/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/ScopeGuard.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

// Uses PartitionedRocksDBStore::LogState to find the list of logs that have
// at least one record in any of our LocalLogStores.
// Doesn't include internal logs: they don't live in partitions and so don't
// have LogState associated with them.

class InfoStoredLogs : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  bool extended_ = false;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "extended", boost::program_options::bool_switch(&extended_))(
        "json", boost::program_options::bool_switch(&json_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}
  std::string getUsage() override {
    return "info stored_logs [--extended] [--json]";
  }

  void run() override {
    InfoStoredLogsTable table(!json_,
                              "Log ID",
                              "Shard",
                              "Highest LSN",
                              "Highest partition",
                              "Highest timestamp approx");

    if (server_->getProcessor()->runningOnStorageNode()) {
      auto sharded_store = server_->getShardedLocalLogStore();

      for (int shard = 0; shard < sharded_store->numShards(); ++shard) {
        auto store = sharded_store->getByIndex(shard);
        auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(store);
        if (partitioned_store == nullptr) {
          if (store->acceptingWrites() == E::DISABLED) {
            continue;
          } else {
            out_.printf("Error: storage is not partitioned\r\n");
            return;
          }
        }
        partitioned_store->listLogs(
            [&](logid_t log,
                lsn_t highest_lsn,
                uint64_t highest_partition,
                RecordTimestamp highest_timestamp_approx) {
              table.next().set<0>(log);
              if (extended_) {
                table.set<1>((uint32_t)shard)
                    .set<2>(highest_lsn)
                    .set<3>(highest_partition)
                    .set<4>(highest_timestamp_approx.toMilliseconds());
              }
            },
            /* only_log_ids */ !extended_);
      }
    }

    size_t columns = extended_ ? table.numCols() : 1;
    json_ ? table.printJson(out_, columns) : table.print(out_, columns);
  }
};

}}} // namespace facebook::logdevice::commands
