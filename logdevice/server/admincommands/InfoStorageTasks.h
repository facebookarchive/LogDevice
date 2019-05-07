/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoStorageTasks : public AdminCommand {
 private:
  shard_index_t shard_ = -1;
  bool json_ = false;

 public:
  virtual void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard", boost::program_options::value<shard_index_t>(&shard_))(
        "json", boost::program_options::bool_switch(&json_));
  }
  virtual void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
  }
  virtual std::string getUsage() override {
    return "info storage_tasks [<shard>] [--json]";
  }

  virtual void run() override {
    InfoStorageTasksTable table(!json_,
                                "Shard",
                                "Priority",
                                "Is Write Queue",
                                "Sequence No",
                                "Thread type",
                                "Task type",
                                "Enqueue time",
                                "Durability",
                                "Log ID",
                                "LSN",
                                "Client ID",
                                "Client address",
                                "Extra info");

    if (!server_->getProcessor()->runningOnStorageNode()) {
      if (!json_) {
        out_.printf("Not a storage node.\r\n\r\n");
      }
      return;
    }
    auto sharded_store = server_->getShardedLocalLogStore();

    shard_index_t shard_lo = 0;
    shard_index_t shard_hi = sharded_store->numShards() - 1;

    if (shard_ != -1) {
      if (shard_ < shard_lo || shard_ > shard_hi) {
        if (!json_) {
          out_.printf("Shard index %d out of range [%d, %d]\r\n",
                      shard_,
                      shard_lo,
                      shard_hi);
        }
        return;
      }
      shard_lo = shard_hi = shard_;
    }

    for (shard_index_t shard_idx = shard_lo; shard_idx <= shard_hi;
         ++shard_idx) {
      StorageThreadPool* pool =
          &server_->getServerProcessor()
               ->sharded_storage_thread_pool_->getByIndex(shard_idx);
      pool->getStorageTaskDebugInfo(table);
    }

    if (json_) {
      table.printJson(out_, table.numCols());
    } else {
      table.print(out_, table.numCols());
    }
  }
};

}}} // namespace facebook::logdevice::commands
