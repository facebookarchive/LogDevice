/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/CompactionRequest.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice { namespace commands {

class Compact : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  shard_index_t shard_{-1};
  bool all_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard", boost::program_options::value<shard_index_t>(&shard_))(
        "all", boost::program_options::bool_switch(&all_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
  }
  std::string getUsage() override {
    return "compact <shard>|--all";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Cannot process compact command, not a storage node\r\n");
      return;
    }

    auto sharded_store = server_->getShardedLocalLogStore();
    bool none_compactable = true;
    bool all_compactable = true;
    for (int i = 0; i < sharded_store->numShards(); ++i) {
      auto store = sharded_store->getByIndex(i);
      if (dynamic_cast<RocksDBLocalLogStore*>(store)) {
        none_compactable = false;
      } else {
        all_compactable = false;
      }
    }
    if (none_compactable) {
      out_.printf(
          "Cannot process compact command, all working shards use partitioned "
          "log store; use `logsdb compact' command\r\n");
      return;
    }
    if (!all_compactable) {
      out_.printf(
          "Note: some shards use partitioned log store or are disabled; "
          "for partitioned store use `logsdb compact' command\r\n");
    }

    ld_info("Compact command 'compact %u%s' received.",
            shard_,
            all_ ? " --all" : "");

    if ((shard_ != -1) == all_) {
      out_.printf("USAGE %s\r\n", getUsage().c_str());
      return;
    }

    const ShardedStorageThreadPool* const sharded_pool =
        server_->getServerProcessor()->sharded_storage_thread_pool_;

    folly::Optional<int> shard_idx;
    if (!all_) {
      ld_check(shard_ != -1);
      shard_size_t num_shards = sharded_pool->numShards();
      if (shard_ < 0 || shard_ >= num_shards) {
        out_.printf("Invalid value for <shard>. This storage node has %u "
                    "shards\r\n",
                    num_shards);
        return;
      }
      shard_idx = shard_;
    }

    // To prevent blocking the CommandListener thread, send a request to
    // eventually let a storage thread execute the compaction.
    std::unique_ptr<Request> request =
        std::make_unique<CompactionRequest>(shard_idx);

    // Noted that success here only means we posted the request to processor
    if (server_->getProcessor()->postRequest(request) == 0) {
      out_.printf("Successfully scheduled manual Compaction on rocksdb local "
                  "store\r\n");
    } else {
      out_.printf(
          "Failed to schedule manual Compaction on rocksdb local store\r\n");
    }
  }
};

}}} // namespace facebook::logdevice::commands
