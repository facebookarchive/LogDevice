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

namespace facebook { namespace logdevice { namespace commands {

class InfoRecordCache : public AdminCommand {
  folly::Optional<logid_t> log_id_;
  shard_index_t shard_ = -1;
  bool json_ = false;

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "logid",
        boost::program_options::value<logid_t::raw_type>()->notifier(
            [this](logid_t::raw_type id) { log_id_ = logid_t(id); }))(
        "shard", boost::program_options::value<shard_index_t>(&shard_))(
        "json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }

  std::string getUsage() override {
    return "info record_cache [<logid>] [--shard <shard>] [--json]";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      return;
    }

    auto sharded_store = server_->getShardedLocalLogStore();
    shard_size_t num_shards = sharded_store->numShards();

    if (shard_ != -1 && (shard_ < 0 || shard_ >= num_shards)) {
      out_.printf("Invalid value for --shard. This storage node has %u "
                  "shards\r\n",
                  num_shards);
      return;
    }

    auto pool = server_->getServerProcessor()->sharded_storage_thread_pool_;
    ld_check(pool);
    if (!pool) {
      // There should be a storage thread pool on storage nodes.
      out_.printf("Internal error.\r\n");
      return;
    }

    LogStorageStateMap& state_map =
        server_->getServerProcessor()->getLogStorageStateMap();

    InfoRecordCacheTable table(!json_,
                               "Log ID",
                               "Shard",
                               "Epoch",
                               "Payload Bytes",
                               "Num Records",
                               "Consistent",
                               "Disabled",
                               "Head ESN",
                               "Max ESN",
                               "First LNG",
                               "Offset within epoch",
                               "tail_record_lsn",
                               "tail_record_ts");

    auto process_one = [&](logid_t /*logid*/, const LogStorageState& state) {
      state.record_cache_->getDebugInfo(table);
      return 0;
    };

    auto find_and_process_one = [&](logid_t logid, shard_index_t s) {
      LogStorageState* log_state = state_map.find(logid, s);
      if (log_state != nullptr) {
        process_one(logid, *log_state);
      }
    };

    if (log_id_.hasValue()) {
      if (shard_ == -1) {
        for (shard_index_t s = 0; s < num_shards; ++s) {
          find_and_process_one(log_id_.value(), s);
        }
      } else {
        find_and_process_one(log_id_.value(), shard_);
      }
    } else {
      if (shard_ == -1) {
        state_map.forEachLog(process_one);
      } else {
        state_map.forEachLogOnShard(shard_, process_one);
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
