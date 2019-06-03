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
#include "logdevice/common/util.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"

namespace facebook { namespace logdevice { namespace commands {

class EraseRecord : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  folly::Optional<shard_index_t> shard_;
  logid_t::raw_type logid_;
  std::string lsn_;

  ShardedRocksDBLocalLogStore* getShardedStore() {
    if (server_->getProcessor()->runningOnStorageNode()) {
      return server_->getShardedLocalLogStore();
    } else {
      out_.printf("Error: not storage node\r\n");
      return nullptr;
    }
  }

  LocalLogStore* getLocalLogStore(shard_index_t shard) {
    auto sharded_store = getShardedStore();
    if (!sharded_store) {
      return nullptr;
    }

    if (shard < 0 || shard >= sharded_store->numShards()) {
      out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                  shard,
                  sharded_store->numShards() - 1);
      return nullptr;
    }

    return sharded_store->getByIndex(shard);
  }

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>()->notifier(
            [this](shard_index_t shard) { shard_ = shard; }))(
        "logid",
        boost::program_options::value<logid_t::raw_type>(&logid_)->required())(
        "lsn", boost::program_options::value<std::string>(&lsn_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
    out_options.add("lsn", 1);
  }
  std::string getUsage() override {
    return "record erase <logid> <lsn> [--shard <shard>]";
  }

  void run() override {
    if (shard_.hasValue()) {
      runOnShard(shard_.value());
    } else {
      auto sharded_store = getShardedStore();
      if (!sharded_store) {
        return;
      }
      for (shard_index_t sidx = 0; sidx < sharded_store->numShards(); sidx++) {
        runOnShard(sidx);
      }
    }
  }

  void runOnShard(shard_index_t shard) {
    auto store = getLocalLogStore(shard);
    if (store == nullptr) {
      return;
    }

    lsn_t lsn;
    if (string_to_lsn(lsn_, lsn) == -1) {
      out_.printf("Invalid LSN.\r\n");
      return;
    }

    ld_info("Erasing record for logid=%lu, shard=%hu and lsn=%s, triggered by "
            "admin command",
            logid_,
            shard,
            lsn_.c_str());

    DeleteWriteOp op = {logid_t(logid_), lsn};

    int rv = store->writeMulti(std::vector<const WriteOp*>(1, &op));

    if (rv != 0) {
      out_.printf("Error: %s\r\n", error_description(err));
      return;
    }

    out_.printf("Record deleted.\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
