/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/ScopeGuard.h>

#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

namespace facebook { namespace logdevice { namespace commands {

class Failsafe : public AdminCommand {
  using AdminCommand::AdminCommand; // inherit constructor
 private:
  shard_index_t shard_;
  std::string reason_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shard",
        boost::program_options::value<shard_index_t>(&shard_)->required())(
        "reason",
        boost::program_options::value<std::string>(&reason_)->required());
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("shard", 1);
    out_options.add("reason", 1);
  }
  std::string getUsage() override {
    return "failsafe <shard> <reason>";
  }

  void run() override {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      out_.printf("Error: not storage node\r\n");
      return;
    }

    auto sharded_store = server_->getShardedLocalLogStore();

    if (shard_ < 0 || shard_ >= sharded_store->numShards()) {
      out_.printf("Error: shard index %d out of range [0, %d]\r\n",
                  shard_,
                  sharded_store->numShards() - 1);
      return;
    }

    auto store = sharded_store->getByIndex(shard_);

    ld_info("Entering fail-safe mode on shard %d, triggered by admin command",
            (int)shard_);
    bool entered = store->enterFailSafeMode(
        "(from admin command) nothing", // "[...] failed, entering fail safe
                                        // mode"
        reason_.c_str());

    out_.printf(entered ? "Entered fail-safe mode.\r\n"
                        : "The shard is already in fail-safe mode.\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
