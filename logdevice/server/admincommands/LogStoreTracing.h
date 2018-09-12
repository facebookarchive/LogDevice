/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/AdminCommand.h"
#include "logdevice/common/Processor.h"

namespace facebook { namespace logdevice { namespace commands {

class LogStoreTracing : public AdminCommand {
  shard_index_t shard_{-1};
  bool enable_{false};
  bool disable_{false};

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "enable", boost::program_options::bool_switch(&enable_))(
        "disable", boost::program_options::bool_switch(&disable_))(
        "shard", boost::program_options::value<shard_index_t>(&shard_));
  }

  std::string getUsage() override {
    return "log_store_tracing [--shard=<shard_number>] [--enable | --disable]";
  }

  void run() override {
    auto* shard_store = server_->getShardedLocalLogStore();
    if (shard_store == nullptr) {
      out_.printf("log_store_tracing: Server is not configured as a "
                  "storage node. No local log store to trace.\r\n");
      return;
    }

    int cur_shard = 0;
    int last_shard = shard_store->numShards() - 1;
    bool set = false;

    if (shard_ > last_shard) {
      out_.printf("log_store_tracing: --shard too large. "
                  "Must be between 0 and %d\r\n",
                  last_shard);
      return;
    }
    if (shard_ >= 0) {
      cur_shard = last_shard = shard_;
    }

    if (enable_ || disable_) {
      if (enable_ && disable_) {
        out_.printf("log_store_tracing: tracing cannot be both "
                    "enabled and disabled.\r\n");
        return;
      }
      set = true;
    }
    for (; cur_shard <= last_shard; ++cur_shard) {
      auto* store = shard_store->getByIndex(cur_shard);
      out_.printf("Shard %d tracing %s%s\r\n",
                  cur_shard,
                  store->tracingEnabled() ? "enabled" : "disabled",
                  set ? (enable_ ? " -> enabled" : " -> disabled") : "");
      if (set) {
        store->setTracingEnabled(enable_);
      }
    }
  }
};

}}} // namespace facebook::logdevice::commands
