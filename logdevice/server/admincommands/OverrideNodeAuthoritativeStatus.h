/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/util.h"
#include "logdevice/server/AdminCommand.h"
#include "logdevice/common/request_util.h"

namespace facebook { namespace logdevice { namespace commands {

class OverrideNodeAuthoritativeStatus : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  node_index_t nid_;
  std::string status_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "nid",
        boost::program_options::value<uint64_t>()->notifier([&](uint64_t nid) {
          nid_ = node_index_t(nid);
        }))("status",
            boost::program_options::value<std::string>()->notifier(
                [&](std::string status) { status_ = status; }));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("nid", 1);
    out_options.add("status", 2);
  }
  std::string getUsage() override {
    return "override_node_authoritative_status [nid] [status]";
  }

  void run() override {
    AuthoritativeStatus status;
    if (status_ == "FULLY_AUTHORITATIVE") {
      status = AuthoritativeStatus::FULLY_AUTHORITATIVE;
    } else if (status_ == "UNDERREPLICATION") {
      status = AuthoritativeStatus::UNDERREPLICATION;
    } else if (status_ == "AUTHORITATIVE_EMPTY") {
      status = AuthoritativeStatus::AUTHORITATIVE_EMPTY;
    } else {
      out_.printf("ERROR: invalid status. Must be one of "
                  "FULLY_AUTHORITATIVE|UNDERREPLICATION|AUTHORITATIVE_EMPTY"
                  "\r\n");
      return;
    }

    auto processor = server_->getProcessor();
    auto config = processor->config_;
    const auto* node = config->getServerConfig()->getNode(nid_);
    if (!node || !node->isReadableStorageNode()) {
      out_.printf("Error: node %u is not in the config or is not a storage "
                  "node\r\n",
                  nid_);
      return;
    }
    const size_t num_shards = node->num_shards;

    run_on_all_workers(server_->getProcessor(), [&]() {
      Worker* w = Worker::onThisThread();
      for (shard_index_t shard = 0; shard < num_shards; ++shard) {
        w->clientReadStreams().overrideShardStatus(
            ShardID(nid_, shard), status);
        for (const auto& it : w->runningLogRecoveries().map) {
          it.second->overrideShardStatus(ShardID(nid_, shard), status);
        }
      }
      return 0;
    });
  }
};

}}} // namespace facebook::logdevice::commands
