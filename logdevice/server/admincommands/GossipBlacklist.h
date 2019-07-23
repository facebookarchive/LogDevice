/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class GossipBlacklist : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  node_index_t node_idx_{-1};
  bool blacklist_;

 public:
  explicit GossipBlacklist(
      bool blacklist,
      RestrictionLevel restrictionLevel = RestrictionLevel::UNRESTRICTED)
      : AdminCommand(restrictionLevel), blacklist_(blacklist) {}

  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "node-idx", boost::program_options::value<node_index_t>(&node_idx_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("node-idx", 1);
  }

  std::string getUsage() override {
    return "gossip blacklist|whitelist [node idx]";
  }

  void run() override {
    auto detector = server_->getServerProcessor()->failure_detector_.get();

    do {
      if (detector == nullptr) {
        out_.printf("Failure detector not used.\r\n");
        break;
      }

      auto nodes_conf = server_->getParameters()
                            ->getUpdateableConfig()
                            ->getNodesConfiguration();

      node_index_t lo = 0;
      node_index_t hi = nodes_conf->getMaxNodeIndex();

      if (node_idx_ != node_index_t(-1)) {
        if (node_idx_ > hi) {
          out_.printf("Node index expected to be in the [0, %u] range\r\n", hi);
          break;
        }
        lo = hi = node_idx_;
        detector->setBlacklisted(node_idx_, blacklist_);
      }

      for (node_index_t idx = lo; idx <= hi; ++idx) {
        out_.printf(
            "GOSSIP N%u %s\r\n",
            idx,
            detector->isBlacklisted(idx) ? "BLACKLISTED" : "WHITELISTED");
      }

    } while (0);
  }
};

}}} // namespace facebook::logdevice::commands
