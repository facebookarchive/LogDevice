/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/FailureDetector.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/server/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoGossip : public AdminCommand {
 private:
  node_index_t node_idx_{-1};

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "node-idx", boost::program_options::value<node_index_t>(&node_idx_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("node-idx", 1);
  }

  std::string getUsage() override {
    return "info gossip [node idx]";
  }

  void run() override {
    auto detector = server_->getServerProcessor()->failure_detector_.get();

    do {
      if (detector == nullptr) {
        out_.printf("Failure detector not used.\r\n");
        break;
      }

      auto conf = server_->getParameters()->getUpdateableConfig()->get();

      node_index_t lo = 0;
      node_index_t hi = conf->serverConfig()->getMaxNodeIdx();

      if (node_idx_ != node_index_t(-1)) {
        if (node_idx_ > hi) {
          out_.printf("Node index expected to be in the [0, %u] range\r\n", hi);
          break;
        }
        lo = hi = node_idx_;
      }

      auto cs = server_->getProcessor()->cluster_state_.get();
      auto config = conf->serverConfig();
      for (node_index_t idx = lo; idx <= hi; ++idx) {
        if (config->getNode(idx) == nullptr) {
          continue;
        }
        out_.printf("GOSSIP N%u %s %s %s\r\n",
                    idx,
                    cs->isNodeAlive(idx) ? "ALIVE" : "DEAD",
                    detector->getStateString(idx).c_str(),
                    cs->isNodeBoycotted(idx) ? "BOYCOTTED" : "-");
      }

      if (node_idx_ == node_index_t(-1)) {
        // print domain isolation status in case "info gossip" is issued
        // without index
        out_.printf("%s", detector->getDomainIsolationString().c_str());
        // print current ISOLATION value
        out_.printf(
            "ISOLATED %s\r\n", detector->isIsolated() ? "true" : "false");
      }

    } while (0);
  }
};

}}} // namespace facebook::logdevice::commands
