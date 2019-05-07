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
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class BoycottReset : public AdminCommand {
 private:
  node_index_t node_idx_{-1};

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "node-idx",
        boost::program_options::value<node_index_t>(&node_idx_)->required());
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("node-idx", 1);
  }

  std::string getUsage() override {
    return "boycott_reset <node idx>";
  }

  void run() override {
    ld_info("Received an admin command to reset the boycott of N%i", node_idx_);

    // use do-while to be able to break out if any invalid state is detected
    do {
      if (node_idx_ < 0) {
        out_.printf("Invalid node id %i given\r\n", node_idx_);
        break;
      }

      auto detector = server_->getServerProcessor()->failure_detector_.get();
      if (detector == nullptr) {
        out_.printf("Failure detector not used.\r\n");
        break;
      }

      detector->resetBoycottedNode(node_idx_);
      out_.printf(
          "The boycott on node %i was successfully reset\r\n", node_idx_);
    } while (0);
  }
};
}}} // namespace facebook::logdevice::commands
