/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRsm : public AdminCommand {
  using AdminCommand::AdminCommand;

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
    return "info rsm [node idx]";
  }

  void run() override {
    printPretty();
  }

  void printPretty() {
    auto fd = server_->getServerProcessor()->failure_detector_.get();
    if (fd == nullptr) {
      out_.printf("Failure detector not present.\r\n");
      return;
    }

    const auto& nodes_configuration =
        server_->getProcessor()->getNodesConfiguration();
    node_index_t lo = 0;
    node_index_t hi = nodes_configuration->getMaxNodeIndex();

    if (node_idx_ != node_index_t(-1)) {
      if (node_idx_ > hi) {
        out_.printf("Node index expected to be in the [0, %u] range\r\n", hi);
        return;
      }
      lo = hi = node_idx_;
    }

    for (node_index_t idx = lo; idx <= hi; ++idx) {
      if (!nodes_configuration->isNodeInServiceDiscoveryConfig(idx)) {
        continue;
      }

      std::map<logid_t, lsn_t> node_rsm_info;
      auto st = fd->getRSMVersionsForNode(idx, node_rsm_info);
      if (st == E::OK) {
        std::string node_rsm_str;
        size_t num_rsms = node_rsm_info.size();
        size_t i = 0;
        for (auto& r : node_rsm_info) {
          node_rsm_str += folly::to<std::string>(r.first.val_) + ":" +
              lsn_to_string(r.second);
          if (++i != num_rsms) {
            node_rsm_str += ", ";
          }
        }
        out_.printf("N%u RSM [%s]; ", idx, node_rsm_str.c_str());
      } else {
        out_.printf("N%u RSM []; ", idx);
      }

      std::array<membership::MembershipVersion::Type, 3> ncm_result;
      st = fd->getNCMVersionsForNode(idx, ncm_result);
      if (st == E::OK) {
        out_.printf("NCM[nc:%lu, seq:%lu, storage:%lu]\r\n",
                    ncm_result[0].val_,
                    ncm_result[1].val_,
                    ncm_result[2].val_);
      } else {
        out_.printf("NCM[]\r\n");
      }
    }
  }
};

}}} // namespace facebook::logdevice::commands
