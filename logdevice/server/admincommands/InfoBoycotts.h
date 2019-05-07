/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoBoycotts : public AdminCommand {
 private:
  node_index_t node_idx_{-1};
  bool json_ = false;

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()(
        "node-idx",
        boost::program_options::value<node_index_t>(&node_idx_)->required());
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("node-idx", 1);
  }

  std::string getUsage() override {
    return "info boycotts [--json] <node_idx>";
  }

  void run() override {
    auto detector = server_->getServerProcessor()->failure_detector_.get();

    if (detector == nullptr) {
      out_.printf("Failure detector not used.\r\n");
      return;
    }

    const auto& boycott = detector->getNodeBoycottObject(node_idx_);

    InfoBoycottTable table(!json_,
                           "Node Index",
                           "Is Boycotted?",
                           "Boycott Duration",
                           "Boycott Start Time");

    table.next()
        .set<0>(node_idx_)
        .set<1>(boycott.hasValue())
        .set<2>(boycott.hasValue() ? boycott.value().boycott_duration
                                   : std::chrono::milliseconds(0))
        .set<3>(boycott.hasValue()
                    ? std::chrono::duration_cast<std::chrono::milliseconds>(
                          boycott.value().boycott_in_effect_time)
                    : std::chrono::milliseconds(0));

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
