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

class InfoGraylist : public AdminCommand {
 private:
  bool json_ = false;

 public:
  using SummaryTable = AdminCommandTable<int, // worker_id
                                         int  // graylied_node_idx
                                         >;
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {}

  std::string getUsage() override {
    return "info graylist [--json]";
  }

  void run() override {
    auto get_graylist = [this]() -> std::unordered_set<node_index_t> {
      return Worker::onThisThread()->getGraylistedNodes();
    };

    const auto& graylists = run_on_worker_pool(
        server_->getProcessor(), WorkerType::GENERAL, get_graylist);

    SummaryTable table(!json_, "Worker ID", "Graylisted Node Index");
    for (int i = 0; i < graylists.size(); i++) {
      for (const auto& node : graylists[i]) {
        table.next().set<0>(i).set<1>(node);
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
