/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerCallback.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoAppendOutliers : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}

  std::string getUsage() override {
    return "info append_outliers [--json]";
  }

  void run() override {
    InfoAppendOutliersTable table(!json_,
                                  "Observed Node ID",
                                  "Appends Success",
                                  "Appends Failed",
                                  "Msec since",
                                  "Is Outlier");
    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoAppendOutliersTable t(table);

      auto node_stats_controller =
          ServerWorker::onThisThread()->nodeStatsControllerCallback();

      /* Node stats controllers only live on one worker */
      if (node_stats_controller != nullptr) {
        node_stats_controller->getDebugInfo(&t);
      }
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
