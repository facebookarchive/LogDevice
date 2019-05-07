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
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoLogsConfigRsm : public AdminCommand {
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
    return "info logsconfig_rsm [--json]";
  }

  void run() override {
    InfoReplicatedStateMachineTable table(!json_,
                                          "Delta log id",
                                          "Snapshot log id",
                                          "Version",
                                          "Delta read ptr",
                                          "Delta replay tail",
                                          "Snapshot read ptr",
                                          "Snapshot replay tail",
                                          "Stalled waiting for snapshot",
                                          "Delta appends in flight",
                                          "Deltas pending confirmation",
                                          "Snapshot in flight",
                                          "Delta log bytes",
                                          "Delta log records",
                                          "Delta log healthy",
                                          "Propagated version");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoReplicatedStateMachineTable t(table);
      Worker* w = Worker::onThisThread();
      if (w->logsconfig_manager_ &&
          w->logsconfig_manager_->getStateMachine() != nullptr) {
        w->logsconfig_manager_->getStateMachine()->getDebugInfo(t);
      }
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    ld_check_le(table.numRows(), 1ul);

    // Omit the last column: "Propagated version". We're not populating it yet.
    json_
        ? table.printJson(out_, InfoReplicatedStateMachineTable::numCols() - 1)
        : table.printRowVertically(
              0, out_, InfoReplicatedStateMachineTable::numCols() - 1);
  }
};

}}} // namespace facebook::logdevice::commands
