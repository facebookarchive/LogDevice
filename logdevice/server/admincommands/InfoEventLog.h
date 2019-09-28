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
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoEventLog : public AdminCommand {
  using AdminCommand::AdminCommand;

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
    return "info event_log [--json]";
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
                                          "Propagated read ptr");

    std::atomic<lsn_t> min_propagated_version{LSN_MAX};
    lsn_t rsm_version = LSN_INVALID;
    lsn_t read_ptr = LSN_INVALID;

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoReplicatedStateMachineTable t(table);
      Worker* w = Worker::onThisThread();

      // Find the minimum version across all workers'
      // ShardAuthoritativeStatusManager-s. We'll use it to calculate
      // "Propagated read ptr".
      atomic_fetch_min(min_propagated_version,
                       w->shardStatusManager()
                           .getShardAuthoritativeStatusMap()
                           .getVersion());

      if (w->event_log_) {
        w->event_log_->getDebugInfo(t);
        rsm_version = w->event_log_->getVersion();
        read_ptr = w->event_log_->getDeltaReadPtr();

        ld_check_eq(w->rebuilding_coordinator_ != nullptr,
                    server_->getRebuildingCoordinator() != nullptr);
      }
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    ld_check_eq(1ul, table.numRows());
    ld_check(min_propagated_version.load() < LSN_MAX);

    table.set<14>(min_propagated_version.load() == rsm_version
                      ? read_ptr
                      : min_propagated_version.load());

    json_ ? table.printJson(out_) : table.printRowVertically(0, out_);
  }
};

}}} // namespace facebook::logdevice::commands
