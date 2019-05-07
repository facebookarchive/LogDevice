/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoSyncSequencerRequests : public AdminCommand {
 private:
  bool json_ = false;

  using Table =
      AdminCommandTable<logid_t,                  /* Log id */
                        admin_command_table::LSN, /* Until LSN */
                        admin_command_table::LSN, /* Last Released LSN */
                        std::string               /* Last Status */
                        >;

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
    return "info sync_sequencer_requests [--json]";
  }

  void run() override {
    Table table(
        !json_, "Log id", "Until LSN", "Last Released LSN", "Last Status");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      Table t(table);
      Worker* w = Worker::onThisThread();
      for (auto& it : w->runningSyncSequencerRequests().getList()) {
        t.next().set<0>(it.getLogID());
        const auto next_lsn = it.getNextLSN();
        const auto last_released = it.getLastReleasedLSN();
        const auto last_status = it.getLastStatus();
        if (next_lsn.hasValue()) {
          t.set<1>(next_lsn.value());
        }
        if (last_released.hasValue()) {
          t.set<2>(last_released.value());
        }
        if (last_status.hasValue()) {
          t.set<3>(error_name(last_status.value()));
        }
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
