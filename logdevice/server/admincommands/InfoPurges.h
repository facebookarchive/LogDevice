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
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoPurges : public AdminCommand {
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
    return "info purges [--json]";
  }

  void run() override {
    InfoPurgesTable table(!json_,
                          "Log ID",
                          "State",
                          "Current Last Clean Epoch",
                          "Purge To",
                          "New Last Clean Epoch",
                          "Sequencer",
                          "Epoch state");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoPurgesTable t(table);
      ServerWorker* w = ServerWorker::onThisThread();
      for (auto& it : w->activePurges().map) {
        it.getDebugInfo(t);
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
