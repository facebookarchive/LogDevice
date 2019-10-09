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

class InfoWriteMetaDataRecord : public AdminCommand {
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
    return "info write_metadata_record [--json]";
  }

  void run() override {
    InfoWriteMetaDataRecordTable table(!json_,
                                       "Log ID",
                                       "Epoch",
                                       "State",
                                       "Target LSN",
                                       "Recovery Only",
                                       "Recovery Status",
                                       "Releases Sent",
                                       "Created On");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoWriteMetaDataRecordTable t(table);
      ServerWorker* w = ServerWorker::onThisThread();
      for (auto& it : w->runningWriteMetaDataRecords().map) {
        ld_check(it.second != nullptr);
        it.second->getDebugInfo(t);
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
