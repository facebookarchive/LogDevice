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
#include "logdevice/include/types.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoCatchupQueues : public AdminCommand {
 private:
  std::string type_;
  folly::Optional<uint64_t> id_;
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
    return "info catchup_queues [--json]";
  }

  void run() override {
    InfoCatchupQueuesTable table(!json_,
                                 "Client",
                                 "Queued total",
                                 "Queued Immediate",
                                 "Queued delayed",
                                 "Record Bytes Queued",
                                 "Storage task in flight",
                                 "Ping Timer Active",
                                 "Blocked");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoCatchupQueuesTable t(table);
      ServerWorker* w = ServerWorker::onThisThread();
      w->serverReadStreams().getCatchupQueuesDebugInfo(t);
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
