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
#include "logdevice/common/Socket.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoSockets : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  std::string getUsage() override {
    return "info sockets [--json]";
  }

  void run() override {
    InfoSocketsTable table(!json_,
                           "State",
                           "Name",
                           "Pending (KB)",
                           "Available (KB)",
                           "Read (MB)",
                           "Write (MB)",
                           "Read-cnt",
                           "Write-cnt",
                           "Proto",
                           "Sendbuf",
                           "Peer Config Version",
                           "Is ssl",
                           "FD");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoSocketsTable t(table);
      getSocketsDebugInfo(Worker::onThisThread()->sender(), t);
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }

 private:
  void getSocketsDebugInfo(Sender& sender, InfoSocketsTable& table) {
    sender.forEachSocket(
        [&table](const Socket& socket) { socket.getDebugInfo(table); });
  }
};

}}} // namespace facebook::logdevice::commands
