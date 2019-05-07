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
#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRecoveries : public AdminCommand {
 private:
  folly::Optional<logid_t> logid_;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "logid",
        boost::program_options::value<uint64_t>()->notifier(
            [&](uint64_t logid) { logid_ = logid_t(logid); }))(
        "json", boost::program_options::bool_switch(&json_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }
  std::string getUsage() override {
    return "info recoveries [logid] [--json]";
  }

  void run() override {
    InfoRecoveriesTable table(getInfoRecoveriesTableColumns(), !json_);

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoRecoveriesTable t(table);
      Worker* w = Worker::onThisThread();
      if (logid_.hasValue()) {
        auto it = w->runningLogRecoveries().map.find(logid_.value());
        if (it != w->runningLogRecoveries().map.end()) {
          it->second->getDebugInfo(t);
        }
      } else {
        for (const auto& it : w->runningLogRecoveries().map) {
          it.second->getDebugInfo(t);
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
