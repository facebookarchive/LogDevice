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

class InfoReaders : public AdminCommand {
 private:
  std::string type_;
  folly::Optional<uint64_t> id_;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    // clang-format off
    out_options.add_options()
      ("type", boost::program_options::value<std::string>(&type_)
        ->notifier([&] (const std::string& type) {
          if (!std::set<std::string>({"log", "client", "all"}).count(type)) {
            throw boost::program_options::error("Invalid type: " + type);
          }
        })->required())
      ("id", boost::program_options::value<uint64_t>()
        ->notifier([&] (uint64_t id) {
          id_ = id;
        }))
      ("json", boost::program_options::bool_switch(&json_));
    // clang-format on
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("type", 1);
    out_options.add("id", 1);
  }

  std::string getUsage() override {
    return "info readers client|log|all [<clientid>|<logid>] [--json]";
  }

  void run() override {
    if (type_ == "log") {
      if (!id_.hasValue() || id_.value() <= 0) {
        out_.printf(
            "Invalid parameter for 'info readers logid' "
            "command. Expected a positive log id, "
            "got %s.\r\n",
            id_.hasValue() ? std::to_string(id_.value()).c_str() : "nothing");
        return;
      }
    } else if (type_ == "client") {
      if (!id_.hasValue() ||
          id_.value() > std::numeric_limits<uint32_t>::max() ||
          !ClientID::valid(static_cast<int32_t>(id_.value()))) {
        out_.printf(
            "Invalid parameter for 'info readers client' "
            "command. Expected a valid client id, "
            "got %s.\r\n",
            id_.hasValue() ? std::to_string(id_.value()).c_str() : "nothing");
        return;
      }
    } else if (type_ == "all") {
      if (id_.hasValue()) {
        out_.printf("Unexpected parameter for 'info readers all'.\r\n");
        return;
      }
    } else {
      ld_check(false);
    }

    InfoReadersTable table(!json_,
                           "Shard",
                           "Client",
                           "Log id",
                           "Start LSN",
                           "Until LSN",
                           "Read Pointer",
                           "Last Delivered",
                           "Last Record",
                           "Window High",
                           "Last Released",
                           "Catching UP",
                           "Window END",
                           "Known down",
                           "Filter version",
                           "Last Batch Status",
                           "Created",
                           "Last Enqueue Time",
                           "Last Batch Started Time",
                           "Storage task in flight",
                           "version",
                           "Throttled",
                           "Throttled since(msec)",
                           "ReadShaping(Meter Level)");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoReadersTable t(table);
      ServerWorker* w = ServerWorker::onThisThread();
      if (type_ == "log") {
        w->serverReadStreams().getReadStreamsDebugInfo(logid_t(id_.value()), t);
      } else if (type_ == "client") {
        w->serverReadStreams().getReadStreamsDebugInfo(
            ClientID(id_.value()), t);
      } else {
        w->serverReadStreams().getReadStreamsDebugInfo(t);
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
