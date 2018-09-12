/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Worker.h"
#include "logdevice/server/AdminCommand.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/LogRebuilding.h"
#include "logdevice/server/RebuildingCoordinator.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRebuildings : public AdminCommand {
 private:
  bool shards_ = false;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "shards", boost::program_options::bool_switch(&shards_))(
        "json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}

  std::string getUsage() override {
    return "info rebuildings [--shards] [--json]";
  }

  void run() override {
    if (shards_) {
      // Show a summary for each shard being rebuilt.
      showShardsRebuildings();
    } else {
      // Show all LogRebuilding state machines.
      showLogRebuildings();
    }
  }

  void showLogRebuildings() {
    InfoRebuildingsTable table(!json_,
                               "Log id",
                               "Shard",
                               "Started",
                               "Rebuilding set",
                               "Version",
                               "Until LSN",
                               "Max timestamp",
                               "Rebuilt up to",
                               "Num replicated",
                               "Bytes replicated",
                               "RR In flight",
                               "NonDurable Stores",
                               "Durable Stores",
                               "RRA In Flight",
                               "NonDurable Amends",
                               "Last storage task status");

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoRebuildingsTable t(table);
      for (auto& r : Worker::onThisThread()->runningLogRebuildings().map) {
        checked_downcast<LogRebuilding*>(r.second.get())->getDebugInfo(t);
      }
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }

  void showShardsRebuildings() {
    InfoShardsRebuildingTable table(!json_,
                                    "Shard id",
                                    "Rebuilding set",
                                    "Version",
                                    "Global window end",
                                    "Local window end",
                                    "Num logs waiting for plan",
                                    "Num logs catching up",
                                    "Num logs queued for catch up",
                                    "Num logs in restart queue",
                                    "Total memory used",
                                    "Stall timer active",
                                    "Num Restart timers active",
                                    "Num active logs",
                                    "Participating");

    auto tables = run_on_workers(
        server_->getProcessor(), {0}, WorkerType::GENERAL, [&]() {
          InfoShardsRebuildingTable t(table);
          if (server_->getRebuildingCoordinator()) {
            server_->getRebuildingCoordinator()->getDebugInfo(t);
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
