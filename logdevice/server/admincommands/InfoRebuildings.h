/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/LogRebuilding.h"
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRebuildingShards : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  std::string getUsage() override {
    return "info rebuilding shards [--json]";
  }

  void run() override {
    runImpl(json_, server_, out_);
  }

  static void runImpl(bool json, Server* server, EvbufferTextOutput& out) {
    InfoRebuildingShardsTable table(!json,
                                    "Shard id",                     // 0
                                    "Rebuilding set",               // 1
                                    "Version",                      // 2
                                    "Global window end",            // 3
                                    "Local window end",             // 4
                                    "Num logs waiting for plan",    // 5
                                    "Num logs catching up",         // 6
                                    "Num logs queued for catch up", // 7
                                    "Num logs in restart queue",    // 8
                                    "Total memory used",            // 9
                                    "Stall timer active",           // 10
                                    "Num Restart timers active",    // 11
                                    "Num active logs",              // 12
                                    "Participating",                // 13
                                    "Time by state",                // 14
                                    "Task in flight",               // 15
                                    "Persistent error",             // 16
                                    "Read buffer bytes",            // 17
                                    "Records in flight",            // 18
                                    "Read pointer",                 // 19
                                    "Progress");                    // 20

    auto tables =
        run_on_workers(server->getProcessor(), {0}, WorkerType::GENERAL, [&]() {
          InfoRebuildingShardsTable t(table);
          if (server->getRebuildingCoordinator()) {
            server->getRebuildingCoordinator()->getDebugInfo(t);
          }
          return t;
        });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json ? table.printJson(out) : table.print(out);
  }
};

class InfoRebuildingLogs : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  std::string getUsage() override {
    return "info rebuilding logs [--json]";
  }

  void run() override {
    runImpl(json_, server_, out_);
  }

  static void runImpl(bool json, Server* server, EvbufferTextOutput& out) {
    InfoRebuildingLogsTable table(!json,
                                  "Log id",                    // 0
                                  "Shard",                     // 1
                                  "Started",                   // 2
                                  "Rebuilding set",            // 3
                                  "Version",                   // 4
                                  "Until LSN",                 // 5
                                  "Max timestamp",             // 6
                                  "Rebuilt up to",             // 7
                                  "Num replicated",            // 8
                                  "Bytes replicated",          // 9
                                  "RR In flight",              // 10
                                  "NonDurable Stores",         // 11
                                  "Durable Stores",            // 12
                                  "RRA In Flight",             // 13
                                  "NonDurable Amends",         // 14
                                  "Last storage task status"); // 15

    if (server->getParameters()->getRebuildingSettings()->enable_v2) {
      auto funcs = run_on_workers(
          server->getProcessor(), {0}, WorkerType::GENERAL, [&]() {
            if (server->getRebuildingCoordinator()) {
              return server->getRebuildingCoordinator()
                  ->beginGetLogsDebugInfo();
            }
            return std::function<void(InfoRebuildingLogsTable&)>();
          });
      ld_check_le(funcs.size(), 1);
      if (funcs.size() == 1 && funcs[0]) {
        funcs[0](table);
      }
    } else {
      auto tables = run_on_all_workers(server->getProcessor(), [&]() {
        InfoRebuildingLogsTable t(table);
        for (auto& r : Worker::onThisThread()->runningLogRebuildings().map) {
          checked_downcast<LogRebuilding*>(r.second.get())->getDebugInfo(t);
        }
        return t;
      });

      for (int i = 0; i < tables.size(); ++i) {
        table.mergeWith(std::move(tables[i]));
      }
    }

    json ? table.printJson(out) : table.print(out);
  }
};

class InfoRebuildingChunks : public AdminCommand {
 private:
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  std::string getUsage() override {
    return "info rebuilding chunks [--json]";
  }

  void run() override {
    InfoRebuildingChunksTable table(!json_,
                                    "Log id",               // 0
                                    "Shard",                // 1
                                    "Min LSN",              // 2
                                    "Max LSN",              // 3
                                    "Chunk ID",             // 4
                                    "Block ID",             // 5
                                    "Total bytes",          // 6
                                    "Oldest timestamp",     // 7
                                    "Stores in flight",     // 8
                                    "Amends in flight",     // 9
                                    "Amend self in flight", // 10
                                    "Started");             // 11

    auto tables = run_on_all_workers(server_->getProcessor(), [&]() {
      InfoRebuildingChunksTable t(table);
      for (auto& c :
           ServerWorker::onThisThread()->runningChunkRebuildings().map) {
        c.second->getDebugInfo(t);
      }
      return t;
    });

    for (int i = 0; i < tables.size(); ++i) {
      table.mergeWith(std::move(tables[i]));
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

// TODO (#35636262): Migrate ldquery away from it and remove.
//                   Also get rid of the runImpl()s above.
class InfoRebuildingsLegacy : public AdminCommand {
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

  std::string getUsage() override {
    return "info rebuildings [--shards] [--json]\nDEPRECATED: use 'info "
           "rebuilding shards', 'info rebuilding logs' and 'info rebuilding "
           "chunks'";
  }

  void run() override {
    if (shards_) {
      InfoRebuildingShards::runImpl(json_, server_, out_);
    } else {
      InfoRebuildingLogs::runImpl(json_, server_, out_);
    }
  }
};

}}} // namespace facebook::logdevice::commands
