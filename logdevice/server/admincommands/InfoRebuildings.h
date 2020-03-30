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
#include "logdevice/server/RebuildingCoordinator.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRebuildingShards : public AdminCommand {
  using AdminCommand::AdminCommand;

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

  static void runImpl(bool json, Server* server, folly::io::Appender& out) {
    InfoRebuildingShardsTable table(!json,
                                    "Shard id",                  // 0
                                    "Rebuilding set",            // 1
                                    "Version",                   // 2
                                    "Global window end",         // 3
                                    "Progress timestamp",        // 4
                                    "Num logs waiting for plan", // 5
                                    "Total memory used",         // 6
                                    "Num active logs",           // 7
                                    "Participating",             // 8
                                    "Time by state",             // 9
                                    "Task in flight",            // 10
                                    "Persistent error",          // 11
                                    "Read buffer bytes",         // 12
                                    "Records in flight",         // 13
                                    "Read pointer",              // 14
                                    "Progress");                 // 15

    auto workerType = EventLogStateMachine::workerType(server->getProcessor());
    auto workerIdx = EventLogStateMachine::getWorkerIdx(
        server->getProcessor()->getWorkerCount(workerType));
    auto tables =
        run_on_workers(server->getProcessor(), {workerIdx}, workerType, [&]() {
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
  using AdminCommand::AdminCommand;

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

  static void runImpl(bool json, Server* server, folly::io::Appender& out) {
    InfoRebuildingLogsTable table(!json,
                                  "Log id",            // 0
                                  "Shard",             // 1
                                  "Until LSN",         // 2
                                  "Rebuilt up to",     // 3
                                  "Num replicated",    // 4
                                  "Bytes replicated"); // 5

    auto workerType = EventLogStateMachine::workerType(server->getProcessor());
    auto workerIdx = EventLogStateMachine::getWorkerIdx(
        server->getProcessor()->getWorkerCount(workerType));
    auto funcs =
        run_on_workers(server->getProcessor(), {workerIdx}, workerType, [&]() {
          if (server->getRebuildingCoordinator()) {
            return server->getRebuildingCoordinator()->beginGetLogsDebugInfo();
          }
          return std::function<void(InfoRebuildingLogsTable&)>();
        });
    ld_check_le(funcs.size(), 1);
    if (funcs.size() == 1 && funcs[0]) {
      funcs[0](table);
    }

    json ? table.printJson(out) : table.print(out);
  }
};

class InfoRebuildingChunks : public AdminCommand {
  using AdminCommand::AdminCommand;

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

// TODO (#35636262): Remove this after ldquery stops using it. If you're reading
//                   this after June 2020, please go ahead and remove this
//                   class along with its usage in AdminCommandFactory.cpp
class InfoRebuildingsLegacy : public AdminCommand {
  using AdminCommand::AdminCommand;

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
