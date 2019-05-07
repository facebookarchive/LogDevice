/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <time.h>

#include <folly/String.h>
#include <folly/json.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/UpdateableSecurityInfo.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/util.h"

namespace facebook { namespace logdevice { namespace commands {

class Info : public AdminCommand {
 private:
  bool json_ = false;
  bool include_buildinfo_ = false;

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
    opts.add_options()(
        "buildinfo", boost::program_options::bool_switch(&include_buildinfo_));
  }

  std::string getShardsMissingData() {
    if (!server_->getProcessor()->runningOnStorageNode()) {
      return "none";
    }

    auto sharded_store = server_->getShardedLocalLogStore();
    ld_check(sharded_store);
    shard_size_t num_shards = sharded_store->numShards();

    std::vector<uint32_t> rebuilding_shards;
    for (uint32_t shard = 0; shard < num_shards; ++shard) {
      if (server_->getProcessor()->isDataMissingFromShard(shard)) {
        rebuilding_shards.push_back(shard);
      }
    }

    if (rebuilding_shards.empty()) {
      return "none";
    }

    return folly::join(",", rebuilding_shards);
  }

  std::string getUsage() override {
    return "info [--json] [--buildinfo]";
  }

  void run() override {
    InfoTable table(!json_,
                    "PID",
                    "Version",
                    "Build Info",
                    "Package",
                    "Build User",
                    "Build Time",
                    "Start Time",
                    "Uptime",
                    "Server ID",
                    "Shards Missing Data",
                    "Logging Level",
                    "Min Proto",
                    "Max Proto",
                    "Is Auth Enabled",
                    "Auth Type",
                    "Is Unauthenticated Allowed",
                    "Is Permission Checking Enabled",
                    "Permission Checker Type",
                    "RocksDB Version",
                    "Node ID",
                    "Is LogsConfig Manager Enabled");

    auto start_time = server_->getStartTime();
    auto uptime = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now() - start_time);
    char start_time_str[sizeof "2011-10-08T07:07:09+0000"] = {};
    time_t t = std::chrono::system_clock::to_time_t(start_time);
    // Formatting to ISO8601 for better SQL support
    tm startime_tm;
    localtime_r(&t, &startime_tm);
    strftime(start_time_str, sizeof(start_time_str), "%FT%T%z", &startime_tm);

    auto processor = server_->getProcessor();
    auto config = processor->config_;
    auto build_info =
        processor->getPluginRegistry()->getSinglePlugin<BuildInfo>(
            PluginType::BUILD_INFO);

    const auto& security_info = processor->security_info_;
    auto permission_checker = security_info->getPermissionChecker();
    auto principal_parser = security_info->getPrincipalParser();

    table.next()
        .set<0>(getpid())
        .set<1>(build_info->version())
        .set<3>(build_info->packageNameWithVersion())
        .set<4>(build_info->buildUser())
        .set<5>(build_info->buildTimeStr())
        .set<6>(start_time_str)
        .set<7>(uptime.count())
        .set<9>(getShardsMissingData())
        .set<10>(dbg::loglevelToString(dbg::currentLevel))
        .set<11>(Compatibility::MIN_PROTOCOL_SUPPORTED)
        .set<12>(processor->settings()->max_protocol)
        .set<13>(principal_parser != nullptr)
        .set<16>(permission_checker != nullptr)
        .set<18>(std::to_string(ROCKSDB_MAJOR) + "." +
                 std::to_string(ROCKSDB_MINOR) + "." +
                 std::to_string(ROCKSDB_PATCH))
        .set<19>(processor->describeMyNode())
        .set<20>(processor->updateableSettings()->enable_logsconfig_manager);

    if (include_buildinfo_) {
      folly::dynamic map = folly::dynamic::object;
      for (const auto& pair : build_info->fullMap()) {
        map[pair.first] = pair.second;
      }
      table.set<2>(folly::toJson(map));
    } else {
      table.set<2>("<omitted>");
    }
    auto server_id = server_->getServerSettings()->server_id;
    if (!server_id.empty()) {
      table.set<8>(server_id);
    }
    if (principal_parser != nullptr) {
      table.set<14>(AuthenticationTypeTranslator::toString(
                        principal_parser->getAuthenticationType())
                        .c_str());
      table.set<15>(config->get()->serverConfig()->allowUnauthenticated());
    }

    if (permission_checker) {
      table.set<17>(PermissionCheckerTypeTranslator::toString(
                        permission_checker->getPermissionCheckerType())
                        .c_str());
    }

    json_ ? table.printJson(out_) : table.printRowVertically(0, out_);
  }
};

}}} // namespace facebook::logdevice::commands
