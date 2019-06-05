/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoShardOperationalState : public AdminCommand {
 private:
  shard_index_t shard_;
  node_index_t node_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "node", boost::program_options::value<node_index_t>(&node_))(
        "shard", boost::program_options::value<shard_index_t>(&shard_));
  }
  std::string getUsage() override {
    return "info shardopstate [<node>] [<shard>]";
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("node", 1);
    out_options.add("shard", 1);
  }

  void run() override {
    auto mm = server_->getMaintenanceManager();
    if (mm) {
      auto result = mm->getShardOperationalState(ShardID(node_, shard_)).get();
      if (result.hasValue()) {
        out_.printf("%s\r\n",
                    apache::thrift::util::enumNameSafe(result.value()).c_str());
      } else {
        out_.printf("%s\n", toString(result.error()).c_str());
      }
    } else {
      out_.printf("Not running MaintenanceManager");
    }
  }
};

}}} // namespace facebook::logdevice::commands
