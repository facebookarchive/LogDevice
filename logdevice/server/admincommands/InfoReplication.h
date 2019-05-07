/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoReplication : public AdminCommand {
 private:
  bool json_ = false;
  Semaphore semaphore_;

 public:
  std::string getUsage() override {
    return "info replication [--json]";
  }

  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& /*out_options*/)
      override {}
  void run() override {
    // Show LogsConfig Replication Information
    showReplicationInformation();
  }

  void showReplicationInformation() {
    auto rc = run_on_worker(server_->getProcessor(), 0, [&]() {
      ReplicationInfoTable table(!json_,
                                 "Log Tree Version",
                                 "Narrowest Replication",
                                 "Smallest Replication Factor",
                                 "Tolerable Failure Domains");
      Worker* w = Worker::onThisThread();
      auto config = w->getConfig();
      auto logsconfig = config->localLogsConfig();
      // Tolerable Failure Domain : {RACK: 2} which means that you can take
      // down 2 racks and we _should_ be still read-available. This is only an
      // approximation and is not guaranteed since you might have older data
      // that was replicated with an old replication policy that is more
      // restrictive.
      ld_check(logsconfig != nullptr);
      ReplicationProperty repl = logsconfig->getNarrowestReplication();
      // create the narrowest replication property json
      folly::dynamic narrowest_json = folly::dynamic::object;
      for (auto item : repl.getDistinctReplicationFactors()) {
        narrowest_json[NodeLocation::scopeNames()[item.first]] = item.second;
      }
      // Tolerable Failure Domain
      folly::dynamic tolerable_json = folly::dynamic::object;
      tolerable_json
          [NodeLocation::scopeNames()[repl.getBiggestReplicationScope()]] =
              repl.getReplication(repl.getBiggestReplicationScope()) - 1;

      folly::json::serialization_opts opts;
      table.next()
          .set<0>(logsconfig->getVersion())
          // Narrowest Replication
          .set<1>(folly::json::serialize(narrowest_json, opts))
          // Smallest Replication Factor
          .set<2>(repl.getReplicationFactor())
          // Tolerable Failure Domain
          .set<3>(folly::json::serialize(tolerable_json, opts));

      json_ ? table.printJson(out_) : table.printRowVertically(0, out_);
      return true;
    });
  }
};

}}} // namespace facebook::logdevice::commands
