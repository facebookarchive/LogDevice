/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoRsm : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  bool json_ = false;
  std::string action_;
  uint64_t second_option_{std::numeric_limits<uint64_t>::max()};
  Semaphore semaphore_;
  Status st_{E::INVALID_PARAM};
  lsn_t durable_ver_{LSN_INVALID};

  void onGotDurableSnapshot(Status st, lsn_t durable_ver) {
    st_ = st;
    durable_ver_ = durable_ver;
    semaphore_.post();
  }

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
    opts.add_options()(
        "action",
        boost::program_options::value<std::string>(&action_)->required());
    opts.add_options()(
        "second-option",
        boost::program_options::value<uint64_t>(&second_option_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& opts) override {
    opts.add("action", 1);
    opts.add("second-option", 2);
  }

  std::string getUsage() override {
    return "info rsm (versions [node_index] --json | get_trimmable_version "
           "<rsm_delta_log_id> --json)";
  }

  void run() override {
    // Argument validation
    if (action_ != "get_trimmable_version" && action_ != "versions") {
      ld_info("Unknown action \"%s\"", action_.c_str());
      out_.printf("USAGE %s\r\n", getUsage().c_str());
      return;
    }

    if (action_ == "versions") {
      printRsmVersions();
    } else if (action_ == "get_trimmable_version") {
      getTrimmableVersion();
    } else {
      out_.printf("Invalid options\r\n");
    }
  }

  void getTrimmableVersion() {
    if (second_option_ == std::numeric_limits<uint64_t>::max()) {
      out_.printf("USAGE %s\r\n", getUsage().c_str());
      return;
    }
    auto rsm_type = logid_t(second_option_);
    bool run_on_worker_rc{false};
    RSMSnapshotStore* store_ptr;
    switch (rsm_type.val_) {
      case configuration::InternalLogs::EVENT_LOG_DELTAS.val_: {
        auto eventlog_worker_type =
            EventLogStateMachine::workerType(server_->getProcessor());
        auto event_log_owner = EventLogStateMachine::getWorkerIdx(
            server_->getProcessor()->getWorkerCount(eventlog_worker_type));
        run_on_worker_rc = run_on_worker(
            server_->getProcessor(),
            event_log_owner,
            WorkerType::GENERAL,
            [&]() {
              Worker* w = Worker::onThisThread();
              if (w->event_log_) {
                store_ptr = w->event_log_->getSnapshotStore();
                auto durable_ver_cb = [&](Status st_out,
                                          lsn_t durable_ver_out) {
                  this->onGotDurableSnapshot(st_out, durable_ver_out);
                };
                store_ptr->getDurableVersion(durable_ver_cb);
                return true;
              } else {
                return false;
              }
            });
      } break;

      case configuration::InternalLogs::CONFIG_LOG_DELTAS.val_: {
        auto logsconfig_worker_type =
            LogsConfigManager::workerType(server_->getProcessor());
        auto logsconfig_owner_worker =
            LogsConfigManager::getLogsConfigManagerWorkerIdx(
                server_->getProcessor()->getWorkerCount(
                    logsconfig_worker_type));
        run_on_worker_rc = run_on_worker(
            server_->getProcessor(),
            logsconfig_owner_worker,
            logsconfig_worker_type,
            [&]() {
              Worker* w = Worker::onThisThread();
              if (w->logsconfig_manager_ &&
                  w->logsconfig_manager_->getStateMachine()) {
                store_ptr = w->logsconfig_manager_->getStateMachine()
                                ->getSnapshotStore();
                auto durable_ver_cb = [&](Status st_out,
                                          lsn_t durable_ver_out) {
                  this->onGotDurableSnapshot(st_out, durable_ver_out);
                };
                store_ptr->getDurableVersion(durable_ver_cb);
                return true;
              } else {
                return false;
              }
            });
      } break;
      default:
        out_.printf("st:INVALID_PARAM trimmable_ver:LSN_INVALID, rsm:%lu\r\n",
                    rsm_type.val_);
        return;
    }

    if (run_on_worker_rc) {
      semaphore_.wait();
      out_.printf("st:%s trimmable_ver:%s\r\n",
                  error_name(st_),
                  lsn_to_string(durable_ver_).c_str());
    }
  }

  void printRsmVersions() {
    auto fd = server_->getServerProcessor()->failure_detector_.get();
    if (fd == nullptr) {
      out_.printf("Failure detector not present.\r\n");
      return;
    }

    const auto& nodes_configuration =
        server_->getProcessor()->getNodesConfiguration();
    node_index_t lo = 0;
    node_index_t hi = nodes_configuration->getMaxNodeIndex();

    if (second_option_ != std::numeric_limits<uint64_t>::max()) {
      auto node_idx = node_index_t(second_option_);
      if (node_idx > hi) {
        out_.printf("Node index expected to be in the [0, %u] range\r\n", hi);
        return;
      }
      lo = hi = node_idx;
    }

    InfoRsmTable table(!json_,
                       "Peer ID",
                       "State",
                       "logsconfig in-memory version",
                       "logsconfig durable version",
                       "eventlog in-memory version",
                       "eventlog durable version");

    for (node_index_t idx = lo; idx <= hi; ++idx) {
      if (!nodes_configuration->isNodeInServiceDiscoveryConfig(idx)) {
        continue;
      }
      std::map<logid_t, lsn_t> node_rsm_info;
      fd->getRSMVersionsForNode(idx, table);
    }
    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
