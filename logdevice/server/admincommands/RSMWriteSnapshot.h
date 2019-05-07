/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class RSMWriteSnapShot : public AdminCommand {
 private:
  std::string snapshot_type_;
  Semaphore semaphore_;
  Status st_;

  void onSnapShotCreated(Status st) {
    st_ = st;
    semaphore_.post();
  }

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "snapshot_type",
        boost::program_options::value<std::string>(&snapshot_type_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("snapshot_type", 1);
  }
  std::string getUsage() override {
    return "rsm write-snapshot eventlog|logsconfig";
  }

  void run() override {
    if (snapshot_type_.empty()) {
      out_.printf("snapshot type is not provided\r\n");
    } else if (snapshot_type_ == "eventlog") {
      auto event_log_owner = EventLogStateMachine::getWorkerIdx(
          server_->getProcessor()->getWorkerCount(WorkerType::GENERAL));
      auto rc = run_on_worker(
          server_->getProcessor(), event_log_owner, WorkerType::GENERAL, [&]() {
            Worker* w = Worker::onThisThread();
            if (w->event_log_) {
              auto cb = [&](Status st) { this->onSnapShotCreated(st); };
              w->event_log_->snapshot(cb);
              return true;
            } else {
              return false;
            }
          });
      if (!rc) {
        // callback is skipped so no need to wait for the semaphore
        out_.printf(
            "This node is not running with an event log state machine\r\n");
      } else {
        semaphore_.wait();
        if (st_ == E::OK) {
          out_.printf("Successfully created eventlog snapshot\r\n");
        } else {
          out_.printf(
              "Could not create eventlog snapshot:%s\r\n", error_name(st_));
        }
      }
    } else if (snapshot_type_ == "logsconfig") {
      auto logsconfig_worker_type =
          LogsConfigManager::workerType(server_->getProcessor());
      auto logsconfig_owner_worker =
          LogsConfigManager::getLogsConfigManagerWorkerIdx(
              server_->getProcessor()->getWorkerCount(logsconfig_worker_type));
      auto rc = run_on_worker(
          server_->getProcessor(),
          logsconfig_owner_worker,
          logsconfig_worker_type,
          [&]() {
            Worker* w = Worker::onThisThread();
            if (w->logsconfig_manager_ &&
                w->logsconfig_manager_->getStateMachine()) {
              auto cb = [&](Status st) { this->onSnapShotCreated(st); };
              w->logsconfig_manager_->getStateMachine()->snapshot(cb);
              return true;
            } else {
              return false;
            }
          });
      if (!rc) {
        out_.printf(
            "This node is not running with an logs config state machine\r\n");
      } else {
        semaphore_.wait();
        if (st_ == E::OK) {
          out_.printf("Successfully created logsconfig snapshot\r\n");
        } else {
          out_.printf(
              "Could not create logsconfig snapshot:%s\r\n", error_name(st_));
        }
      }
    } else {
      out_.printf("Snapshot type not supported\r\n");
    }
  }
};

}}} // namespace facebook::logdevice::commands
