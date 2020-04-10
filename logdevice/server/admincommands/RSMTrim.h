/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
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

class RSMTrim : public AdminCommand {
  using AdminCommand::AdminCommand;

 private:
  std::string rsm_type_;
  Semaphore semaphore_;
  Status st_;

  void onTrimmed(Status st) {
    st_ = st;
    semaphore_.post();
  }

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "rsm_type", boost::program_options::value<std::string>(&rsm_type_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("rsm_type", 1);
  }
  std::string getUsage() override {
    return "rsm trim eventlog|logsconfig";
  }

  void run() override {
    if (rsm_type_.empty()) {
      out_.printf("snapshot type is not provided\r\n");
    } else if (rsm_type_ == "eventlog") {
      auto event_log_owner = EventLogStateMachine::getWorkerIdx(
          server_->getProcessor()->getWorkerCount(WorkerType::GENERAL));
      auto rc = run_on_worker(
          server_->getProcessor(), event_log_owner, WorkerType::GENERAL, [&]() {
            Worker* w = Worker::onThisThread();
            if (w->event_log_) {
              auto cb = [&](Status st) { this->onTrimmed(st); };
              w->event_log_->trim(cb);
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
        } else if (st_ == E::UPTODATE) {
          out_.printf("Eventlog snapshot is already uptodate.\r\n");
        } else {
          out_.printf(
              "Could not create eventlog snapshot:%s\r\n", error_name(st_));
        }
      }
    } else if (rsm_type_ == "logsconfig") {
      auto logsconfig_worker_type =
          LogsConfigManager::workerType(server_->getProcessor());
      auto logsconfig_owner_worker =
          LogsConfigManager::getLogsConfigManagerWorkerIdx(
              server_->getProcessor()->getWorkerCount(logsconfig_worker_type));
      auto rc =
          run_on_worker(server_->getProcessor(),
                        logsconfig_owner_worker,
                        logsconfig_worker_type,
                        [&]() {
                          Worker* w = Worker::onThisThread();
                          if (w->logsconfig_manager_ &&
                              w->logsconfig_manager_->getStateMachine()) {
                            auto cb = [&](Status st) { this->onTrimmed(st); };
                            w->logsconfig_manager_->getStateMachine()->trim(cb);
                            return true;
                          } else {
                            return false;
                          }
                        });
      if (!rc) {
        out_.printf(
            "This node is not running with a logs config state machine\r\n");
      } else {
        semaphore_.wait();
        if (st_ == E::OK) {
          out_.printf("Successfully trimmed logsconfig\r\n");
        } else {
          out_.printf(
              "Could not create logsconfig snapshot:%s\r\n", error_name(st_));
        }
      }
    } else {
      out_.printf("Snapshot type '%s' not supported\r\n", rsm_type_.c_str());
    }
  }
};

}}} // namespace facebook::logdevice::commands
