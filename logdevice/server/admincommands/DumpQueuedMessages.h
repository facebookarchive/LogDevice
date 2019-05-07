/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class DumpQueuedMessagesRequest : public Request {
 public:
  explicit DumpQueuedMessagesRequest(worker_id_t idx, WorkerType type)
      : Request(RequestType::ADMIN_CMD_DUMP_QUEUED_MESSAGE),
        idx_(idx),
        worker_type_(type) {}

  Execution execute() override {
    Worker* w = Worker::onThisThread();
    ld_info("Messages queued in all sockets on %s: %s",
            w->getName().c_str(),
            w->sender().dumpQueuedMessages(Address(ClientID::INVALID)).c_str());
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return idx_.val_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  worker_id_t idx_;
  WorkerType worker_type_;
};

class DumpQueuedMessages : public AdminCommand {
  using AdminCommand::AdminCommand;

 public:
  void run() override {
    for (int i = 0; i < numOfWorkerTypes(); i++) {
      WorkerType worker_type = workerTypeByIndex(i);
      int nworkers = server_->getProcessor()->getWorkerCount(worker_type);
      int success = 0;
      for (worker_id_t idx(0); idx.val_ < nworkers; ++idx.val_) {
        std::unique_ptr<Request> req =
            std::make_unique<DumpQueuedMessagesRequest>(idx, worker_type);
        success += server_->getProcessor()->postRequest(req) == 0;
      }
      out_.printf("Posted to %d of %d workers (%s)\r\n",
                  success,
                  nworkers,
                  workerTypeStr(worker_type));
    }
  }
};

}}} // namespace facebook::logdevice::commands
