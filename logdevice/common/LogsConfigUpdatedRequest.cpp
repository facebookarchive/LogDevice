/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LogsConfigUpdatedRequest.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

namespace facebook { namespace logdevice {

Request::Execution LogsConfigUpdatedRequest::execute() {
  Worker* worker = Worker::onThisThread();

  // notify the current worker with the config update
  worker->onLogsConfigUpdated();

  if (clients_to_notify_.empty()) {
    // first run
    clients_to_notify_ = worker->configChangeSubscribers_;
  }

  std::set<ClientID> clients_not_notified;

  for (auto it = clients_to_notify_.begin(); it != clients_to_notify_.end();
       ++it) {
    CONFIG_CHANGED_Header hdr{0,
                              CONFIG_VERSION_INVALID,
                              NodeID(),
                              CONFIG_CHANGED_Header::ConfigType::LOGS_CONFIG,
                              CONFIG_CHANGED_Header::Action::RELOAD};
    auto msg = std::make_unique<CONFIG_CHANGED_Message>(hdr);
    int rv = worker->sender().sendMessage(std::move(msg), *it);
    if (rv != 0) {
      if (err == E::NOBUFS) {
        // will retry this client later
        clients_not_notified.insert(*it);
      } else if ((err != E::UNREACHABLE) && (err != E::SHUTDOWN)) {
        // not inserting here, because this isn't supposed to happen.
        // not inserting on E::UNREACHABLE or E::SHUTDOWN, because we can't
        // reach that ClientID anymore.
        ld_error("Unexpected error: %d (%s)", int(err), error_name(err));
      }
    }
  }

  if (clients_not_notified.size() > 0) {
    // can't send message to these clients - retry after some time
    clients_to_notify_ = clients_not_notified;

    if (!timer_.isAssigned()) {
      timer_.assign([this] { execute(); });
    }
    timer_.activate(timeout_);
  } else {
    // sent to everyone
    delete this;
  }

  // This request is self-owned, returning CONTINUE
  return Execution::CONTINUE;
}

}} // namespace facebook::logdevice
