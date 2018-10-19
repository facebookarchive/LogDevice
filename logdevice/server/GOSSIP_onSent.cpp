/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/GOSSIP_onReceived.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

void GOSSIP_onSent(const GOSSIP_Message& msg,
                   Status st,
                   const Address& to,
                   const SteadyTimestamp /*enqueue-time*/) {
  ServerWorker* w = ServerWorker::onThisThread();
  auto failure_detector = w->processor_->failure_detector_.get();
  // only call the onGossipMessageSent callback if it was a gossip sent by the
  // failure detector thread
  if (failure_detector && w->worker_type_ == WorkerType::FAILURE_DETECTOR) {
    failure_detector->onGossipMessageSent(st, to, msg.msg_id_);
  }
}
}} // namespace facebook::logdevice
