/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerTraceRequest.h"

#include "logdevice/server/ServerWorker.h"

namespace facebook { namespace logdevice {

Request::Execution NodeStatsControllerTraceRequest::execute() {
  ServerWorker* worker = ServerWorker::onThisThread();
  auto controller = worker->nodeStatsControllerCallback();
  if (!controller) {
    ld_critical("Controller not found on this thread even though this node "
                "just boycotted a sequencer node!");
    return Execution::COMPLETE;
  }

  controller->traceBoycott(boycotted_node_, start_time_, duration_);
  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
