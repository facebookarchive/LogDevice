/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ClusterStateUpdatedRequest.h"

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

Request::Execution ClusterStateUpdatedRequest::execute() {
  Worker* worker = Worker::onThisThread();
  auto config = worker->getConfig();
  auto cs = worker->getClusterState();
  ld_check(cs);

  for (const auto& entry : config->serverConfig()->getNodes()) {
    if (cs->getNodeState(entry.first) == ClusterState::NodeState::DEAD) {
      NodeID nid(entry.first, entry.second.generation);
      worker->sender().closeServerSocket(nid, E::PEER_UNAVAILABLE);
    }
  }

  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
