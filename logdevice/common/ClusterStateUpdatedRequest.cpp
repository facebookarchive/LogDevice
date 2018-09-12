/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ClusterStateUpdatedRequest.h"

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

Request::Execution ClusterStateUpdatedRequest::execute() {
  Worker* worker = Worker::onThisThread();
  auto config = worker->getConfig();
  auto cs = worker->getClusterState();
  ld_check(cs);

  auto nodes = config->serverConfig()->getNodes();
  for (node_index_t i = 0; i < nodes.size(); i++) {
    if (cs->getNodeState(i) == ClusterState::NodeState::DEAD) {
      NodeID nid(i, nodes[i].generation);
      worker->sender().closeServerSocket(nid, E::PEER_UNAVAILABLE);
    }
  }

  return Execution::COMPLETE;
}

}} // namespace facebook::logdevice
