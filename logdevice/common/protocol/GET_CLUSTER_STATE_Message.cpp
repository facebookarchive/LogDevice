/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_CLUSTER_STATE_Message.h"

#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/GET_CLUSTER_STATE_REPLY_Message.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition
GET_CLUSTER_STATE_Message::onReceived(const Address& from) {
  Status status = E::OK;
  std::vector<uint8_t> nodes_state;
  std::vector<node_index_t> boycotted_nodes;
  Worker* w = Worker::onThisThread();

  // check if cluster state is enabled
  auto cs = Worker::getClusterState();
  if (!cs) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Cluster state requested by %s is not available",
                   Sender::describeConnection(from).c_str());
    status = E::NOTSUPPORTED;
  } else if (!(w && w->processor_ &&
               w->processor_->isFailureDetectorRunning())) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "FailureDetector is not running on this node, "
                   "returning E::NOTSUPPORTED");
    status = E::NOTSUPPORTED;
  } else {
    auto my_node_id = Worker::onThisThread()->processor_->getMyNodeID();
    if (!cs->isNodeAlive(my_node_id.index())) {
      RATELIMIT_INFO(
          std::chrono::seconds(5), 1, "Failure detector is not ready");
      status = E::NOTREADY;
    } else {
      size_t count = w->getNodesConfiguration()->getMaxNodeIndex() + 1;
      nodes_state.resize(count);

      for (node_index_t i = 0; i < count; i++) {
        nodes_state[i] = cs->getNodeState(i);

        if (cs->isNodeBoycotted(i)) {
          boycotted_nodes.emplace_back(i);
        }
      }
    }
  }

  GET_CLUSTER_STATE_REPLY_Header hdr({header_.client_rqid, status});
  auto msg = std::make_unique<GET_CLUSTER_STATE_REPLY_Message>(
      hdr, std::move(nodes_state), std::move(boycotted_nodes));
  Worker::onThisThread()->sender().sendMessage(std::move(msg), from);

  return Disposition::NORMAL;
}

template <>
void GET_CLUSTER_STATE_Message::onSent(Status st, const Address& /*to*/) const {
  if (st != E::OK) {
    auto& rqmap = Worker::onThisThread()->runningGetClusterState().map;
    auto it = rqmap.find(header_.client_rqid);
    if (it != rqmap.end()) {
      it->second->onError(st);
    }
  }
}

}} // namespace facebook::logdevice
