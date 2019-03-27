/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/NodesStateAPIHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/server/Server.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
void NodesStateAPIHandler::toNodeState(thrift::NodeState& out,
                                       thrift::NodeIndex index,
                                       bool force) {
  std::shared_ptr<EventLogRebuildingSet> rebuilding_set =
      processor_->rebuilding_set_.get();
  // Rebuilding Set can be nullptr if rebuilding is disabled, or if event_log is
  // not ready yet.
  if (!force && rebuilding_set == nullptr) {
    thrift::NodeNotReady err;
    err.set_message("Node does not have the shard states yet, try a "
                    "different node (or use force=true)");
    throw err;
  }
  auto nodes_configuration = processor_->getNodesConfiguration();
  const ClusterState* cluster_state = processor_->cluster_state_.get();
  // We have the node, let's fill the data that we have into NodeState
  fillNodeState(
      out, index, *nodes_configuration, rebuilding_set.get(), cluster_state);
}

void NodesStateAPIHandler::getNodesState(
    thrift::NodesStateResponse& out,
    std::unique_ptr<thrift::NodesStateRequest> req) {
  auto nodes_configuration = processor_->getNodesConfiguration();
  std::vector<thrift::NodeState> result_states;
  bool force = false;
  thrift::NodesFilter* filter = nullptr;
  if (req) {
    filter = req->get_filter();
    force = req->get_force() ? *req->get_force() : false;
  }
  forFilteredNodes(*nodes_configuration, filter, [&](node_index_t index) {
    thrift::NodeState node_state;
    toNodeState(node_state, index, force);
    result_states.push_back(std::move(node_state));
  });
  out.set_states(std::move(result_states));
  out.set_version(
      static_cast<int64_t>(nodes_configuration->getVersion().val()));
}

}} // namespace facebook::logdevice
