/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/NodesStateAPIHandler.h"
#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/server/Server.h"
#include "logdevice/server/ServerProcessor.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {
void NodesStateAPIHandler::toNodeState(thrift::NodeState& out,
                                       thrift::NodeIndex index,
                                       const configuration::Node& node,
                                       bool force) {
  std::shared_ptr<EventLogRebuildingSet> rebuilding_set =
      processor_->rebuilding_set_.get();
  // Rebuilding Set can be nullptr if rebuilding is disabled, or if event_log is
  // not ready yet. Let's figure out which is which.
  if (!ld_server_->getParameters()
           ->getRebuildingSettings()
           ->disable_rebuilding &&
      !rebuilding_set) {
    if (!force && rebuilding_set == nullptr) {
      thrift::NodeNotReady err;
      err.set_message("Node does not have the shard states yet, try a "
                      "different node");
      throw err;
    }
  }
  auto server_config = processor_->config_->getServerConfig();
  const ClusterState* cluster_state = processor_->cluster_state_.get();
  node_index_t my_node_index = server_config->getMyNodeID().index();
  const FailureDetector* failure_detector = processor_->failure_detector_.get();
  // We have the node, let's fill the data that we have into NodeState
  fillNodeState(out,
                my_node_index,
                index,
                node,
                rebuilding_set.get(),
                failure_detector,
                cluster_state);
}

void NodesStateAPIHandler::getNodesState(
    thrift::NodesStateResponse& out,
    std::unique_ptr<thrift::NodesStateRequest> req) {
  auto server_config = processor_->config_->getServerConfig();
  std::vector<thrift::NodeState> result_states;
  bool force = false;
  thrift::NodesFilter* filter = nullptr;
  if (req) {
    filter = req->get_filter();
    force = req->get_force() ? *req->get_force() : false;
  }
  forFilteredNodes(
      server_config->getNodes(),
      filter,
      [&](const std::pair<const node_index_t, configuration::Node>& it) {
        thrift::NodeState node_state;
        toNodeState(node_state, it.first, it.second, force);
        result_states.push_back(std::move(node_state));
      });
  out.set_states(std::move(result_states));
  out.set_version(static_cast<int64_t>(server_config->getVersion().val()));
}

}} // namespace facebook::logdevice
