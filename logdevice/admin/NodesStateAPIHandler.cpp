/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/NodesStateAPIHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"

using namespace facebook::logdevice;
using facebook::logdevice::thrift::InvalidRequest;
using facebook::logdevice::thrift::NodesFilter;
using facebook::logdevice::thrift::NodesStateRequest;
using facebook::logdevice::thrift::NodesStateResponse;

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

folly::SemiFuture<std::unique_ptr<NodesStateResponse>>
NodesStateAPIHandler::semifuture_getNodesState(
    std::unique_ptr<NodesStateRequest> req) {
  /*
   * In the case of using the maintenance manager, we will do use the MM's
   * internal version of the nodes configuration for consistency.
   */
  if (req == nullptr) {
    throw InvalidRequest("Cannot accept getNodesState without arguments");
  }

  auto filter = req->filter_ref().value_or(NodesFilter());
  if (isMaintenanceManagerEnabled()) {
    return maintenance_manager_->getNodesState(filter)
        .via(getThreadManager())
        .thenValue([](auto&& expected_output) {
          if (expected_output.hasError()) {
            expected_output.error().throwThriftException();
            ld_check(false);
          }
          return std::make_unique<NodesStateResponse>(expected_output.value());
        });
  } else {
    // In case we don't have maintenance manager, we try to "approximate" the
    // state.
    auto out = std::make_unique<NodesStateResponse>();
    auto nodes_configuration = processor_->getNodesConfiguration();
    bool force = false;
    if (req) {
      force = req->force_ref().value_or(false);
    }
    std::vector<thrift::NodeState> result_states;
    forFilteredNodes(*nodes_configuration, &filter, [&](node_index_t index) {
      thrift::NodeState node_state;
      toNodeState(node_state, index, force);
      result_states.push_back(std::move(node_state));
    });
    out->set_states(std::move(result_states));
    out->set_version(
        static_cast<int64_t>(nodes_configuration->getVersion().val()));
    return std::move(out);
  }
}

}} // namespace facebook::logdevice
