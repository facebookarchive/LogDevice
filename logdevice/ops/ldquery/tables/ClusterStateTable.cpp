/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/tables/ClusterStateTable.h"

#include <folly/Conv.h>
#include <folly/json.h>

#include "../Table.h"
#include "../Utils.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/lib/ClientImpl.h"

using facebook::logdevice::Configuration;

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

TableColumns ClusterStateTable::getColumns() const {
  return {{"node_id", DataType::BIGINT, "Id of the node."},
          {"status", DataType::TEXT, "Status of the node."},
          {"dead_nodes",
           DataType::TEXT,
           "List of node IDs that this node believes "
           "to be dead."},
          {"boycotted_nodes", DataType::TEXT, "List of boycotted nodes."}};
}

void ClusterStateTable::addResult(node_index_t node_id,
                                  Status status,
                                  std::vector<uint8_t> nodes_state,
                                  std::vector<node_index_t> boycotted_nodes) {
  ClusterStateRequestResult res;
  res.node_id = node_id;
  res.status = status;
  res.nodes_state = std::move(nodes_state);
  res.boycotted_nodes = std::move(boycotted_nodes);

  {
    std::lock_guard<std::mutex> guard(mutex_);
    results_.push_back(std::move(res));
  }
}

void ClusterStateTable::clearResults() {
  std::lock_guard<std::mutex> guard(mutex_);
  results_.clear();
}

std::string ClusterStateTable::nodesStateToString(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    std::vector<uint8_t> nodes_state) {
  std::vector<node_index_t> dead_nodes;
  if (!nodes_state.empty()) {
    for (auto const& kv : *nodes_configuration.getServiceDiscovery()) {
      if (nodes_state[kv.first] != 0) {
        dead_nodes.push_back(kv.first);
      }
    }
  }

  return folly::join(',', dead_nodes);
}

std::string ClusterStateTable::boycottedNodesToString(
    std::vector<node_index_t> boycotted_nodes) {
  return folly::join(',', boycotted_nodes);
}

std::shared_ptr<TableData> ClusterStateTable::getData(QueryContext& ctx) {
  clearResults();
  auto client = ld_ctx_->getClient();

  ld_check(client);
  ClientImpl* client_impl = static_cast<ClientImpl*>(client.get());
  auto config = client_impl->getConfig();
  const auto& nodes_configuration = config->getNodesConfiguration();

  // If the query contains a constraint on the node id, make sure we only query
  // that node.
  std::string expr;
  node_index_t requested_node_id = -1;
  if (columnHasEqualityConstraint(0, ctx, expr)) {
    requested_node_id = folly::to<node_index_t>(expr);
  }

  int posted_requests = 0;
  Semaphore sem;
  for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
    node_index_t node_id = kv.first;
    if (requested_node_id > -1 && requested_node_id != node_id) {
      continue;
    }

    auto cb = [&sem, node_id, this](Status status,
                                    std::vector<uint8_t> nodes_state,
                                    std::vector<node_index_t> boycotted_nodes) {
      addResult(
          node_id, status, std::move(nodes_state), std::move(boycotted_nodes));
      sem.post();
    };

    std::unique_ptr<Request> req = std::make_unique<GetClusterStateRequest>(
        std::chrono::milliseconds(500), // 500ms timeout
        std::chrono::seconds(60),       // wave timeout is useless here
        std::move(cb),
        nodes_configuration->getNodeID(node_id));

    auto rv = client_impl->getProcessor().postImportant(req);
    ld_check(rv == 0);
    ++posted_requests;
  }

  while (posted_requests > 0) {
    sem.wait();
    --posted_requests;
  }

  auto table = std::make_shared<TableData>();
  {
    std::lock_guard<std::mutex> guard(mutex_);
    for (auto& result : results_) {
      table->cols["node_id"].push_back(s(result.node_id));
      const char* status = "ALIVE";
      if (result.status == E::TIMEDOUT) {
        status = "SUSPECT";
      } else if (result.status != E::OK) {
        status = "DEAD";
      }
      table->cols["status"].push_back(s(status));
      if (result.status == E::OK) {
        table->cols["dead_nodes"].push_back(
            nodesStateToString(*nodes_configuration, result.nodes_state));
        table->cols["boycotted_nodes"].push_back(
            boycottedNodesToString(result.boycotted_nodes));
      } else {
        table->cols["dead_nodes"].push_back(std::string());
        table->cols["boycotted_nodes"].push_back(std::string());
      }
    }
  }

  return table;
}

}}}} // namespace facebook::logdevice::ldquery::tables
