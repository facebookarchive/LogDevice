/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <utility>
#include <vector>

#include "../Context.h"
#include "../Table.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook {
  namespace logdevice {
    namespace ldquery {
      namespace tables {

class ClusterStateTable : public Table {
 public:
  explicit ClusterStateTable(std::shared_ptr<Context> ctx) : Table(ctx) {}
  static std::string getName() {
    return "cluster_state";
  }
  std::string getDescription() override {
    return "Fetches the state of the gossip-based failure detector from the "
           "nodes of the cluster.  When the status column is OK, the "
           "dead_nodes column contains a list of dead nodes as seen by the "
           "node in question. When status is OK, the unhealthy_nodes column "
           "contains a list of unhealthy nodes as seen by the node in "
           "question. When status is OK, the overloaded_nodes column contains "
           "a list of overloaded nodes as seen by the node in question. When "
           "the status is anything but OK, it means the request failed for "
           "this node, and it may be dead itself.";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;

 protected:
  struct ClusterStateRequestResult {
    node_index_t node_id;
    Status status;
    std::vector<std::pair<node_index_t, uint16_t>> nodes_state;
    std::vector<node_index_t> boycotted_nodes;
    std::vector<std::pair<node_index_t, uint16_t>> nodes_status;
  };

  void clearResults();

  void addResult(node_index_t node_id,
                 Status status,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_state,
                 std::vector<node_index_t> boycotted_nodes,
                 std::vector<std::pair<node_index_t, uint16_t>> nodes_status);

  std::string nodesStateToString(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      std::vector<std::pair<node_index_t, uint16_t>> nodes_state);

  std::string boycottedNodesToString(std::vector<node_index_t> boycotted_nodes);

  std::string nodesStatusToUnhealthy(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      std::vector<std::pair<node_index_t, uint16_t>> nodes_status);
  std::string nodesStatusToOverloaded(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      std::vector<std::pair<node_index_t, uint16_t>> nodes_status);

 private:
  std::mutex mutex_;
  std::vector<ClusterStateRequestResult> results_;
};

}}}} // namespace facebook::logdevice::ldquery::tables
