/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
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
           "nodes_state column contains a string-representation of the cluster "
           "state as seen by the node, as comma-separated list of elements of "
           "the form <node_id>:{A|D}, where A means alive and D means dead. "
           "eg: N1:A,N2:A,N3:D,N4:A.  When the status is anything but OK, it "
           "means the request failed for this node, and it may be dead itself.";
  }
  TableColumns getColumns() const override;
  std::shared_ptr<TableData> getData(QueryContext& ctx) override;

 protected:
  struct ClusterStateRequestResult {
    node_index_t node_id;
    Status status;
    std::vector<uint8_t> nodes_state;
    std::vector<node_index_t> boycotted_nodes;
  };

  void clearResults();

  void addResult(node_index_t node_id,
                 Status status,
                 std::vector<uint8_t> nodes_state,
                 std::vector<node_index_t> boycotted_nodes);

  std::string nodesStateToString(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      std::vector<uint8_t> nodes_state);

  std::string boycottedNodesToString(std::vector<node_index_t> boycotted_nodes);

 private:
  std::mutex mutex_;
  std::vector<ClusterStateRequestResult> results_;
};

}}}} // namespace facebook::logdevice::ldquery::tables
