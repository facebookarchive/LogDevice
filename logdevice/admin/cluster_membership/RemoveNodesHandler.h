/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Optional.h>

#include "logdevice/admin/if/gen-cpp2/cluster_membership_types.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace admin {
namespace cluster_membership {

class RemoveNodesHandler {
 public:
  struct Result {
    std::vector<thrift::NodeID> to_be_removed{};
    configuration::nodes::NodesConfiguration::Update update{};
  };

  /**
   * Given a list of NodesFilters, returns a NodesConfiguration updates that
   * removes all the nodes matching the union of those filters. A single node,
   * not meeting the precondition will fail the whole request.
   */
  folly::Expected<Result, thrift::ClusterMembershipOperationFailed>
  buildNodesConfigurationUpdates(
      const std::vector<thrift::NodesFilter>&,
      const configuration::nodes::NodesConfiguration&,
      const ClusterState&) const;

 private:
  /**
   * Given a list of NodesFilter, return a list of node indicies which is the
   * union of the matches of each filter.
   */
  std::vector<node_index_t>
  findNodes(const std::vector<thrift::NodesFilter>&,
            const configuration::nodes::NodesConfiguration&) const;

  /**
   * Checks that the precondition for the remove are respected. A single failure
   * in any of the nodes, fails the preconditions.
   * The preconditions for the node to be removed are:
   *   - The node must be dead.
   *   - If it's a storage node, all the shards should have state=NONE.
   *   - If it's a sequencer node, sequencing must be disabled on this node.
   */
  folly::Optional<thrift::ClusterMembershipOperationFailed> checkPreconditions(
      const std::vector<node_index_t>& node_idxs,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      const ClusterState& cluster_state) const;

  /**
   * Fills the passed update structure with the remove update for this node.
   */
  void buildNodesConfigurationUpdateForNode(
      configuration::nodes::NodesConfiguration::Update&,
      node_index_t node_idx,
      const configuration::nodes::NodesConfiguration& nodes_configuration)
      const;
};
}}}} // namespace facebook::logdevice::admin::cluster_membership
