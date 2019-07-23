/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/container/F14Set.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice {
class RandomNodeSelector {
 public:
  using NodeSourceSet = folly::F14FastSet<node_index_t>;

  /**
   * See ObjectPoller::SourceSelectionFunc in ObjectPoller.h for params and
   * returns.
   *
   * @param cluster_state_filter    when provided (!=nullptr), do not select
   *                                nodes that are considered dead according
   *                                to the filter.
   */
  static NodeSourceSet select(const NodeSourceSet& candidates,
                              const NodeSourceSet& existing,
                              const NodeSourceSet& blacklist,
                              const NodeSourceSet& graylist,
                              size_t num_required,
                              size_t num_extras,
                              ClusterState* cluster_state_filter = nullptr);

  /**
   * @params nodes_configuration      The server nodes config, used to get
   *                                  the nodes
   * @params exclude  Exclude this node. If exclude is the _only_ node in the
   *                  config, it will still be chosen
   * @returns Returns a random node among the nodes in the config, excluding
   *          exclude if there are other options
   */
  static NodeID
  getNode(const configuration::nodes::NodesConfiguration& nodes_configuration,
          NodeID exclude = NodeID());

  /**
   * @params nodes_configuration      The server nodes config, used to get
   *                                  the nodes
   * @params filter   Select only from alive nodes according to cluster state,
   *                  if null acts as getNode().
   * @params exclude  Exclude this node. If exclude is the _only_ node in the
   *                  config, it will still be chosen
   * @returns         Returns a random node among the alive nodes in the config,
   *                  if there is no alive node, picks first.
   */
  static NodeID getAliveNode(
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      ClusterState* filter,
      NodeID exclude = NodeID());
};

}} // namespace facebook::logdevice
