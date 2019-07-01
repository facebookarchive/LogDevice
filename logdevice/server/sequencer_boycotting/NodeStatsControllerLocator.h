/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/configuration/Node.h"

/**
 * @file NodeStatsControllerLocator is used to decide which nodes in the cluster
 * should be NodeStatsControllers. It is deterministic, the whole cluster will
 * come to the same conclusion. It also tries to place controllers in different
 * racks if possible.
 */

namespace facebook { namespace logdevice {
class NodeStatsControllerLocator {
 public:
  using NodesConfiguration = configuration::nodes::NodesConfiguration;
  using StateList = std::vector<ClusterState::NodeState>;

  virtual ~NodeStatsControllerLocator() = default;
  /**
   * Will try to place the controllers in different racks. If this is not
   * possible due to lack of information, or because lack of nodes, will
   * simply place them anywhere in the cluster. If there are not enough valid
   * nodes, will simply choose as many valid nodes as possible.
   *
   * @params node   The node index of the node to check if it's a controller
   * @params count  The max amount of controllers that may exist in the
                    cluster
   * @return        True if it's a controller, false if not
   */
  bool isController(NodeID node, int count);

 protected:
  virtual std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const;
  virtual StateList getNodeState(node_index_t max_node_index) const;

 private:
  /**
   * Will try to place the controllers in different racks. If this is not
   * possible due to lack of information, or because lack of nodes, will
   * simply place them anywhere in the cluster. If there are not enough valid
   * nodes, will simply choose as many valid nodes as possible.
   *
   * @params controller_count The amount of controllers to find
   * @return                  The ids of the /controller_count/ nodes that
   *                          are controllers, ordered by index
   */
  std::vector<NodeID> locateControllers(int controller_count);

  /**
   * Will try to locate controller_count amount of controllers, but will stop
   * once the weight of a selected controller is 0. Selected controllers are
   * appended to the back of controller_indices. After a controller is selected
   * the weights will be updated by calling the weight_updater with the new
   * controller index and the list of weights as argument.
   *
   * @params controller_count   The max amount of controllers that will be
   *                            appended to controller_indices
   * @params weights            The list of weights
   * @params weight_updater     A function taking a newly selected controller
   *                            node index and a list of weights. Used to
   *                            update the weights once a controller node has
   *                            been selected.
   * @params controller_indices The function will append new controller nodes to
   *                            the end of this list
   */
  void
  locate(int controller_count,
         std::function<void(node_index_t, std::vector<double>*)> weight_updater,
         /*modifiable*/ std::vector<double>* weights,
         /*modifiable*/ std::vector<node_index_t>* controller_indices);

  /**
   * Creates a weight for each node in the state list. 1 if ALIVE, 0 if any
   * other state
   */
  std::vector<double> initialWeightVector(const StateList& states) const;
  /**
   * Sets the weight to 0 for the nodes that share a rack with the banned_rack.
   * Nodes without location also have their weights set to 0, they are assumed
   * to share location with the banned_rack
   */
  void banRack(const NodeLocation& banned_rack,
               const NodesConfiguration& nc,
               /*modifiable*/ std::vector<double>* weight_vector) const;
};
}} // namespace facebook::logdevice
