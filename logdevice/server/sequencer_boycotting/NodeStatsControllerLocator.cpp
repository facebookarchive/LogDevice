/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "NodeStatsControllerLocator.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/hash.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"

namespace facebook { namespace logdevice {

namespace {
void reportNotEnoughNodes(int count) {
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    1,
                    "NodeStatsControllerLocator could not find enough "
                    "controllers, only %i chosen",
                    count);
}
} // namespace

bool NodeStatsControllerLocator::isController(NodeID node, int count) {
  if (count <= 0) {
    return false;
  }

  auto controllers = locateControllers(count);

  return std::find(controllers.begin(), controllers.end(), node) !=
      controllers.end();
}

std::vector<NodeID>
NodeStatsControllerLocator::locateControllers(int controller_count) {
  const auto nodes = getNodes();
  const auto max_node_index = getMaxNodeIndex(nodes);
  const auto states = getNodeState(max_node_index);

  std::vector<node_index_t> controller_indices;
  controller_indices.reserve(controller_count);

  auto weights = initialWeightVector(states);
  ld_check(max_node_index < weights.size());

  // remove weight for any nodes without location before using the rack-aware
  // locator
  for (const auto& node : nodes) {
    if (!node.second.location) {
      weights[node.first] = 0.0;
    }
  }

  // rack-aware locator
  locate(controller_count,
         [this, &nodes](
             node_index_t new_controller, std::vector<double>* weights) {
           auto node = nodes.at(new_controller);
           // only nodes with location should be considered this time
           ld_check(node.location);
           this->banRack(node.location.value(), nodes, weights);
         },
         &weights,
         &controller_indices);

  if (controller_indices.size() < controller_count) {
    weights = initialWeightVector(states);
    ld_check(max_node_index < weights.size());

    for (const auto& controller : controller_indices) {
      weights[controller] = 0.0;
    }
    locate(controller_count,
           // only disallow placing multiple controller on the same node
           [](node_index_t new_controller, std::vector<double>* weights) {
             weights->at(new_controller) = 0.0;
           },
           &weights,
           &controller_indices);
  }

  if (controller_indices.size() < controller_count) {
    reportNotEnoughNodes(controller_indices.size());
  }

  std::sort(controller_indices.begin(), controller_indices.end());

  std::vector<NodeID> controllers;
  for (auto index : controller_indices) {
    controllers.emplace_back(
        index, static_cast<node_gen_t>(nodes.at(index).generation));
  }
  return controllers;
}

void NodeStatsControllerLocator::locate(
    int controller_count,
    std::function<void(node_index_t, std::vector<double>*)> weight_updater,
    /*modifiable*/ std::vector<double>* weights,
    /*modifiable*/ std::vector<node_index_t>* controller_indices) {
  while (controller_indices->size() < controller_count) {
    auto index = hashing::weighted_ch(42 /*no special key needed*/, *weights);

    ld_check(index < weights->size());
    if (weights->at(index) == 0.0) {
      break;
    }

    weight_updater(index, weights);
    controller_indices->emplace_back(index);
  }
}

NodeStatsControllerLocator::Nodes NodeStatsControllerLocator::getNodes() const {
  return Worker::onThisThread()->getServerConfig()->getNodes();
}

node_index_t
NodeStatsControllerLocator::getMaxNodeIndex(const Nodes& nodes) const {
  node_index_t max_node_index = 0;
  for (auto& entry : nodes) {
    max_node_index = std::max(max_node_index, entry.first);
  }
  return max_node_index;
}

NodeStatsControllerLocator::StateList
NodeStatsControllerLocator::getNodeState(node_index_t max_node_index) const {
  const auto cluster_state = Worker::onThisThread()->getClusterState();

  std::vector<ClusterState::NodeState> states;
  states.reserve(max_node_index + 1);

  for (size_t i = 0; i <= max_node_index; ++i) {
    states.emplace_back(cluster_state->getNodeState(i));
  }
  return states;
}

std::vector<double>
NodeStatsControllerLocator::initialWeightVector(const StateList& states) const {
  std::vector<double> weights;
  weights.reserve(states.size());

  for (size_t i = 0; i < states.size(); ++i) {
    weights.emplace_back(states[i] == ALIVE ? 1 : 0);
  }

  return weights;
}

void NodeStatsControllerLocator::banRack(
    const NodeLocation& banned_rack,
    const Nodes& nodes,
    /*modifiable*/ std::vector<double>* weight_vector) const {
  for (auto& entry : nodes) {
    if (!entry.second.location ||
        entry.second.location.value().sharesScopeWith(
            banned_rack, NodeLocationScope::RACK)) {
      weight_vector->at(entry.first) = 0;
    }
  }
}
}} // namespace facebook::logdevice
