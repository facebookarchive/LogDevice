/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "RandomNodeSelector.h"

#include <folly/Random.h>

namespace facebook { namespace logdevice {
namespace {
template <class Iterator>
void advanceIfNodeIsExcluded(NodeID& exclude, Iterator& it) {
  if (exclude.isNodeID() && it->first == exclude.index()) {
    ++it;
  }
}

NodeID selectNodeIdHelper(const configuration::Nodes& nodes, NodeID exclude) {
  NodeID new_node;

  // Pick a random node from nodes, excluding `exclude`.
  size_t count = nodes.size();
  ld_check(count >= 1);
  if (exclude.isNodeID() && nodes.count(exclude.index())) {
    --count;
  } else {
    exclude = NodeID();
  }

  if (count == 0) {
    // can't change node_id because the excluded node_id is the only one
    new_node = exclude;
  } else {
    size_t offset = folly::Random::rand32() % count;
    auto it = nodes.begin();
    // Advance iterator `offset` times.
    for (size_t i = 0; i < offset; ++i) {
      ld_check(it != nodes.end());

      advanceIfNodeIsExcluded(exclude, it);
      ld_check(it != nodes.end());
      ++it;
    }
    advanceIfNodeIsExcluded(exclude, it);

    ld_check(it != nodes.end());
    new_node = NodeID(it->first, it->second.generation);
  }
  return new_node;
}
} // namespace

NodeID RandomNodeSelector::getAliveNode(const ServerConfig& cfg,
                                        ClusterState* cluster_state,
                                        NodeID exclude) {
  if (cluster_state == nullptr) {
    return getNode(cfg, exclude);
  }

  configuration::Nodes alive_nodes;
  for (const auto& node : cfg.getNodes()) {
    if (cluster_state->isNodeAlive(node.first)) {
      alive_nodes.emplace(node.first, node.second);
    }
  }
  if (alive_nodes.size() == 0) {
    ld_check(cfg.getNodes().size() > 0);
    const size_t offset = folly::Random::rand32() % cfg.getNodes().size();
    const auto node = std::next(cfg.getNodes().begin(), offset);
    return NodeID(node->first, node->second.generation);
  }
  return selectNodeIdHelper(alive_nodes, exclude);
}

NodeID RandomNodeSelector::getNode(const ServerConfig& cfg, NodeID exclude) {
  const auto& nodes = cfg.getNodes();
  return selectNodeIdHelper(nodes, exclude);
}
}} // namespace facebook::logdevice
