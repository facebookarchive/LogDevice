/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/RandomNodeSelector.h"

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

namespace {

using NodeSourceSet = RandomNodeSelector::NodeSourceSet;

// randomly pick n nodes from candidates and insert them to @param out. if
// candidates does not have n nodes, pick all of them.
// @return number of nodes picked
size_t randomlySelectNodes(std::vector<node_index_t> candidates,
                           size_t n,
                           NodeSourceSet* out) {
  ld_check(n > 0);
  ld_check(out != nullptr);

  if (candidates.size() <= n) {
    out->insert(candidates.begin(), candidates.end());
    return candidates.size();
  }

  std::shuffle(candidates.begin(), candidates.end(), folly::ThreadLocalPRNG());
  out->insert(candidates.begin(), candidates.begin() + n);
  return n;
}

std::vector<node_index_t> filterCandidates(const NodeSourceSet& candidates,
                                           const NodeSourceSet& existing,
                                           const NodeSourceSet& blacklist,
                                           const NodeSourceSet& graylist,
                                           ClusterState* cluster_state_filter) {
  std::vector<node_index_t> filtered_candidates;
  for (const auto n : candidates) {
    if (existing.count(n) == 0 && blacklist.count(n) == 0 &&
        graylist.count(n) == 0 &&
        (cluster_state_filter == nullptr ||
         cluster_state_filter->isNodeAlive(n))) {
      filtered_candidates.push_back(n);
    }
  }
  return filtered_candidates;
}

template <typename US>
US unorderedSetIntersection(const US& A, const US& B) {
  US result;
  for (const auto n : A) {
    if (B.count(n) > 0) {
      result.insert(n);
    }
  }
  return result;
}

} // namespace

/*static*/
RandomNodeSelector::NodeSourceSet
RandomNodeSelector::select(const NodeSourceSet& candidates,
                           const NodeSourceSet& existing,
                           const NodeSourceSet& blacklist,
                           const NodeSourceSet& graylist,
                           size_t num_required,
                           size_t num_extras,
                           ClusterState* cluster_state_filter) {
  NodeSourceSet result;
  std::vector<node_index_t> filtered_candidates = filterCandidates(
      candidates, existing, blacklist, graylist, cluster_state_filter);
  const size_t target = num_required + num_extras;
  size_t selected =
      randomlySelectNodes(std::move(filtered_candidates), target, &result);
  ld_check(selected == result.size());
  ld_check(selected <= target);
  if (selected == target) {
    return result;
  }
  // needs to pick more nodes from the graylist and candidate intersection
  filtered_candidates =
      filterCandidates(unorderedSetIntersection(graylist, candidates),
                       existing,
                       blacklist,
                       {},
                       cluster_state_filter);
  randomlySelectNodes(
      std::move(filtered_candidates), target - selected, &result);

  if (result.size() < num_required) {
    // selection failed
    return {};
  }

  ld_check(result.size() <= target);
  return result;
}

}} // namespace facebook::logdevice
