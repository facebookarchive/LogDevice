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

namespace random_node_select_detail {

using NodeSourceSet = RandomNodeSelector::NodeSourceSet;

node_index_t getNodeIndex(const node_index_t& idx) {
  return idx;
}

// override for map type of (node_index_t, attribute) pair
template <typename MapValue>
node_index_t getNodeIndex(const MapValue& pair) {
  return pair.first;
}

template <typename USA, typename USB>
USA unorderedSetIntersection(const USA& A, const USB& B) {
  USA result;
  for (auto it = A.begin(); it != A.end(); ++it) {
    node_index_t n = getNodeIndex(*it);
    if (B.count(n) > 0) {
      result.insert(n);
    }
  }
  return result;
}

template <typename CandidatesSet>
std::vector<node_index_t> filterCandidates(const CandidatesSet& candidates,
                                           const NodeSourceSet& existing,
                                           const NodeSourceSet& blacklist,
                                           const NodeSourceSet& graylist,
                                           ClusterState* cluster_state_filter) {
  std::vector<node_index_t> filtered_candidates;
  for (auto it = candidates.begin(); it != candidates.end(); ++it) {
    node_index_t n = getNodeIndex(*it);
    if (existing.count(n) == 0 && blacklist.count(n) == 0 &&
        graylist.count(n) == 0 &&
        (cluster_state_filter == nullptr ||
         cluster_state_filter->isNodeAlive(n))) {
      filtered_candidates.push_back(n);
    }
  }
  return filtered_candidates;
}

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

// @type CandidateSet              an iteratable map/set container type whose
//                                 key is of node_index_t type
template <typename CandidatesSet>
RandomNodeSelector::NodeSourceSet select(const CandidatesSet& candidates,
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

} // namespace random_node_select_detail

/*static*/
RandomNodeSelector::NodeSourceSet
RandomNodeSelector::select(const NodeSourceSet& candidates,
                           const NodeSourceSet& existing,
                           const NodeSourceSet& blacklist,
                           const NodeSourceSet& graylist,
                           size_t num_required,
                           size_t num_extras,
                           ClusterState* cluster_state_filter) {
  return random_node_select_detail::select(candidates,
                                           existing,
                                           blacklist,
                                           graylist,
                                           num_required,
                                           num_extras,
                                           cluster_state_filter);
}

/*static*/
NodeID RandomNodeSelector::getAliveNode(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    ClusterState* filter,
    NodeID exclude) {
  NodeSourceSet graylist;
  if (exclude.isNodeID()) {
    // use `exclude' as the graylist so that it's less favourable than the rest
    // of the cluster
    graylist.insert(exclude.index());
  }

  auto result_set = random_node_select_detail::select(
      /*candidates*/ *nodes_configuration.getServiceDiscovery(),
      /*existing*/ {},
      /*blacklist*/ {},
      /*graylist*/ std::move(graylist),
      /*num_required*/ 1,
      /*num_extras*/ 0,
      /*cluster_state_filter*/ filter);

  if (result_set.empty()) {
    return NodeID();
  }

  ld_check(result_set.size() == 1);
  return nodes_configuration.getNodeID(*result_set.begin());
}

/*static*/
NodeID RandomNodeSelector::getNode(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    NodeID exclude) {
  return getAliveNode(nodes_configuration, nullptr, exclude);
}

}} // namespace facebook::logdevice
