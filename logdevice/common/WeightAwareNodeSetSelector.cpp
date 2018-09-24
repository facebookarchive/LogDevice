/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WeightAwareNodeSetSelector.h"

#include <map>
#include <queue>
#include <utility>
#include <folly/Random.h>

namespace facebook { namespace logdevice {

std::tuple<NodeSetSelector::Decision, std::unique_ptr<StorageSet>>
WeightAwareNodeSetSelector::getStorageSet(
    logid_t log_id,
    const std::shared_ptr<Configuration>& cfg,
    const StorageSet* prev,
    const Options* options) {
  auto logcfg = cfg->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    return std::make_tuple(Decision::FAILED, nullptr);
  }
  ReplicationProperty replication_property =
      ReplicationProperty::fromLogAttributes(logcfg->attrs());

  // Nodeset will span all domains of this scope.
  NodeLocationScope replication_scope;
  // We'll try to pick at least this many nodes from each domain of scope
  // replication_scope.
  int min_nodes_per_domain;

  auto replication_factors =
      replication_property.getDistinctReplicationFactors();
  if (replication_factors[0].first <= NodeLocationScope::NODE) {
    // No cross-domain replication and no sequencer locality.
    replication_scope = NodeLocationScope::ROOT;
    min_nodes_per_domain = replication_factors[0].second;
  } else {
    replication_scope = replication_factors[0].first;
    // If some domain is smaller than this, the current implementation of
    // WeightedCopySetSelector will underutilize that domain.
    min_nodes_per_domain =
        replication_factors.back().second - replication_factors[0].second + 1;
  }

  int target_size = logcfg->attrs().nodeSetSize().value().value_or(
      std::numeric_limits<int>::max());

  struct Domain {
    int num_picked = 0;
    uint64_t priority;

    // A sorted vector of hashes of nodes.
    std::vector<std::pair<uint64_t, ShardID>> node_hashes;
  };
  std::map<std::string, Domain> domains;

  for (const auto& it : cfg->serverConfig()->getNodes()) {
    node_index_t i = it.first;
    const Configuration::Node* node = &it.second;
    assert(node != nullptr);

    // Filter nodes excluded from `options`.
    if (options != nullptr && options->exclude_nodes.count(i)) {
      continue;
    }
    // Filter nodes that shouldn't be included in nodesets
    if (!node->includeInNodesets()) {
      continue;
    }

    std::string location_str;
    if (replication_scope == NodeLocationScope::ROOT) {
      // All nodes are in the same replication domain.
    } else {
      if (!node->location.hasValue()) {
        ld_error("Can't select nodeset because node %d (%s) does not have "
                 "location information",
                 i,
                 node->address.toString().c_str());
        return std::make_tuple(Decision::FAILED, nullptr);
      }
      const NodeLocation& location = node->location.value();
      assert(!location.isEmpty());
      if (!location.scopeSpecified(replication_scope)) {
        ld_error("Can't select nodeset because location %s of node %d (%s) "
                 "doesn't have location for scope %s.",
                 location.toString().c_str(),
                 i,
                 node->address.toString().c_str(),
                 NodeLocation::scopeNames()[replication_scope].c_str());
        return std::make_tuple(Decision::FAILED, nullptr);
      }
      location_str = location.getDomain(replication_scope, i);
    }

    assert(node->getNumShards() > 0);
    shard_index_t shard_idx = mapLogToShard_(log_id, node->getNumShards());

    ShardID current_shard_id = ShardID(i, shard_idx);
    uint64_t curr_hash;

    // If consistent hashing is toggled, hash the log id along with the shard
    // ID, thereby allowing us to create a unique, deterministic ranking of
    // Shard ID's for each log. Else, hash the log id along with a random
    // number, which is equivalent to randomly sorting the Shard ID's for
    // each log.
    if (consistentHashing_) {
      curr_hash = folly::hash::hash_128_to_64(
          log_id.val(), ShardID::Hash()(current_shard_id));
    } else {
      curr_hash = folly::Random::rand64();
    }
    domains[location_str].node_hashes.push_back(
        std::make_pair(curr_hash, current_shard_id));
  }

  // Form the nodeset by repeatedly taking a random unpicked node from the
  // domain with the smallest number of picked nodes. If there's a tie, take a
  // random domain. This way the nodeset is distrinuted across domains as evenly
  // as possible.
  auto cmp = [](Domain* a, Domain* b) {
    return std::tie(a->num_picked, a->priority) >
        std::tie(b->num_picked, b->priority);
  };
  std::priority_queue<Domain*, std::vector<Domain*>, decltype(cmp)> queue(cmp);

  for (auto& kv : domains) {
    Domain* d = &kv.second;
    d->priority = consistentHashing_ ? folly::hash::fnv64(kv.first)
                                     : folly::Random::rand64();
    std::sort(d->node_hashes.begin(), d->node_hashes.end());
    queue.push(d);
  }

  auto result = std::make_unique<StorageSet>();
  while (!queue.empty()) {
    Domain* d = queue.top();
    queue.pop();
    assert(d->num_picked != d->node_hashes.size());
    if (d->num_picked >= min_nodes_per_domain &&
        result->size() >= target_size) {
      // The nodeset is big enough and has enough nodes from each domain.
      break;
    }
    result->push_back(d->node_hashes[d->num_picked++].second);
    if (d->num_picked != d->node_hashes.size()) {
      queue.push(d);
    }
  }

  // Sort the output.
  std::sort(result->begin(), result->end());

  // Check that we have enough nodes and domains to satisfy the replication
  // requirement.
  const auto& all_nodes = cfg->serverConfig()->getNodes();
  if (!ServerConfig::validStorageSet(
          all_nodes, *result, replication_property)) {
    ld_error("Not enough storage nodes to select nodeset for log %lu, "
             "replication: %s, selected: %s.",
             log_id.val_,
             replication_property.toString().c_str(),
             toString(*result).c_str());
    return std::make_tuple(Decision::FAILED, nullptr);
  }

  if (prev && *prev == *result) {
    return std::make_tuple(Decision::KEEP, nullptr);
  }

  return std::make_tuple(Decision::NEEDS_CHANGE, std::move(result));
}

}} // namespace facebook::logdevice
