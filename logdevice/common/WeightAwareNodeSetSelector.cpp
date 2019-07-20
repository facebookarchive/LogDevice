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

#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/hash.h"

namespace facebook { namespace logdevice {

NodeSetSelector::Result WeightAwareNodeSetSelector::getStorageSet(
    logid_t log_id,
    const Configuration* cfg,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    nodeset_size_t target_nodeset_size,
    uint64_t seed,
    const EpochMetaData* prev,
    const Options* options) {
  Result res;
  res.decision = Decision::FAILED;

  auto logcfg = cfg->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    return res;
  }

  ReplicationProperty replication_property =
      ReplicationProperty::fromLogAttributes(logcfg->attrs());

  res.signature = hash_tuple({15345803578954886993ul, // random salt
                              consistentHashing_,
                              seed,
                              nodes_configuration.getStorageNodesHash(),
                              target_nodeset_size});
  if (prev != nullptr && prev->nodeset_params.signature == res.signature &&
      prev->replication == replication_property) {
    res.decision = Decision::KEEP;
    return res;
  }

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

  struct CandidateNode {
    uint64_t shard_id_hash;
    ShardID shard_id;
    bool is_writable;
  };

  struct Domain {
    int num_picked = 0;
    int num_picked_writable = 0;

    uint64_t priority;

    // The list of nodes sorted by hash.
    // To pick a node, we pop one from the back.
    std::vector<CandidateNode> nodes;
  };
  std::map<std::string, Domain> domains;

  const auto& membership = nodes_configuration.getStorageMembership();
  for (const auto node : *membership) {
    // Filter nodes excluded from `options`.
    if (options != nullptr && options->exclude_nodes.count(node)) {
      continue;
    }

    const auto num_shards = nodes_configuration.getNumShards(node);
    ld_check(num_shards > 0);
    shard_index_t shard_idx = mapLogToShard_(log_id, num_shards);
    ShardID shard = ShardID(node, shard_idx);

    // Filter nodes that shouldn't be included in nodesets
    if (!configuration::nodes::shouldIncludeInNodesetSelection(
            nodes_configuration, shard)) {
      continue;
    }

    const auto* sd = nodes_configuration.getNodeServiceDiscovery(node);
    ld_check(sd != nullptr);

    std::string location_str;
    if (replication_scope == NodeLocationScope::ROOT) {
      // All nodes are in the same replication domain.
    } else {
      if (!sd->location.hasValue()) {
        ld_error("Can't select nodeset because node %d (%s) does not have "
                 "location information",
                 node,
                 sd->address.toString().c_str());
        return res;
      }

      const NodeLocation& location = sd->location.value();
      assert(!location.isEmpty());
      if (!location.scopeSpecified(replication_scope)) {
        ld_error("Can't select nodeset because location %s of node %d (%s) "
                 "doesn't have location for scope %s.",
                 location.toString().c_str(),
                 node,
                 sd->address.toString().c_str(),
                 NodeLocation::scopeNames()[replication_scope].c_str());
        return res;
      }
      location_str = location.getDomain(replication_scope, node);
    }

    CandidateNode n;
    n.shard_id = shard;
    n.is_writable = membership->canWriteToShard(shard);

    // If consistent hashing is toggled, hash the log id along with the shard
    // ID, thereby allowing us to create a unique, deterministic ranking of
    // Shard ID's for each log. Else, hash the log id along with a random
    // number, which is equivalent to randomly sorting the Shard ID's for
    // each log.
    if (consistentHashing_) {
      n.shard_id_hash =
          hash_tuple({seed, log_id.val(), ShardID::Hash()(n.shard_id)});
    } else {
      n.shard_id_hash = folly::Random::rand64();
    }
    domains[location_str].nodes.push_back(n);
  }

  for (auto& kv : domains) {
    Domain* d = &kv.second;
    d->priority = consistentHashing_
        ? hash_tuple({seed, log_id.val(), folly::hash::fnv64(kv.first)})
        : folly::Random::rand64();
    std::sort(d->nodes.begin(),
              d->nodes.end(),
              [](const CandidateNode& a, const CandidateNode& b) {
                // Sort in order of decreasing hash, so that we pick nodes with
                // smaller hash first (for historical reasons).
                return std::tie(a.shard_id_hash, a.shard_id) >
                    std::tie(b.shard_id_hash, b.shard_id);
              });
  }

  // Form the nodeset by repeatedly popping entries from the shuffled node list
  // of the domain with the smallest number of picked nodes. If there's a tie,
  // take a random domain. This way the nodeset is distributed across domains as
  // evenly as possible. Keep picking nodes until the nodeset is "good", i.e.
  // big enough and has enough nodes in each available domain.
  //
  // There's a subtlety about whether or not to allow picking temporarily
  // disabled ("unwritable") nodes. On the one hand, we want to pick disabled
  // nodes because, if a full domain gets temporarily disabled and then comes
  // back, we want the domain to be included in historical nodesets, so that it
  // can receive writes from rebuilding and recovery. On the other hand, if lots
  // of nodes are disabled, we don't want to end up with a nodeset that consists
  // mostly of unwritable nodes and doesn't have enough writable nodes to
  // replicate new records.
  //
  // So we do a bit of both: we build a nodeset that is "good" is we count
  // only writable nodes and also "good" if we count both writable and
  // unwritable nodes. Moreover, the writable part of the nodeset has the same
  // distribution as if the unwritable nodes didn't exist; this ensures that
  // the data is evenly distributed among the writable nodes. The resulting
  // nodeset is at most twice as big as it would be without this whole trick.
  //
  // This is done by building the nodeset in two stages:
  //  1. Allow picking both writable and unwritable nodes. Pick nodes until
  //     the nodeset is "good".
  //  2. Continue picking nodes but allow picking only writable nodes, and
  //     change the comparator to pick domain with the smallest number of
  //     _writable_ nodes picked so far. Do it until the writable part of the
  //     nodeset is good.
  //
  // Note that, although the data distribution is correct for the current state
  // of the nodes, it may become biased when the unwritable nodes become
  // writable again. Consider this example. Replication factor is 1, nodeset
  // size is 1, there are 2 nodes: N0 is writable, N1 is unwritable.
  // We'll select nodeset {N0} for ~half the logs, {N0, N1} for the other ~half.
  // When N1 becomes writable, it'll receive only 25% of writes.

  auto select_nodes = [&](bool only_writable) {
    auto cmp = [&](Domain* a, Domain* b) {
      return std::tie(only_writable ? a->num_picked_writable : a->num_picked,
                      a->priority) >
          std::tie(only_writable ? b->num_picked_writable : b->num_picked,
                   b->priority);
    };
    std::priority_queue<Domain*, std::vector<Domain*>, decltype(cmp)> queue(
        cmp);

    // Initialize queue.
    size_t result_size = 0;
    for (auto& kv : domains) {
      Domain* d = &kv.second;
      result_size += only_writable ? d->num_picked_writable : d->num_picked;
      if (!d->nodes.empty()) {
        queue.push(d);
      }
    }

    // Pick the nodes.
    while (!queue.empty()) {
      Domain* d = queue.top();
      queue.pop();
      assert(!d->nodes.empty());
      int picked_in_domain =
          only_writable ? d->num_picked_writable : d->num_picked;
      if (picked_in_domain >= min_nodes_per_domain &&
          result_size >= target_nodeset_size) {
        // The nodeset is big enough and has enough nodes from each domain.
        break;
      }
      if (res.storage_set.size() >= NODESET_SIZE_MAX) {
        // Nodeset is not allowed to grow any bigger.
        RATELIMIT_INFO(std::chrono::seconds(10),
                       2,
                       "Hit max allowed nodeset size %d when selecting nodeset "
                       "for log %lu. Truncating the nodeset.",
                       (int)NODESET_SIZE_MAX,
                       log_id.val());
        break;
      }
      if (!only_writable && res.storage_set.size() >= NODESET_SIZE_MAX / 2) {
        // We're in the first stage but are already getting close to the max
        // allowed nodeset size. Skip to the second stage early to make sure
        // we pick enough writable nodes.
        break;
      }
      if (only_writable) {
        // Skip unwritable nodes.
        while (!d->nodes.empty() && !d->nodes.back().is_writable) {
          d->nodes.pop_back();
        }
        if (d->nodes.empty()) {
          continue;
        }
      }
      ++d->num_picked;
      if (d->nodes.back().is_writable) {
        ++d->num_picked_writable;
      }
      res.storage_set.push_back(d->nodes.back().shard_id);
      d->nodes.pop_back();
      ++result_size;

      if (!d->nodes.empty()) {
        queue.push(d);
      }
    }
  };

  select_nodes(false);
  select_nodes(true);

  ld_check(res.storage_set.size() <= NODESET_SIZE_MAX);

  // Sort the output.
  std::sort(res.storage_set.begin(), res.storage_set.end());

  // Check that we have enough nodes and domains to satisfy the replication
  // requirement.
  if (!configuration::nodes::validStorageSet(
          nodes_configuration, res.storage_set, replication_property)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "Not enough storage nodes to select nodeset for log %lu, "
                    "replication: %s, selected: %s.",
                    log_id.val_,
                    replication_property.toString().c_str(),
                    toString(res.storage_set).c_str());
    res.decision = Decision::FAILED;
    return res;
  }

  res.decision = (prev && prev->shards == res.storage_set)
      ? Decision::KEEP
      : Decision::NEEDS_CHANGE;
  return res;
}

}} // namespace facebook::logdevice
