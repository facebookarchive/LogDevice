/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "RandomNodeSetSelector.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <memory>
#include <numeric>
#include <random>

#include <folly/Memory.h>
#include <folly/String.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

// randomly select a nodeset of size @nodeset_size from a pool of candidate
// nodes @eligible_nodes
std::unique_ptr<StorageSet> RandomNodeSetSelector::randomlySelectNodes(
    logid_t log_id,
    const std::shared_ptr<Configuration>& config,
    const NodeSetIndices& eligible_nodes,
    size_t nodeset_size,
    const Options* options) {
  if (nodeset_size > eligible_nodes.size()) {
    return nullptr;
  }

  auto candidates = std::make_unique<StorageSet>();
  const auto& nodes_configuration =
      config->serverConfig()->getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);
  const auto& membership = nodes_configuration->getStorageMembership();

  for (const node_index_t i : eligible_nodes) {
    if (!membership->hasNode(i)) {
      // eligible_nodes should be picked from the storage membership in config
      ld_check(false);
      return nullptr;
    }

    // filter nodes excluded from @param options
    if (options != nullptr && options->exclude_nodes.count(i)) {
      // skip the node
      continue;
    }

    const auto num_shards = nodes_configuration->getNumShards(i);
    ld_check(num_shards > 0);
    shard_index_t shard_idx = map_log_to_shard_(log_id, num_shards);
    ShardID shard = ShardID(i, shard_idx);
    if (!configuration::nodes::shouldIncludeInNodesetSelection(
            *nodes_configuration, shard)) {
      continue;
    }

    candidates->push_back(shard);
  }

  if (candidates->size() < nodeset_size) {
    ld_error("Cannot select nodeset: Not enough eligible nodes for log %lu "
             "after exluding shards from certain nodes. Nodes eligible: %lu, "
             "required: %lu",
             log_id.val_,
             candidates->size(),
             nodeset_size);
    return nullptr;
  }

  std::shuffle(candidates->begin(), candidates->end(), rnd_);
  candidates->resize(nodeset_size);

  ld_check(nodeset_size == candidates->size());
  return candidates;
}

storage_set_size_t RandomNodeSetSelector::getStorageSetSize(
    logid_t log_id,
    const std::shared_ptr<Configuration>& cfg,
    folly::Optional<int> storage_set_size_target,
    ReplicationProperty replication,
    const Options* /*options*/) {
  size_t storage_set_count = 0;

  const auto& nodes_configuration =
      cfg->serverConfig()->getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);
  const auto& membership = nodes_configuration->getStorageMembership();

  for (const auto node : *membership) {
    const auto num_shards = nodes_configuration->getNumShards(node);
    ld_check(num_shards > 0);
    ShardID shard = ShardID(node, map_log_to_shard_(log_id, num_shards));
    if (configuration::nodes::shouldIncludeInNodesetSelection(
            *nodes_configuration, shard)) {
      ++storage_set_count;
    }
  };

  return std::min(std::max(storage_set_size_target.hasValue()
                               ? storage_set_size_target.value()
                               : storage_set_count,
                           size_t(replication.getReplicationFactor())),
                  storage_set_count);
}

std::tuple<NodeSetSelector::Decision, std::unique_ptr<StorageSet>>
RandomNodeSetSelector::getStorageSet(logid_t log_id,
                                     const std::shared_ptr<Configuration>& cfg,
                                     const StorageSet* prev,
                                     const Options* options) {
  auto logcfg = cfg->getLogGroupByIDShared(log_id);
  if (!logcfg) {
    err = E::NOTFOUND;
    return std::make_tuple(Decision::FAILED, nullptr);
  }

  auto replication_property =
      ReplicationProperty::fromLogAttributes(logcfg->attrs());

  if (replication_property.getBiggestReplicationScope() !=
      NodeLocationScope::NODE) {
    ld_error("Cannot select node set for log %lu, this copyset selector "
             "does not support cross-domain replication %s.",
             log_id.val_,
             replication_property.toString().c_str());
    return std::make_tuple(Decision::FAILED, nullptr);
  }

  const auto& nodes_configuration =
      cfg->serverConfig()->getNodesConfiguration();
  ld_check(nodes_configuration != nullptr);
  const size_t nodeset_size = getStorageSetSize(log_id,
                                                cfg,
                                                *logcfg->attrs().nodeSetSize(),
                                                replication_property,
                                                options);
  ld_check(nodeset_size > 0);

  std::vector<node_index_t> all_nodes_indices =
      nodes_configuration->getStorageNodes();
  std::sort(all_nodes_indices.begin(), all_nodes_indices.end());
  std::unique_ptr<StorageSet> candidates = randomlySelectNodes(
      log_id, cfg, all_nodes_indices, nodeset_size, options);

  if (candidates == nullptr) {
    // We select from the entire cluster, a valid configuration should
    // guarantee that we have enough eligible nodes to pick. However,
    // with excluded shards in @param options, this is still possible
    return std::make_tuple(Decision::FAILED, nullptr);
  }

  // sort the nodeset
  std::sort(candidates->begin(), candidates->end());

  // TODO T33035439: convert validStorageSet() to use the new NodesConfiguration
  if (!ServerConfig::validStorageSet(
          cfg->serverConfig()->getNodes(), *candidates, replication_property)) {
    ld_error("Invalid nodeset %s for log %lu, check nodes weights.",
             toString(*candidates).c_str(),
             log_id.val_);
    return std::make_tuple(Decision::FAILED, nullptr);
  }

  if (prev && *prev == *candidates) {
    return std::make_tuple(Decision::KEEP, nullptr);
  }
  return std::make_tuple(Decision::NEEDS_CHANGE, std::move(candidates));
}

}} // namespace facebook::logdevice
