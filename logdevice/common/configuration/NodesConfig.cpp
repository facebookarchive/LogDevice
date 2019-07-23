/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/NodesConfig.h"

#include <folly/hash/SpookyHashV2.h>

#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"

namespace facebook { namespace logdevice { namespace configuration {

/**
 * Returns the maximum key in getNodes().
 */
size_t NodesConfig::getMaxNodeIdx_DEPRECATED() const {
  size_t r = 0;
  for (const auto& it : getNodes()) {
    r = std::max(r, (size_t)it.first);
  }
  return r;
}

uint64_t NodesConfig::calculateHash() const {
  static_assert(sizeof(node_index_t) == 2,
                "node_index_t size has changed, "
                "this will cause a recalculation of "
                "nodes config hashes");
  static_assert(sizeof(shard_size_t) == 2,
                "shard_size_t size has changed, "
                "this will cause a recalculation of "
                "nodes config hashes");

  // Generating a list of sorted node IDs, so the order is consistent regardless
  // of std::unordered_map internals
  std::vector<node_index_t> sorted_node_ids(nodes_.size());
  std::transform(nodes_.begin(),
                 nodes_.end(),
                 sorted_node_ids.begin(),
                 [](const auto& src) { return src.first; });
  std::sort(sorted_node_ids.begin(), sorted_node_ids.end());

  std::string hashable_string;

  // for each node, writing out the attributes being hashed
  auto append = [&](const void* ptr, size_t num_bytes) {
    size_t old_size = hashable_string.size();
    size_t new_size = old_size + num_bytes;
    if (new_size > hashable_string.capacity()) {
      size_t new_capacity = std::max(256ul, hashable_string.capacity() * 2);
      hashable_string.reserve(new_capacity);
    }
    hashable_string.resize(new_size);
    memcpy(&hashable_string[old_size], ptr, num_bytes);
  };

  for (auto node_id : sorted_node_ids) {
    auto it = nodes_.find(node_id);
    ld_check(it != nodes_.end());
    const auto& node = it->second;
    std::string location_str = node.locationStr();

    append(&node_id, sizeof(node_id));
    if (node.hasRole(NodeRole::STORAGE)) {
      auto* storage = node.storage_attributes.get();
      append(&storage->capacity, sizeof(storage->capacity));
      append(&storage->state, sizeof(storage->state));
      append(&storage->exclude_from_nodesets,
             sizeof(storage->exclude_from_nodesets));
      append(&storage->num_shards, sizeof(storage->num_shards));
    }

    // appending location str and the terminating null-byte
    append(location_str.c_str(), location_str.size() + 1);
  }
  const uint64_t SEED = 0x9a6bf3f8ebcd8cdfL; // random
  return folly::hash::SpookyHashV2::Hash64(
      hashable_string.data(), hashable_string.size(), SEED);
}

// TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
shard_size_t NodesConfig::calculateNumShards() const {
  for (const auto& it : nodes_) {
    if (!it.second.isReadableStorageNode()) {
      continue;
    }
    ld_check(it.second.getNumShards() > 0);
    return it.second.getNumShards();
    break; // The other storage nodes have the same amount of shards.
  }
  return 0;
}

bool NodesConfig::generateNodesConfiguration(
    const MetaDataLogsConfig& meta_config,
    config_version_t version) {
  auto config = nodes::NodesConfigLegacyConverter::fromLegacyNodesConfig(
      *this, meta_config, version);

  if (config == nullptr) {
    return false;
  }
  nodes_configuration_ = std::move(config);
  return true;
}

folly::dynamic NodesConfig::toJson() const {
  folly::dynamic output_nodes = folly::dynamic::array;

  const auto& nodes = getNodes();
  std::vector<node_index_t> sorted_node_ids(nodes.size());
  std::transform(
      nodes.begin(), nodes.end(), sorted_node_ids.begin(), [](const auto& src) {
        return src.first;
      });
  std::sort(sorted_node_ids.begin(), sorted_node_ids.end());

  for (const auto& nidx : sorted_node_ids) {
    const ServerConfig::Node& node = nodes.at(nidx);

    folly::dynamic node_dict =
        folly::dynamic::object("node_id", nidx)("name", node.name)(
            "host", node.address.toString())("generation", node.generation)(
            "gossip_address", node.gossip_address.toString());

    if (node.hasRole(NodeRole::STORAGE)) {
      // TODO: Remove once all production configs and tooling
      //       No longer use this field.
      node_dict["weight"] = node.getLegacyWeight();
    }

    // Optional Universal Attributes.
    if (node.location.hasValue()) {
      node_dict["location"] = node.locationStr();
    }
    if (node.ssl_address) {
      node_dict["ssl_host"] = node.ssl_address->toString();
    }

    // Sequencer Role Attributes.
    auto roles = folly::dynamic::array();
    if (node.hasRole(configuration::NodeRole::SEQUENCER)) {
      roles.push_back("sequencer");
      node_dict["sequencer"] = node.sequencer_attributes->enabled();
      node_dict["sequencer_weight"] =
          node.sequencer_attributes->getConfiguredWeight();
    }

    // Storage Role Attributes.
    if (node.hasRole(configuration::NodeRole::STORAGE)) {
      roles.push_back("storage");
      auto* storage = node.storage_attributes.get();
      node_dict["storage"] =
          configuration::storageStateToString(storage->state);
      node_dict["storage_capacity"] = storage->capacity;
      node_dict["num_shards"] = storage->num_shards;
      if (storage->exclude_from_nodesets) {
        node_dict["exclude_from_nodesets"] = storage->exclude_from_nodesets;
      }
    }
    node_dict["roles"] = roles;

    output_nodes.push_back(node_dict);
  }
  return output_nodes;
}

}}} // namespace facebook::logdevice::configuration
