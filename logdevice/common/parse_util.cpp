/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/parse_util.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * @param descriptor A descriptor describing one shard or a set of shards.
 *                   For instance, N2:S1 describes ShardID(2, 1), "N2" describes
 *                   all shards on N2, frc.frc3.09.14B.ei describes all shards
 *                   on rack "ei";
 * @param cfg        Server configuration.
 * @param out        Populated set of ShardID objects.
 *
 * @return           0 on success, or -1 if the descriptor did not match any
 *                   shard.
 */
int parseShardIDSet(const std::string& descriptor,
                    const ServerConfig& cfg,
                    ShardSet& out) {
  if (descriptor.empty()) {
    return 0;
  }

  auto add_node = [&](node_index_t nid, const configuration::Node& node) {
    for (shard_index_t i = 0; i < node.num_shards; ++i) {
      out.insert(ShardID(nid, i));
    }
  };

  bool found_matching_location = false;
  for (const auto& n : cfg.getNodes()) {
    const auto& node = n.second;
    if (node.location.hasValue() &&
        node.location.value().matchesPrefix(descriptor)) {
      add_node(n.first, node);
      found_matching_location = true;
    }
  }

  if (found_matching_location) {
    return 0;
  }

  // The descriptor did not match a location, let's try to parse formats "NX" or
  // "NX:SY".

  std::vector<std::string> tokens;
  folly::split(":", descriptor, tokens, /* ignoreEmpty */ false);
  if (tokens.size() > 2) {
    return -1;
  }

  // may throw
  auto extract_nonnegative_int = [](const std::string& s, char prefix) {
    if (s.size() < 1 || s[0] != prefix) {
      return -1;
    }
    int rv = folly::to<int>(s.substr(1));
    return rv >= 0 ? rv : -1;
  };

  try {
    int index = extract_nonnegative_int(tokens[0], 'N');
    if (index < 0 || index > std::numeric_limits<node_index_t>::max()) {
      return -1;
    }
    const auto* node = cfg.getNode(index);
    if (!node) {
      return -1;
    }
    if (tokens.size() == 1) {
      add_node(index, *node);
      return 0;
    }
    int shard_id = extract_nonnegative_int(tokens[1], 'S');
    if (shard_id < 0 || shard_id > std::numeric_limits<shard_index_t>::max()) {
      return -1;
    }
    if (shard_id >= node->num_shards) {
      return -1;
    }
    out.insert(ShardID(index, shard_id));
    return 0;
  } catch (const std::exception& e) {
    return -1;
  }
}

/**
 * @param descriptors A list of descriptors, @see parseShardIDSet.
 * @param cfg          Server configuration.
 * @param out        Populated set of ShardID objects.
 *
 * @return           0 on success, or -1 if one of the descriptors did not match
 *                   any shard.
 */
int parseShardIDSet(const std::vector<std::string>& descriptors,
                    const ServerConfig& cfg,
                    ShardSet& out) {
  for (const std::string& descriptor : descriptors) {
    std::vector<std::string> tokens;
    folly::split(',', descriptor, tokens);
    for (const std::string& d : tokens) {
      if (parseShardIDSet(d, cfg, out) != 0) {
        return -1;
      }
    }
  }
  return 0;
}

}} // namespace facebook::logdevice
