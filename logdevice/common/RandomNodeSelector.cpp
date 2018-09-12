/**
 * Copyright (c) 2017-present, Facebook, Inc.
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
} // namespace

NodeID RandomNodeSelector::getNode(const ServerConfig& cfg, NodeID exclude) {
  NodeID new_node;
  const auto& nodes = cfg.getNodes();

  // Pick a random node from nodes, excluding `exclude`.
  size_t count = nodes.size();
  ld_check(count >= 1);
  if (exclude.isNodeID() && cfg.getNode(exclude)) {
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
}} // namespace facebook::logdevice
