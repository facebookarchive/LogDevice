/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Expected.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

// A special class that generates a new NodeSet for metadata logs. This class
// tries to preserve the existing nodes in the nodes except for the ones that
// need to be removed or the node whose number of writable shards has fallen
// below a configurable threshold
// Currently an existing node is preserved only if it has a
// NUM_WRITEABLE_SHARDS_PER_NODE_RATIO of writeable shards.
//
// This selector picks nodes in a domain from a priority queue that minimizes
// 1. Number of nodes already picked in same region
// 2. Number of nodes already picked in the largest region
// 3. whether the largest scope has no unpicked nodes from original nodeset
// 4. location string

class MetadataNodeSetSelector {
 public:
  static folly::Expected<std::set<node_index_t>, Status>
  getNodeSet(std::shared_ptr<const configuration::nodes::NodesConfiguration>
                 nodes_config,
             const std::unordered_set<node_index_t>& exclude_nodes);
};
}} // namespace facebook::logdevice
