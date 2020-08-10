/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include <folly/dynamic.h>

#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace configuration {

struct MetaDataLogsConfig;

class NodesConfig {
 public:
  explicit NodesConfig() {}

  explicit NodesConfig(Nodes nodes) {
    setNodes(std::move(nodes));
  }

  void setNodes(Nodes nodes) {
    nodes_ = std::move(nodes);
  }
  const Nodes& getNodes() const {
    return nodes_;
  }

  folly::dynamic toJson() const;

  /**
   * NOTE: Only used for consistency checks in NodesConfiguration.
   * NodesConfiguration::getMaxNodeIndex() is what you're looking for.
   *
   * Returns the maximum key in getNodes().
   */
  size_t getMaxNodeIdx_DEPRECATED() const;

 private:
  // NOTE: Only used for consistency checks in NodesConfiguration.
  // NodesConfiguration::getStorageNodesHash() is what you're looking for.
  //
  // calculates a hash of storage-relevant attributes of nodes: for all nodes,
  // hashes their node_ids, weights, num_shards and locations and stores it in
  // `hash_`
  uint64_t calculateHash() const;

  // NOTE: Only used for consistency checks in NodesConfiguration.
  // NodesConfiguration::getNumShards() is what you're looking for.
  //
  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member as well as the check in NodesConfigParser will be removed when the
  // Flexible Log Sharding project is fully implemented.
  // In the mean time, this member is used by state machines that need to
  // convert node_index_t values to ShardID values.
  shard_size_t calculateNumShards() const;

  Nodes nodes_;
};

}}} // namespace facebook::logdevice::configuration
