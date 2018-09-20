/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "Node.h"

namespace facebook { namespace logdevice { namespace configuration {

class NodesConfig {
 public:
  NodesConfig() {}
  explicit NodesConfig(Nodes nodes) {
    setNodes(std::move(nodes));
  }
  void setNodes(Nodes nodes) {
    nodes_ = std::move(nodes);
    calculateHash();
    calculateNumShards();
  }
  const Nodes& getNodes() const {
    return nodes_;
  }
  uint64_t getStorageNodeHash() const {
    return hash_;
  }
  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  shard_size_t getNumShards() const {
    return num_shards_;
  }

 private:
  // calculates a hash of storage-relevant attributes of nodes: for all nodes,
  // hashes their node_ids, weights, num_shards and locations and stores it in
  // `hash_`
  void calculateHash();

  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  void calculateNumShards();

  Nodes nodes_;

  uint64_t hash_{0};

  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member as well as the check in NodesConfigParser will be removed when the
  // Flexible Log Sharding project is fully implemented.
  // In the mean time, this member is used by state machines that need to
  // convert node_index_t values to ShardID values.
  shard_size_t num_shards_{0};
};

}}} // namespace facebook::logdevice::configuration
