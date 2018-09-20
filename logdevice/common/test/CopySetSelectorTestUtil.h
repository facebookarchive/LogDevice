/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/CopySetSelectorDependencies.h"

namespace facebook { namespace logdevice {

class TestCopySetSelectorDeps : public CopySetSelectorDependencies,
                                public NodeAvailabilityChecker {
 public:
  TestCopySetSelectorDeps() = default;

  NodeStatus checkNode(NodeSetState*,
                       ShardID shard,
                       StoreChainLink* destination_out,
                       bool /*ignore_nodeset_state*/,
                       bool /*allow_unencrypted_conections*/) const override {
    auto status = getNodeStatus(shard);
    if (status != NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE) {
      *destination_out = {shard, ClientID()};
    }

    return status;
  }

  const NodeAvailabilityChecker* getNodeAvailability() const override {
    return this;
  }

  void setNodeStatus(ShardID shard,
                     NodeAvailabilityChecker::NodeStatus status) {
    node_status_[shard] = status;
  }

  NodeAvailabilityChecker::NodeStatus getNodeStatus(ShardID shard) const {
    return node_status_.count(shard)
        ? node_status_.at(shard)
        : NodeAvailabilityChecker::NodeStatus::AVAILABLE;
  }

  bool isAvailable(ShardID shard) const {
    return getNodeStatus(shard) !=
        NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE;
  }

  size_t countUnavailable(const StorageSet& shards) const {
    return std::count_if(shards.begin(), shards.end(), [&](ShardID shard) {
      return getNodeStatus(shard) ==
          NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE;
    });
  }

  void setNotAvailableNodes(const StorageSet& nodes) {
    node_status_.clear();
    for (ShardID shard : nodes) {
      node_status_[shard] = NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE;
    }
  }

 private:
  // For nodes that are not in the map status is AVAILABLE.
  std::
      unordered_map<ShardID, NodeAvailabilityChecker::NodeStatus, ShardID::Hash>
          node_status_;
};

}} // namespace facebook::logdevice
