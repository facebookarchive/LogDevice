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
    auto entry = getNodeEntry(shard.asNodeID());
    if (entry.status_ != NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE) {
      *destination_out = {shard, entry.client_id_};
    }

    return entry.status_;
  }

  const NodeAvailabilityChecker* getNodeAvailability() const override {
    return this;
  }

  NodeAvailabilityChecker::NodeStatus getNodeStatus(NodeID node) const {
    return getNodeEntry(node).status_;
  }

  void setNodeStatus(ShardID shard,
                     NodeAvailabilityChecker::NodeStatus status,
                     ClientID client_id) {
    node_status_[shard.asNodeID()] = {status, client_id};
  }

  bool isAvailable(ShardID shard) const {
    return getNodeStatus(shard.asNodeID()) !=
        NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE;
  }

  size_t countUnavailable(const StorageSet& shards) const {
    return std::count_if(shards.begin(), shards.end(), [&](ShardID shard) {
      return getNodeStatus(shard.asNodeID()) ==
          NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE;
    });
  }

  void setNotAvailableNodes(const StorageSet& nodes) {
    node_status_.clear();
    for (ShardID shard : nodes) {
      node_status_[shard.asNodeID()] = {
          NodeAvailabilityChecker::NodeStatus::NOT_AVAILABLE,
          ClientID::INVALID,
          Status::OK};
    }
  }

 protected:
  struct Entry {
    NodeAvailabilityChecker::NodeStatus status_;
    ClientID client_id_{ClientID::INVALID};
    // injected connection errors, if any
    Status connection_state_{Status::OK};
  };

  Entry getNodeEntry(NodeID node) const {
    auto it = node_status_.find(node);
    return it != node_status_.end()
        ? it->second
        : Entry{NodeAvailabilityChecker::NodeStatus::AVAILABLE,
                ClientID::MIN,
                Status::OK};
  }

  // For nodes that are not in the map status is AVAILABLE.
  std::unordered_map<NodeID, Entry, NodeID::Hash> node_status_;
};

}} // namespace facebook::logdevice
