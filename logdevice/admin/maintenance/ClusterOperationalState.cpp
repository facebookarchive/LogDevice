/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ClusterOperationalState.h"

namespace facebook { namespace logdevice {

using NodeState = ClusterOperationalState::NodeState;

ClusterOperationalState::~ClusterOperationalState() = default;

void ClusterOperationalState::initialize(
    const configuration::nodes::NodesConfiguration& config,
    const ShardAuthoritativeStatusMap& map) {
  // Process the Sequencer membership
  auto const& sequencer_membership = config.getSequencerMembership();
  for (auto node : sequencer_membership->getMembershipNodes()) {
    auto& op_node_state = cluster_state_[node];
    if (sequencer_membership->isSequencingEnabled(node)) {
      op_node_state.sequencer_state.state = SequencingState::ENABLED;
    } else {
      op_node_state.sequencer_state.state = SequencingState::DISABLED;
    }
  }
  // Process the Storage membership
  auto const& storage_membership = config.getStorageMembership();
  for (auto node : storage_membership->getMembershipNodes()) {
    auto const& shardStates = storage_membership->getShardStates(node);
    ld_check(shardStates.size() != 0);
    auto& op_node_state = cluster_state_[node];
    for (auto& kv : shardStates) {
      auto& opShardState = op_node_state.shards_state[kv.first];
      opShardState.data_health = ShardDataHealth::HEALTHY;
      if (kv.second.storage_state == membership::StorageState::NONE) {
        opShardState.operational_state = ShardOperationalState::DRAINED;
      } else {
        opShardState.operational_state = ShardOperationalState::ENABLED;
      }
    }
  }
  applyShardStatus(map);
}

int ClusterOperationalState::setShardOperationalState(
    ShardID shard,
    ShardOperationalState state) {
  if (!cluster_state_.count(shard.node())) {
    return -1;
  }

  auto& nodeState = cluster_state_[shard.node()];
  if (!nodeState.shards_state.count(shard.shard())) {
    return -1;
  }

  auto& shardState = nodeState.shards_state[shard.shard()];
  shardState.operational_state = state;
  shardState.operational_state_last_updated = SystemTimestamp::now();
  return 0;
}

int ClusterOperationalState::setSequencingState(node_index_t node,
                                                SequencingState state) {
  if (!cluster_state_.count(node)) {
    return -1;
  }

  auto& nodeState = cluster_state_[node];
  nodeState.sequencer_state.state = state;
  nodeState.sequencer_state.sequencer_state_last_updated =
      SystemTimestamp::now();
  return 0;
}

ShardOperationalState
ClusterOperationalState::getShardOperationalState(ShardID shard) const {
  auto node_it = cluster_state_.find(shard.node());

  if (node_it == cluster_state_.end()) {
    return ShardOperationalState::INVALID;
  }

  auto shard_it = node_it->second.shards_state.find(shard.shard());

  if (shard_it == node_it->second.shards_state.end()) {
    return ShardOperationalState::INVALID;
  }

  return shard_it->second.operational_state;
}

ShardDataHealth
ClusterOperationalState::getShardDataHealth(ShardID shard) const {
  auto node_it = cluster_state_.find(shard.node());

  if (node_it == cluster_state_.end()) {
    return ShardDataHealth::UNKNOWN;
  }

  auto shard_it = node_it->second.shards_state.find(shard.shard());

  if (shard_it == node_it->second.shards_state.end()) {
    return ShardDataHealth::UNKNOWN;
  }

  return shard_it->second.data_health;
}

SequencingState
ClusterOperationalState::getSequencingState(node_index_t node) const {
  auto node_it = cluster_state_.find(node);
  if (node_it == cluster_state_.end()) {
    return SequencingState::INVALID;
  }
  return node_it->second.sequencer_state.state;
}

void ClusterOperationalState::applyShardStatus(
    const ShardAuthoritativeStatusMap& map) {
  for (auto& nodekv : map.getShards()) {
    auto node = nodekv.first;
    if (!cluster_state_.count(node)) {
      continue;
    }
    auto& nodeState = cluster_state_[node];
    for (auto& shardkv : nodekv.second) {
      auto shard = shardkv.first;
      if (!nodeState.shards_state.count(shard)) {
        continue;
      }
      auto& shardState = nodeState.shards_state[shard];
      shardState.data_health = translateShardAuthoritativeStatus(
          shardkv.second.auth_status, shardkv.second.time_ranged_rebuild);
    }
  }
}

ShardDataHealth ClusterOperationalState::translateShardAuthoritativeStatus(
    AuthoritativeStatus status,
    bool is_time_range_rebuilding) {
  switch (status) {
    case AuthoritativeStatus::FULLY_AUTHORITATIVE:
      return is_time_range_rebuilding ? ShardDataHealth::LOST_REGIONS
                                      : ShardDataHealth::HEALTHY;
    case AuthoritativeStatus::UNAVAILABLE:
      return ShardDataHealth::UNAVAILABLE;
    case AuthoritativeStatus::UNDERREPLICATION:
      return ShardDataHealth::LOST_ALL;
    case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
      return ShardDataHealth::EMPTY;
    default:
      ld_warning("Unknown AuthoritativeStatus:%s, Unable to translate to "
                 "ShardDataHealth",
                 toString(status).c_str());
      return ShardDataHealth::UNKNOWN;
  }
}

}} // namespace facebook::logdevice
