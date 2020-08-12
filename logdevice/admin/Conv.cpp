/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/Conv.h"

using facebook::logdevice::configuration::nodes::NodeRole;
using facebook::logdevice::membership::MetaDataStorageState;
using TMetaDataStorageState =
    facebook::logdevice::membership::thrift::MetaDataStorageState;
using facebook::logdevice::membership::StorageState;
using TStorageState = facebook::logdevice::membership::thrift::StorageState;

namespace facebook { namespace logdevice {

template <>
thrift::Role toThrift(const NodeRole& role) {
  switch (role) {
    case NodeRole::SEQUENCER:
      return thrift::Role::SEQUENCER;
    case NodeRole::STORAGE:
      return thrift::Role::STORAGE;
    default:
      return thrift::Role::STORAGE;
  }
  ld_check(false);
  return thrift::Role::STORAGE;
}

template <>
configuration::nodes::NodeRole toLogDevice(const thrift::Role& role) {
  switch (role) {
    case thrift::Role::SEQUENCER:
      return configuration::nodes::NodeRole::SEQUENCER;
    case thrift::Role::STORAGE:
      return configuration::nodes::NodeRole::STORAGE;
  }
  ld_check(false);
  return configuration::nodes::NodeRole::SEQUENCER;
}

// DEPRECATED
template <>
thrift::ShardStorageState
toThrift(const configuration::StorageState& storage_state) {
  switch (storage_state) {
    case configuration::StorageState::DISABLED:
      return thrift::ShardStorageState::DISABLED;
    case configuration::StorageState::READ_ONLY:
      return thrift::ShardStorageState::READ_ONLY;
    case configuration::StorageState::READ_WRITE:
      return thrift::ShardStorageState::READ_WRITE;
  }
  ld_check(false);
  return thrift::ShardStorageState::DISABLED;
}

// DEPRECATED
template <>
configuration::StorageState
toLogDevice(const thrift::ShardStorageState& storage_state) {
  switch (storage_state) {
    case thrift::ShardStorageState::DISABLED:
      return configuration::StorageState::DISABLED;
    case thrift::ShardStorageState::DATA_MIGRATION:
    case thrift::ShardStorageState::READ_ONLY:
      return configuration::StorageState::READ_ONLY;
    case thrift::ShardStorageState::READ_WRITE:
      return configuration::StorageState::READ_WRITE;
  }
  ld_check(false);
  return configuration::StorageState::DISABLED;
}

// From Membership.thrift
template <>
TStorageState toThrift(const StorageState& storage_state) {
  return static_cast<TStorageState>(storage_state);
}

// From Membership.thrift
template <>
StorageState toLogDevice(const TStorageState& storage_state) {
  return static_cast<StorageState>(storage_state);
}

// From Membership.thrift
template <>
TMetaDataStorageState toThrift(const MetaDataStorageState& storage_state) {
  return static_cast<TMetaDataStorageState>(storage_state);
}

// From Membership.thrift
template <>
MetaDataStorageState toLogDevice(const TMetaDataStorageState& storage_state) {
  return static_cast<MetaDataStorageState>(storage_state);
}

template <>
NodeLocationScope toLogDevice(const thrift::LocationScope& location_scope) {
  switch (location_scope) {
#define NODE_LOCATION_SCOPE(name)   \
  case thrift::LocationScope::name: \
    return NodeLocationScope::name;
#include "logdevice/include/node_location_scopes.inc"
  }
  ld_check(false);
  return NodeLocationScope::INVALID;
}

template <>
thrift::ServiceState toThrift(const ClusterStateNodeState& input) {
  switch (input) {
    case ClusterStateNodeState::DEAD:
      return thrift::ServiceState::DEAD;
    case ClusterStateNodeState::FULLY_STARTED:
      return thrift::ServiceState::ALIVE;
    case ClusterStateNodeState::STARTING:
      return thrift::ServiceState::STARTING_UP;
    case ClusterStateNodeState::FAILING_OVER:
      return thrift::ServiceState::SHUTTING_DOWN;
  }
  return thrift::ServiceState::UNKNOWN;
}

template <>
thrift::ServiceHealthStatus toThrift(const NodeHealthStatus& input) {
  switch (input) {
    case NodeHealthStatus::UNHEALTHY:
      return thrift::ServiceHealthStatus::UNHEALTHY;
    case NodeHealthStatus::HEALTHY:
      return thrift::ServiceHealthStatus::HEALTHY;
    case NodeHealthStatus::OVERLOADED:
      return thrift::ServiceHealthStatus::OVERLOADED;
    case NodeHealthStatus::UNDEFINED:
      return thrift::ServiceHealthStatus::UNDEFINED;
  }
  return thrift::ServiceHealthStatus::UNKNOWN;
}

template <>
thrift::LocationScope toThrift(const NodeLocationScope& location_scope) {
  switch (location_scope) {
    case NodeLocationScope::INVALID:
      // We don't have INVALID in thrift because we don't need it.
      return thrift::LocationScope::ROOT;
#define NODE_LOCATION_SCOPE(name) \
  case NodeLocationScope::name:   \
    return thrift::LocationScope::name;
#include "logdevice/include/node_location_scopes.inc"
  }
  ld_check(false);
  return thrift::LocationScope::ROOT;
}

template <>
ReplicationProperty
toLogDevice(const thrift::ReplicationProperty& replication) {
  std::vector<ReplicationProperty::ScopeReplication> vec;
  for (const auto& it : replication) {
    vec.push_back(
        std::make_pair(toLogDevice<NodeLocationScope>(it.first), it.second));
  }
  return ReplicationProperty(std::move(vec));
}

template <>
thrift::ShardID toThrift(const ShardID& shard) {
  thrift::ShardID output;
  output.set_shard_index(shard.shard());
  // We do not set the address of the node in the output. This can be useful
  // in the future if we needed to always locate the nodes with their address
  // instead of the index. However, it's not necessary right now.
  // TODO: Also return node address & name information.
  thrift::NodeID node_identifier;
  node_identifier.set_node_index(shard.node());
  output.set_node(std::move(node_identifier));
  return output;
}

template <>
thrift::ReplicationProperty toThrift(const ReplicationProperty& replication) {
  thrift::ReplicationProperty output;
  for (const auto& scope_replication :
       replication.getDistinctReplicationFactors()) {
    output.insert(
        std::make_pair(toThrift<thrift::LocationScope>(scope_replication.first),
                       scope_replication.second));
  }
  return output;
}

thrift::ShardDataHealth toShardDataHealth(AuthoritativeStatus auth_status,
                                          bool has_dirty_ranges) {
  switch (auth_status) {
    case AuthoritativeStatus::FULLY_AUTHORITATIVE:
      return has_dirty_ranges ? thrift::ShardDataHealth::LOST_REGIONS
                              : thrift::ShardDataHealth::HEALTHY;
    case AuthoritativeStatus::UNDERREPLICATION:
      return thrift::ShardDataHealth::LOST_ALL;
    case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
      return thrift::ShardDataHealth::EMPTY;
    case AuthoritativeStatus::UNAVAILABLE:
      return thrift::ShardDataHealth::UNAVAILABLE;
    default:
      return thrift::ShardDataHealth::UNKNOWN;
  }
}

template <>
thrift::Location toThrift(const folly::Optional<NodeLocation>& input) {
  thrift::Location output;
  if (input) {
    auto insert_scope = [&](thrift::LocationScope t, std::string value) {
      if (!value.empty()) {
        output[t] = std::move(value);
      }
    };
#define NODE_LOCATION_SCOPE(name) \
  insert_scope(                   \
      thrift::LocationScope::name, input->getLabel(NodeLocationScope::name));
#include "logdevice/include/node_location_scopes.inc"
  }
  return output;
}

}} // namespace facebook::logdevice
