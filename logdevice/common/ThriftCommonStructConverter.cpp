/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ThriftCommonStructConverter.h"

using facebook::logdevice::configuration::nodes::NodeRole;

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
