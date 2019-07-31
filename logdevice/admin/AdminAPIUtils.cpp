/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIUtils.h"

#include "logdevice/admin/Conv.h"
#include "logdevice/admin/toString.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfigLegacyConverter.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

bool match_by_address(const configuration::nodes::NodeServiceDiscovery& node_sd,
                      const thrift::SocketAddress& address) {
  // UNIX
  // Match address on exact path for unix sockets
  if (node_sd.address.isUnixAddress() &&
      address.address_family == thrift::SocketAddressFamily::UNIX) {
    if (address.address_ref().has_value()) {
      // match by the address value if it's set.
      return node_sd.address.getPath() == address.address_ref().value();
    }
    return true;
  }

  // INET
  // Only match fields when they are actually present. Also use Folly parsing
  // to correctly deal with IPv6 exploded versus compressed notations
  //
  // Fields:
  // 1. Address
  // 2. Port
  if (!node_sd.address.isUnixAddress() &&
      address.address_family == thrift::SocketAddressFamily::INET) {
    if (!address.address_ref().has_value()) {
      if (address.port_ref().has_value()) {
        return node_sd.address.port() == address.port_ref().value();
      }
      return false;
    } else {
      auto node_address = folly::SocketAddress(
          node_sd.address.getAddress().str(),
          address.port_ref().has_value() ? node_sd.address.port() : 0);
      auto other_address = folly::SocketAddress(
          address.address_ref().value(),
          address.port_ref().has_value() ? address.port_ref().value() : 0);
      return node_address == other_address;
    }
  }
  return false;
}

void forFilteredNodes(
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const thrift::NodesFilter* filter,
    NodeFunctor fn) {
  folly::Optional<configuration::nodes::NodeRole> role_filter;

  if (filter && filter->role_ref()) {
    configuration::nodes::NodeRole ld_role =
        toLogDevice<configuration::nodes::NodeRole>(filter->role_ref().value());
    role_filter.assign(ld_role);
  }

  auto matches =
      [&](node_index_t index,
          const configuration::nodes::NodeServiceDiscovery& node_sd) -> bool {
    if (!filter) {
      // We don't have a filter, we accept all nodes.
      return true;
    }
    bool res = true;
    // filter by role
    if (role_filter) {
      res &= node_sd.hasRole(*role_filter);
    }
    // filter by node
    if (filter->node_ref().has_value()) {
      res &= nodeMatchesID(index, node_sd, filter->node_ref().value());
    }
    // filter by location
    if (filter->location_ref().has_value()) {
      std::string location_filter_str = filter->location_ref().value();
      res &= (node_sd.location &&
              node_sd.location->matchesPrefix(location_filter_str));
    }
    return res;
  };

  for (const auto& kv : *nodes_configuration.getServiceDiscovery()) {
    if (matches(kv.first, kv.second)) {
      fn(kv.first);
    }
  }
}

// TODO: Deprecate and use Maintenance Manager instead.
thrift::ShardOperationalState
toShardOperationalState(membership::StorageState storage_state,
                        const EventLogRebuildingSet::NodeInfo* node_info) {
  switch (storage_state) {
    case membership::StorageState::INVALID:
      return thrift::ShardOperationalState::INVALID;
    case membership::StorageState::NONE:
      return thrift::ShardOperationalState::DRAINED;
    case membership::StorageState::PROVISIONING:
      return thrift::ShardOperationalState::PROVISIONING;
    case membership::StorageState::NONE_TO_RO:
    case membership::StorageState::RW_TO_RO:
    case membership::StorageState::DATA_MIGRATION:
    case membership::StorageState::READ_ONLY:
      // The node will be in READ_ONLY if we are draining.
      if (node_info) {
        if (node_info->auth_status ==
            AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
          return thrift::ShardOperationalState::DRAINED;
        } else if (node_info->auth_status ==
                       AuthoritativeStatus::FULLY_AUTHORITATIVE &&
                   !node_info->drain) {
          // As long as the drain flag is not set, no rebuilding _should_ be
          // running.
          return thrift::ShardOperationalState::MAY_DISAPPEAR;
        } else {
          // We are UNAVAILABLE/UNDERREPLICATION or (FULLY_AUTH+drain)
          return thrift::ShardOperationalState::MIGRATING_DATA;
        }
      }
      // If we don't have authoritative status, we lean toward using
      // MAY_DISAPPEAR since the shard is READ_ONLY. This aligns with how
      // MaintenanceManager decides on the state.
      return thrift::ShardOperationalState::MAY_DISAPPEAR;
    case membership::StorageState::READ_WRITE:
      return thrift::ShardOperationalState::ENABLED;
  }
  ld_check(false);
  return thrift::ShardOperationalState::INVALID;
}

void fillNodeConfig(
    thrift::NodeConfig& out,
    node_index_t node_index,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  out.set_node_index(node_index);

  const auto* node_sd = nodes_configuration.getNodeServiceDiscovery(node_index);
  // caller should ensure node_index exists in nodes_configuration
  ld_check(node_sd != nullptr);

  // Name
  out.set_name(node_sd->name);

  // Roles
  std::set<thrift::Role> roles;
  if (node_sd->hasRole(nodes::NodeRole::SEQUENCER)) {
    roles.insert(thrift::Role::SEQUENCER);
    const auto& seq_membership = nodes_configuration.getSequencerMembership();
    const auto result = seq_membership->getNodeState(node_index);
    if (result.hasValue()) {
      // Sequencer Config
      thrift::SequencerConfig sequencer_config;
      sequencer_config.set_weight(result->getConfiguredWeight());
      out.set_sequencer(std::move(sequencer_config));
    }
  }

  if (node_sd->hasRole(nodes::NodeRole::STORAGE)) {
    roles.insert(thrift::Role::STORAGE);
    const auto* storage_attr =
        nodes_configuration.getNodeStorageAttribute(node_index);
    if (storage_attr) {
      // Storage Node Config
      thrift::StorageConfig storage_config;
      storage_config.set_weight(storage_attr->capacity);
      storage_config.set_num_shards(storage_attr->num_shards);
      out.set_storage(std::move(storage_config));
    }
  }

  out.set_roles(std::move(roles));
  out.set_location(node_sd->locationStr());
  out.set_location_per_scope(toThrift<thrift::Location>(node_sd->location));

  thrift::SocketAddress data_address;
  fillSocketAddress(data_address, node_sd->address);
  out.set_data_address(std::move(data_address));

  // Other Addresses
  thrift::Addresses other_addresses;
  if (node_sd->gossip_address) {
    thrift::SocketAddress gossip_address;
    fillSocketAddress(gossip_address, node_sd->gossip_address.value());
    other_addresses.set_gossip(std::move(gossip_address));
  }
  if (node_sd->ssl_address) {
    thrift::SocketAddress ssl_address;
    fillSocketAddress(ssl_address, node_sd->ssl_address.value());
    other_addresses.set_ssl(std::move(ssl_address));
  }
  out.set_other_addresses(std::move(other_addresses));
}

void fillSocketAddress(thrift::SocketAddress& out, const Sockaddr& addr) {
  if (addr.isUnixAddress()) {
    out.set_address_family(thrift::SocketAddressFamily::UNIX);
    out.set_address(addr.getPath());
  } else {
    out.set_address_family(thrift::SocketAddressFamily::INET);
    out.set_address(addr.getAddress().str());
    out.set_port(addr.port());
  }
}

void fillNodeState(
    thrift::NodeState& out,
    node_index_t node_index,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    const EventLogRebuildingSet* rebuilding_set,
    const ClusterState* cluster_state) {
  out.set_node_index(node_index);

  thrift::NodeConfig node_config;
  fillNodeConfig(node_config, node_index, nodes_configuration);
  out.set_config(std::move(node_config));

  if (cluster_state) {
    out.set_daemon_state(toThrift<thrift::ServiceState>(
        cluster_state->getNodeState(node_index)));
  }

  const auto* node_sd = nodes_configuration.getNodeServiceDiscovery(node_index);
  // caller should ensure node_index exists in nodes_configuration
  ld_check(node_sd != nullptr);

  // Sequencer State
  if (node_sd->hasRole(nodes::NodeRole::SEQUENCER)) {
    thrift::SequencerState sequencer;
    thrift::SequencingState state = thrift::SequencingState::DISABLED;
    const auto& seq_membership = nodes_configuration.getSequencerMembership();

    if (seq_membership->isSequencerEnabledFlagSet(node_index)) {
      state = thrift::SequencingState::ENABLED;
      // let's see if we have a failure-detector state about this sequencer
      if (cluster_state && cluster_state->isNodeBoycotted(node_index)) {
        state = thrift::SequencingState::BOYCOTTED;
      }
    }
    sequencer.set_state(state);
    // TODO: Fill the sequencer_state_last_updated when we have that.
    out.set_sequencer_state(std::move(sequencer));
  }

  // Storage State
  if (node_sd->hasRole(nodes::NodeRole::STORAGE)) {
    const auto& storage_membership = nodes_configuration.getStorageMembership();
    std::vector<thrift::ShardState> shard_states;
    for (int shard_index = 0;
         shard_index < nodes_configuration.getNumShards(node_index);
         shard_index++) {
      // For every shard in storage membership
      ShardID shard(node_index, shard_index);
      auto result = storage_membership->getShardState(shard);
      if (!result.hasValue()) {
        // shard does not exist in membership
        continue;
      }

      thrift::ShardState state;
      auto node_info = rebuilding_set
          ? rebuilding_set->getNodeInfo(node_index, shard_index)
          : nullptr;
      // DEPRECATED
      state.set_current_storage_state(
          toThrift<thrift::ShardStorageState>(result->storage_state));

      state.set_storage_state(
          toThrift<membership::thrift::StorageState>(result->storage_state));

      state.set_metadata_state(
          toThrift<membership::thrift::MetaDataStorageState>(
              result->metadata_state));

      state.set_current_operational_state(
          toShardOperationalState(result->storage_state, node_info));
      AuthoritativeStatus auth_status =
          AuthoritativeStatus::FULLY_AUTHORITATIVE;
      bool has_dirty_ranges = false;
      if (node_info) {
        has_dirty_ranges = !node_info->dc_dirty_ranges.empty();
        auth_status = node_info->auth_status;
      }
      state.set_data_health(toShardDataHealth(auth_status, has_dirty_ranges));
      shard_states.push_back(std::move(state));
    }
    out.set_shard_states(std::move(shard_states));
  }
}

folly::Optional<node_index_t> findNodeIndex(
    const thrift::NodeID& node,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  node_index_t found_index = -1;
  for (const auto& kv : *nodes_configuration.getServiceDiscovery()) {
    if (nodeMatchesID(kv.first, kv.second, node)) {
      if (found_index > -1) {
        // we have seen a match before. We can't match multiple nodes here.
        found_index = -1;
        break;
      }
      found_index = kv.first;
    }
  }

  if (found_index > -1) {
    return found_index;
  }
  return folly::none;
}

ShardSet resolveShardOrNode(
    const thrift::ShardID& shard,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    bool ignore_missing,
    bool ignore_non_storage_nodes) {
  ShardSet output;

  const auto& serv_disc = nodes_configuration.getServiceDiscovery();
  shard_index_t shard_index = (shard.shard_index < 0) ? -1 : shard.shard_index;
  shard_size_t num_shards = 1;
  if (!isNodeIDSet(shard.get_node())) {
    throw thrift::InvalidRequest(
        "NodeID object must have at least one attribute set");
  }
  folly::Optional<node_index_t> found_node =
      findNodeIndex(shard.get_node(), nodes_configuration);
  // Node is not in nodes configuration
  if (!found_node) {
    if (ignore_missing) {
      return output;
    }
    // We didn't find the node.
    throw thrift::InvalidRequest("Node was not found in the nodes config");
  }

  // Node must be a storage node
  if (!nodes_configuration.isStorageNode(*found_node)) {
    // We didn't find the node.
    if (ignore_non_storage_nodes) {
      return output;
    }
    throw thrift::InvalidRequest(
        folly::sformat("Node {} is not a storage node", *found_node));
  }

  if (shard_index == -1) {
    num_shards = nodes_configuration.getNumShards(*found_node);
    for (int i = 0; i < num_shards; i++) {
      output.emplace(ShardID(*found_node, i));
    }
  } else {
    output.emplace(ShardID(*found_node, shard_index));
  }
  return output;
}

ShardSet expandShardSet(
    const thrift::ShardSet& thrift_shards,
    const configuration::nodes::NodesConfiguration& nodes_configuration,
    bool ignore_missing,
    bool ignore_non_storage_nodes) {
  ShardSet output;
  for (const auto& it : thrift_shards) {
    ShardSet expanded = resolveShardOrNode(
        it, nodes_configuration, ignore_missing, ignore_non_storage_nodes);
    output.insert(expanded.begin(), expanded.end());
  }
  return output;
}

bool isNodeIDSet(const thrift::NodeID& id) {
  return (id.address_ref().has_value() || id.node_index_ref().has_value() ||
          id.name_ref().has_value());
}

bool nodeMatchesID(node_index_t node_index,
                   const configuration::nodes::NodeServiceDiscovery& node_sd,
                   const thrift::NodeID& id) {
  // if address string is set, use it to match.
  if (id.address_ref().has_value() &&
      !match_by_address(node_sd, id.address_ref().value())) {
    return false;
  }

  // match by index.
  if (id.node_index_ref().has_value() &&
      id.node_index_ref().value() != node_index) {
    return false;
  }

  // if name is set, use it to match.
  if (id.name_ref().has_value() && id.name_ref().value() != node_sd.name) {
    return false;
  }

  return true;
}

bool isInNodeIDs(node_index_t node_id,
                 const std::vector<thrift::NodeID>& nodes) {
  // returns true if we can find a NodeID object that has node_index set with
  // the supplied value (node_id)
  return nodes.end() !=
      std::find_if(nodes.begin(),
                   nodes.end(),
                   [&node_id](const thrift::NodeID& matched_node) {
                     auto node_index = matched_node.node_index_ref();
                     if (node_index.has_value()) {
                       return node_index.value() == node_id;
                     } else {
                       return false;
                     }
                   });
}

thrift::ShardID mkShardID(node_index_t node_id, shard_index_t shard) {
  thrift::NodeID new_node;
  new_node.set_node_index(node_id);
  thrift::ShardID new_shard;
  new_shard.set_node(std::move(new_node));
  new_shard.set_shard_index(shard);
  return new_shard;
}

thrift::ShardID mkShardID(const ShardID& shard) {
  thrift::NodeID new_node;
  new_node.set_node_index(shard.node());
  thrift::ShardID new_shard;
  new_shard.set_node(new_node);
  new_shard.set_shard_index(shard.shard());
  return new_shard;
}

thrift::NodeID mkNodeID(node_index_t node_id) {
  thrift::NodeID new_node;
  new_node.set_node_index(node_id);
  return new_node;
}

}} // namespace facebook::logdevice
