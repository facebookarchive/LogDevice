/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/ServerAddressRouter.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

namespace {

static const Sockaddr kNonExistentAddress = Sockaddr("/nonexistent");

// Checks whether node with given index belongs to another partition. This is
// intended for error injection mechahism allowing us to simulate network
// partitions w/o creating them on network layer.
bool isPartitioned(node_index_t idx,
                   const std::vector<node_index_t>& same_partition_nodes) {
  return !same_partition_nodes.empty() &&
      std::find(same_partition_nodes.begin(),
                same_partition_nodes.end(),
                idx) == same_partition_nodes.end();
}
} // namespace

const Sockaddr& ServerAddressRouter::getAddress(
    node_index_t idx,
    const NodeServiceDiscovery& node_svc,
    SocketType socket_type,
    ConnectionType connection_type,
    bool is_server,
    bool use_dedicated_server_to_server_address,
    bool use_dedicated_gossip_port,
    const std::vector<node_index_t>& same_partition_nodes,
    folly::Optional<NodeServiceDiscovery::ClientNetworkPriority>
        network_priority) const {
  {
    //  If the node we're trying to reach is not in our partition then we're
    //  going to return an invalid address to simulate it being unreachable.
    if (isPartitioned(idx, same_partition_nodes)) {
      return kNonExistentAddress;
    }
  }

  // If use_dedicated_gossip_port is false, it means we should use the data
  // port for connections.
  if (socket_type == SocketType::GOSSIP && !use_dedicated_gossip_port) {
    socket_type = SocketType::DATA;
  }

  switch (socket_type) {
    case SocketType::GOSSIP:
      return node_svc.getGossipAddress();

    case SocketType::DATA:
      if (is_server && use_dedicated_server_to_server_address) {
        if (!node_svc.server_to_server_address.has_value()) {
          return Sockaddr::INVALID;
        }
        return node_svc.server_to_server_address.value();
      }

      if (!is_server && network_priority.has_value()) {
        if (node_svc.addresses_per_priority.count(network_priority.value())) {
          return node_svc.addresses_per_priority.at(network_priority.value());
        } else {
          RATELIMIT_ERROR(std::chrono::minutes{10},
                          1,
                          "No address found with priority '%s' to server %s",
                          NodeServiceDiscovery::networkPriorityToString(
                              network_priority.value())
                              .c_str(),
                          NodeID(idx).toString().c_str());
          return Sockaddr::INVALID;
        }
      }

      if (connection_type == ConnectionType::SSL) {
        if (!node_svc.ssl_address.has_value()) {
          return Sockaddr::INVALID;
        }
        return node_svc.ssl_address.value();
      }
      return node_svc.default_client_data_address;

    default:
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         2,
                         "Unexpected Socket Type:%d!",
                         (int)socket_type);
      ld_check(false);
  }
  return Sockaddr::INVALID;
}

const Sockaddr&
ServerAddressRouter::getAddress(node_index_t idx,
                                const NodeServiceDiscovery& node_svc,
                                SocketType socket_type,
                                ConnectionType connection_type,
                                const Settings& settings) const {
  return getAddress(idx,
                    node_svc,
                    socket_type,
                    connection_type,
                    settings.server,
                    settings.use_dedicated_server_to_server_address,
                    settings.send_to_gossip_port,
                    settings.test_same_partition_nodes,
                    settings.enable_port_based_qos
                        ? settings.client_default_network_priority
                        : folly::none);
}

folly::Optional<Sockaddr> ServerAddressRouter::getThriftAddress(
    node_index_t idx,
    const NodeServiceDiscovery& node_svc,
    bool is_server,
    const std::vector<node_index_t>& same_partition_nodes) const {
  if (isPartitioned(idx, same_partition_nodes)) {
    return kNonExistentAddress;
  }
  return is_server ? node_svc.server_thrift_api_address
                   : node_svc.client_thrift_api_address;
}

folly::Optional<Sockaddr>
ServerAddressRouter::getThriftAddress(node_index_t idx,
                                      const NodeServiceDiscovery& node_svc,
                                      const Settings& settings) const {
  return getThriftAddress(
      idx, node_svc, settings.server, settings.test_same_partition_nodes);
}

}}}} // namespace facebook::logdevice::configuration::nodes
