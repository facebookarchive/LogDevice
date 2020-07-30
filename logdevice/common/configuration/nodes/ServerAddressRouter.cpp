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

const Sockaddr& ServerAddressRouter::getAddress(
    node_index_t idx,
    const NodeServiceDiscovery& node_svc,
    SocketType socket_type,
    ConnectionType connection_type,
    bool is_server,
    bool use_dedicated_server_to_server_address,
    bool use_dedicated_gossip_port,
    const std::vector<node_index_t>& same_partition_nodes) const {
  {
    // An error injection mechanism to simulate isolation. If the node we're
    // trying to reach is not in our partition. We're going to return an invalid
    // address to simulate it being unreachable.
    if (!same_partition_nodes.empty()) {
      if (std::find(same_partition_nodes.begin(),
                    same_partition_nodes.end(),
                    idx) == same_partition_nodes.end()) {
        static const Sockaddr non_existent_address{"/nonexistent"};
        return non_existent_address;
      }
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
                    settings.test_same_partition_nodes);
}

}}}} // namespace facebook::logdevice::configuration::nodes
