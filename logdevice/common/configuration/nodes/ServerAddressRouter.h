/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class ServerAddressRouter {
 public:
  /**
   * Returns the corresponding socket address for a specific channel.
   *
   * @param idx             The idx of the node we're trying to get the address
   *                        for.
   * @param node_svc        The service discovery info of the node we're trying
   *                        to get the address for.
   * @param socket_type     Type of socket (DATA, GOSSIP, etc.)
   * @param connection_type Type of connection (PLAIN, SSL, etc.)
   * @param is_server       Whether the caller is a server.
   * @param use_dedicated_server_to_server_address Temporary switch to control
   *        whether nodes use a dedicated address to talk to other nodes.
   *        Ignored if the peer type is not NODE.
   * @param use_dedicated_gossip_port If set, will use the node's gossip address
   *        for gossip, otherwise fallbacks to data ports. whether nodes use a
   *        dedicated address to talk to other nodes. Ignored if the peer type
   *        is not NODE.
   */
  const Sockaddr& getAddress(node_index_t idx,
                             const NodeServiceDiscovery& node_svc,
                             SocketType socket_type,
                             ConnectionType connection_type,
                             bool is_server,
                             bool use_dedicated_server_to_server_address,
                             bool use_dedicated_gossip_port) const;

  const Sockaddr& getAddress(node_index_t idx,
                             const NodeServiceDiscovery& node_svc,
                             SocketType socket_type,
                             ConnectionType connection_type,
                             const Settings& settings) const;
};

}}}} // namespace facebook::logdevice::configuration::nodes
