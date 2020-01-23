/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/ConnectionFactory.h"

#include "logdevice/common/ClientID.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/SocketTypes.h"

namespace facebook { namespace logdevice {

std::unique_ptr<Connection>
ConnectionFactory::createConnection(NodeID node_id,
                                    SocketType socket_type,
                                    ConnectionType connection_type,
                                    PeerType peer_type,
                                    FlowGroup& flow_group,
                                    std::unique_ptr<SocketDependencies> deps) {
  return std::make_unique<Connection>(node_id,
                                      socket_type,
                                      connection_type,
                                      peer_type,
                                      flow_group,
                                      std::move(deps));
}

std::unique_ptr<Connection> ConnectionFactory::createConnection(
    int fd,
    ClientID client_name,
    const Sockaddr& client_address,
    ResourceBudget::Token connection_token,
    SocketType type,
    ConnectionType connection_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) const {
  return std::make_unique<Connection>(fd,
                                      client_name,
                                      client_address,
                                      std::move(connection_token),
                                      type,
                                      connection_type,
                                      flow_group,
                                      std::move(deps));
}
}} // namespace facebook::logdevice
