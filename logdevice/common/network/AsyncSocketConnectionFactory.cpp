/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/AsyncSocketConnectionFactory.h"

#include <folly/io/async/EventBase.h>

#include "logdevice/common/Connection.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/network/AsyncSocketAdapter.h"

namespace facebook { namespace logdevice {
std::unique_ptr<Connection> AsyncSocketConnectionFactory::createConnection(
    NodeID node_id,
    SocketType type,
    ConnectionType connection_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) {
  std::unique_ptr<AsyncSocketAdapter> sock_adapter;
  if (connection_type == ConnectionType::SSL) {
    sock_adapter = std::make_unique<AsyncSocketAdapter>(
        deps->getSSLContext(false /* accepting */), base_);
  } else {
    sock_adapter = std::make_unique<AsyncSocketAdapter>(base_);
  }
  return std::make_unique<Connection>(node_id,
                                      type,
                                      connection_type,
                                      flow_group,
                                      std::move(deps),
                                      std::move(sock_adapter));
}

std::unique_ptr<Connection> AsyncSocketConnectionFactory::createConnection(
    int fd,
    ClientID client_name,
    const Sockaddr& client_address,
    ResourceBudget::Token connection_token,
    SocketType type,
    ConnectionType connection_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) const {
  std::unique_ptr<AsyncSocketAdapter> sock_adapter;
  if (connection_type == ConnectionType::SSL) {
    sock_adapter = std::make_unique<AsyncSocketAdapter>(
        deps->getSSLContext(true /* accepting */),
        base_,
        folly::NetworkSocket(fd));
  } else {
    sock_adapter =
        std::make_unique<AsyncSocketAdapter>(base_, folly::NetworkSocket(fd));
  }
  return std::make_unique<Connection>(fd,
                                      client_name,
                                      client_address,
                                      std::move(connection_token),
                                      type,
                                      connection_type,
                                      flow_group,
                                      std::move(deps),
                                      std::move(sock_adapter));
}
}} // namespace facebook::logdevice
