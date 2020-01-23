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
    SocketType socket_type,
    ConnectionType connection_type,
    PeerType peer_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) {
  std::unique_ptr<AsyncSocketAdapter> sock_adapter;
  if (connection_type == ConnectionType::SSL) {
    const auto& sets = deps->getSettings();
    bool use_fizz = sets.server ? sets.server_connect_with_fizz
                                : sets.client_connect_with_fizz;
    SocketDependencies::SSLCtxPtr ssl_ctx;
    SocketDependencies::FizzClientCtxPair fizz_ctx;
    if (use_fizz) {
      fizz_ctx = deps->getFizzClientContext();
      ld_check(fizz_ctx.first);
    } else {
      ssl_ctx = deps->getSSLContext(false /* accepting */);
      ld_check(ssl_ctx);
    }
    sock_adapter = std::make_unique<AsyncSocketAdapter>(
        fizz_ctx.first, fizz_ctx.second, ssl_ctx, base_);
  } else {
    sock_adapter = std::make_unique<AsyncSocketAdapter>(base_);
  }
  return std::make_unique<Connection>(node_id,
                                      socket_type,
                                      connection_type,
                                      peer_type,
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
    auto fizz_ctx = deps->getFizzServerContext();
    ld_check(fizz_ctx);
    auto ssl_ctx = deps->getSSLContext(true /* accepting */);
    ld_check(ssl_ctx);
    ld_check(deps->getSettings().server);
    sock_adapter = std::make_unique<AsyncSocketAdapter>(
        fizz_ctx, ssl_ctx, base_, folly::NetworkSocket(fd));
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
