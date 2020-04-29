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
#include "logdevice/common/settings/Settings.h"

namespace facebook { namespace logdevice {

// Setting the env forces all sockets to be SSL-enabled. Aiming to load the
// env just once.
static bool forceSSLSockets() {
  static std::atomic<int> force_ssl{-1};
  int val = force_ssl.load();
  if (val == -1) {
    const char* env = getenv("LOGDEVICE_TEST_FORCE_SSL");
    // Return false for null, "" and "0", true otherwise.
    val = env != nullptr && strlen(env) > 0 && strcmp(env, "0") != 0;

    force_ssl.store(val);
  }
  return val;
}

AsyncSocketConnectionFactory::AsyncSocketConnectionFactory(
    folly::EventBase* base)
    : base_(base) {}

std::unique_ptr<Connection> AsyncSocketConnectionFactory::createConnection(
    NodeID node_id,
    SocketType socket_type,
    ConnectionType connection_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) {
  std::unique_ptr<AsyncSocketAdapter> sock_adapter;
  if (connection_type != ConnectionType::SSL &&
      (forceSSLSockets() && socket_type != SocketType::GOSSIP)) {
    connection_type = ConnectionType::SSL;
  }
  if (connection_type == ConnectionType::SSL) {
    auto ssl_ctx = deps->getSSLContext();
    ld_check(ssl_ctx);
    sock_adapter = std::make_unique<AsyncSocketAdapter>(ssl_ctx, base_);
  } else {
    sock_adapter = std::make_unique<AsyncSocketAdapter>(base_);
  }
  const auto throttle_setting = deps->getSettings().connect_throttle;
  auto connection = std::make_unique<Connection>(node_id,
                                                 socket_type,
                                                 connection_type,
                                                 flow_group,
                                                 std::move(deps),
                                                 std::move(sock_adapter));
  auto it = connect_throttle_map_.find(node_id);
  if (it == connect_throttle_map_.end()) {
    auto res = connect_throttle_map_.emplace(
        node_id, std::make_unique<ConnectThrottle>(throttle_setting));
    ld_check(res.second);
    it = res.first;
    ld_check(it->second);
  }
  connection->setConnectThrottle(it->second.get());
  return connection;
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
  if (connection_type != ConnectionType::SSL &&
      (forceSSLSockets() && type != SocketType::GOSSIP)) {
    connection_type = ConnectionType::SSL;
  }
  std::unique_ptr<AsyncSocketAdapter> sock_adapter;
  if (connection_type == ConnectionType::SSL) {
    auto ssl_ctx = deps->getSSLContext();
    ld_check(ssl_ctx);
    ld_check(deps->getSettings().server);
    sock_adapter = std::make_unique<AsyncSocketAdapter>(
        ssl_ctx, base_, folly::NetworkSocket(fd));
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
