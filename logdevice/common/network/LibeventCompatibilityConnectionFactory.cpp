/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/network/LibeventCompatibilityConnectionFactory.h"

#include "logdevice/common/ConnectThrottle.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/network/AsyncSocketConnectionFactory.h"
#include "logdevice/common/network/ConnectionFactory.h"

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

LibeventCompatibilityConnectionFactory::LibeventCompatibilityConnectionFactory(
    EvBase& base) {
  if (base.getType() == EvBase::LEGACY_EVENTBASE) {
    concrete_factory_ = std::make_unique<ConnectionFactory>();
  } else if (base.getType() == EvBase::FOLLY_EVENTBASE) {
    concrete_factory_ =
        std::make_unique<AsyncSocketConnectionFactory>(base.getEventBase());
  } else {
    ld_error("EvBase sent to factory of unrecognized type.");
    throw ConstructorFailed();
  }
}

std::unique_ptr<Connection>
LibeventCompatibilityConnectionFactory::createConnection(
    NodeID node_id,
    SocketType socket_type,
    ConnectionType connection_type,
    PeerType peer_type,
    FlowGroup& flow_group,
    std::unique_ptr<SocketDependencies> deps) {
  const auto throttle_setting = deps->getSettings().connect_throttle;
  if (connection_type != ConnectionType::SSL &&
      (forceSSLSockets() && socket_type != SocketType::GOSSIP)) {
    connection_type = ConnectionType::SSL;
  }
  auto connection = concrete_factory_->createConnection(node_id,
                                                        socket_type,
                                                        connection_type,
                                                        peer_type,
                                                        flow_group,
                                                        std::move(deps));
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

std::unique_ptr<Connection>
LibeventCompatibilityConnectionFactory::createConnection(
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
  return concrete_factory_->createConnection(fd,
                                             client_name,
                                             client_address,
                                             std::move(connection_token),
                                             type,
                                             connection_type,
                                             flow_group,
                                             std::move(deps));
}
}} // namespace facebook::logdevice
