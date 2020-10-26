/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/thrift/compat/ThriftSender.h"

#include <folly/Random.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/NetworkDependencies.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/if/gen-cpp2/LogDeviceAPIAsyncClient.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/thrift/ThriftClientFactory.h"
#include "logdevice/common/thrift/ThriftRouter.h"

using facebook::logdevice::thrift::LogDeviceAPIAsyncClient;

namespace facebook { namespace logdevice {

ThriftSender::ThriftSender(SocketType socket_type,
                           folly::Optional<NodeLocation> my_location,
                           ThriftRouter& thrift_router,
                           std::unique_ptr<NetworkDependencies> deps)
    : socket_type_(socket_type),
      my_location_(std::move(my_location)),
      thrift_router_(thrift_router),
      deps_(std::move(deps)) {
  ld_check(deps_);
}

// TODO(T71759283): Either support it or deprecate completely
bool ThriftSender::canSendToImpl(const Address& address,
                                 TrafficClass /* unused */,
                                 BWAvailableCallback& /* unused */) {
  // TODO(mmhg): Implement this
  ld_check(false);
  return false;
}

int ThriftSender::sendMessageImpl(std::unique_ptr<Message>&& message,
                                  const Address& address,
                                  BWAvailableCallback* /* unused */,
                                  SocketCallback* on_close_cb) {
  // TODO(mmhg): Implement this
  ld_check(false);
  return 0;
}

int ThriftSender::connect(node_index_t node) {
  // err set by getOrCreateSession
  return getOrCreateSession(node) ? 0 : -1;
}

int ThriftSender::checkServerConnection(node_index_t node) const {
  const auto* session = findServerSession(node);
  if (!session) {
    err = E::NOTFOUND;
    return -1;
  }

  if (session->getState() == ThriftSession::State::NEW) {
    err = E::NOTCONN;
    return -1;
  }

  if (!session->getInfo().isSSL() && requiresSSL(node)) {
    err = E::SSLREQUIRED;
    return -1;
  }

  return 0;
}

bool ThriftSender::requiresSSL(node_index_t node) const {
  if (socket_type_ == SocketType::GOSSIP) {
    return deps_->getSettings().ssl_on_gossip_port;
  }
  // Determine whether we need to use SSL by comparing our location with the
  // location of the target node.
  NodeLocationScope ssl_boundary = deps_->getSettings().ssl_boundary;

  bool cross_boundary = configuration::nodes::getNodeSSL(
      *deps_->getNodesConfiguration(), my_location_, node, ssl_boundary);

  // Determine whether we need to use an SSL Connection for authentication.
  bool authentication = (deps_->getServerConfig()->getAuthenticationType() ==
                         AuthenticationType::SSL);

  return cross_boundary || authentication;
}

int ThriftSender::checkClientConnection(ClientID,
                                        bool check_peer_is_node) const {
  // TODO(mmhg): Implement this
  ld_check(false);
  return 0;
}

int ThriftSender::closeConnection(const Address& address, Status reason) {
  auto* session = findSession(address);
  if (!session) {
    err = E::NOTFOUND;
    return -1;
  }

  session->close(reason);
  if (address.isClientAddress()) {
    client_sessions_.erase(address.id_.client_);
  } else {
    server_sessions_.erase(address.asNodeID().index());
  }

  return 0;
}

bool ThriftSender::isClosed(const Address& address) const {
  const auto* session = findSession(address);
  return !session || session->isClosed();
}

int ThriftSender::registerOnConnectionClosed(const Address& address,
                                             SocketCallback& cb) {
  // TODO(mmhg): Implement this
  ld_check(false);
  return 0;
}

void ThriftSender::beginShutdown() {
  // TODO(mmhg): Implement this
  ld_check(false);
}

void ThriftSender::forceShutdown() {
  // TODO(mmhg): Implement this
  ld_check(false);
}

bool ThriftSender::isShutdownCompleted() const {
  // TODO(mmhg): Implement this
  ld_check(false);
  return false;
}

void ThriftSender::forEachConnection(
    const std::function<void(const ConnectionInfo&)>& cb) const {
  forAllSessions([cb](ThriftSession& session) { cb(session.getInfo()); });
}

const ConnectionInfo* FOLLY_NULLABLE
ThriftSender::getConnectionInfo(const Address& address) const {
  const auto* session = findSession(address);
  return session ? &session->getInfo() : nullptr;
}

bool ThriftSender::setConnectionInfo(const Address& address,
                                     ConnectionInfo&& new_info) {
  auto* session = findSession(address);
  if (session) {
    session->setInfo(std::move(new_info));
  }
  return session != nullptr;
}

std::pair<Sender::ExtractPeerIdentityResult, PrincipalIdentity>
ThriftSender::extractPeerIdentity(const Address&) {
  // TODO(T70882358): Implement this
  ld_check(false);
  return {};
}

void ThriftSender::fillDebugInfo(InfoSocketsTable& table) const {
  forAllSessions(
      [&table](ThriftSession& session) { session.fillDebugInfo(table); });
}

void ThriftSender::setPeerShuttingDown(node_index_t) {
  // TODO(mmhg): Implement this
  ld_check(false);
}

void ThriftSender::noteConfigurationChanged(
    std::shared_ptr<const configuration::nodes::NodesConfiguration> new_nodes) {
  // TODO(T70882102): Close connections to nodes changed their IPs
}

void ThriftSender::onSettingsUpdated(
    std::shared_ptr<const Settings> new_settings) {
  // TODO(mmhg): Consider removing
}

ThriftSession* FOLLY_NULLABLE
ThriftSender::findSession(const Address& address) const {
  if (address.isClientAddress()) {
    return findClientSession(address.id_.client_);
  } else {
    return findServerSession(address.asNodeID().index());
  }
}

ServerSession* FOLLY_NULLABLE
ThriftSender::findServerSession(node_index_t node) const {
  ld_check(node >= 0);

  auto it = server_sessions_.find(node);
  if (it == server_sessions_.end()) {
    return nullptr;
  }

  auto session = it->second.get();
  ld_check(session);
  ld_check(session->peerNodeID().index() == node);
  return session;
}

ClientSession* FOLLY_NULLABLE
ThriftSender::findClientSession(const ClientID& client_id) const {
  auto it = client_sessions_.find(client_id);

  if (it == client_sessions_.end()) {
    return nullptr;
  }

  auto session = it->second.get();
  ld_check(session);
  ld_check(session->peerClientID() == client_id);
  return session;
}

ThriftSession* FOLLY_NULLABLE
ThriftSender::getOrCreateSession(node_index_t node) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return nullptr;
  }

  auto* session = findServerSession(node);
  // for all connections :
  //     create new connection if the existing connection is closed.
  // for DATA connection:
  //     create new connection if the existing connection is not SSL but
  //     should be.
  // for GOSSIP connection:
  //     create new connection if the existing connection is not SSL but
  //     ssl_on_gossip_port is true or the existing connection is SSL but
  //     the ssl_on_gossip_port is false.
  bool exists = session && session->getState() != ThriftSession::State::CLOSED;
  bool ssl_mismatch = false;
  if (exists) {
    bool is_ssl = session->getInfo().isSSL();
    ssl_mismatch = is_ssl != requiresSSL(node);
    if (socket_type_ != SocketType::GOSSIP && is_ssl) {
      // for DATA connections, it's fine if we're using SSL even if we have not
      // been asked for this.
      ssl_mismatch = false;
    }
  }

  // No need to reconnect
  if (exists && !ssl_mismatch) {
    return session;
  }

  // Create a new session object
  auto new_session = createServerSession(node);
  if (!new_session) {
    // err set by createServerSession
    return nullptr;
  }

  // Start connection procedure
  int rv = new_session->connect();
  if (rv != 0) {
    // err set by connect
    return nullptr;
  }

  // Close existing session if any
  if (exists) {
    ld_check(ssl_mismatch);
    session->close(E::SSLREQUIRED);
  }

  ServerSession* result = new_session.get();
  server_sessions_.insert_or_assign(node, std::move(new_session));
  return result;
}

ClientSession* FOLLY_NULLABLE
ThriftSender::getClientSession(const ClientID& client_id) const {
  auto* session = findClientSession(client_id);
  if (!session) {
    err = E::UNREACHABLE;
    return nullptr;
  }
  return session;
}

std::unique_ptr<ServerSession>
ThriftSender::createServerSession(node_index_t node) {
  if (shutting_down_) {
    err = E::SHUTDOWN;
    return nullptr;
  }
  Sockaddr sockaddr;
  auto client = thrift_router_.getApiClient(node, &sockaddr);
  if (!client) {
    err = E::NOTINCONFIG;
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Cannot find Thrift address to connect to node N%d",
                    node);
    return nullptr;
  }
  ConnectionType conn_type =
      requiresSSL(node) ? ConnectionType::SSL : ConnectionType::PLAIN;
  ConnectionInfo info{Address(node), sockaddr, socket_type_, conn_type};
  return std::make_unique<ServerSession>(
      std::move(info), *deps_, std::move(client));
}

void ThriftSender::forAllSessions(
    const std::function<void(ThriftSession&)>& cb) const {
  forClientSessions(cb);
  forServerSessions(cb);
}

void ThriftSender::forClientSessions(
    const std::function<void(ClientSession&)>& cb) const {
  for (auto& [_, session] : client_sessions_) {
    cb(*session);
  }
}

void ThriftSender::forServerSessions(
    const std::function<void(ServerSession&)>& cb) const {
  for (auto& [_, session] : server_sessions_) {
    cb(*session);
  }
}
}} // namespace facebook::logdevice
