/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/thrift/compat/ThriftSession.h"

namespace facebook { namespace logdevice {

class BWAvailableCallback;
class NetworkDependencies;
struct Settings;
class SocketCallback;
class ThriftClientFactory;
class ThriftRouter;

namespace thrift {
class LogDeviceAPIAsyncClient;
}

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

/**
 * Implementation of Sender interface using Thrift instead of raw sockets under
 * the hood.
 */
class ThriftSender : public Sender {
 public:
  ThriftSender(SocketType socket_type,
               folly::Optional<NodeLocation> my_location,
               ThriftRouter& thrift_router,
               std::unique_ptr<NetworkDependencies> deps);

  int connect(node_index_t) override;

  int checkServerConnection(node_index_t) const override;

  int checkClientConnection(ClientID, bool check_peer_is_node) const override;

  int closeConnection(const Address&, Status reason) override;

  bool isClosed(const Address&) const override;

  int registerOnConnectionClosed(const Address&, SocketCallback& cb) override;

  void beginShutdown() override;

  void forceShutdown() override;

  bool isShutdownCompleted() const override;

  void forEachConnection(
      const std::function<void(const ConnectionInfo&)>& cb) const override;

  const ConnectionInfo* FOLLY_NULLABLE
  getConnectionInfo(const Address&) const override;

  bool setConnectionInfo(const Address&, ConnectionInfo&&) override;

  std::pair<Sender::ExtractPeerIdentityResult, PrincipalIdentity>
  extractPeerIdentity(const Address&) override;

  void fillDebugInfo(InfoSocketsTable&) const override;

  void setPeerShuttingDown(node_index_t) override;

  void noteConfigurationChanged(
      std::shared_ptr<const configuration::nodes::NodesConfiguration>) override;

  void onSettingsUpdated(std::shared_ptr<const Settings>) override;

 protected:
  bool canSendToImpl(const Address&,
                     TrafficClass,
                     BWAvailableCallback&) override;

  int sendMessageImpl(std::unique_ptr<Message>&&,
                      const Address&,
                      BWAvailableCallback* /* unused */,
                      SocketCallback* on_close_cb) override;

 private:
  // Type of sockets served by this Sender
  const SocketType socket_type_;
  // The location of this node (or client). Used to determine whether to use
  // SSL when connecting.
  const folly::Optional<NodeLocation> my_location_;
  ThriftRouter& thrift_router_;
  std::unique_ptr<NetworkDependencies> deps_;
  // Returns new globally unique session id for Thrift session.
  // The following methods try to find existing session by address, returns
  // session pointer if found or nullptr otherwise.
  ThriftSession* FOLLY_NULLABLE findSession(const Address&) const;
  ClientSession* FOLLY_NULLABLE findClientSession(const ClientID&) const;
  ServerSession* FOLLY_NULLABLE findServerSession(node_index_t) const;

  /**
   * Tries to get existing session by address and create it
   * if absent. Returns either pointer to alive sesson or nullptr and set
   * and error code to global variable.
   *
   * @return 0 on sucesss, -1 otherwise with err set to
   *    SHUTDOWN        Sender has been shut down
   *    NOTINCONFIG     Node is not present in the current cluster config
   *    INTERNAL        Something bad and unexpected happened, check logs for
   *                    details
   */
  ThriftSession* FOLLY_NULLABLE getOrCreateSession(node_index_t);

  // Returns client session if exists or sets global error and returns nullptr
  ClientSession* FOLLY_NULLABLE getClientSession(const ClientID&) const;

  // Creates a new server session to node with given idx. Returns either session
  // if succeeds or nullptr with global error set if fails.
  std::unique_ptr<ServerSession> createServerSession(node_index_t);

  // Funcntions for iteration over all active sessions
  void forAllSessions(const std::function<void(ThriftSession&)>& cb) const;
  void forClientSessions(const std::function<void(ClientSession&)>& cb) const;
  void forServerSessions(const std::function<void(ServerSession&)>& cb) const;

  // Checks whether SSL connection is required for connection to this node
  // TODO(mmhg): Deprecate this and move SSL enforcement to Thrift
  bool requiresSSL(node_index_t) const;

  // if true, disallow sending messages and initiating connections
  bool shutting_down_ = false;

  folly::F14NodeMap<node_index_t, std::unique_ptr<ServerSession>>
      server_sessions_;

  folly::F14NodeMap<ClientID, std::unique_ptr<ClientSession>, ClientID::Hash>
      client_sessions_;

  friend class ThriftSenderTest;
};

}} // namespace facebook::logdevice
