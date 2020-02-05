/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include "folly/ScopeGuard.h"
#include "logdevice/common/ProtocolHandler.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/network/MessageReader.h"
#include "logdevice/common/network/SocketAdapter.h"
#include "logdevice/common/network/SocketConnectCallback.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket_DEPRECATED(server_name,
                        socket_type,
                        connection_type,
                        peer_type,
                        flow_group,
                        std::move(deps)) {
  ld_check(legacy_connection_);
}

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket_DEPRECATED(server_name,
                        socket_type,
                        connection_type,
                        peer_type,
                        flow_group,
                        std::move(deps),
                        nullptr) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, getDeps()->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket_DEPRECATED(fd,
                        client_name,
                        client_addr,
                        std::move(conn_token),
                        type,
                        conntype,
                        flow_group,
                        std::move(deps)) {
  ld_check(legacy_connection_);
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket_DEPRECATED(fd,
                        client_name,
                        client_addr,
                        std::move(conn_token),
                        type,
                        conntype,
                        flow_group,
                        std::move(deps),
                        nullptr) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, getDeps()->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
  // Set the read callback.
  read_cb_.reset(new MessageReader(*proto_handler_, proto_));
  proto_handler_->sock()->setReadCB(read_cb_.get());
}

Connection::~Connection() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}
}} // namespace facebook::logdevice
