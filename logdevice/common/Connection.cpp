/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include "logdevice/common/Socket.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {
Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group)
    : Socket(server_name, type, conntype, flow_group) {}

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(server_name, type, conntype, flow_group, std::move(deps)) {}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group)
    : Socket(fd,
             client_name,
             client_addr,
             std::move(conn_token),
             type,
             conntype,
             flow_group) {}
Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(fd,
             client_name,
             client_addr,
             std::move(conn_token),
             type,
             conntype,
             flow_group,
             std::move(deps)) {}

Connection::~Connection() {
  close(E::SHUTDOWN);
}

void Connection::close(Status reason) {
  Socket::close(reason);
}

void Connection::onConnected() {
  Socket::onConnected();
}
int Connection::onReceived(ProtocolHeader ph, struct evbuffer* inbuf) {
  int rv = Socket::onReceived(ph, inbuf);
  return rv;
}

void Connection::onConnectTimeout() {
  Socket::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  Socket::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  Socket::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  Socket::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  Socket::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  Socket::onPeerClosed();
}

}} // namespace facebook::logdevice
