/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include "folly/ScopeGuard.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/protocol/Message.h"

namespace facebook { namespace logdevice {

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
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}

void Connection::close(Status reason) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::close(reason);
}

void Connection::onConnected() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnected();
}
int Connection::onReceived(ProtocolHeader ph, struct evbuffer* inbuf) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  return Socket::onReceived(ph, inbuf);
}

void Connection::onConnectTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onPeerClosed();
}

void Connection::onBytesPassedToTCP(size_t nbytes_drained) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onBytesPassedToTCP(nbytes_drained);
}

}} // namespace facebook::logdevice
