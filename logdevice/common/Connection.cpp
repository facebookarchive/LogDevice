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

void Connection::initializeContext() {
  ld_check(!context_);
  // Get the current context and make a copy. Restore it back again.
  context_ = folly::RequestContext::saveContext();
  folly::RequestContext::setContext(context_);
}

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group)
    : Socket(server_name, type, conntype, flow_group) {
  initializeContext();
}

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(server_name, type, conntype, flow_group, std::move(deps)) {
  initializeContext();
}

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
             flow_group) {
  initializeContext();
}

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
             std::move(deps)) {
  initializeContext();
}

Connection::~Connection() {
  folly::RequestContextScopeGuard g(context_);
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}

void Connection::close(Status reason) {
  folly::RequestContextScopeGuard g(context_);
  Socket::close(reason);
}

void Connection::onConnected() {
  folly::RequestContextScopeGuard g(context_);
  Socket::onConnected();
}
int Connection::onReceived(ProtocolHeader ph, struct evbuffer* inbuf) {
  folly::RequestContextScopeGuard g(context_);
  int rv = Socket::onReceived(ph, inbuf);
  return rv;
}

void Connection::onConnectTimeout() {
  folly::RequestContextScopeGuard g(context_);
  Socket::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  folly::RequestContextScopeGuard g(context_);
  Socket::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  folly::RequestContextScopeGuard g(context_);
  Socket::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  folly::RequestContextScopeGuard g(context_);
  Socket::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  folly::RequestContextScopeGuard g(context_);
  Socket::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  folly::RequestContextScopeGuard g(context_);
  Socket::onPeerClosed();
}

}} // namespace facebook::logdevice
