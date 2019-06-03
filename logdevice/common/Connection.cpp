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
    : Socket(server_name, type, conntype, flow_group),
      worker_(Worker::onThisThread(false /* enforce_worker */)) {}

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(server_name, type, conntype, flow_group, std::move(deps)),
      worker_(Worker::onThisThread(false /* enforce_worker */)) {}

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
             flow_group),
      worker_(Worker::onThisThread(false /* enforce_worker */)) {}

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
             std::move(deps)),
      worker_(Worker::onThisThread(false /* enforce_worker */)) {}

Connection::~Connection() {
  WorkerContextScopeGuard g(worker_);
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}

void Connection::close(Status reason) {
  WorkerContextScopeGuard g(worker_);
  Socket::close(reason);
}

void Connection::onConnected() {
  WorkerContextScopeGuard g(worker_);
  Socket::onConnected();
}
int Connection::onReceived(ProtocolHeader ph, struct evbuffer* inbuf) {
  WorkerContextScopeGuard g(worker_);
  return Socket::onReceived(ph, inbuf);
}

void Connection::onConnectTimeout() {
  WorkerContextScopeGuard g(worker_);
  Socket::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  WorkerContextScopeGuard g(worker_);
  Socket::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  WorkerContextScopeGuard g(worker_);
  Socket::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  WorkerContextScopeGuard g(worker_);
  Socket::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  WorkerContextScopeGuard g(worker_);
  Socket::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  WorkerContextScopeGuard g(worker_);
  Socket::onPeerClosed();
}

void Connection::onBytesPassedToTCP(size_t nbytes_drained) {
  WorkerContextScopeGuard g(worker_);
  Socket::onBytesPassedToTCP(nbytes_drained);
}

}} // namespace facebook::logdevice
