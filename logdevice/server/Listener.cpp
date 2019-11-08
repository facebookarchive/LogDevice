/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/Listener.h"

#include <memory>
#include <string>
#include <variant>

#include <folly/ScopeGuard.h>
#include <folly/io/async/AsyncSocketException.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

bool Listener::isSSL() const {
  return iface_.isSSL();
}

Listener::Listener(const InterfaceDef& iface, KeepAlive loop)
    : iface_(iface), loop_(loop) {
  ld_info("Created Listener: %s", iface.describe().c_str());
}

Listener::~Listener() {
  stopAcceptingConnections().wait();
}

folly::SemiFuture<bool> Listener::startAcceptingConnections() {
  ld_info("Start called");
  folly::Promise<bool> promise;
  auto res = promise.getSemiFuture();
  loop_->add([this, promise = std::move(promise)]() mutable {
    promise.setValue(setupAsyncSocket());
  });
  return res;
}

folly::SemiFuture<folly::Unit> Listener::stopAcceptingConnections() {
  folly::Promise<folly::Unit> promise;
  auto res = promise.getSemiFuture();
  loop_->add([this, promise = std::move(promise)]() mutable {
    if (socket_) {
      socket_->stopAccepting();
    }
    socket_.reset();
    promise.setValue();
  });
  return res;
}

bool Listener::setupAsyncSocket() {
  ld_check(!socket_);
  ld_info("Setup called");

  auto base = loop_.get();

  try {
    auto socket = folly::AsyncServerSocket::newSocket(base);
    iface_.bind(socket.get());
    socket->addAcceptCallback(this, nullptr);
    socket->listen(128);
    socket->startAccepting();
    socket_ = std::move(socket);
  } catch (const folly::AsyncSocketException& e) {
    ld_check(!socket_);
    ld_error("cannot start listener: %s", e.what());
    return false;
  }

  return true;
}

void Listener::connectionAccepted(
    folly::NetworkSocket fd,
    const folly::SocketAddress& clientAddr) noexcept {
  acceptCallback(fd.toFd(), clientAddr);
}

void Listener::acceptError(const std::exception& ex) noexcept {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  5,
                  "accept failed with an exception: %s",
                  ex.what());
}

bool Listener::isTLSHeader(const TLSHeader& buf) {
  return buf[0] == SecureConnectionTag::SSL_HANDSHAKE_RECORD_TAG &&
      buf[1] == SecureConnectionTag::TLS_TAG &&
      buf[2] >= SecureConnectionTag::TLS_MIN_PROTOCOL &&
      buf[2] <= SecureConnectionTag::TLS_MAX_PROTOCOL;
}

}} // namespace facebook::logdevice
