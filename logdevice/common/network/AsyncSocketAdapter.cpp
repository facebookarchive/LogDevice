/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/network/AsyncSocketAdapter.h"

#include <utility>

#include <folly/SocketAddress.h>
#include <folly/io/SocketOptionMap.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/SSLContext.h>
#include <folly/net/NetworkSocket.h>

#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

using Destructor = folly::DelayedDestruction::Destructor;

AsyncSocketAdapter::AsyncSocketAdapter()
    : transport_(new folly::AsyncSocket(), Destructor()) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb)
    : transport_(new folly::AsyncSocket(evb), Destructor()) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       const folly::SocketAddress& address,
                                       uint32_t connectTimeout)
    : transport_(new folly::AsyncSocket(evb, address, connectTimeout),
                 Destructor()) {}

AsyncSocketAdapter::AsyncSocketAdapter(
    const std::shared_ptr<const fizz::client::FizzClientContext>& fizzClientCtx,
    const std::shared_ptr<const fizz::CertificateVerifier>& fizzCertVerifier,
    const std::shared_ptr<folly::SSLContext>& sslCtx,
    folly::EventBase* evb)
    : transport_(fizzClientCtx ? new folly::AsyncSocket(evb)
                               : new folly::AsyncSSLSocket(sslCtx, evb),
                 Destructor()),
      fizzClientCtx_(fizzClientCtx),
      fizzCertVerifier_(fizzCertVerifier) {
  ld_check(fizzClientCtx_ || sslCtx);
}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       const std::string& ip,
                                       uint16_t port,
                                       uint32_t connectTimeout)
    : transport_(new folly::AsyncSocket(evb, ip, port, connectTimeout),
                 Destructor()) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       folly::NetworkSocket fd,
                                       uint32_t zeroCopyBufId)
    : transport_(new folly::AsyncSocket(evb, fd, zeroCopyBufId), Destructor()) {
}

AsyncSocketAdapter::AsyncSocketAdapter(
    const std::shared_ptr<const fizz::server::FizzServerContext>& fizzCtx,
    const std::shared_ptr<folly::SSLContext>& sslCtx,
    folly::EventBase* evb,
    folly::NetworkSocket fd)
    : sslCtx_(sslCtx) {
  ld_check(sslCtx_);

  auto* fizz_server = new fizz::server::AsyncFizzServer(
      folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb, fd)), fizzCtx);
  transport_.reset(fizz_server);
  fizz_server->accept(this);
}

AsyncSocketAdapter::~AsyncSocketAdapter() {}

void AsyncSocketAdapter::connect(
    ConnectCallback* callback,
    const folly::SocketAddress& address,
    int timeout,
    const folly::SocketOptionMap& options,
    const folly::SocketAddress& bindAddr) noexcept {
  ld_check(!clientHandshake_);

  if (fizzClientCtx_) {
    clientHandshake_.reset(
        new FizzClientHandshake(*this,
                                callback,
                                std::move(fizzClientCtx_),
                                std::move(fizzCertVerifier_)));
    callback = clientHandshake_.get();
  }
  toSocket()->connect(callback, address, timeout, options, bindAddr);
}

void AsyncSocketAdapter::closeNow() {
  transport_->closeNow();
}

void AsyncSocketAdapter::close() {
  transport_->close();
}

bool AsyncSocketAdapter::good() const {
  return transport_->good();
}

bool AsyncSocketAdapter::readable() const {
  return transport_->readable();
}

bool AsyncSocketAdapter::connecting() const {
  return transport_->connecting();
}

void AsyncSocketAdapter::getLocalAddress(folly::SocketAddress* address) const {
  transport_->getLocalAddress(address);
}

void AsyncSocketAdapter::getPeerAddress(folly::SocketAddress* address) const {
  transport_->getPeerAddress(address);
}

folly::NetworkSocket AsyncSocketAdapter::getNetworkSocket() const {
  return toSocket()->getNetworkSocket();
}

const folly::AsyncTransportCertificate*
AsyncSocketAdapter::getPeerCertificate() const {
  return transport_->getPeerCertificate();
}

size_t AsyncSocketAdapter::getRawBytesWritten() const {
  return transport_->getRawBytesWritten();
}

size_t AsyncSocketAdapter::getRawBytesReceived() const {
  return transport_->getRawBytesReceived();
}

void AsyncSocketAdapter::setReadCB(SocketAdapter::ReadCallback* callback) {
  return transport_->setReadCB(callback);
}

SocketAdapter::ReadCallback* AsyncSocketAdapter::getReadCallback() const {
  return transport_->getReadCallback();
}

void AsyncSocketAdapter::writeChain(WriteCallback* callback,
                                    std::unique_ptr<folly::IOBuf>&& buf,
                                    folly::WriteFlags flags) {
  transport_->writeChain(callback, std::move(buf), flags);
}

int AsyncSocketAdapter::setSendBufSize(size_t bufsize) {
  return toSocket()->setSendBufSize(bufsize);
}

int AsyncSocketAdapter::setRecvBufSize(size_t bufsize) {
  return toSocket()->setRecvBufSize(bufsize);
}

int AsyncSocketAdapter::setSockOptVirtual(int level,
                                          int optname,
                                          void const* optval,
                                          socklen_t optlen) {
  return toSocket()->setSockOptVirtual(level, optname, optval, optlen);
}

int AsyncSocketAdapter::getSockOptVirtual(int level,
                                          int optname,
                                          void* optval,
                                          socklen_t* optlen) {
  return toSocket()->getSockOptVirtual(level, optname, optval, optlen);
}

namespace {

std::string getPeerAddressNoExcept(AsyncSocketAdapter& sock) {
  try {
    folly::SocketAddress addr;
    sock.getPeerAddress(&addr);
    return addr.describe();
  } catch (const std::system_error& ex) {
    RATELIMIT_INFO(
        std::chrono::seconds(10), 1, "getPeerAddress failure: %s", ex.what());
    return "UNKNOWN";
  }
}

} // namespace

void AsyncSocketAdapter::fizzHandshakeSuccess(
    fizz::server::AsyncFizzServer* /* fizz_server */) noexcept {
  RATELIMIT_DEBUG(std::chrono::seconds(10),
                  1,
                  "FizzHandshakeSuccess for %s",
                  getPeerAddressNoExcept(*this).c_str());
  sslCtx_.reset();
}

void AsyncSocketAdapter::fizzHandshakeError(
    fizz::server::AsyncFizzServer* /* fizz_server */,
    folly::exception_wrapper ex) noexcept {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  1,
                  "FizzHandshakeError for %s: %s",
                  getPeerAddressNoExcept(*this).c_str(),
                  ex.what().c_str());
  sslCtx_.reset();
  // let the read callback propagate the error
}

void AsyncSocketAdapter::fizzHandshakeAttemptFallback(
    std::unique_ptr<folly::IOBuf> clientHello) {
  ld_check(sslCtx_);

  // we change transport so we need to reset the callback
  auto* read_cb = getReadCallback();
  setReadCB(nullptr); // don't propagate EOF, it's expected
  auto fd = toSocket()->detachNetworkSocket().toFd(); // results in EOF

  auto* ssl_socket = new folly::AsyncSSLSocket(
      sslCtx_, transport_->getEventBase(), folly::NetworkSocket::fromFd(fd));

  // release memory
  sslCtx_.reset();

  transport_.reset(ssl_socket);
  setReadCB(read_cb);
  ssl_socket->setPreReceivedData(std::move(clientHello));
  ssl_socket->sslAccept(nullptr /*handshakecb*/);
}

folly::AsyncSocket* AsyncSocketAdapter::toSocket() const {
  ld_check(transport_);

  if (auto* server =
          dynamic_cast<fizz::server::AsyncFizzServer*>(transport_.get())) {
    return server->getUnderlyingTransport<folly::AsyncSocket>();
  }

  if (auto* client =
          dynamic_cast<fizz::client::AsyncFizzClient*>(transport_.get())) {
    return client->getUnderlyingTransport<folly::AsyncSocket>();
  }

  auto* socket = dynamic_cast<folly::AsyncSocket*>(transport_.get());
  ld_check(socket);
  return socket;
}

void AsyncSocketAdapter::FizzClientHandshake::connectSuccess() noexcept {
  DelayedDestruction::DestructorGuard guard(this);
  ld_check(fizzClientCtx_);
  auto* client = new fizz::client::AsyncFizzClient(
      std::move(owner_.transport_), fizzClientCtx_);
  owner_.transport_.reset(client);

  // Start Fizz handshake
  client->connect(this, fizzCertVerifier_, folly::none, folly::none);
}

void AsyncSocketAdapter::FizzClientHandshake::connectErr(
    const folly::AsyncSocketException& ex) noexcept {
  DelayedDestruction::DestructorGuard guard(this);
  if (user_cb_) {
    user_cb_->connectErr(ex);
  }
  deleteThis();
}

void AsyncSocketAdapter::FizzClientHandshake::preConnect(
    folly::NetworkSocket fd) {
  DelayedDestruction::DestructorGuard guard(this);
  if (user_cb_) {
    user_cb_->preConnect(fd);
  }
}

void AsyncSocketAdapter::FizzClientHandshake::fizzHandshakeSuccess(
    fizz::client::AsyncFizzClient*) noexcept {
  DelayedDestruction::DestructorGuard guard(this);
  if (user_cb_) {
    user_cb_->connectSuccess();
  }
  deleteThis();
}

void AsyncSocketAdapter::FizzClientHandshake::fizzHandshakeError(
    fizz::client::AsyncFizzClient*,
    folly::exception_wrapper exw) noexcept {
  DelayedDestruction::DestructorGuard guard(this);
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  1,
                  "FizzClientHandshake error for %s: %s",
                  getPeerAddressNoExcept(owner_).c_str(),
                  exw.what().c_str());
  if (user_cb_) {
    exw.handle(
        [&](const folly::AsyncSocketException& ex) {
          user_cb_->connectErr(ex);
        },
        [&](...) {
          user_cb_->connectErr(folly::AsyncSocketException(
              folly::AsyncSocketException::UNKNOWN, exw.what().c_str()));
        });
  }
  deleteThis();
}

void AsyncSocketAdapter::FizzClientHandshake::deleteThis() noexcept {
  ld_check(this == owner_.clientHandshake_.get());
  owner_.clientHandshake_.reset();
}

}} // namespace facebook::logdevice
