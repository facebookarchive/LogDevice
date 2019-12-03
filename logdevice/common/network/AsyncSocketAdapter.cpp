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
    const std::shared_ptr<folly::SSLContext>& ctx,
    folly::EventBase* evb)
    : transport_(new folly::AsyncSSLSocket(ctx, evb), Destructor()) {}

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
    : transport_(
          new fizz::server::AsyncFizzServer(
              folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb, fd)),
              fizzCtx),
          Destructor()),
      sslCtx_(sslCtx) {
  ld_check(sslCtx_);
  toServer()->accept(this);
}

AsyncSocketAdapter::~AsyncSocketAdapter() {}

void AsyncSocketAdapter::connect(
    ConnectCallback* callback,
    const folly::SocketAddress& address,
    int timeout,
    const folly::AsyncSocket::OptionMap& options,
    const folly::SocketAddress& bindAddr) noexcept {
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

void AsyncSocketAdapter::fizzHandshakeSuccess(
    fizz::server::AsyncFizzServer* /* fizz_server_ */) noexcept {
  folly::SocketAddress addr;
  getPeerAddress(&addr);
  ld_debug("fizzHandshakeSuccess for %s", addr.describe().c_str());
  sslCtx_.reset();
}

void AsyncSocketAdapter::fizzHandshakeError(
    fizz::server::AsyncFizzServer* /* fizz_server_ */,
    folly::exception_wrapper ex) noexcept {
  folly::SocketAddress addr;
  getPeerAddress(&addr);
  ld_warning("fizzHandshakeError for %s: %s",
             addr.describe().c_str(),
             ex.get_exception()->what());
  sslCtx_.reset();
  // let the read callback propagate the error
}

void AsyncSocketAdapter::fizzHandshakeAttemptFallback(
    std::unique_ptr<folly::IOBuf> clientHello) {
  ld_check(sslCtx_);

  auto* socket = toSocket();
  auto evb = socket->getEventBase();

  // we change transport so we need to reset the callback
  auto* read_cb = getReadCallback();
  setReadCB(nullptr); // don't propagate EOF, it's expected
  auto fd = socket->detachNetworkSocket().toFd(); // results in EOF

  auto* ssl_socket =
      new folly::AsyncSSLSocket(sslCtx_, evb, folly::NetworkSocket::fromFd(fd));

  transport_.reset(ssl_socket);
  setReadCB(read_cb);
  ssl_socket->setPreReceivedData(std::move(clientHello));
  ssl_socket->sslAccept(nullptr /*handshakecb*/);
}

fizz::server::AsyncFizzServer* AsyncSocketAdapter::toServer() const {
  ld_check(transport_);
  auto* server = dynamic_cast<fizz::server::AsyncFizzServer*>(transport_.get());
  ld_check(server);
  return server;
}

folly::AsyncSocket* AsyncSocketAdapter::toSocket() const {
  ld_check(transport_);
  auto* socket = dynamic_cast<folly::AsyncSocket*>(transport_.get());
  if (!socket) {
    socket = toServer()->getUnderlyingTransport<folly::AsyncSocket>();
  }

  ld_check(socket);
  return socket;
}

}} // namespace facebook::logdevice
