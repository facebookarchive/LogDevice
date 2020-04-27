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
    const std::shared_ptr<folly::SSLContext>& sslCtx,
    folly::EventBase* evb)
    : transport_(new folly::AsyncSSLSocket(sslCtx, evb), Destructor()) {
  ld_check(sslCtx);
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
    const std::shared_ptr<folly::SSLContext>& ctx,
    folly::EventBase* evb,
    folly::NetworkSocket fd)
    : transport_(
          folly::AsyncSocket::UniquePtr(new folly::AsyncSSLSocket(ctx, evb, fd),
                                        Destructor())) {
  auto transport = dynamic_cast<folly::AsyncSSLSocket*>(transport_.get());
  transport->sslAccept(nullptr /*handshakecb*/);
}

AsyncSocketAdapter::~AsyncSocketAdapter() {}

void AsyncSocketAdapter::connect(
    ConnectCallback* callback,
    const folly::SocketAddress& address,
    int timeout,
    const folly::SocketOptionMap& options,
    const folly::SocketAddress& bindAddr) noexcept {
  transport_->connect(callback, address, timeout, options, bindAddr);
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
  return transport_->getNetworkSocket();
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
  return transport_->setSendBufSize(bufsize);
}

int AsyncSocketAdapter::setRecvBufSize(size_t bufsize) {
  return transport_->setRecvBufSize(bufsize);
}

int AsyncSocketAdapter::setSockOptVirtual(int level,
                                          int optname,
                                          void const* optval,
                                          socklen_t optlen) {
  return transport_->setSockOptVirtual(level, optname, optval, optlen);
}

int AsyncSocketAdapter::getSockOptVirtual(int level,
                                          int optname,
                                          void* optval,
                                          socklen_t* optlen) {
  return transport_->getSockOptVirtual(level, optname, optval, optlen);
}

bool AsyncSocketAdapter::getSSLSessionReused() const {
  auto transport = dynamic_cast<folly::AsyncSSLSocket*>(transport_.get());
  ld_check(transport);
  return transport->getSSLSession();
}

folly::ssl::SSLSessionUniquePtr AsyncSocketAdapter::getSSLSession() {
  auto transport = dynamic_cast<folly::AsyncSSLSocket*>(transport_.get());
  ld_check(transport);
  return folly::ssl::SSLSessionUniquePtr(transport->getSSLSession());
}

void AsyncSocketAdapter::setSSLSession(
    folly::ssl::SSLSessionUniquePtr session) {
  auto transport = dynamic_cast<folly::AsyncSSLSocket*>(transport_.get());
  ld_check(transport);
  transport->setSSLSession(session.release(), /* takeOwnership= */ true);
}

}} // namespace facebook::logdevice
