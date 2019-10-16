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

namespace facebook { namespace logdevice {

using Destructor = folly::DelayedDestruction::Destructor;

AsyncSocketAdapter::AsyncSocketAdapter()
    : sock_(folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(),
                                          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb)
    : sock_(folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(evb),
                                          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       const folly::SocketAddress& address,
                                       uint32_t connectTimeout)
    : sock_(folly::AsyncSocket::UniquePtr(
          new folly::AsyncSocket(evb, address, connectTimeout),
          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(
    const std::shared_ptr<folly::SSLContext>& ctx,
    folly::EventBase* evb,
    bool deferSecurityNegotiation)
    : sock_(folly::AsyncSocket::UniquePtr(
          new folly::AsyncSSLSocket(ctx, evb, deferSecurityNegotiation),
          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       const std::string& ip,
                                       uint16_t port,
                                       uint32_t connectTimeout)
    : sock_(folly::AsyncSocket::UniquePtr(
          new folly::AsyncSocket(evb, ip, port, connectTimeout),
          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(folly::EventBase* evb,
                                       folly::NetworkSocket fd,
                                       uint32_t zeroCopyBufId)
    : sock_(folly::AsyncSocket::UniquePtr(
          new folly::AsyncSocket(evb, fd, zeroCopyBufId),
          Destructor())) {}

AsyncSocketAdapter::AsyncSocketAdapter(
    const std::shared_ptr<folly::SSLContext>& ctx,
    folly::EventBase* evb,
    folly::NetworkSocket fd,
    const std::string& serverName,
    bool deferSecurityNegotiation)
    : sock_(folly::AsyncSocket::UniquePtr(
          new folly::AsyncSSLSocket(ctx,
                                    evb,
                                    fd,
                                    serverName,
                                    deferSecurityNegotiation),
          Destructor())) {}

AsyncSocketAdapter::~AsyncSocketAdapter() {}

void AsyncSocketAdapter::connect(
    ConnectCallback* callback,
    const folly::SocketAddress& address,
    int timeout,
    const folly::AsyncSocket::OptionMap& options,
    const folly::SocketAddress& bindAddr) noexcept {
  sock_->connect(callback, address, timeout, options, bindAddr);
}

void AsyncSocketAdapter::closeNow() {
  sock_->closeNow();
}

bool AsyncSocketAdapter::good() const {
  return sock_->good();
}

bool AsyncSocketAdapter::readable() const {
  return sock_->readable();
}

bool AsyncSocketAdapter::connecting() const {
  return sock_->connecting();
}

void AsyncSocketAdapter::getLocalAddress(folly::SocketAddress* address) const {
  sock_->getLocalAddress(address);
}

void AsyncSocketAdapter::getPeerAddress(folly::SocketAddress* address) const {
  sock_->getPeerAddress(address);
}

folly::NetworkSocket AsyncSocketAdapter::getNetworkSocket() const {
  return sock_->getNetworkSocket();
}

const folly::AsyncTransportCertificate*
AsyncSocketAdapter::getPeerCertificate() const {
  return sock_->getPeerCertificate();
}

size_t AsyncSocketAdapter::getRawBytesWritten() const {
  return sock_->getRawBytesWritten();
}

size_t AsyncSocketAdapter::getRawBytesReceived() const {
  return sock_->getRawBytesReceived();
}

void AsyncSocketAdapter::setReadCB(SocketAdapter::ReadCallback* callback) {
  sock_->setReadCB(callback);
}

SocketAdapter::ReadCallback* AsyncSocketAdapter::getReadCallback() const {
  return sock_->getReadCallback();
}

void AsyncSocketAdapter::writeChain(WriteCallback* callback,
                                    std::unique_ptr<folly::IOBuf>&& buf,
                                    folly::WriteFlags flags) {
  sock_->writeChain(
      callback, std::forward<std::unique_ptr<folly::IOBuf>>(buf), flags);
}

int AsyncSocketAdapter::setSendBufSize(size_t bufsize) {
  return sock_->setSendBufSize(bufsize);
}

int AsyncSocketAdapter::setRecvBufSize(size_t bufsize) {
  return sock_->setRecvBufSize(bufsize);
}

int AsyncSocketAdapter::getSockOptVirtual(int level,
                                          int optname,
                                          void* optval,
                                          socklen_t* optlen) {
  return sock_->getSockOptVirtual(level, optname, optval, optlen);
}

int AsyncSocketAdapter::setSockOptVirtual(int level,
                                          int optname,
                                          void const* optval,
                                          socklen_t optlen) {
  return sock_->setSockOptVirtual(level, optname, optval, optlen);
}
}} // namespace facebook::logdevice
