/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "logdevice/common/network/SocketAdapter.h"

namespace facebook { namespace logdevice {

class MockSocketAdapter : public SocketAdapter {
 public:
  MOCK_METHOD5(connect_,
               void(folly::AsyncSocket::ConnectCallback*,
                    const folly::SocketAddress&,
                    int,
                    const folly::AsyncSocket::OptionMap&,
                    const folly::SocketAddress&));
  void connect(folly::AsyncSocket::ConnectCallback* callback,
               const folly::SocketAddress& address,
               int timeout,
               const folly::AsyncSocket::OptionMap& options,
               const folly::SocketAddress& bindAddr) noexcept override {
    connect_(callback, address, timeout, options, bindAddr);
  }

  MOCK_METHOD0(close, void());
  MOCK_METHOD0(closeNow, void());
  MOCK_CONST_METHOD0(good, bool());
  MOCK_CONST_METHOD0(readable, bool());
  MOCK_CONST_METHOD0(writable, bool());
  MOCK_CONST_METHOD0(connecting, bool());
  MOCK_CONST_METHOD1(getLocalAddress, void(folly::SocketAddress*));
  MOCK_CONST_METHOD1(getPeerAddress, void(folly::SocketAddress*));
  MOCK_CONST_METHOD0(getNetworkSocket, folly::NetworkSocket());
  MOCK_CONST_METHOD0(getPeerCertificate,
                     const folly::AsyncTransportCertificate*());
  MOCK_CONST_METHOD0(getRawBytesWritten, size_t());
  MOCK_CONST_METHOD0(getRawBytesReceived, size_t());
  MOCK_METHOD1(setReadCB, void(folly::AsyncSocket::ReadCallback*));
  MOCK_CONST_METHOD0(getReadCallback, folly::AsyncSocket::ReadCallback*());
  MOCK_METHOD3(writeChain_,
               void(folly::AsyncSocket::WriteCallback*,
                    folly::IOBuf*,
                    folly::WriteFlags));
  void writeChain(WriteCallback* callback,
                  std::unique_ptr<folly::IOBuf>&& buf,
                  folly::WriteFlags flags) override {
    writeChain_(callback, buf.release(), flags);
  }
  MOCK_METHOD1(setSendBufSize, int(size_t));
  MOCK_METHOD1(setRecvBufSize, int(size_t));
  MOCK_METHOD4(getSockOptVirtual, int(int, int, void*, socklen_t*));
  MOCK_METHOD4(setSockOptVirtual, int(int, int, void const*, socklen_t));
};

}} // namespace facebook::logdevice
