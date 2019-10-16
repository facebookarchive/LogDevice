/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/SocketAddress.h>
#include <folly/futures/Promise.h>
#include <folly/io/async/AsyncSocket.h>

#include "logdevice/include/Err.h"

namespace folly {
struct NetworkSocket;
class AsyncSocketException;
} // namespace folly

namespace facebook { namespace logdevice {
class SocketAdapter;

class SocketConnectCallback : public folly::AsyncSocket::ConnectCallback {
 public:
  SocketConnectCallback() = default;

  /**
   * connectSuccess() will be invoked when the connection has been
   * successfully established.
   */
  void connectSuccess() noexcept override;

  /**
   * connectErr() will be invoked if the connection attempt fails.
   *
   * @param ex        An exception describing the error that occurred.
   */
  void connectErr(const folly::AsyncSocketException& ex) noexcept override;

  /**
   * preConnect() will be invoked just before the actual connect happens,
   *              default is no-ops.
   *
   * @param fd      An underneath created socket, use for connection.
   *
   */
  void preConnect(folly::NetworkSocket /*fd*/) override {}

  folly::SemiFuture<folly::AsyncSocketException> getConnectStatus() {
    return connect_status_.getSemiFuture();
  }

 private:
  folly::Promise<folly::AsyncSocketException> connect_status_;
};

}} // namespace facebook::logdevice
