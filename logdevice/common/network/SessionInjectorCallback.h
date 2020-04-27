/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocket.h>

namespace folly {
struct NetworkSocket;
class AsyncSocketException;
} // namespace folly

namespace facebook { namespace logdevice {
class SocketAdapter;
class SSLSessionCache;

/**
 * SessionInjectorCallback is a wrapper that implements socket
 * connection callbacks and is used to support TLS session resumption.
 * This class does mainly two things :
 *   1. Hooks into the connectSuccess callback and caches the used SSL session.
 *   2. Hooks into the preConnect callback to inject a previously cached SSL
 *     session.
 */
class SessionInjectorCallback : public folly::AsyncSocket::ConnectCallback {
 public:
  SessionInjectorCallback(
      std::unique_ptr<folly::AsyncSocket::ConnectCallback> cb,
      SSLSessionCache* session_cache,
      SocketAdapter* socket);

  void connectSuccess() noexcept override;
  void connectErr(const folly::AsyncSocketException& ex) noexcept override;
  void preConnect(folly::NetworkSocket /*fd*/) override;

 private:
  std::unique_ptr<folly::AsyncSocket::ConnectCallback> callback_;
  SSLSessionCache* ssl_session_cache_;
  SocketAdapter* socket_;
  bool found_session_;
};

}} // namespace facebook::logdevice
