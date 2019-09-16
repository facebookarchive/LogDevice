/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <variant>
#include <vector>

#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * An abstract class that wraps evconnlisteners and handles new connections.
 */
class Listener : public folly::AsyncServerSocket::AcceptCallback {
 public:
  using KeepAlive = folly::Executor::KeepAlive<folly::EventBase>;
  /**
   * Defines the interface used by this Listener. Can be a TCP port or the path
   * of a unix domain socket.
   */
  class InterfaceDef {
   public:
    explicit InterfaceDef(int port, bool ssl) : val_(port), ssl_(ssl) {}
    explicit InterfaceDef(std::string path, bool ssl)
        : val_(std::move(path)), ssl_(ssl) {}

    bool isPort() const {
      return val_.index() == 0;
    }

    int port() const {
      ld_check(isPort());
      return std::get<int>(val_);
    }

    const std::string& path() const {
      ld_check(!isPort());
      return std::get<std::string>(val_);
    }

    void bind(folly::AsyncServerSocket* socket) const {
      if (isPort()) {
        socket->bind(port());
      } else {
        unlink(path().c_str());
        socket->bind(folly::SocketAddress::makeFromPath(path()));
      }
    }

    bool isSSL() const {
      return ssl_;
    }

    // Convenient function for logging.
    std::string describe() const {
      std::string description =
          isPort() ? "port " + std::to_string(port()) : "socket " + path();

      if (ssl_) {
        description += " (SSL)";
      }
      return description;
    }

   private:
    std::variant<int, std::string> val_;
    bool ssl_;
  };

  explicit Listener(const InterfaceDef& iface, KeepAlive loop);

  virtual ~Listener();

  /**
   * Starts listening on a specified port and registers the listener event with
   * this Listener's thread's event base.
   */
  folly::SemiFuture<bool> startAcceptingConnections();

  /**
   * Stops listening and frees all events from EventLoop
   */
  folly::SemiFuture<folly::Unit> stopAcceptingConnections();

 protected:
  /**
   * [DEPRECATED] Triggered by libevent when there is a new incoming connection.
   * Please use connectionAccepted instead.
   */
  virtual void acceptCallback(evutil_socket_t /* sock */,
                              const folly::SocketAddress& /* addr */) {}

  void acceptError(const std::exception& ex) noexcept override;

  void
  connectionAccepted(folly::NetworkSocket fd,
                     const folly::SocketAddress& clientAddr) noexcept override;

  bool isSSL() const;

 private:
  bool setupAsyncSocket();

 private:
  // Tcp port or path to unix domain socket we'll use to listen for connections.
  const InterfaceDef iface_;

  // EventLoop on which listener is running
  KeepAlive loop_;

  // Must be accessed only from evenloop thread
  std::shared_ptr<folly::AsyncServerSocket> socket_;
};

}} // namespace facebook::logdevice
