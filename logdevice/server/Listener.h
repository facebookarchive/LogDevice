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
#include <vector>

#include <boost/variant.hpp>
#include <folly/Executor.h>
#include <folly/SocketAddress.h>
#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>

#include "event2/event.h"
#include "event2/listener.h"
#include "logdevice/common/EventLoop.h"

namespace facebook { namespace logdevice {

/**
 * An abstract class that wraps evconnlisteners and handles new connections.
 */
class Listener {
 public:
  using KeepAlive = folly::Executor::KeepAlive<EventLoop>;
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
      return val_.which() == 0;
    }

    int port() const {
      ld_check(isPort());
      return boost::get<int>(val_);
    }

    const std::string& path() const {
      ld_check(!isPort());
      return boost::get<std::string>(val_);
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
    boost::variant<int, std::string> val_;
    bool ssl_;
  };

  explicit Listener(InterfaceDef iface, KeepAlive loop);

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
   * Triggered by libevent when there is a new incoming connection.
   */
  virtual void acceptCallback(evutil_socket_t sock,
                              const folly::SocketAddress& addr) = 0;

  /* Returns true if we're listening on an SSL port */
  bool isSSL() const {
    return iface_.isSSL();
  }

 private:
  bool setupEvConnListeners();

  void closeEvConnListeners();

  // Tcp port or path to unix domain socket we'll use to listen for connections.
  InterfaceDef iface_;

  // EventLoop on which listener is running
  KeepAlive loop_;

  // list of pointers to evconnlistener structs, used to ensure they're properly
  // released when this object is destroyed
  typedef std::unique_ptr<evconnlistener, std::function<void(evconnlistener*)>>
      EvconnListenerUniquePtr;
  std::vector<EvconnListenerUniquePtr> evconnlisteners_;

  // static wrapper around acceptCallback() that we can pass to libevent
  static void staticAcceptCallback(struct evconnlistener* listener,
                                   evutil_socket_t sock,
                                   struct sockaddr* addr,
                                   int len,
                                   void* arg);
};

}} // namespace facebook::logdevice
