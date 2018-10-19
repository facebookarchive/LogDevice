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
#include <vector>

#include <boost/variant.hpp>

#include "event2/event.h"
#include "event2/listener.h"
#include "logdevice/common/EventLoop.h"

namespace facebook { namespace logdevice {

/**
 * An abstract class that wraps evconnlisteners and handles new connections.
 */
class Listener : public EventLoop {
 public:
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

  explicit Listener(InterfaceDef iface, std::string thread_name);

  ~Listener() override;

  /**
   * Starts listening on a specified port and registers the listener event with
   * this Listener's thread's event base.
   */
  int startAcceptingConnections();

  /**
   * For tests: immediately close any new connections, to simulate
   * network problems.
   */
  void acceptNewConnections(bool accept) {
    accept_.store(accept);
  }

 protected:
  /**
   * Triggered by libevent when there is a new incoming connection.
   */
  virtual void acceptCallback(evutil_socket_t sock,
                              struct sockaddr* addr,
                              int len) = 0;

  /* Returns true if we're listening on an SSL port */
  bool isSSL() {
    return iface_.isSSL();
  }

 private:
  // file descriptors of all sockets this Listener should accept connections on
  // (this is typically just variants of localhost like ipv4, ipv6 etc); when
  // evconnlistener is created for a socket (by startAcceptingConnections), its
  // fd is removed from this list
  std::list<int> socket_fds_;

  // Tcp port or path to unix domain socket we'll use to listen for connections.
  InterfaceDef iface_;

  // list of pointers to evconnlistener structs, used to ensure they're properly
  // released when this object is destroyed
  typedef std::unique_ptr<evconnlistener, std::function<void(evconnlistener*)>>
      EvconnListenerUniquePtr;
  std::vector<EvconnListenerUniquePtr> evconnlisteners_;

  // sets up listener sockets
  int setup_sockets();

  // sets up listener sockets on tcp port.
  int setupTcpSockets();

  // sets up listener socket on path.
  int setupUnixSocket();

  // For tests: immediately close any new connections, to simulate
  // network problems.
  std::atomic_bool accept_{true};

  // static wrapper around acceptCallback() that we can pass to libevent
  static void staticAcceptCallback(struct evconnlistener* listener,
                                   evutil_socket_t sock,
                                   struct sockaddr* addr,
                                   int len,
                                   void* arg);
};

}} // namespace facebook::logdevice
