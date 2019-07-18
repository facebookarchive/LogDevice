/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

#include "event2/bufferevent.h"
#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/Listener.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/admincommands/AdminCommandFactory.h"

namespace facebook { namespace logdevice {

/**
 * @file Listens on a specified port for admin commands and executes them.
 */

class Server;

class CommandListener : public Listener {
 public:
  explicit CommandListener(Listener::InterfaceDef iface,
                           KeepAlive loop,
                           Server* server);

 protected:
  /**
   * Called by libevent when there is a new connection.
   */
  void acceptCallback(evutil_socket_t sock,
                      const folly::SocketAddress& addr) override;

 private:
  typedef std::vector<std::string> cmd_args_t;
  typedef size_t conn_id_t;

  static constexpr int COMMAND_RCVBUF = 8 * 1024;
  static constexpr int COMMAND_SNDBUF = 1 * 1024 * 1024;

  enum ConnectionType { UNKNOWN, PLAIN, ENCRYPTED };
  class ConnectionState {
   public:
    ConnectionState(CommandListener* parent,
                    conn_id_t id,
                    struct bufferevent* bev,
                    Sockaddr address)
        : parent_(parent),
          id_(id),
          bev_(bev),
          close_when_drained_(false),
          address_(address) {
      ld_check(bev_ != nullptr);
    }
    ~ConnectionState();

    // owning CommandListener object
    CommandListener* parent_;

    // id of this connection
    conn_id_t id_;

    // bufferevent associated with the connection
    struct bufferevent* bev_;

    // flag indicating that the connection should be closed after draining the
    // output evbuffer
    bool close_when_drained_;

    // address of the remote end of this connection
    Sockaddr address_;

    // last command we received from this connection
    std::string last_command_;

    // indicate whether the connection is over SSL or not
    ConnectionType type_{UNKNOWN};
  };

  void init();

  // parse the command and call corresponding handler
  void processCommand(struct bufferevent* bev,
                      const char* command,
                      const bool isLocalhost,
                      ConnectionState* state);

  // called by libevent when there's some data to be read
  static void readCallback(struct bufferevent* bev, void* arg);

  // called by libevent when some data has been written to the underlying
  // socket
  static void writeCallback(struct bufferevent* bev, void* arg);

  // called by libevent when the connection is established, closed, etc.
  static void eventCallback(struct bufferevent* bev, short events, void* arg);

  // create an openssl bufferevent to handle that connection
  bool upgradeToSSL(ConnectionState* state);

  Server* server_;
  UpdateableSettings<ServerSettings> server_settings_;
  std::unique_ptr<AdminCommandFactory> command_factory_;

  // id assigned to the next connection
  conn_id_t next_conn_id_ = 0;

  // a map of connections handled by this listener, indexed by their connection
  // ids.
  std::map<conn_id_t, std::unique_ptr<ConnectionState>> conns_;

  // SSL context manager
  SSLFetcher ssl_fetcher_;

  KeepAlive loop_;
};

}} // namespace facebook::logdevice
