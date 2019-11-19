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

#include <fizz/server/AsyncFizzServer.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/AsyncTransport.h>
#include <folly/io/async/DelayedDestruction.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/SSLFetcher.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/Listener.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/admincommands/CommandProcessor.h"

namespace facebook { namespace logdevice {

/**
 * @file Listens on a specified port for admin commands and executes them.
 */

class Server;
class CommandListener;

class AdminCommandConnection
    : public folly::DelayedDestruction,
      public folly::AsyncReader::ReadCallback,
      public fizz::server::AsyncFizzServer::HandshakeCallback {
 public:
  using UniquePtr = std::unique_ptr<AdminCommandConnection, Destructor>;

  AdminCommandConnection(size_t id,
                         folly::NetworkSocket fd,
                         CommandListener& listener,
                         const folly::SocketAddress& addr,
                         folly::EventBase* evb);
  void getReadBuffer(void** bufReturn, size_t* lenReturn) override;
  void readDataAvailable(size_t length) noexcept override;
  void readEOF() noexcept override;
  void readErr(const folly::AsyncSocketException& ex) noexcept override;
  bool detectTLS();

 private:
  class ReadEventHandler;
  friend ReadEventHandler;

  ~AdminCommandConnection() override;
  void closeConnectionAndDestroyObject();
  void fizzHandshakeSuccess(
      fizz::server::AsyncFizzServer* transport) noexcept override;
  void fizzHandshakeError(fizz::server::AsyncFizzServer* transport,
                          folly::exception_wrapper ex) noexcept override;
  void fizzHandshakeAttemptFallback(
      std::unique_ptr<folly::IOBuf> clientHello) override;

  size_t id_;
  const folly::SocketAddress addr_;
  folly::IOBufQueue read_buffer_{folly::IOBufQueue::cacheChainLength()};
  folly::io::QueueAppender cursor_;
  CommandListener& listener_;
  bool shutdown_{false};
  folly::EventBase* evb_;
  folly::NetworkSocket fd_;
  folly::AsyncSocket::UniquePtr socket_;
  std::unique_ptr<ReadEventHandler> read_event_handler_;
  fizz::server::AsyncFizzServer::UniquePtr fizz_server_;
};

class CommandListener : public Listener {
 public:
  friend AdminCommandConnection;
  explicit CommandListener(Listener::InterfaceDef iface,
                           KeepAlive loop,
                           Server* server);

  ~CommandListener() override;

 protected:
  void connectionAccepted(folly::NetworkSocket sock,
                          const folly::SocketAddress& addr) noexcept override;

  Server* server_;
  UpdateableSettings<ServerSettings> server_settings_;
  std::unique_ptr<AdminCommandFactory> command_factory_;

  // id assigned to the next connection
  size_t next_conn_id_ = 0;

  // a map of connections handled by this listener, indexed by their connection
  // ids.
  std::map<size_t, AdminCommandConnection::UniquePtr> conns_;

  // SSL context manager
  SSLFetcher ssl_fetcher_;

  CommandProcessor command_processor_;

  KeepAlive loop_;
};

}} // namespace facebook::logdevice
