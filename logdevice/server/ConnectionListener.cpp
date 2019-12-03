/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ConnectionListener.h"

#include <memory>
#include <netdb.h>
#include <pthread.h>
#include <string>

#include <sys/socket.h>

#include "event2/listener.h"
#include "event2/util.h"
#include "logdevice/common/ClientID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/FailureDetector.h"

namespace facebook { namespace logdevice {

ConnectionListener::ConnectionListener(
    Listener::InterfaceDef iface,
    KeepAlive loop,
    std::shared_ptr<SharedState> shared_state,
    ListenerType listener_type,
    ResourceBudget& connection_backlog_budget)
    : Listener(std::move(iface), loop),
      loop_(loop),
      connection_backlog_budget_(connection_backlog_budget),
      shared_state_(shared_state),
      listener_type_(listener_type) {
  ld_check(shared_state);
}

const SimpleEnumMap<ConnectionListener::ListenerType, std::string>&
ConnectionListener::listenerTypeNames() {
  // Note that thread names are limited to 16 characters. Use them wisely.
  static SimpleEnumMap<ConnectionListener::ListenerType, std::string>
      listener_names({
          {ConnectionListener::ListenerType::DATA, "ld:conn-listen"},
          {ConnectionListener::ListenerType::DATA_SSL, "ld:sconn-listen"},
          {ConnectionListener::ListenerType::GOSSIP, "ld:gossip"},
      });
  return listener_names;
}

ConnectionType
ConnectionListener::getConnectionType(folly::NetworkSocket sock) {
  TLSHeader buf{};
  ssize_t peeked_bytes =
      recv(sock.toFd(), &buf, buf.size(), MSG_PEEK | MSG_DONTWAIT);
  return peeked_bytes >= 3
      ? isTLSHeader(buf) ? ConnectionType::SSL : ConnectionType::PLAIN
      : ConnectionType::NONE;
}
void ConnectionListener::ReadEventHandler::handlerReady(
    uint16_t events) noexcept {
  cancelTimeout();
  if (events == ReadEventHandler::EventFlags::READ) {
    ConnectionType conn_type = getConnectionType(sock_);
    if (conn_type == ConnectionType::NONE) {
      ld_error("Error peeking header message from client socket");
      folly::netops::close(sock_);
      connection_listener_->read_event_handlers_.erase(sock_);
      return;
    }
    connection_request_->setConnectionType(conn_type);
    std::unique_ptr<Request> request = std::move(connection_request_);
    int rv;
    STAT_INCR(processor_.stats_, num_backlog_connections);
    // The processor will route gossip request because WorkerType is
    // WorkerType::FAILURE_DETECTOR in that case
    rv = processor_.postRequest(request);

    if (rv != 0) {
      STAT_DECR(processor_.stats_, num_backlog_connections);
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Error passing accepted connection to %s thread. "
          "postRequest() reported %s.",
          listenerTypeNames()[connection_listener_->listener_type_].c_str(),
          error_description(err));
      folly::netops::close(sock_);
      // ~NewConnectionRequest() will also destroy the token, thus releasing the
      // fd from connection_backlog_budget_.
    }
  } else {
    // Triggered by wrong event
    ld_error("ReaderEventHandler::handlerReady() triggered by wrong event: %d",
             events);
    folly::netops::close(sock_);
  }
  connection_listener_->read_event_handlers_.erase(sock_);
}

void ConnectionListener::ReadEventHandler::timeoutExpired() noexcept {
  unregisterHandler();
  ld_error("registerHandler() on file descriptor %d failed to read before "
           "timeout.",
           sock_.toFd());
  folly::netops::close(sock_);
  connection_listener_->read_event_handlers_.erase(sock_);
}

void ConnectionListener::connectionAccepted(
    folly::NetworkSocket fd,
    const folly::SocketAddress& clientAddr) noexcept {
  ld_check(processor_ != nullptr);
  ServerProcessor* processor = checked_downcast<ServerProcessor*>(processor_);
  Sockaddr sockaddr(clientAddr);

  // Check if accepting this connection pushed us over the limit.  Since there's
  // only one ConnectionListener thread, and this is called soon after accept(),
  // we're able to react promptly in case there's a burst of new connections.
  auto token = processor->conn_budget_incoming_.acquireToken();
  if (!token) {
    STAT_INCR(processor->stats_, dropped_connection_limit);
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Rejecting a connection from %s because the limit "
                      "has been reached.",
                      sockaddr.toString().c_str());
    folly::netops::close(fd);
    if (conn_limit_reached_cb_) {
      conn_limit_reached_cb_();
    }
    return;
  }
  auto conn_backlog_token = connection_backlog_budget_.acquireToken();

  if (!conn_backlog_token) {
    STAT_INCR(processor->stats_, dropped_connection_burst);
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Rejecting a connection from %s because the burst limit "
                      "has been reached.",
                      sockaddr.toString().c_str());
    folly::netops::close(fd);
    return;
  }
  // By default we want the processor to select a worker thread for us.
  worker_id_t wid{-1};
  SocketType sock_type;
  WorkerType target_worker_type = WorkerType::GENERAL;

  if (listener_type_ == ListenerType::GOSSIP) {
    ld_check(processor->failure_detector_);
    sock_type = SocketType::GOSSIP;
    target_worker_type = WorkerType::FAILURE_DETECTOR;
  } else {
    sock_type = SocketType::DATA;
  }
  //  Storing relevant info and creating a one time event triggered by a read or
  //  a timeout.
  read_event_handlers_.insert(
      {fd,
       std::make_unique<ReadEventHandler>(
           loop_->getEventBase(),
           fd,
           *processor,
           std::make_unique<NewConnectionRequest>(fd.toFd(),
                                                  wid,
                                                  sockaddr,
                                                  std::move(token),
                                                  std::move(conn_backlog_token),
                                                  sock_type,
                                                  ConnectionType::NONE,
                                                  target_worker_type),
           this)});
  auto& ret = read_event_handlers_.at(fd);
  bool read_reg = ret->registerHandler(ReadEventHandler::EventFlags::READ);
  if (!read_reg) {
    ld_error("registerHandler() failed. errno=%d (%s)", errno, strerror(errno));
    folly::netops::close(fd);
    read_event_handlers_.erase(fd);
    return;
  }
  read_reg = ret->scheduleTimeout(
      processor->updateableSettings()->handshake_timeout.count());
  if (!read_reg) {
    ld_error("scheduleTimeout() failed. errno=%d (%s)", errno, strerror(errno));
    // Remove added read event.
    ret->unregisterHandler();
    folly::netops::close(fd);
    read_event_handlers_.erase(fd);
  }
} // namespace logdevice

folly::SemiFuture<folly::Unit> ConnectionListener::stopAcceptingConnections() {
  auto res = Listener::stopAcceptingConnections().via(loop_).then(
      [this](folly::Try<folly::Unit> res) mutable {
        for (auto const& rli : read_event_handlers_) {
          folly::netops::close(rli.first);
        }
        read_event_handlers_.clear();
        return res;
      });
  return res;
}

void ConnectionListener::setConnectionLimitReachedCallback(
    ConnectionLimitReachedCallback cb) {
  ld_check(!conn_limit_reached_cb_);
  conn_limit_reached_cb_ = cb;
}

}} // namespace facebook::logdevice
