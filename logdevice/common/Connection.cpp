/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include "folly/ScopeGuard.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/network/SocketAdapter.h"
#include "logdevice/common/network/SocketConnectCallback.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(server_name, type, conntype, flow_group, std::move(deps)) {}

Connection::Connection(NodeID server_name,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket(server_name, type, conntype, flow_group, std::move(deps)),
      sock_(std::move(sock_adapter)),
      proto_handler_(
          std::make_shared<ProtocolHandler>(this, getDeps()->getEvBase())) {}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket(fd,
             client_name,
             client_addr,
             std::move(conn_token),
             type,
             conntype,
             flow_group,
             std::move(deps)) {}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket(fd,
             client_name,
             client_addr,
             std::move(conn_token),
             type,
             conntype,
             flow_group,
             std::move(deps)),
      sock_(std::move(sock_adapter)),
      proto_handler_(
          std::make_shared<ProtocolHandler>(this, getDeps()->getEvBase())) {}

Connection::~Connection() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}

int Connection::connect() {
  if (!sock_) {
    return Socket::connect();
  }

  int rv = preConnectAttempt();
  if (rv != 0) {
    return rv;
  }

  auto fut = asyncConnect();

  fd_ = sock_->getNetworkSocket().toFd();
  conn_closed_ = std::make_shared<std::atomic<bool>>(false);
  next_pos_ = 0;
  drain_pos_ = 0;

  // enqueue hello message into the socket.
  Socket::sendHello();

  auto complete_connection = [this](Status st) {
    err = st;
    if (err == E::ISCONN) {
      Socket::transitionToConnected();
      // TODO(gauresh) : Set the read callback here.
    }
  };

  if (!fut.isReady()) {
    std::move(fut).thenValue(
        [connect_completion = std::move(complete_connection)](Status st) {
          connect_completion(st);
        });
  } else {
    complete_connection(std::move(fut.value()));
  }

  STAT_INCR(deps_->getStats(), num_connections);

  RATELIMIT_DEBUG(std::chrono::seconds(1),
                  10,
                  "Connected %s socket via %s channel to %s, immediate_connect "
                  "%d, immediate_fail %d",
                  getSockType() == SocketType::DATA ? "DATA" : "GOSSIP",
                  getConnType() == ConnectionType::SSL ? "SSL" : "PLAIN",
                  peerSockaddr().toString().c_str(),
                  connected_,
                  !sock_->good());
  return 0;
}

static folly::AsyncSocket::OptionMap
getDefaultSocketOptions(const folly::SocketAddress& sock_addr,
                        const Settings& settings) {
  folly::AsyncSocket::OptionMap options;
  sa_family_t sa_family = sock_addr.getFamily();
  bool is_tcp = !(sa_family == AF_UNIX);

  using OptionKey = folly::AsyncSocket::OptionKey;

  // Set send buffer size
  int sndbuf_size = settings.tcp_sendbuf_kb * 1024;
  options.emplace(OptionKey{SOL_SOCKET, SO_SNDBUF}, sndbuf_size);

  // Set receive buffer size.
  int rcvbuf_size = settings.tcp_rcvbuf_kb * 1024;
  options.emplace(OptionKey{SOL_SOCKET, SO_RCVBUF}, rcvbuf_size);

  if (is_tcp) {
    if (!settings.nagle) {
      options.emplace(OptionKey{IPPROTO_TCP, TCP_NODELAY}, 1);
    }
  }

  bool keep_alive = settings.use_tcp_keep_alive;
  if (is_tcp && keep_alive) {
    int keep_alive_time = settings.tcp_keep_alive_time;
    int keep_alive_intvl = settings.tcp_keep_alive_intvl;
    int keep_alive_probes = settings.tcp_keep_alive_probes;
    options.emplace(OptionKey{SOL_SOCKET, SO_KEEPALIVE}, keep_alive);
    if (keep_alive_time > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPIDLE}, keep_alive_time);
    }
    if (keep_alive_intvl > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPINTVL}, keep_alive_intvl);
    }
    if (keep_alive_probes > 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_KEEPCNT}, keep_alive_probes);
    }
  }

#ifdef __linux__
  if (is_tcp) {
    int tcp_user_timeout = settings.tcp_user_timeout;
    if (tcp_user_timeout >= 0) {
      options.emplace(OptionKey{SOL_TCP, TCP_USER_TIMEOUT}, tcp_user_timeout);
    }
  }
#endif

  const uint8_t default_dscp = settings.server ? settings.server_dscp_default
                                               : settings.client_dscp_default;
  const int diff_svcs = default_dscp << 2;
  switch (sa_family) {
    case AF_INET: {
      options.emplace(OptionKey{IPPROTO_IP, IP_TOS}, diff_svcs);
      break;
    }
    case AF_INET6: {
      options.emplace(OptionKey{IPPROTO_IPV6, IPV6_TCLASS}, diff_svcs);
      break;
    }
    default:
      break;
  }
  return options;
}

folly::Future<Status> Connection::asyncConnect() {
  std::chrono::milliseconds timeout = getSettings().connect_timeout;
  size_t max_retries = getSettings().connection_retries;
  auto connect_timeout_retry_multiplier =
      getSettings().connect_timeout_retry_multiplier;
  folly::AsyncSocket::OptionMap options(getDefaultSocketOptions(
      peer_sockaddr_.getSocketAddress(), getSettings()));

  for (size_t retry_count = 1; retry_count < max_retries; ++retry_count) {
    timeout += std::chrono::duration_cast<std::chrono::milliseconds>(
        getSettings().connect_timeout *
        pow(connect_timeout_retry_multiplier, retry_count));
  }

  auto connect_cb = std::make_unique<SocketConnectCallback>();

  /* TODO(gauresh) : Go to worker in future. using unsafe future for now.
  auto executor = worker_ != nullptr ? worker_->getExecutor()
                                     : &folly::InlineExecutor::instance();
                                     */
  auto fut = connect_cb->getConnectStatus().toUnsafeFuture();

  sock_->connect(connect_cb.get(),
                 peer_sockaddr_.getSocketAddress(),
                 timeout.count(),
                 options);

  auto dispatch_status = [this](const folly::AsyncSocketException& ex) mutable {
    err = ProtocolHandler::translateToLogDeviceStatus(ex);
    if (err != E::ISCONN) {
      proto_handler_->notifyErrorOnSocket(ex);
    }
    return err;
  };
  if (fut.isReady()) {
    folly::AsyncSocketException ex(std::move(fut.value()));
    return folly::makeFuture<Status>(dispatch_status(ex));
  }

  return std::move(fut).thenValue(
      [connect_cb = std::move(connect_cb),
       dispatch_status = std::move(dispatch_status)](
          const folly::AsyncSocketException& ex) mutable {
        return dispatch_status(ex);
      });
}

void Connection::close(Status reason) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  if (isClosed()) {
    return;
  }
  Socket::close(reason);
  if (sock_ && sock_->good()) {
    sock_->closeNow();
  }
}

bool Connection::isClosed() const {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  return Socket::isClosed();
}

void Connection::setSocketAdapter(std::unique_ptr<SocketAdapter> adapter) {
  ld_check(!sock_);
  sock_ = std::move(adapter);
}

void Connection::onConnected() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnected();
}

int Connection::onReceived(ProtocolHeader ph, struct evbuffer* inbuf) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  return Socket::onReceived(ph, inbuf);
}

void Connection::onConnectTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onPeerClosed();
}

void Connection::onBytesPassedToTCP(size_t nbytes_drained) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket::onBytesPassedToTCP(nbytes_drained);
}
}} // namespace facebook::logdevice
