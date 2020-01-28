/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Connection.h"

#include "folly/ScopeGuard.h"
#include "logdevice/common/ProtocolHandler.h"
#include "logdevice/common/SocketDependencies.h"
#include "logdevice/common/network/MessageReader.h"
#include "logdevice/common/network/SocketAdapter.h"
#include "logdevice/common/network/SocketConnectCallback.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket_DEPRECATED(server_name,
                        socket_type,
                        connection_type,
                        peer_type,
                        flow_group,
                        std::move(deps)) {
  ld_check(legacy_connection_);
}

Connection::Connection(NodeID server_name,
                       SocketType socket_type,
                       ConnectionType connection_type,
                       PeerType peer_type,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket_DEPRECATED(server_name,
                        socket_type,
                        connection_type,
                        peer_type,
                        flow_group,
                        std::move(deps),
                        nullptr) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, getDeps()->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps)
    : Socket_DEPRECATED(fd,
                        client_name,
                        client_addr,
                        std::move(conn_token),
                        type,
                        conntype,
                        flow_group,
                        std::move(deps)) {
  ld_check(legacy_connection_);
}

Connection::Connection(int fd,
                       ClientID client_name,
                       const Sockaddr& client_addr,
                       ResourceBudget::Token conn_token,
                       SocketType type,
                       ConnectionType conntype,
                       FlowGroup& flow_group,
                       std::unique_ptr<SocketDependencies> deps,
                       std::unique_ptr<SocketAdapter> sock_adapter)
    : Socket_DEPRECATED(fd,
                        client_name,
                        client_addr,
                        std::move(conn_token),
                        type,
                        conntype,
                        flow_group,
                        std::move(deps),
                        nullptr) {
  ld_check(!legacy_connection_);
  proto_handler_ = std::make_shared<ProtocolHandler>(
      this, std::move(sock_adapter), conn_description_, getDeps()->getEvBase());
  sock_write_cb_ = SocketWriteCallback(proto_handler_.get());
  proto_handler_->getSentEvent()->attachCallback([this] { drainSendQueue(); });
  // Set the read callback.
  read_cb_.reset(new MessageReader(*proto_handler_, proto_));
  proto_handler_->sock()->setReadCB(read_cb_.get());
}

Connection::~Connection() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  // Close the socket here as close accesses Worker::onThisThread(). Do not want
  // to setContext indefinitely as well and call the Socket destructor, this was
  // the best we could do.
  close(E::SHUTDOWN);
}

int Connection::connect() {
  if (legacy_connection_) {
    return Socket_DEPRECATED::connect();
  }

  int rv = preConnectAttempt();
  if (rv != 0) {
    return rv;
  }

  auto fut = asyncConnect();

  fd_ = proto_handler_->sock()->getNetworkSocket().toFd();
  conn_closed_ = std::make_shared<std::atomic<bool>>(false);
  next_pos_ = 0;
  drain_pos_ = 0;

  if (good()) {
    // enqueue hello message into the socket.
    Socket_DEPRECATED::sendHello();
  }

  auto complete_connection = [this](Status st) {
    auto g = folly::makeGuard(getDeps()->setupContextGuard());
    if (st == E::ISCONN) {
      Socket_DEPRECATED::transitionToConnected();
      read_cb_.reset(new MessageReader(*proto_handler_, proto_));
      proto_handler_->sock()->setReadCB(read_cb_.get());
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
  if (isSSL()) {
    STAT_INCR(deps_->getStats(), num_ssl_connections);
  }

  RATELIMIT_DEBUG(std::chrono::seconds(1),
                  10,
                  "Connected %s socket via %s channel to %s, immediate_connect "
                  "%d, immediate_fail %d",
                  getSockType() == SocketType::DATA ? "DATA" : "GOSSIP",
                  getConnType() == ConnectionType::SSL ? "SSL" : "PLAIN",
                  peerSockaddr().toString().c_str(),
                  connected_,
                  !proto_handler_->good());
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

  proto_handler_->sock()->connect(connect_cb.get(),
                                  peer_sockaddr_.getSocketAddress(),
                                  timeout.count(),
                                  options);

  auto dispatch_status = [this](const folly::AsyncSocketException& ex) mutable {
    err = ProtocolHandler::translateToLogDeviceStatus(ex);
    if (err != E::ISCONN) {
      proto_handler_->notifyErrorOnSocket(ex);
    }
    if (err == E::TIMEDOUT) {
      STAT_INCR(deps_->getStats(), connection_timeouts);
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

Socket_DEPRECATED::SendStatus
Connection::sendBuffer(std::unique_ptr<folly::IOBuf>&& buffer_chain) {
  if (!legacy_connection_) {
    if (proto_handler_->good()) {
      if (sendChain_) {
        ld_check(sched_write_chain_.isScheduled());
        sendChain_->prependChain(std::move(buffer_chain));
      } else {
        sendChain_ = std::move(buffer_chain);
        ld_check(!sched_write_chain_.isScheduled());
        sched_write_chain_.attachCallback([this]() { scheduleWriteChain(); });
        sched_write_chain_.scheduleTimeout(0);
        sched_start_time_ = SteadyTimestamp::now();
      }
    }
    return Socket_DEPRECATED::SendStatus::SCHEDULED;
  } else {
    return Socket_DEPRECATED::sendBuffer(
        std::forward<std::unique_ptr<folly::IOBuf>>(buffer_chain));
  }
}

void Connection::scheduleWriteChain() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  ld_check(!legacy_connection_);
  if (!proto_handler_->good()) {
    return;
  }
  ld_check(sendChain_);
  auto now = SteadyTimestamp::now();
  STAT_ADD(deps_->getStats(),
           sock_write_sched_delay,
           to_msec(now - sched_start_time_).count());

  // Get bytes that are added to sendq but not yet added in the asyncSocket.
  auto bytes_in_sendq = Socket_DEPRECATED::getBufferedBytesSize();
  sock_write_cb_.write_chains.emplace_back(
      SocketWriteCallback::WriteUnit{bytes_in_sendq, now});
  // These bytes are now buffered in socket and will be removed from sendq.
  sock_write_cb_.bytes_buffered += bytes_in_sendq;
  proto_handler_->sock()->writeChain(&sock_write_cb_, std::move(sendChain_));
  // All the bytes will be now removed from sendq now that we have written into
  // the asyncsocket.
  onBytesAdmittedToSend(bytes_in_sendq);
}

void Connection::close(Status reason) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  if (isClosed()) {
    return;
  }
  // Calculate buffered bytes before clearing any member variables
  size_t buffered_bytes = getBufferedBytesSize();
  Socket_DEPRECATED::close(reason);
  if (!legacy_connection_) {
    // Clear read callback on close.
    proto_handler_->sock()->setReadCB(nullptr);
    if (buffered_bytes != 0 && !deps_->shuttingDown()) {
      getDeps()->noteBytesDrained(buffered_bytes,
                                  getPeerType(),
                                  /* message_type */ folly::none);
    }
    sock_write_cb_.clear();
    sendChain_.reset();
    sched_write_chain_.cancelTimeout();
    // Invoke closeNow before deleting the writing callback below.
    proto_handler_->sock()->closeNow();
    ld_check(getBufferedBytesSize() == 0);
  }
}

void Connection::flushOutputAndClose(Status reason) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  if (legacy_connection_) {
    return Socket_DEPRECATED::flushOutputAndClose(reason);
  }

  if (isClosed()) {
    return;
  }

  if (getBufferedBytesSize() > 0) {
    close_reason_ = reason;
    // Set the readcallback to nullptr as we know that socket is getting closed.
    proto_handler_->sock()->setReadCB(nullptr);
  } else {
    close(reason);
  }
}

bool Connection::isClosed() const {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  return Socket_DEPRECATED::isClosed();
}

bool Connection::good() const {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  auto is_good = Socket_DEPRECATED::good();

  // Outgoing message send checks in Sender if the socket is closed or good
  // before using it to send message. If the socket is already bad, Sender takes
  // the decision to create a new connection. This is a tiny optimization
  // helps messages from being written into a bad socket only to end up with
  // onSent error.
  if (!legacy_connection_) {
    return is_good && proto_handler_->good();
  }

  return is_good;
}

void Connection::onConnected() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onConnected();
}

int Connection::dispatchMessageBody(ProtocolHeader header,
                                    std::unique_ptr<folly::IOBuf> msg_buffer) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  auto body_clone = msg_buffer->clone();
  int rv =
      Socket_DEPRECATED::dispatchMessageBody(header, std::move(msg_buffer));
  if (rv != 0 && err == E::NOBUFS && !legacy_connection_) {
    // No space to push more messages on the worker, disable the read callback.
    // Retry this message and if successful it will add back the ReadCallback.
    ld_check(!retry_receipt_of_message_.isScheduled());
    retry_receipt_of_message_.attachCallback(
        [this, hdr = header, payload = std::move(body_clone)]() mutable {
          if (proto_handler_->dispatchMessageBody(hdr, std::move(payload)) ==
              0) {
            proto_handler_->sock()->setReadCB(read_cb_.get());
          }
        });
    retry_receipt_of_message_.scheduleTimeout(0);
    proto_handler_->sock()->setReadCB(nullptr);
  }
  return rv;
}

size_t Connection::getBufferedBytesSize() const {
  // This covers the bytes in sendChain_
  size_t buffered_bytes = Socket_DEPRECATED::getBufferedBytesSize();
  // This covers the bytes buffered in asyncsocket.
  if (!legacy_connection_) {
    buffered_bytes += sock_write_cb_.bytes_buffered;
  }
  return buffered_bytes;
}

size_t Connection::getBytesPending() const {
  // Covers bytes in various queues.
  size_t bytes_pending = Socket_DEPRECATED::getBytesPending();
  if (!legacy_connection_) {
    // Covers bytes in sendChain and asyncsocket.
    bytes_pending += getBufferedBytesSize();
  }
  return bytes_pending;
}

folly::ssl::X509UniquePtr Connection::getPeerCert() const {
  if (legacy_connection_) {
    return Socket_DEPRECATED::getPeerCert();
  }
  ld_check(isSSL());
  auto sock_peer_cert = proto_handler_->sock()->getPeerCertificate();
  if (sock_peer_cert) {
    return sock_peer_cert->getX509();
  }
  return nullptr;
}

void Connection::onConnectTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onConnectTimeout();
}

void Connection::onHandshakeTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onHandshakeTimeout();
}

void Connection::onConnectAttemptTimeout() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onConnectAttemptTimeout();
}

void Connection::onSent(std::unique_ptr<Envelope> e,
                        Status st,
                        Message::CompletionMethod cm) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onSent(std::move(e), st, cm);
}

void Connection::onError(short direction, int socket_errno) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onError(direction, socket_errno);
}

void Connection::onPeerClosed() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onPeerClosed();
}

void Connection::onBytesAdmittedToSend(size_t nbytes_drained) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  Socket_DEPRECATED::onBytesAdmittedToSend(nbytes_drained);
}

void Connection::onBytesPassedToTCP(size_t nbytes) {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  if (legacy_connection_) {
    // In case of legacy sockets it is alright to update sender level stats at
    // this point as the bytes are passed into the tcp socket.
    Socket_DEPRECATED::onBytesPassedToTCP(nbytes);
  }
}

void Connection::drainSendQueue() {
  auto g = folly::makeGuard(getDeps()->setupContextGuard());
  ld_check(!legacy_connection_);
  auto& cb = sock_write_cb_;
  size_t total_bytes_drained = 0;
  for (size_t& i = cb.num_success; i > 0; --i) {
    total_bytes_drained += cb.write_chains.front().length;
    STAT_ADD(deps_->getStats(),
             sock_write_sched_size,
             cb.write_chains.front().length);
    cb.write_chains.pop_front();
  }

  ld_check(cb.bytes_buffered >= total_bytes_drained);
  cb.bytes_buffered -= total_bytes_drained;
  Socket_DEPRECATED::onBytesPassedToTCP(total_bytes_drained);

  // flushOutputAndClose sets close_reason_ and waits for all buffers to drain.
  // Check if all buffers were drained here if that is the case close the
  // connection.
  if (close_reason_ != E::UNKNOWN && cb.write_chains.size() == 0 &&
      !sendChain_) {
    close(close_reason_);
  }
}
}} // namespace facebook::logdevice
