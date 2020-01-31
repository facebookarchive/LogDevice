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
  auto bytes_in_sendq = getBufferedBytesSize() - sock_write_cb_.bytes_buffered;
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
