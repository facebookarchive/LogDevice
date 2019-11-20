/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ProtocolHandler.h"

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncSocketException.h>

#include "logdevice/common/Connection.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {
ProtocolHandler::ProtocolHandler(Connection* conn,
                                 const std::string& conn_description,
                                 EvBase* evBase)
    : conn_(conn),
      conn_description_(conn_description),
      buffer_passed_to_tcp_(evBase),
      set_error_on_socket_(evBase) {}

folly::Future<Status> ProtocolHandler::asyncConnect(const folly::SocketAddress&,
                                                    const Settings&) {
  return folly::makeFuture<Status>(E::OK);
}

void ProtocolHandler::sendBuffer(SocketObserver::observer_id_t,
                                 std::unique_ptr<folly::IOBuf>&&) {}

void ProtocolHandler::close(Status) {}

void ProtocolHandler::closeNow(Status) {}

SocketObserver::observer_id_t
ProtocolHandler::registerSocketObserver(SocketObserver*) {
  return SocketObserver::kInvalidObserver;
}

void ProtocolHandler::unregisterSocketObserver(SocketObserver::observer_id_t) {}

bool ProtocolHandler::validateProtocolHeader(const ProtocolHeader& hdr) const {
  static_assert(
      sizeof(hdr) == sizeof(ProtocolHeader), "hdr type is not ProtocolHeader");
  // 1. Read first 2 fields of ProtocolHeader to extract message type
  size_t min_protohdr_bytes =
      sizeof(ProtocolHeader) - sizeof(ProtocolHeader::cksum);

  if (hdr.len <= min_protohdr_bytes) {
    ld_error("PROTOCOL ERROR: got message length %u from peer %s, expected "
             "at least %zu given sizeof(ProtocolHeader)=%zu",
             hdr.len,
             conn_description_.c_str(),
             min_protohdr_bytes + 1,
             sizeof(ProtocolHeader));
    err = E::BADMSG;
    return false;
  }

  size_t protohdr_bytes = ProtocolHeader::bytesNeeded(hdr.type, 0 /* unused */);

  if (hdr.len > Message::MAX_LEN + protohdr_bytes) {
    err = E::BADMSG;
    ld_error("PROTOCOL ERROR: got invalid message length %u from peer %s "
             "for msg:%s. Expected at most %u. min_protohdr_bytes:%zu",
             hdr.len,
             conn_description_.c_str(),
             messageTypeNames()[hdr.type].c_str(),
             Message::MAX_LEN,
             min_protohdr_bytes);
    return false;
  }

  if (!conn_->isHandshaken() && !isHandshakeMessage(hdr.type)) {
    ld_error("PROTOCOL ERROR: got a message of type %s on a brand new "
             "connection to/from %s). Expected %s.",
             messageTypeNames()[hdr.type].c_str(),
             conn_description_.c_str(),
             isHELLOMessage(hdr.type) ? "HELLO" : "ACK");
    err = E::PROTO;
    return false;
  }
  return true;
}

int ProtocolHandler::dispatchMessageBody(const ProtocolHeader& hdr,
                                         std::unique_ptr<folly::IOBuf> body) {
  // If some write hit an error it could close the socket. Return from here.
  if (conn_->isClosed()) {
    return -1;
  }
  auto body_clone = body->clone();
  int rv = conn_->dispatchMessageBody(hdr, std::move(body));
  if (rv != 0) {
    if ((err == E::PROTONOSUPPORT || err == E::INVALID_CLUSTER ||
         err == E::ACCESS || err == E::DESTINATION_MISMATCH) &&
        isHELLOMessage(hdr.type)) {
      conn_->flushOutputAndClose(err);
      return rv;
    }
    if (err == E::NOBUFS) {
      // Skip closing socket on ENOBUFS, Connection will take steps to uninstall
      // the read callback which will stop reading.
      return rv;
    }
    conn_->close(err);
  }

  return rv;
}

void ProtocolHandler::notifyErrorOnSocket(
    const folly::AsyncSocketException& ex) {
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Socket %s hit error %s",
                 conn_->conn_description_.c_str(),
                 ex.what());
  auto err_code = ProtocolHandler::translateToLogDeviceStatus(ex);
  if (!set_error_on_socket_.isScheduled()) {
    set_error_on_socket_.attachCallback(
        [&, error_code = err_code] { conn_->close(error_code); });
    set_error_on_socket_.scheduleTimeout(0);
  }
}

Status
ProtocolHandler::translateToLogDeviceStatus(folly::AsyncSocketException ex) {
  auto ex_type = ex.getType();
  switch (ex_type) {
    case folly::AsyncSocketException::END_OF_FILE:
      return E::PEER_CLOSED;
    case folly::AsyncSocketException::CORRUPTED_DATA:
      return E::BADMSG;
    case folly::AsyncSocketException::TIMED_OUT:
      return E::TIMEDOUT;
    case folly::AsyncSocketException::BAD_ARGS:
    case folly::AsyncSocketException::INVALID_STATE:
      return E::INTERNAL;
    case folly::AsyncSocketException::ALREADY_OPEN:
      return E::ISCONN;
    case folly::AsyncSocketException::NOT_OPEN:
    // AsyncSocket returns INTERNAL ERROR for various reasons it is better to
    // mark as CONN_FAILED.
    case folly::AsyncSocketException::INTERNAL_ERROR:
    case folly::AsyncSocketException::NETWORK_ERROR:
    case folly::AsyncSocketException::SSL_ERROR:
      return E::CONNFAILED;
    default:
      ld_check(false);
      return E::CONNFAILED;
  }
  return E::CONNFAILED;
}

void ProtocolHandler::notifyBytesWritten() {
  if (!buffer_passed_to_tcp_.isScheduled()) {
    buffer_passed_to_tcp_.scheduleTimeout(0);
  }
}

bool ProtocolHandler::good() const {
  return !set_error_on_socket_.isScheduled();
}

}} // namespace facebook::logdevice
