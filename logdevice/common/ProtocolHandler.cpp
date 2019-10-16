/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ProtocolHandler.h"

#include <folly/io/async/AsyncSocketException.h>

#include "logdevice/common/Connection.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/MessageType.h"
#include "logdevice/common/protocol/MessageTypeNames.h"

namespace facebook { namespace logdevice {
ProtocolHandler::ProtocolHandler(Connection* conn, EvBase* evBase)
    : conn_(conn),
      buffer_passed_to_tcp_(evBase),
      set_error_on_socket_(evBase) {}

bool ProtocolHandler::validateProtocolHeader(
    const ProtocolHeader& /* hdr */) const {
  return true;
}

int ProtocolHandler::dispatchMessageBody(
    const ProtocolHeader& /* hdr */,
    std::unique_ptr<folly::IOBuf> /* body */) {
  return 0;
}

void ProtocolHandler::notifyErrorOnSocket(
    const folly::AsyncSocketException& ex) {
  ld_info(
      "Socket %s hit error %s", conn_->conn_description_.c_str(), ex.what());
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
}} // namespace facebook::logdevice
