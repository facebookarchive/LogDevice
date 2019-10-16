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
}

}} // namespace facebook::logdevice
