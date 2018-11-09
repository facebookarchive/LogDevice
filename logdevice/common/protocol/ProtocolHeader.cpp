/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ProtocolHeader.h"

#include "logdevice/common/protocol/Compatibility.h"

namespace facebook { namespace logdevice {

namespace {
// TODO: add this util method in MessageType and remove from Socket.h
bool isHELLOMessage(MessageType type) {
  return type == MessageType::HELLO;
}

bool isACKMessage(MessageType type) {
  return type == MessageType::ACK;
}

bool isHandshakeMessage(MessageType type) {
  return isHELLOMessage(type) || isACKMessage(type);
}
} // namespace

bool ProtocolHeader::needChecksumInHeader(MessageType msgtype, uint16_t proto) {
  return msgtype != MessageType::INVALID && !isHandshakeMessage(msgtype) &&
      proto >= Compatibility::CHECKSUM_SUPPORT;
}

std::size_t ProtocolHeader::bytesNeeded(MessageType msgtype, uint16_t proto) {
  std::size_t bytes_needed = sizeof(ProtocolHeader);
  if (!needChecksumInHeader(msgtype, proto)) {
    bytes_needed -= sizeof(ProtocolHeader::cksum);
  }

  return bytes_needed;
}

}} // namespace facebook::logdevice
