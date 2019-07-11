/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/ProtocolHeader.h"

#include "logdevice/common/protocol/Compatibility.h"

namespace facebook { namespace logdevice {

bool ProtocolHeader::needChecksumInHeader(MessageType msgtype, uint16_t proto) {
  return msgtype != MessageType::INVALID && !isHandshakeMessage(msgtype);
}

std::size_t ProtocolHeader::bytesNeeded(MessageType msgtype, uint16_t proto) {
  std::size_t bytes_needed = sizeof(ProtocolHeader);
  if (!needChecksumInHeader(msgtype, proto)) {
    bytes_needed -= sizeof(ProtocolHeader::cksum);
  }

  return bytes_needed;
}

}} // namespace facebook::logdevice
