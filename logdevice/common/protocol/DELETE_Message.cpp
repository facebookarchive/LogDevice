/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "DELETE_Message.h"

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

#include <cstdlib>

namespace facebook { namespace logdevice {

size_t DELETE_Header::getExpectedSize(uint16_t proto) {
  if (proto < Compatibility::SHARD_ID_IN_DELETE_MSG) {
    return offsetof(DELETE_Header, shard);
  } else {
    return sizeof(DELETE_Header);
  }
}

DELETE_Message::DELETE_Message(const DELETE_Header& header)
    : Message(MessageType::DELETE, TrafficClass::APPEND), header_(header) {}

void DELETE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, DELETE_Header::getExpectedSize(writer.proto()));
}

MessageReadResult DELETE_Message::deserialize(ProtocolReader& reader) {
  const auto proto = reader.proto();
  DELETE_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr, DELETE_Header::getExpectedSize(proto));
  return reader.result([&] { return new DELETE_Message(hdr); });
}

Message::Disposition DELETE_Message::onReceived(const Address&) {
  // Receipt handler lives in server/DELETE_onReceived.cpp; this should never
  // get called.
  std::abort();
}

}} // namespace facebook::logdevice
