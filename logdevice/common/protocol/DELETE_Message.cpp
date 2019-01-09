/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/DELETE_Message.h"

#include <cstdlib>

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

DELETE_Message::DELETE_Message(const DELETE_Header& header)
    : Message(MessageType::DELETE, TrafficClass::APPEND), header_(header) {}

void DELETE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult DELETE_Message::deserialize(ProtocolReader& reader) {
  DELETE_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new DELETE_Message(hdr); });
}

Message::Disposition DELETE_Message::onReceived(const Address&) {
  // Receipt handler lives in server/DELETE_onReceived.cpp; this should never
  // get called.
  std::abort();
}

}} // namespace facebook::logdevice
