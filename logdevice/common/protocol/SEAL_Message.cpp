/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/SEAL_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/LogRecoveryRequest.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

SEAL_Message::SEAL_Message(const SEAL_Header& header)
    : Message(MessageType::SEAL, TrafficClass::RECOVERY), header_(header) {}

void SEAL_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult SEAL_Message::deserialize(ProtocolReader& reader) {
  SEAL_Header hdr;
  reader.read(&hdr);
  return reader.result([&] { return new SEAL_Message(hdr); });
}

Message::Disposition SEAL_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/SEAL_onReceived.cpp; this should never
  // get called.
  std::abort();
}

void SEAL_Message::onSent(Status status, const Address& to) const {
  auto& rqmap = Worker::onThisThread()->runningLogRecoveries().map;
  auto it = rqmap.find(header_.log_id);
  if (it == rqmap.end()) {
    return;
  }

  ld_check(header_.shard != -1);
  it->second->onSealMessageSent(
      ShardID(to.id_.node_.index(), header_.shard), header_.seal_epoch, status);
}

}} // namespace facebook::logdevice
