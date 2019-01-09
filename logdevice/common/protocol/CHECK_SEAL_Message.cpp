/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CHECK_SEAL_Message.h"

#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

CHECK_SEAL_Message::CHECK_SEAL_Message(const CHECK_SEAL_Header& header)
    : Message(MessageType::CHECK_SEAL, TrafficClass::RECOVERY),
      header_(header) {}

void CHECK_SEAL_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, CHECK_SEAL_Header::headerSize(writer.proto()));
}

MessageReadResult CHECK_SEAL_Message::deserialize(ProtocolReader& reader) {
  CHECK_SEAL_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;

  reader.read(&hdr, CHECK_SEAL_Header::headerSize(reader.proto()));

  return reader.result([&] { return new CHECK_SEAL_Message(hdr); });
}

void CHECK_SEAL_Message::onSent(Status st, const Address& to) const {
  Worker* w = Worker::onThisThread();
  auto& map = w->runningCheckSeals().map;
  auto it = map.find(header_.rqid);
  if (it != map.end()) {
    it->second->onSent(ShardID(to.id_.node_.index(), header_.shard), st);
  }
}

Message::Disposition CHECK_SEAL_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/CHECK_SEAL_onReceived.cpp; this should
  // never get called.
  std::abort();
}

}} // namespace facebook::logdevice
