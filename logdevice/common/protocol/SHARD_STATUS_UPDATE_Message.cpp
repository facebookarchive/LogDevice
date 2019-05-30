/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/SHARD_STATUS_UPDATE_Message.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

namespace facebook { namespace logdevice {

SHARD_STATUS_UPDATE_Message::SHARD_STATUS_UPDATE_Message(
    const SHARD_STATUS_UPDATE_Header& header,
    ShardAuthoritativeStatusMap shard_status)
    : Message(MessageType::SHARD_STATUS_UPDATE, TrafficClass::FAILURE_DETECTOR),
      header_(header),
      shard_status_(std::move(shard_status)) {}

void SHARD_STATUS_UPDATE_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
  shard_status_.serialize(writer);
}

MessageReadResult
SHARD_STATUS_UPDATE_Message::deserialize(ProtocolReader& reader) {
  SHARD_STATUS_UPDATE_Header hdr{};
  reader.read(&hdr);
  ShardAuthoritativeStatusMap map(hdr.version);
  map.deserialize(reader);
  return reader.result(
      [&] { return new SHARD_STATUS_UPDATE_Message(hdr, std::move(map)); });
}

Message::Disposition
SHARD_STATUS_UPDATE_Message::onReceived(const Address& from) {
  if (Worker::onThisThread()->processor_->hasMyNodeID()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Server got SHARD_STATUS_UPDATE message. This is unexpected. Messages "
        "of this type are supposed to be sent to clients only. Ignoring.");
    return Disposition::NORMAL;
  }

  ld_debug("SHARD_STATUS_UPDATE message from %s: %s",
           Sender::describeConnection(from).c_str(),
           shard_status_.describe().c_str());
  UpdateShardAuthoritativeMapRequest::broadcastToAllWorkers(shard_status_);
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
