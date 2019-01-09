/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/GET_TRIM_POINT_REPLY_Message.h"

#include "logdevice/common/GetTrimPointRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_TRIM_POINT_REPLY_Message::GET_TRIM_POINT_REPLY_Message(
    const GET_TRIM_POINT_REPLY_Header& header)
    : Message(MessageType::GET_TRIM_POINT_REPLY, TrafficClass::READ_BACKLOG),
      header_(header) {}

void GET_TRIM_POINT_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult
GET_TRIM_POINT_REPLY_Message::deserialize(ProtocolReader& reader) {
  GET_TRIM_POINT_REPLY_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new GET_TRIM_POINT_REPLY_Message(hdr); });
}

Message::Disposition
GET_TRIM_POINT_REPLY_Message::onReceived(const Address& from) {
  ld_debug("Received GET_TRIM_POINT_REPLY message");

  Worker* worker = Worker::onThisThread();
  auto& rqmap = worker->runningGetTrimPoint().map;
  auto it = rqmap.find(header_.log_id);
  if (it != rqmap.end()) {
    shard_index_t shard_idx = header_.shard;
    it->second->onReply(ShardID(from.id_.node_.index(), shard_idx),
                        header_.status,
                        header_.trim_point);
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
