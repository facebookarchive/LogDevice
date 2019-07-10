/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_TRIM_POINT_Message.h"

#include "logdevice/common/GetTrimPointRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_TRIM_POINT_Message::GET_TRIM_POINT_Message(
    const GET_TRIM_POINT_Header& header)
    : Message(MessageType::GET_TRIM_POINT, TrafficClass::READ_BACKLOG),
      header_(header) {}

void GET_TRIM_POINT_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult GET_TRIM_POINT_Message::deserialize(ProtocolReader& reader) {
  GET_TRIM_POINT_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new GET_TRIM_POINT_Message(hdr); });
}

Message::Disposition
GET_TRIM_POINT_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/GET_TRIM_POINT_onReceived.cpp; this should
  // never get called.
  std::abort();
}

void GET_TRIM_POINT_Message::onSent(Status status, const Address& to) const {
  ld_debug(": message=GET_TRIM_POINT st=%s to=%s",
           error_name(status),
           Sender::describeConnection(to).c_str());

  // Inform the GetTrimPointRequest of the outcome of sending the message
  auto& rqmap = Worker::onThisThread()->runningGetTrimPoint().map;
  auto it = rqmap.find(header_.log_id);
  if (it != rqmap.end()) {
    ShardID shard(to.id_.node_.index(), header_.shard);
    it->second->onMessageSent(shard, status);
  }
}

}} // namespace facebook::logdevice
