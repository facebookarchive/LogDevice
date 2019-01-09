/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/IS_LOG_EMPTY_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/IsLogEmptyRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

IS_LOG_EMPTY_Message::IS_LOG_EMPTY_Message(const IS_LOG_EMPTY_Header& header)
    : Message(MessageType::IS_LOG_EMPTY, TrafficClass::READ_BACKLOG),
      header_(header) {}

void IS_LOG_EMPTY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult IS_LOG_EMPTY_Message::deserialize(ProtocolReader& reader) {
  IS_LOG_EMPTY_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new IS_LOG_EMPTY_Message(hdr); });
}

Message::Disposition IS_LOG_EMPTY_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/IS_LOG_EMPTY_onReceived.cpp; this should
  // never get called.
  std::abort();
}

void IS_LOG_EMPTY_Message::onSent(Status status, const Address& to) const {
  ld_debug(": message=IS_LOG_EMPTY st=%s to=%s",
           error_name(status),
           Sender::describeConnection(to).c_str());

  // Inform the IsLogEmptyRequest of the outcome of sending the message
  auto& rqmap = Worker::onThisThread()->runningIsLogEmpty().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    ShardID shard(to.id_.node_.index(), header_.shard);
    it->second->onMessageSent(shard, status);
  }
}

}} // namespace facebook::logdevice
