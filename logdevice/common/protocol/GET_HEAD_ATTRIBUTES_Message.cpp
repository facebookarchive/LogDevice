/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/GetHeadAttributesRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_HEAD_ATTRIBUTES_Message::GET_HEAD_ATTRIBUTES_Message(
    const GET_HEAD_ATTRIBUTES_Header& header)
    : Message(MessageType::GET_HEAD_ATTRIBUTES, TrafficClass::READ_BACKLOG),
      header_(header) {}

void GET_HEAD_ATTRIBUTES_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult
GET_HEAD_ATTRIBUTES_Message::deserialize(ProtocolReader& reader) {
  GET_HEAD_ATTRIBUTES_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result([&] { return new GET_HEAD_ATTRIBUTES_Message(hdr); });
}

Message::Disposition
GET_HEAD_ATTRIBUTES_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/GET_HEAD_ATTRIBUTES_onReceived.cpp; this
  // should never get called.
  std::abort();
}

void GET_HEAD_ATTRIBUTES_Message::onSent(Status status,
                                         const Address& to) const {
  ld_debug(": message=GET_HEAD_ATTRIBUTES st=%s to=%s",
           error_name(status),
           Sender::describeConnection(to).c_str());

  auto& rqmap = Worker::onThisThread()->runningGetHeadAttributes().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    ShardID shard(to.id_.node_.index(), header_.shard);
    it->second->onMessageSent(shard, status);
  }
}

}} // namespace facebook::logdevice
