/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_REPLY_Message.h"

#include "logdevice/common/GetHeadAttributesRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

GET_HEAD_ATTRIBUTES_REPLY_Message::GET_HEAD_ATTRIBUTES_REPLY_Message(
    const GET_HEAD_ATTRIBUTES_REPLY_Header& header)
    : Message(MessageType::GET_HEAD_ATTRIBUTES_REPLY,
              TrafficClass::READ_BACKLOG),
      header_(header) {}

void GET_HEAD_ATTRIBUTES_REPLY_Message::serialize(
    ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult
GET_HEAD_ATTRIBUTES_REPLY_Message::deserialize(ProtocolReader& reader) {
  GET_HEAD_ATTRIBUTES_REPLY_Header hdr;
  // Defaults for old protocols
  hdr.shard = -1;
  reader.read(&hdr);
  return reader.result(
      [&] { return new GET_HEAD_ATTRIBUTES_REPLY_Message(hdr); });
}

Message::Disposition
GET_HEAD_ATTRIBUTES_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "got GET_HEAD_ATTRIBUTES_REPLY message from client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ld_debug("Received GET_HEAD_ATTRIBUTES_REPLY message");

  Worker* worker = Worker::onThisThread();
  auto& rqmap = worker->runningGetHeadAttributes().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    auto scfg = worker->getServerConfig();
    shard_index_t shard_idx = header_.shard;
    it->second->onReply(
        ShardID(from.id_.node_.index(), shard_idx),
        header_.status,
        {header_.trim_point,
         std::chrono::milliseconds(header_.trim_point_timestamp)});
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
