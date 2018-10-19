/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/DATA_SIZE_REPLY_Message.h"

#include "logdevice/common/DataSizeRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

DATA_SIZE_REPLY_Message::DATA_SIZE_REPLY_Message(
    const DATA_SIZE_REPLY_Header& header)
    : Message(MessageType::DATA_SIZE_REPLY, TrafficClass::READ_BACKLOG),
      header_(header) {}

void DATA_SIZE_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(header_);
}

MessageReadResult DATA_SIZE_REPLY_Message::deserialize(ProtocolReader& reader) {
  DATA_SIZE_REPLY_Header hdr;
  reader.read(&hdr);
  return reader.result([&] { return new DATA_SIZE_REPLY_Message(hdr); });
}

Message::Disposition DATA_SIZE_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "got DATA_SIZE_REPLY message from client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ld_spew("Received DATA_SIZE_REPLY message: rqid: %lu, status=%s, size=%lu",
          header_.client_rqid.val(),
          error_name(header_.status),
          header_.size);

  Worker* worker = Worker::onThisThread();
  auto& rqmap = worker->runningDataSize().map;
  auto it = rqmap.find(header_.client_rqid);
  if (it != rqmap.end()) {
    auto scfg = worker->getServerConfig();
    shard_index_t shard_idx = header_.shard;
    it->second->onReply(ShardID(from.id_.node_.index(), shard_idx),
                        header_.status,
                        header_.size);
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
