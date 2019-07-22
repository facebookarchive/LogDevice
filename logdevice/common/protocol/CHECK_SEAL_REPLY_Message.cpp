/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/CHECK_SEAL_REPLY_Message.h"

#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

CHECK_SEAL_REPLY_Message::CHECK_SEAL_REPLY_Message(
    const CHECK_SEAL_REPLY_Header& header)
    : Message(MessageType::CHECK_SEAL_REPLY, TrafficClass::RECOVERY),
      header_(header) {}

void CHECK_SEAL_REPLY_Message::serialize(ProtocolWriter& writer) const {
  writer.write(&header_, CHECK_SEAL_REPLY_Header::headerSize(writer.proto()));
}

MessageReadResult
CHECK_SEAL_REPLY_Message::deserialize(ProtocolReader& reader) {
  CHECK_SEAL_REPLY_Header hdr{};
  // Defaults for old protocols
  hdr.shard = -1;

  reader.read(&hdr, CHECK_SEAL_REPLY_Header::headerSize(reader.proto()));

  return reader.result([&] { return new CHECK_SEAL_REPLY_Message(hdr); });
}

Message::Disposition CHECK_SEAL_REPLY_Message::onReceived(const Address& from) {
  if (from.isClientAddress()) {
    ld_error("Received CHECK_SEAL_REPLY from client %s for log:%lu",
             Sender::describeConnection(from).c_str(),
             header_.log_id.val());
    err = E::PROTO;
    return Disposition::ERROR;
  }

  ld_spew("Received CHECK_SEAL_REPLY message for log:%lu"
          " for CheckSealRequest rqid:%lu from %s(%s), with status=%s",
          header_.log_id.val(),
          header_.rqid.val(),
          from.id_.node_.toString().c_str(),
          Sender::describeConnection(from).c_str(),
          error_name(header_.status));

  Worker* w = Worker::onThisThread();
  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard = header_.shard;
  ld_check(shard != -1);
  if (shard >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got CHECK_SEAL_REPLY from %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard,
                    n_shards);
    return Disposition::NORMAL;
  }

  auto& map = w->runningCheckSeals().map;
  auto it = map.find(header_.rqid);
  if (it != map.end()) {
    it->second->onReply(ShardID(from.id_.node_.index(), shard), *this);
  } else {
    ld_spew("Received a CHECK_SEAL_REPLY for log:%lu, rqid:%lu, from %s(%s),"
            " but the corresponding request is not present.",
            header_.log_id.val_,
            header_.rqid.val(),
            from.id_.node_.toString().c_str(),
            Sender::describeConnection(from).c_str());
  }

  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
