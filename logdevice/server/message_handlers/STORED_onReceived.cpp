/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/message_handlers/STORED_onReceived.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Sender.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"

namespace facebook { namespace logdevice {

Message::Disposition STORED_onReceived(STORED_Message* msg,
                                       const Address& from) {
  if (from.isClientAddress()) {
    ld_error("PROTOCOL ERROR: got a STORED message for record %s from "
             "client %s. STORED can only arrive from servers",
             msg->header_.rid.toString().c_str(),
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ServerWorker* w = ServerWorker::onThisThread();

  shard_index_t shard_idx = msg->header_.shard;
  ld_check(shard_idx != -1);
  ShardID shard(from.id_.node_.index(), shard_idx);

  if (msg->header_.flags & STORED_Header::REBUILDING) {
    auto& chunk_rebuildings = w->runningChunkRebuildings();
    if (!chunk_rebuildings.map.empty() &&
        msg->rebuilding_id_ == CHUNK_REBUILDING_ID_INVALID) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Rebuilding got a STORED for %s without chunk ID from "
                      "%s. Unexpected.",
                      msg->header_.rid.toString().c_str(),
                      Sender::describeConnection(from).c_str());
    }
    auto it = chunk_rebuildings.map.find(msg->rebuilding_id_);
    if (it != chunk_rebuildings.map.end() &&
        it->second->onStored(msg->header_,
                             shard,
                             msg->rebuilding_version_,
                             msg->rebuilding_wave_,
                             msg->rebuilding_id_,
                             msg->serverInstanceId_,
                             msg->flushToken_)) {
      return Message::Disposition::NORMAL;
    }

    RATELIMIT_INFO(std::chrono::seconds(1),
                   5,
                   "Couldn't find RecordRebuilding for STORED_Message from %s"
                   "for record %lu%s; this is expected if rebuilding set "
                   "changed or store was slow",
                   Sender::describeConnection(from).c_str(),
                   msg->header_.rid.logid.val_,
                   lsn_to_string(msg->header_.rid.lsn()).c_str());

    return Message::Disposition::NORMAL;
  }

  return msg->onReceivedCommon(from);
}
}} // namespace facebook::logdevice
