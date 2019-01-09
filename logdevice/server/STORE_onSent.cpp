/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/STORE_onSent.h"

#include "logdevice/common/Address.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Sender.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"

namespace facebook { namespace logdevice {

void STORE_onSent(const STORE_Message& msg,
                  Status st,
                  const Address& to,
                  const SteadyTimestamp /* enqueue_time */) {
  ld_check(!to.isClientAddress());

  if (msg.my_pos_in_copyset_ >= 0) {
    // message is being forwarded by a storage node to the next link
    // in the delivery chain
    ld_check(msg.my_pos_in_copyset_ + 1 < msg.header_.copyset_size);
    ld_assert(msg.copyset_[msg.my_pos_in_copyset_ + 1]
                  .destination.asNodeID()
                  .equalsRelaxed(to.id_.node_));
    ld_check(msg.header_.flags & STORE_Header::CHAIN);

    if (st != Status::OK) {
      msg.onForwardingFailure(st);
    }
    return;
  }

  if (msg.header_.flags & STORE_Header::REBUILDING) {
    ld_check(msg.header_.copyset_offset < msg.copyset_.size());
    shard_index_t shard_idx =
        msg.copyset_[msg.header_.copyset_offset].destination.shard();
    ShardID shard(to.id_.node_.index(), shard_idx);

    ServerWorker* w = ServerWorker::onThisThread();

    auto log_rebuilding =
        w->runningLogRebuildings().find(msg.header_.rid.logid, shard_idx);
    if (log_rebuilding) {
      RecordRebuildingInterface* r =
          log_rebuilding->findRecordRebuilding(msg.header_.rid.lsn());
      if (r) {
        r->onStoreSent(st,
                       msg.header_,
                       shard,
                       msg.extra_.rebuilding_version,
                       msg.extra_.rebuilding_wave);
        return;
      }
    } else {
      ld_check(msg.extra_.rebuilding_id != LOG_REBUILDING_ID_INVALID);
      auto& chunk_rebuildings = w->runningChunkRebuildings();
      auto it = chunk_rebuildings.map.find(msg.extra_.rebuilding_id);
      if (it != chunk_rebuildings.map.end()) {
        if (it->second->onStoreSent(st,
                                    msg.header_,
                                    shard,
                                    msg.extra_.rebuilding_version,
                                    msg.extra_.rebuilding_wave)) {
          return;
        }
      }
    }

    RATELIMIT_INFO(std::chrono::seconds(1),
                   5,
                   "Couldn't find RecordRebuilding for STORE_Message to %s"
                   "for record %lu%s; this is expected if rebuilding set "
                   "changed or sending was slow",
                   Sender::describeConnection(to).c_str(),
                   msg.header_.rid.logid.val_,
                   lsn_to_string(msg.header_.rid.lsn()).c_str());

    return;
  }

  msg.onSentCommon(st, to);
}

}} // namespace facebook::logdevice
