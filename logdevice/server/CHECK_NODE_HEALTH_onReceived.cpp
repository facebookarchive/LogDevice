/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/CHECK_NODE_HEALTH_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/CHECK_NODE_HEALTH_REPLY_Message.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static void sendReply(Status status,
                      CHECK_NODE_HEALTH_Header header,
                      const Address& to) {
  ld_check(to.id_.client_.valid());
  CHECK_NODE_HEALTH_REPLY_Header reply_header{header.rqid, status, 0};
  auto msg = std::make_unique<CHECK_NODE_HEALTH_REPLY_Message>(reply_header);
  int rv = Worker::onThisThread()->sender().sendMessage(
      std::move(msg), to.id_.client_);
  if (rv != 0) {
    ld_warning("Failed to send CHECK_NODE_HEALTH_REPLY for %lu to %s: %s",
               header.rqid.val_,
               Sender::describeConnection(to).c_str(),
               error_description(err));
    return;
  }
  ld_spew("Sent CHECK_NODE_HEALTH_REPLY to %s with Status:%s",
          Sender::describeConnection(to).c_str(),
          error_name(status));
}

Message::Disposition
CHECK_NODE_HEALTH_onReceived(CHECK_NODE_HEALTH_Message* msg,
                             const Address& from) {
  ServerWorker* worker = ServerWorker::onThisThread();
  const CHECK_NODE_HEALTH_Header& header = msg->getHeader();

  ld_spew("Got an CHECK_NODE_HEALTH message from %s. rqid = %" PRIu64,
          Sender::describeConnection(from).c_str(),
          uint64_t(header.rqid));

  if (!from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got "
                    "CHECK_NODE_HEALTH message from non client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (!worker->isAcceptingWork()) {
    ld_debug("Worker not accpeting work. returning DISABLED");
    sendReply(E::DISABLED, header, from);
    return Message::Disposition::NORMAL;
  }

  if (!worker->processor_->runningOnStorageNode()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "Received a CHECK_NODE_HEALTH request for shard:%u from %s"
                    " on a non-storage node",
                    header.shard_idx,
                    Sender::describeConnection(from).c_str());
    sendReply(E::NOTSTORAGE, header, from);
    return Message::Disposition::NORMAL;
  }

  const ShardedStorageThreadPool* sharded_pool =
      worker->processor_->sharded_storage_thread_pool_;

  if (header.shard_idx < 0 || header.shard_idx >= sharded_pool->numShards()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "ERROR: got a CHECK_NODE_HEALTH message for an invalid "
                    "shard %d.",
                    header.shard_idx);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  if (worker->processor_->isDataMissingFromShard(header.shard_idx)) {
    ld_debug("Got CHECK_NODE_HEALTH %lu from %s but shard %u is empty and "
             "waiting for rebuilding",
             header.rqid.val_,
             Sender::describeConnection(from).c_str(),
             header.shard_idx);
    sendReply(E::DISABLED, header, from);
    return Message::Disposition::NORMAL;
  }

  Status accepting = sharded_pool->getByIndex(header.shard_idx)
                         .getLocalLogStore()
                         .acceptingWrites();
  if (accepting != E::OK) {
    if ((accepting == E::NOSPC || accepting == E::LOW_ON_SPC) &&
        header.flags & CHECK_NODE_HEALTH_Header::SPACE_BASED_RETENTION) {
      auto* sharded_store = worker->processor_->sharded_storage_thread_pool_
                                ->getShardedLocalLogStore();
      if (sharded_store != nullptr) {
        ld_debug("Setting sequencer initiated space based retention"
                 " for shard %d",
                 header.shard_idx);
        sharded_store->setSequencerInitiatedSpaceBasedRetention(
            header.shard_idx);
      }
    }
    // OVERLOADED takes precedence over LOW_ON_SPC,
    // but not over NOSPC and DISABLED
    if (accepting != E::LOW_ON_SPC) {
      sendReply(accepting, header, from);
      return Message::Disposition::NORMAL;
    }
  }

  if (worker->getStorageTaskQueueForShard(header.shard_idx)->isOverloaded() ||
      sharded_pool->getByIndex(header.shard_idx)
              .getLocalLogStore()
              .getWriteThrottleState() ==
          LocalLogStore::WriteThrottleState::REJECT_WRITE) {
    sendReply(E::OVERLOADED, header, from);
    return Message::Disposition::NORMAL;
  }

  if (accepting == E::LOW_ON_SPC) {
    sendReply(E::LOW_ON_SPC, header, from);
    return Message::Disposition::NORMAL;
  }

  sendReply(E::OK, header, from);
  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
