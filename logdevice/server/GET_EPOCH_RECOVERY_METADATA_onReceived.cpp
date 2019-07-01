/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GET_EPOCH_RECOVERY_METADATA_onReceived.h"

#include "logdevice/common/Metadata.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/server/GetEpochRecoveryMetadataStorageTask.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace {
void send_reply(
    const Address& to,
    const GET_EPOCH_RECOVERY_METADATA_Header& header,
    Status status,
    std::unique_ptr<EpochRecoveryStateMap> epoch_recovery_state = nullptr) {
  ld_check(status != E::OK || epoch_recovery_state != nullptr);
  ld_check(status == E::OK || epoch_recovery_state == nullptr);
  GET_EPOCH_RECOVERY_METADATA_REPLY::createAndSend(
      to,
      header.log_id,
      header.shard,
      header.purging_shard,
      header.purge_to,
      header.start,
      header.end,
      0,
      status,
      header.id,
      std::move(epoch_recovery_state));
}
} // namespace

Message::Disposition
GET_EPOCH_RECOVERY_METADATA_onReceived(GET_EPOCH_RECOVERY_METADATA_Message* msg,
                                       const Address& from) {
  const GET_EPOCH_RECOVERY_METADATA_Header& header = msg->getHeader();

  if (!from.isClientAddress()) {
    ld_error("got GET_EPOCH_RECOVERY_METADATA_Message message from "
             "non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring GET_EPOCH_RECOVERY_METADATA_Message: "
             "not accepting more work");
    send_reply(from, header, E::SHUTDOWN);
    return Message::Disposition::NORMAL;
  }

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    send_reply(from, header, E::NOTSTORAGE);
    return Message::Disposition::NORMAL;
  }

  if (header.start == EPOCH_INVALID || header.end == EPOCH_INVALID ||
      header.start > header.end) {
    ld_error("Got GET_EPOCH_RECOVERY_METADATA_Message with invalid epoch "
             "from: %s, start:%u,end:%u",
             Sender::describeConnection(from).c_str(),
             header.start.val_,
             header.end.val_);
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards(
      worker->processor_->getMyNodeID().index());
  ld_check(n_shards > 0); // We already checked we are a storage node.

  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got GET_EPOCH_RECOVERY_METADATA_Message message from "
                    "client %s with invalid shard %u, this node only has "
                    "%u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    ld_spew("got GET_EPOCH_RECOVERY_METADATA_Message from client %s but "
            "rebuilding is in progress",
            Sender::describeConnection(from).c_str());
    send_reply(from, header, E::REBUILDING);
    return Message::Disposition::NORMAL;
  }

  LogStorageState* log_state =
      processor->getLogStorageStateMap().insertOrGet(header.log_id, shard_idx);
  if (log_state == nullptr) {
    // LogStorageStateMap is at capacity, should be rare
    ld_check(err == E::NOBUFS);
    send_reply(from, header, E::FAILED);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<epoch_t> last_clean = log_state->getLastCleanEpoch();
  if (last_clean.hasValue() && last_clean.value() < header.start) {
    // the epoch is not clean on the node, send E::NOTREADY directly
    send_reply(from, header, E::NOTREADY);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
  if (trim_point.hasValue() && lsn_to_epoch(trim_point.value()) > header.end) {
    // the epoch is already been trimmed, consider it empty
    // Note: since trim point move asynchronously on storage nodes, purging
    //       may aggressively treat the epoch as empty while the epoch
    //       still has data on other storage nodes.
    send_reply(from, header, E::EMPTY);
    return Message::Disposition::NORMAL;
  }

  if (log_state->hasPermanentError()) {
    send_reply(from, header, E::FAILED);
    return Message::Disposition::NORMAL;
  }

  // create storage task to read metadata
  worker->getStorageTaskQueueForShard(shard_idx)->putTask(
      std::make_unique<GetEpochRecoveryMetadataStorageTask>(
          header, from, shard_idx));

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
