/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/SEAL_onReceived.h"

#include "logdevice/common/Seal.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/SEALED_Message.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/SealStorageTask.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

Message::Disposition SEAL_onReceived(SEAL_Message* msg, const Address& from) {
  const SEAL_Header& header = msg->getHeader();
  ServerWorker* worker = ServerWorker::onThisThread();

  if (header.log_id == LOGID_INVALID || !epoch_valid(header.seal_epoch)) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "Received an invalid SEAL message from %s: log id %lu, "
                       "epoch %u.",
                       Sender::describeConnection(from).c_str(),
                       header.log_id.val_,
                       header.seal_epoch.val_);
    err = E::BADMSG;
    return Message::Disposition::ERROR;
  }

  if (!header.sealed_by.isNodeID()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "SEAL message from %s for log %lu contains an invalid "
                    "NodeID",
                    Sender::describeConnection(from).c_str(),
                    header.log_id.val_);

    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring SEAL message: not accepting more work");
    SEALED_Message::createAndSend(
        from, header.log_id, header.shard, header.seal_epoch, E::SHUTDOWN);
    return Message::Disposition::NORMAL;
  }

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    ld_warning("Received a SEAL message from %s, but not a storage node",
               Sender::describeConnection(from).c_str());

    SEALED_Message::createAndSend(
        from, header.log_id, header.shard, header.seal_epoch, E::NOTSTORAGE);
    return Message::Disposition::NORMAL;
  }

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards(
      worker->processor_->getMyNodeID().index());
  ld_check(n_shards > 0); // We already checked we are a storage node.

  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got SEAL message from client %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    ld_debug("Received a SEAL message from %s, but rebuilding is in progress",
             Sender::describeConnection(from).c_str());

    SEALED_Message::createAndSend(
        from, header.log_id, header.shard, header.seal_epoch, E::REBUILDING);
    return Message::Disposition::NORMAL;
  }

  Seal seal;
  seal.epoch = header.seal_epoch;
  seal.seq_node = header.sealed_by;

  LogStorageState* log_state =
      processor->getLogStorageStateMap().insertOrGet(header.log_id, shard_idx);
  if (log_state == nullptr) {
    // LogStorageStateMap is at capacity
    SEALED_Message::createAndSend(
        from, header.log_id, header.shard, header.seal_epoch, E::FAILED);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<Seal> current_seal =
      log_state->getSeal(LogStorageState::SealType::NORMAL);

  if (current_seal.hasValue() && current_seal.value() > seal) {
    // if there is already a Seal in log storage state that can preempt the
    // SEAL message received, send a reply back with the current seal to the
    // sequencer without creating a storage task.
    SEALED_Message::createAndSend(from,
                                  header.log_id,
                                  header.shard,
                                  header.seal_epoch,
                                  E::PREEMPTED,
                                  LSN_INVALID,
                                  /*lng_list*/ std::vector<lsn_t>(),
                                  current_seal.value());
    return Message::Disposition::NORMAL;
  }

  bool tail_optimized = false;
  auto log = worker->getConfig()->getLogGroupByIDShared(header.log_id);
  if (!log) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Log %lu is no longer in cluster config. ",
                    header.log_id.val_);
    // still proceed with sealing
  } else {
    tail_optimized = log->attrs().tailOptimized().value();
  }

  auto task = std::make_unique<SealStorageTask>(
      header.log_id, header.last_clean_epoch, seal, from, tail_optimized);
  worker->getStorageTaskQueueForShard(shard_idx)->putTask(std::move(task));

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
