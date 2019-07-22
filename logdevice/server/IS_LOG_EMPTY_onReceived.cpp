/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/IS_LOG_EMPTY_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/IS_LOG_EMPTY_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       const IS_LOG_EMPTY_Header& request,
                       Status status,
                       bool empty) {
  ld_debug("Sending IS_LOG_EMPTY_REPLY: client_rqid=%lu, status=%s, empty=%s",
           request.client_rqid.val_,
           error_name(status),
           empty ? "TRUE" : "FALSE");

  if (status == E::OK) {
    if (empty) {
      WORKER_LOG_STAT_INCR(request.log_id, is_log_empty_reply_true);
    } else {
      WORKER_LOG_STAT_INCR(request.log_id, is_log_empty_reply_false);
    }
  } else {
    WORKER_LOG_STAT_INCR(request.log_id, is_log_empty_reply_error);
  }

  IS_LOG_EMPTY_REPLY_Header header = {
      request.client_rqid, status, empty, request.shard};
  auto msg = std::make_unique<IS_LOG_EMPTY_REPLY_Message>(header);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

Message::Disposition
IS_LOG_EMPTY_onReceived(IS_LOG_EMPTY_Message* msg,
                        const Address& from,
                        PermissionCheckStatus permission_status) {
  const IS_LOG_EMPTY_Header& header = msg->getHeader();

  Status status = PermissionChecker::toStatus(permission_status);
  if (status != E::OK) {
    RATELIMIT_LEVEL(
        status == E::ACCESS ? dbg::Level::WARNING : dbg::Level::INFO,
        std::chrono::seconds(2),
        1,
        "IS_LOG_EMPTY request from %s for log %lu failed with %s",
        Sender::describeConnection(from).c_str(),
        header.log_id.val_,
        error_description(status));

    send_reply(from, header, status, false);
    return Message::Disposition::NORMAL;
  }

  if (header.log_id == LOGID_INVALID) {
    ld_error("got IS_LOG_EMPTY message from %s with invalid log ID, ignoring",
             Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring IS_LOG_EMPTY message: not accepting more work");
    send_reply(from, header, E::SHUTDOWN, false);
    return Message::Disposition::NORMAL;
  }

  WORKER_LOG_STAT_INCR(header.log_id, is_log_empty_received);

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    send_reply(from, header, E::NOTSTORAGE, false);
    return Message::Disposition::NORMAL;
  }

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();
  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got IS_LOG_EMPTY message from client %s with "
                    "invalid shard %u, this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    send_reply(from, header, E::REBUILDING, false);
    return Message::Disposition::NORMAL;
  }

  LogStorageStateMap& map = processor->getLogStorageStateMap();
  LogStorageState* log_state = map.insertOrGet(header.log_id, shard_idx);
  if (log_state == nullptr || log_state->hasPermanentError()) {
    send_reply(from, header, E::FAILED, false);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
  if (!trim_point.hasValue()) {
    // Trim point is unknown. Try to find it...
    int rv = map.recoverLogState(
        header.log_id,
        shard_idx,
        LogStorageState::RecoverContext::IS_LOG_EMPTY_MESSAGE);

    // And in the meantime tell the client to try again in a bit
    send_reply(from, header, rv == 0 ? E::AGAIN : E::FAILED, false);
    return Message::Disposition::NORMAL;
  }

  ShardedStorageThreadPool* sstp = processor->sharded_storage_thread_pool_;
  LocalLogStore& store = sstp->getByIndex(shard_idx).getLocalLogStore();
  lsn_t last_lsn;
  int rv = store.getHighestInsertedLSN(header.log_id, &last_lsn);

  if (rv == -1) {
    if (err != E::NOTSUPPORTED || err != E::NOTSUPPORTEDLOG) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Unexpected error code from getHighestInsertedLSN(): %s. "
                      "Changing to FAILED.",
                      error_name(err));
      err = E::FAILED;
    }
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Unable to get the highest inserted LSN: %s",
                    error_description(err));
    // an error occurred, reply to the client
    send_reply(from, header, err, false);
    return Message::Disposition::NORMAL;
  }

  ld_debug("IS_LOG_EMPTY(%lu): last_lsn=%lu, trim_point=%lu",
           header.log_id.val_,
           last_lsn,
           trim_point.value());

  bool empty = (last_lsn == LSN_INVALID || last_lsn <= trim_point.value());

  if (empty) {
    // Make sure we're not waiting for, or in, mini-rebuilding.
    auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(&store);
    if (partitioned_store != nullptr &&
        partitioned_store->isUnderReplicated()) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "IS_LOG_EMPTY(%lu): local log store has dirty "
                      "partitions, reporting non-empty",
                      header.log_id.val_);
      send_reply(from, header, E::REBUILDING, false);
      return Message::Disposition::NORMAL;
    }
  } else {
    // Make sure it's not just pseudorecords, such as bridge records.
    auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(&store);
    empty = partitioned_store != nullptr &&
        partitioned_store->isLogEmpty(header.log_id);
  }

  send_reply(from, header, E::OK, empty);
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
