/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/FINDKEY_onReceived.h"

#include "logdevice/common/FindKeyTracer.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/FINDKEY_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/FindKeyStorageTask.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       request_id_t client_rqid,
                       Status status,
                       lsn_t result_lo,
                       lsn_t result_hi,
                       shard_index_t shard,
                       FindKeyTracer& tracer) {
  FINDKEY_REPLY_Header header = {
      client_rqid, status, result_lo, result_hi, shard};
  auto msg = std::make_unique<FINDKEY_REPLY_Message>(header);
  tracer.trace(status, result_lo, result_hi);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

static void send_error(const Address& to,
                       request_id_t client_rqid,
                       Status status,
                       shard_index_t shard,
                       FindKeyTracer& tracer) {
  send_reply(to, client_rqid, status, LSN_INVALID, LSN_INVALID, shard, tracer);
}

/**
 * Optimization: try to run approximate findTime() or findKey() on worker
 * thread since it usually does not require disk operations in case of
 * PartitionedRocksDBStore local storage.
 * @return        true if non blocking operation succeed and false otherwise
 */
static bool runNonBlockingFindKey(const FINDKEY_Message& msg,
                                  lsn_t trim_point,
                                  lsn_t last_per_epoch_released_lsn,
                                  const Address& from,
                                  shard_index_t shard_idx,
                                  FindKeyTracer tracer);

Message::Disposition
FINDKEY_onReceived(FINDKEY_Message* msg,
                   const Address& from,
                   PermissionCheckStatus permission_status) {
  ServerWorker* worker = ServerWorker::onThisThread();
  FindKeyTracer tracer(
      worker->getTraceLogger(), Sender::sockaddrOrInvalid(from), msg->header_);

  std::chrono::milliseconds timestamp;
  timestamp = std::chrono::milliseconds(msg->header_.timestamp);
  tracer.setTimestamp(timestamp);
  tracer.setKey(msg->key_.value_or(""));

  Status status = PermissionChecker::toStatus(permission_status);
  if (status != E::OK) {
    RATELIMIT_LEVEL(
        status == E::ACCESS ? dbg::Level::WARNING : dbg::Level::INFO,
        std::chrono::seconds(2),
        1,
        "FINDKEY request from %s for log %lu failed with %s",
        Sender::describeConnection(from).c_str(),
        msg->header_.log_id.val_,
        error_description(status));

    send_error(
        from, msg->header_.client_rqid, status, msg->header_.shard, tracer);
    return Message::Disposition::NORMAL;
  }

  if (!from.isClientAddress()) {
    ld_error("got FINDKEY message from non-client %s",
             Sender::describeConnection(from).c_str());
    tracer.trace(E::PROTO, LSN_INVALID, LSN_INVALID);
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (msg->header_.log_id == LOGID_INVALID) {
    ld_error("got FINDKEY message from %s with invalid log ID, "
             "ignoring",
             Sender::describeConnection(from).c_str());
    tracer.trace(E::INVALID_PARAM, LSN_INVALID, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }

  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring FINDKEY message: not accepting more work");
    send_error(from,
               msg->header_.client_rqid,
               E::SHUTDOWN,
               msg->header_.shard,
               tracer);
    return Message::Disposition::NORMAL;
  }

  WORKER_LOG_STAT_INCR(msg->header_.log_id, findkey_received);

  ServerProcessor* processor = worker->processor_;

  if (!processor->runningOnStorageNode()) {
    send_error(from,
               msg->header_.client_rqid,
               E::NOTSTORAGE,
               msg->header_.shard,
               tracer);
    return Message::Disposition::NORMAL;
  }

  const auto& log_map = Worker::settings().dont_serve_findtimes_logs;
  if (log_map.find(msg->header_.log_id) != log_map.end()) {
    Status status = Worker::settings().dont_serve_findtimes_status;
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "Denying findtimes for log %lu based on settings with status %s",
        msg->header_.log_id.val(),
        error_description(status));
    send_error(
        from, msg->header_.client_rqid, status, msg->header_.shard, tracer);
    return Message::Disposition::NORMAL;
  }

  shard_index_t shard_idx = msg->header_.shard;
  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got FINDKEY message from client %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    send_error(
        from, msg->header_.client_rqid, E::REBUILDING, shard_idx, tracer);
    return Message::Disposition::NORMAL;
  }

  auto flags = msg->header_.flags;
  lsn_t last_per_epoch_released_lsn = LSN_INVALID;
  folly::Optional<lsn_t> trim_point;

  if (!(flags & FINDKEY_Header::USER_KEY)) {
    LogStorageState* log_state = processor->getLogStorageStateMap().insertOrGet(
        msg->header_.log_id, shard_idx);
    if (log_state == nullptr ||        // LogStorageStateMap is at capacity
        log_state->hasPermanentError() // LogStorageState may be stale.
    ) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Got FINDKEY message from client %s but the "
                      "LogStorageStateMap is at capacity (%d) or is in "
                      "permanent error (%d)",
                      Sender::describeConnection(from).c_str(),
                      log_state == nullptr,
                      log_state->hasPermanentError());
      send_error(from, msg->header_.client_rqid, E::FAILED, shard_idx, tracer);
      return Message::Disposition::NORMAL;
    }

    // Get last per-epoch released LSN. It may be ahead of the last released
    // LSN in case recovery is running and per-epoch releases are enabled.
    last_per_epoch_released_lsn = log_state->getLastPerEpochReleasedLSN();
    trim_point = log_state->getTrimPoint();
    if (last_per_epoch_released_lsn == LSN_INVALID || !trim_point.hasValue()) {
      // Last per-epoch released LSN or trim point are unknown.  Try to find
      // them...
      processor->getLogStorageStateMap().recoverLogState(
          msg->header_.log_id,
          shard_idx,
          LogStorageState::RecoverContext::FINDKEY_MESSAGE);

      // And in the meantime tell the client to try again in a bit
      send_error(from, msg->header_.client_rqid, E::AGAIN, shard_idx, tracer);
      return Message::Disposition::NORMAL;
    }

    if (timestamp == std::chrono::milliseconds::max()) {
      // max() is normally used with findTime() to determine the sequence number
      // of the last log record; we can avoid binary search here and just return
      // last_per_epoch_released_lsn
      send_reply(from,
                 msg->header_.client_rqid,
                 E::OK,
                 std::max(last_per_epoch_released_lsn, trim_point.value()),
                 LSN_MAX,
                 shard_idx,
                 tracer);
      return Message::Disposition::NORMAL;
    }

    if (Worker::settings().findtime_force_approximate) {
      flags |= FINDKEY_Header::APPROXIMATE;
      tracer.setApproximateEnforced(true);
    }
  }

  // The trim point and last released lsn are only used as initial bounds for
  // the binary search inside a partition in findTime(), so we do not need them
  // for findKey().
  lsn_t trim_point_value = trim_point.value_or(LSN_INVALID);

  if (runNonBlockingFindKey(*msg,
                            trim_point_value,
                            last_per_epoch_released_lsn,
                            from,
                            shard_idx,
                            std::move(tracer))) {
    return Message::Disposition::NORMAL;
  }

  auto task_deadline = msg->header_.timeout_ms > 0
      ? std::chrono::steady_clock::now() +
          std::chrono::milliseconds(msg->header_.timeout_ms)
      : std::chrono::steady_clock::time_point::max();

  worker->getStorageTaskQueueForShard(shard_idx)->putTask(
      std::make_unique<FindKeyStorageTask>(
          from.id_.client_,
          msg->header_.client_rqid,
          msg->header_.log_id,
          timestamp,
          std::move(msg->key_),
          std::min(last_per_epoch_released_lsn, msg->header_.hint_hi),
          std::max(trim_point_value, msg->header_.hint_lo),
          flags,
          task_deadline,
          std::move(tracer),
          Worker::onThisThread()->sender().getSockaddr(from)));
  return Message::Disposition::NORMAL;
}

bool runNonBlockingFindKey(const FINDKEY_Message& msg,
                           lsn_t trim_point,
                           lsn_t last_per_epoch_released_lsn,
                           const Address& from,
                           shard_index_t shard_idx,
                           FindKeyTracer tracer) {
  StorageThreadPool& pool =
      ServerWorker::onThisThread()
          ->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx);

  LocalLogStore* store = &pool.getLocalLogStore();
  ld_check(store);

  bool approximate = msg.header_.flags & FINDKEY_Header::APPROXIMATE;

  if (!Worker::settings().allow_reads_on_workers ||
      (!msg.key_.hasValue() && !store->supportsNonBlockingFindTime()) ||
      (msg.key_.hasValue() && !store->supportsNonBlockingFindKey())) {
    return false;
  }

  if (approximate) {
    WORKER_STAT_INCR(approximate_find_key_try_non_blocking);
  } else {
    WORKER_STAT_INCR(strict_find_key_try_non_blocking);
  }

  int rv;
  lsn_t lo;
  lsn_t hi;
  if (!msg.key_.hasValue()) {
    std::chrono::milliseconds timestamp(msg.header_.timestamp);
    lo = std::max(trim_point, msg.header_.hint_lo);
    hi = std::min(last_per_epoch_released_lsn, msg.header_.hint_hi);
    rv = store->findTime(msg.header_.log_id,
                         timestamp,
                         &lo,
                         &hi,
                         approximate,
                         false /* do not allow blocking io */
    );
  } else {
    rv = store->findKey(msg.header_.log_id,
                        msg.key_.value(),
                        &lo,
                        &hi,
                        approximate,
                        false /* do not allow blocking io */
    );
  }
  if (rv == 0) {
    send_reply(from, msg.header_.client_rqid, E::OK, lo, hi, shard_idx, tracer);
    return true;
  } else {
    if (err == E::FAILED) {
      send_error(from, msg.header_.client_rqid, E::FAILED, shard_idx, tracer);
      return true;
    }
  }
  ld_check(err == E::WOULDBLOCK);
  if (approximate) {
    WORKER_STAT_INCR(approximate_find_key_would_block);
  } else {
    WORKER_STAT_INCR(strict_find_key_would_block);
  }
  return false;
}

}} // namespace facebook::logdevice
