/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/DATA_SIZE_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/DATA_SIZE_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       const DATA_SIZE_Header& request,
                       Status status,
                       size_t size) {
  ld_debug("Sending DATA_SIZE_REPLY: client_rqid=%lu, status=%s, size=%lu",
           request.client_rqid.val_,
           error_name(status),
           size);

  if (status == E::OK) {
    WORKER_LOG_STAT_INCR(request.log_id, data_size_reply);
  } else {
    WORKER_LOG_STAT_INCR(request.log_id, data_size_reply_error);
  }

  DATA_SIZE_REPLY_Header header = {
      request.client_rqid, status, size, request.shard};
  auto msg = std::make_unique<DATA_SIZE_REPLY_Message>(header);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

Message::Disposition DATA_SIZE_onReceived(DATA_SIZE_Message* msg,
                                          const Address& from) {
  const DATA_SIZE_Header& header = msg->getHeader();

  if (header.log_id == LOGID_INVALID) {
    ld_error("got DATA_SIZE message from %s with invalid log ID, ignoring",
             Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring DATA_SIZE message: not accepting more work");
    send_reply(from, header, E::SHUTDOWN, 0);
    return Message::Disposition::NORMAL;
  }

  WORKER_LOG_STAT_INCR(header.log_id, data_size_received);

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    send_reply(from, header, E::NOTSTORAGE, 0);
    return Message::Disposition::NORMAL;
  }

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();
  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got DATA_SIZE message from client %s with "
                    "invalid shard %u, this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    send_reply(from, header, E::REBUILDING, 0);
    return Message::Disposition::NORMAL;
  }

  LogStorageStateMap& map = processor->getLogStorageStateMap();
  LogStorageState* log_state = map.insertOrGet(header.log_id, shard_idx);
  if (log_state == nullptr || log_state->hasPermanentError()) {
    send_reply(from, header, E::FAILED, 0);
    return Message::Disposition::NORMAL;
  }

  ShardedStorageThreadPool* sstp = processor->sharded_storage_thread_pool_;
  LocalLogStore& store = sstp->getByIndex(shard_idx).getLocalLogStore();

  auto partitioned_store = dynamic_cast<PartitionedRocksDBStore*>(&store);
  if (!partitioned_store) {
    // Only supported on partitioned, rocksdb-based stores
    send_reply(from, header, E::NOTSUPPORTED, 0);
  }

  ld_debug("DATA_SIZE: log %lu in range [%lu,%lu]",
           header.log_id.val_,
           header.lo_timestamp_ms,
           header.hi_timestamp_ms);

  if (partitioned_store->isUnderReplicated()) {
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    10,
                    "DATA_SIZE(%lu): local log store has dirty partitions, "
                    "reporting transient rebuilding state",
                    header.log_id.val_);
    send_reply(from, header, E::REBUILDING, 0);
    return Message::Disposition::NORMAL;
  }

  size_t size = 0;
  int rv = partitioned_store->dataSize(
      header.log_id,
      std::chrono::milliseconds(header.lo_timestamp_ms),
      std::chrono::milliseconds(header.hi_timestamp_ms),
      &size);
  send_reply(from, header, rv == 0 ? E::OK : E::FAILED, size);
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
