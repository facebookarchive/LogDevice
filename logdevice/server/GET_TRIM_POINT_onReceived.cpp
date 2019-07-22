/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/GET_TRIM_POINT_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/GET_TRIM_POINT_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

static void send_reply(const Address& to,
                       const GET_TRIM_POINT_Header& request,
                       Status status,
                       lsn_t trim_point) {
  ld_debug("Sending GET_TRIM_POINT_REPLY: "
           "log_id=%lu, status=%s, trim point=%lu",
           request.log_id.val_,
           error_name(status),
           trim_point);
  if (status == E::OK) {
    WORKER_LOG_STAT_INCR(request.log_id, get_trim_point_reply_ok);
  } else {
    WORKER_LOG_STAT_INCR(request.log_id, get_trim_point_reply_error);
  }

  GET_TRIM_POINT_REPLY_Header header = {
      status, request.log_id, trim_point, request.shard};
  auto msg = std::make_unique<GET_TRIM_POINT_REPLY_Message>(header);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

Message::Disposition GET_TRIM_POINT_onReceived(GET_TRIM_POINT_Message* msg,
                                               const Address& from) {
  const GET_TRIM_POINT_Header& header = msg->getHeader();
  if (header.log_id == LOGID_INVALID) {
    ld_error("got GET_TRIM_POINT message from %s with invalid log ID, ignoring",
             Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring GET_TRIM_POINT message: not accepting more work");
    send_reply(from, header, E::SHUTDOWN, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }
  WORKER_LOG_STAT_INCR(header.log_id, get_trim_point_received);

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    send_reply(from, header, E::NOTSTORAGE, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();
  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got GET_TRIM_POINT message from client %s with "
                    "invalid shard %u, this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    send_reply(from, header, E::REBUILDING, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }

  LogStorageStateMap& map = processor->getLogStorageStateMap();
  LogStorageState* log_state = map.insertOrGet(header.log_id, shard_idx);
  if (log_state == nullptr || log_state->hasPermanentError()) {
    send_reply(from, header, E::FAILED, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }

  folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
  if (!trim_point.hasValue()) {
    // Trim point is unknown. Try to find it...
    int rv =
        map.recoverLogState(header.log_id,
                            shard_idx,
                            LogStorageState::RecoverContext::GET_TRIM_POINT);

    // And in the meantime tell the client to try again in a bit
    send_reply(from, header, rv == 0 ? E::AGAIN : E::FAILED, LSN_INVALID);
    return Message::Disposition::NORMAL;
  }

  // normal case
  ld_debug("GET_TRIM_POINT(%lu): trim_point=%lu",
           header.log_id.val_,
           trim_point.value());
  send_reply(from, header, E::OK, trim_point.value());
  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
