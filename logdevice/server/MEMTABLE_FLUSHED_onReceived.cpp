/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/MEMTABLE_FLUSHED_onReceived.h"

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/MemtableFlushedRequest.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

Message::Disposition MEMTABLE_FLUSHED_onReceived(MEMTABLE_FLUSHED_Message* msg,
                                                 const Address& from) {
  ServerWorker* w = ServerWorker::onThisThread();
  const MEMTABLE_FLUSHED_Header& header = msg->getHeader();

  ld_spew("Got an MEMTABLE_FLUSHED message from %s with "
          "{nodeIndex:%d shardIdx:%d, FlushToken:%lu, ServerIndstanceId:%lu}",
          Sender::describeConnection(from).c_str(),
          header.node_index_,
          header.shard_idx_,
          header.memtable_id_,
          header.server_instance_id_);

  if (!from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got "
                    "MEMTABLE_FLUSHED message from non-client %s",
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  // Find all log rebuildings and deliver flushed message
  auto config = w->getServerConfig();

  // send an update to all other workers
  ServerProcessor* processor = w->processor_;
  int nworkers = processor->getWorkerCount();
  for (worker_id_t idx(0); idx.val_ < nworkers; ++idx.val_) {
    if (idx == w->idx_) {
      continue;
    }
    std::unique_ptr<Request> req =
        std::make_unique<MemtableFlushedRequest>(idx,
                                                 header.node_index_,
                                                 header.server_instance_id_,
                                                 header.shard_idx_,
                                                 header.memtable_id_);
    if (processor->postWithRetrying(req) != 0) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Failed to post MemtableFlushedRequest with {shard:%d, "
                      "FlushedUpto:%lu} to worker #%d",
                      header.shard_idx_,
                      header.memtable_id_,
                      idx.val_);
    }
  }

  // on the current worker, send an update to all LogRebuilding state machines
  // whose log maps to the shard on which memtable was flushed.
  for (const auto& lr : w->runningLogRebuildings().map) {
    if (lr.first.second == header.shard_idx_) {
      lr.second->onMemtableFlushed(
          header.node_index_, header.server_instance_id_, header.memtable_id_);
    }
  }

  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
