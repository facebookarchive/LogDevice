/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/DELETE_onReceived.h"

#include <memory>

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage/DeleteStorageTask.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

Message::Disposition DELETE_onReceived(DELETE_Message* msg,
                                       const Address& from) {
  const DELETE_Header& header = msg->getHeader();

  ServerWorker* worker = ServerWorker::onThisThread();
  shard_index_t shard_idx = header.shard;
  ld_check(shard_idx != -1);
  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();

  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got DELETE message from %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  if (!from.isClientAddress()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "PROTOCOL ERROR: got a DELETE %s from an outgoing (server) "
                    "connection to %s. DELETE messages can only arrive from "
                    "incoming (client) connections",
                    header.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring DELETE message: not accepting more work");
    return Message::Disposition::NORMAL;
  }

  // Check that we should even be processing this
  if (!worker->processor_->runningOnStorageNode()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got DELETE %s from %s but not configured as a "
                    "storage node",
                    header.rid.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  DeleteWriteOp op = {// TODO #3041039
                      header.rid.logid,
                      compose_lsn(header.rid.epoch, header.rid.esn)};
  worker->getStorageTaskQueueForShard(shard_idx)->putTask(
      std::make_unique<DeleteStorageTask>(op));

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
