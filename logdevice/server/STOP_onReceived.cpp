/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/STOP_onReceived.h"

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/storage/AllCachedDigests.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

Message::Disposition STOP_onReceived(STOP_Message* msg, const Address& from) {
  if (!from.isClientAddress()) {
    ld_error("got STOP message from non-client %s",
             Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  const STOP_Header& header = msg->getHeader();
  ServerWorker* w = ServerWorker::onThisThread();
  if (!w->processor_->runningOnStorageNode()) {
    ld_debug("got STOP message from client %s but not a storage node",
             Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  const shard_size_t n_shards = w->getNodesConfiguration()->getNumShards();
  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got START message from client %s with invalid shard %u, "
                    "this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  // TODO validate log ID and send back error
  if (!w->cachedDigests().eraseDigest(
          from.id_.client_, header.read_stream_id)) {
    w->serverReadStreams().erase(
        from.id_.client_, header.log_id, header.read_stream_id, shard_idx);
  }

  return Message::Disposition::NORMAL;
}

}} // namespace facebook::logdevice
