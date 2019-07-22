/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GET_HEAD_ATTRIBUTES_onReceived.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/GET_HEAD_ATTRIBUTES_REPLY_Message.h"
#include "logdevice/include/LogHeadAttributes.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace {
void send_reply(const Address& to,
                const GET_HEAD_ATTRIBUTES_Header& request,
                Status status,
                LogHeadAttributes attributes = LogHeadAttributes()) {
  ld_debug("Sending GET_HEAD_ATTRIBUTES_REPLY: client_rqid=%lu, status=%s, "
           "trim point=%s, trim point timestamp=%ld",
           request.client_rqid.val_,
           error_name(status),
           lsn_to_string(attributes.trim_point).c_str(),
           attributes.trim_point_timestamp.count());

  GET_HEAD_ATTRIBUTES_REPLY_Header header = {
      request.client_rqid,
      status,
      attributes.trim_point,
      uint64_t(attributes.trim_point_timestamp.count()),
      /* flags */ 0,
      request.shard};
  auto msg = std::make_unique<GET_HEAD_ATTRIBUTES_REPLY_Message>(header);
  Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

class GetHeadAttributesStorageTask : public StorageTask {
 public:
  // Trim point is going to be available if if it was successfully fetched
  // from LogStorageState.
  GetHeadAttributesStorageTask(const GET_HEAD_ATTRIBUTES_Header& header,
                               const Address& reply_to,
                               folly::Optional<lsn_t> trim_point)
      : StorageTask(StorageTask::Type::GET_HEAD_ATTRIBUTES),
        header_(header),
        reply_to_(reply_to),
        trim_point_(trim_point),
        status_(E::UNKNOWN),
        trim_point_timestamp_(std::chrono::milliseconds::max()) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    LocalLogStore& store = storageThreadPool_->getLocalLogStore();

    // Read trim point from store because it was not available in
    // LogStorageState.
    if (!trim_point_.hasValue()) {
      // Update trim point in LogStorageState so that next time this request
      // doesn't have to read from store again.
      LogStorageStateMap& map =
          storageThreadPool_->getProcessor().getLogStorageStateMap();
      LogStorageState* log_state =
          map.find(header_.log_id, storageThreadPool_->getShardIdx());
      // LogStorageState should have been initialized in
      // GET_HEAD_ATTRIBUTES_Message::onReceived
      ld_check(log_state != nullptr);

      TrimMetadata trim_metadata{LSN_INVALID};
      int rv = store.readLogMetadata(header_.log_id, &trim_metadata);
      if (rv != 0 && err != E::NOTFOUND) {
        log_state->notePermanentError("Reading trim point");
        status_ = E::FAILED;
        return;
      }
      trim_point_.assign(trim_metadata.trim_point_);
      log_state->updateTrimPoint(trim_point_.value());
    }
    ld_check(trim_point_.hasValue());
    trim_point_timestamp_ = std::chrono::milliseconds::max();
    int rv = store.getApproximateTimestamp(header_.log_id,
                                           trim_point_.value() + 1,
                                           true, // allow_blocking_io
                                           &trim_point_timestamp_);
    if (rv == 0) {
      status_ = E::OK;
      return;
    }
    ld_check(err != E::WOULDBLOCK);
    if (err == E::NOTFOUND) {
      // timestamp was not found because no records bigger then trim point
      // found.
      status_ = E::OK;
      return;
    } else if (err == E::NOTSUPPORTED) {
      status_ = E::NOTSUPPORTED;
    }
    status_ = E::FAILED;
  }

  void onDone() override {
    send_reply(reply_to_,
               header_,
               status_,
               {trim_point_.value(), trim_point_timestamp_});
  }

  void onDropped() override {
    send_reply(reply_to_, header_, E::DROPPED);
  }

 private:
  GET_HEAD_ATTRIBUTES_Header header_;
  Address reply_to_;
  folly::Optional<lsn_t> trim_point_;
  Status status_;
  std::chrono::milliseconds trim_point_timestamp_;
};
} // namespace

Message::Disposition
GET_HEAD_ATTRIBUTES_onReceived(GET_HEAD_ATTRIBUTES_Message* msg,
                               const Address& from) {
  const GET_HEAD_ATTRIBUTES_Header& header = msg->getHeader();

  if (header.log_id == LOGID_INVALID) {
    ld_error("got GET_HEAD_ATTRIBUTES message from %s with invalid log ID, "
             "ignoring",
             Sender::describeConnection(from).c_str());
    return Message::Disposition::NORMAL;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (!worker->isAcceptingWork()) {
    ld_debug("Ignoring GET_HEAD_ATTRIBUTES message: not accepting more work");
    send_reply(from, header, E::SHUTDOWN);
    return Message::Disposition::NORMAL;
  }

  ServerProcessor* processor = worker->processor_;
  if (!processor->runningOnStorageNode()) {
    send_reply(from, header, E::NOTSTORAGE);
    return Message::Disposition::NORMAL;
  }

  ShardedStorageThreadPool* sstp = processor->sharded_storage_thread_pool_;

  const shard_size_t n_shards = worker->getNodesConfiguration()->getNumShards();
  shard_index_t shard_idx = header.shard;
  if (shard_idx >= n_shards) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Got GET_HEAD_ATTRIBUTES message from client %s with "
                    "invalid shard %u, this node only has %u shards",
                    Sender::describeConnection(from).c_str(),
                    shard_idx,
                    n_shards);
    return Message::Disposition::NORMAL;
  }

  LocalLogStore& store = sstp->getByIndex(shard_idx).getLocalLogStore();

  if (store.acceptingWrites() == E::DISABLED) {
    // Trim point data can be outdated on this node and can not be trusted.
    send_reply(from, header, E::FAILED);
    return Message::Disposition::NORMAL;
  }

  if (processor->isDataMissingFromShard(shard_idx)) {
    send_reply(from, header, E::REBUILDING);
    return Message::Disposition::NORMAL;
  }

  LogStorageStateMap& map = processor->getLogStorageStateMap();
  LogStorageState* log_state = map.insertOrGet(header.log_id, shard_idx);

  folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
  if (trim_point.hasValue() && Worker::settings().allow_reads_on_workers) {
    // Try to read approximate timestamp of trim point without reading from
    // disk as well.
    std::chrono::milliseconds timestamp;
    int rv = store.getApproximateTimestamp(header.log_id,
                                           trim_point.value() + 1,
                                           false, // not allow_blocking_io
                                           &timestamp);
    if (rv == 0) {
      send_reply(from, header, E::OK, {trim_point.value(), timestamp});
      return Message::Disposition::NORMAL;
    }
    if (err != E::WOULDBLOCK) {
      if (err == E::NOTFOUND) {
        // timestamp was not found because no records bigger than trim point
        // found.
        send_reply(from,
                   header,
                   E::OK,
                   {trim_point.value(), std::chrono::milliseconds::max()});
        return Message::Disposition::NORMAL;
      } else if (err == E::NOTSUPPORTED) {
        send_reply(from, header, E::NOTSUPPORTED);
        return Message::Disposition::NORMAL;
      }
      send_reply(from, header, E::FAILED);
      return Message::Disposition::NORMAL;
    }
    ld_check(err == E::WOULDBLOCK);
    // Non blocking read has failed. Continue execution to create storage task.
  }
  auto task =
      std::make_unique<GetHeadAttributesStorageTask>(header, from, trim_point);
  worker->getStorageTaskQueueForShard(shard_idx)->putTask(std::move(task));
  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
