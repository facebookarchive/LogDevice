/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/DELETE_LOG_METADATA_onReceived.h"

#include <cstring>
#include <memory>

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/protocol/DELETE_LOG_METADATA_REPLY_Message.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage/DeleteLogMetadataStorageTask.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"

namespace facebook { namespace logdevice {

namespace {
static void sendReply(const Address& to,
                      DELETE_LOG_METADATA_Header orig_header,
                      Status status) {
  RATELIMIT_INFO(
      std::chrono::seconds(1),
      10,
      "Sending DELETE_LOG_METADATA_REPLY(%s) to %s, src_rqid:%lu, status:%s",
      orig_header.toString().c_str(),
      to.toString().c_str(),
      orig_header.client_rqid.val(),
      error_name(status));
  DELETE_LOG_METADATA_REPLY_Header header;

  std::memcpy(&header, &orig_header, sizeof(orig_header));
  header.status = status;

  int rv = Worker::onThisThread()->sender().sendMessage(
      std::make_unique<DELETE_LOG_METADATA_REPLY_Message>(header), to);
  if (rv != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Failed to send DELETE_LOG_METADATA_REPLY(%s) message to %s, err:%s",
        header.toString().c_str(),
        to.id_.node_.toString().c_str(),
        error_name(err));
  }
}
} // namespace

Message::Disposition
DELETE_LOG_METADATA_onReceived(DELETE_LOG_METADATA_Message* msg,
                               const Address& from) {
  const DELETE_LOG_METADATA_Header& header = msg->getHeader();

  ServerWorker* worker = ServerWorker::onThisThread();

  RATELIMIT_INFO(
      std::chrono::seconds(1),
      10,
      "Got a DELETE_LOG_METADATA(%s) message from %s. rqid = %" PRIu64,
      header.toString().c_str(),
      Sender::describeConnection(from).c_str(),
      uint64_t(header.client_rqid));

  if (!from.isClientAddress()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        10,
        "PROTOCOL ERROR: got a DELETE_LOG_METADATA (%s) from an "
        "outgoing (server) connection to %s. DELETE_LOG_METADATA "
        "messages can only arrive from incoming (client) connections",
        header.toString().c_str(),
        Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (!worker->isAcceptingWork()) {
    RATELIMIT_WARNING(
        std::chrono::seconds(1),
        10,
        "Ignoring DELETE_LOG_METADATA(%s) message: not accepting more work",
        header.toString().c_str());
    return Message::Disposition::NORMAL;
  }

  // Check that we should even be processing this
  if (!worker->processor_->runningOnStorageNode()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got DELETE_LOG_METADATA %s from %s but not configured as "
                    "a storage node",
                    header.toString().c_str(),
                    Sender::describeConnection(from).c_str());
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  if (header.log_metadata_type == LogMetadataType::DEPRECATED_1 ||
      header.log_metadata_type == LogMetadataType::DEPRECATED_2) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got DELETE_LOG_METADATA %s from %s for deprecated "
                    "metadata type %u",
                    header.toString().c_str(),
                    Sender::describeConnection(from).c_str(),
                    (unsigned)header.log_metadata_type);
    err = E::PROTO;
    return Message::Disposition::ERROR;
  }

  auto scfg = worker->getServerConfig();

  DeleteLogMetadataWriteOp op(
      header.first_logid, header.last_logid, header.log_metadata_type);

  DeleteLogMetadataStorageTask::Callback cb = [from, op, header](Status st) {
    sendReply(from, header, st);
  };

  worker->getStorageTaskQueueForShard(header.shard_idx)
      ->putTask(std::make_unique<DeleteLogMetadataStorageTask>(op, cb));

  return Message::Disposition::NORMAL;
}
}} // namespace facebook::logdevice
