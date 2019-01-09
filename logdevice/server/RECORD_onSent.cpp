/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RECORD_onSent.h"

#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/storage/AllCachedDigests.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

namespace facebook { namespace logdevice {

void RECORD_onSent(const RECORD_Message& msg,
                   Status st,
                   const Address& to,
                   const SteadyTimestamp enqueue_time) {
  if (st != E::OK) {
    // If the message failed to send, the messaging layer will be closing the
    // socket, so there's no point in trying to send more messages into it.
    ld_debug("RECORD message to %s failed to send: %s",
             Sender::describeConnection(to).c_str(),
             error_description(st));
    return;
  }

  ServerWorker* w = ServerWorker::onThisThread();
  WORKER_TRAFFIC_CLASS_STAT_INCR(msg.tc_, record_messages_sent);
  WORKER_TRAFFIC_CLASS_STAT_ADD(
      msg.tc_, record_payload_bytes, msg.payload_.size());
  WORKER_LOG_STAT_ADD(
      msg.header_.log_id, record_payload_bytes, msg.payload_.size());
  WORKER_LOG_STAT_INCR(msg.header_.log_id, records_sent);

  if (msg.source_ == RECORD_Message::Source::CACHED_DIGEST) {
    // TODO 10173692: handle E::NOBUFS w/ traffic shaping
    WORKER_STAT_INCR(record_cache_digest_record_sent);
    WORKER_STAT_ADD(
        record_cache_digest_payload_bytes_sent, msg.payload_.size());
    w->cachedDigests().onRecordSent(to.id_.client_, msg);
  } else {
    w->serverReadStreams().onRecordSent(to.id_.client_, msg, enqueue_time);
  }
  // Bump the per-log-group stats
  if (msg.log_group_path_) {
    LOG_GROUP_TIME_SERIES_ADD(Worker::stats(),
                              record_bytes,
                              *msg.log_group_path_,
                              msg.payload_.size());
  }
}

}} // namespace facebook::logdevice
