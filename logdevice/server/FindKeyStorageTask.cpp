/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/FindKeyStorageTask.h"

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/FindKeyTracer.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/FINDKEY_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/storage_tasks/StorageThread.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

FindKeyStorageTask::FindKeyStorageTask(
    ClientID reply_to,
    request_id_t client_rqid,
    logid_t log_id,
    std::chrono::milliseconds target_timestamp,
    folly::Optional<std::string> target_key,
    lsn_t last_released_lsn,
    lsn_t trim_point,
    FINDKEY_flags_t flags,
    std::chrono::steady_clock::time_point task_deadline,
    FindKeyTracer tracer,
    Sockaddr client_address)
    :

      StorageTask(StorageTask::Type::FINDKEY),
      reply_to_(reply_to),
      client_rqid_(client_rqid),
      log_id_(log_id),
      target_timestamp_(target_timestamp),
      target_key_(std::move(target_key)),
      last_released_lsn_(last_released_lsn),
      trim_point_(trim_point),
      flags_(flags),
      task_deadline_(task_deadline),
      tracer_(std::move(tracer)),
      client_address_(client_address) {}

void FindKeyStorageTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  executeImpl(store, storageThreadPool_->stats());
}

void FindKeyStorageTask::executeImpl(const LocalLogStore& store,
                                     StatsHolder* stats) {
  if (std::chrono::steady_clock::now() >= task_deadline_) {
    result_lo_ = LSN_INVALID;
    result_hi_ = LSN_INVALID;
    result_status_ = E::TIMEDOUT;
    STAT_INCR(stats, findkey_timedout_before_run);
    return;
  }

  int rv;
  if (target_key_.hasValue()) {
    rv = store.findKey(log_id_,
                       target_key_.value(),
                       &result_lo_,
                       &result_hi_,
                       flags_ & FINDKEY_Header::APPROXIMATE);
  } else {
    result_lo_ = trim_point_;
    result_hi_ = last_released_lsn_;

    rv = store.findTime(log_id_,
                        target_timestamp_,
                        &result_lo_,
                        &result_hi_,
                        flags_ & FINDKEY_Header::APPROXIMATE);
  }

  if (rv == 0) {
    result_status_ = E::OK;
  } else if (err == E::TIMEDOUT) {
    result_status_ = E::TIMEDOUT;
    STAT_INCR(stats, findkey_timedout_during_run);
  } else {
    result_status_ = E::FAILED;
  }
}

void FindKeyStorageTask::onDone() {
  sendReply();
}

void FindKeyStorageTask::onDropped() {
  result_status_ = E::FAILED;
  sendReply();
}

void FindKeyStorageTask::sendReply() {
  shard_index_t shard = storageThreadPool_->getShardIdx();
  FINDKEY_REPLY_Header header = {
      client_rqid_,
      result_status_,
      result_status_ == E::OK ? result_lo_ : LSN_INVALID,
      result_status_ == E::OK ? result_hi_ : LSN_INVALID,
      shard};
  tracer_.trace(header.status, header.result_lo, header.result_hi);
  Worker::onThisThread()->sender().sendMessage(
      std::make_unique<FINDKEY_REPLY_Message>(header), reply_to_);
}

void FindKeyStorageTask::getDebugInfoDetailed(
    StorageTaskDebugInfo& info) const {
  info.log_id = log_id_;
  info.client_id = reply_to_;
  info.client_address = client_address_;
  info.extra_info = folly::sformat(
      "client: {}, target timestamp: {}, target key: {}, is_approximate: {}",
      reply_to_.toString(),
      format_time(target_timestamp_),
      target_key_.value_or("none"),
      flags_ & FINDKEY_Header::APPROXIMATE);
}

}} // namespace facebook::logdevice
