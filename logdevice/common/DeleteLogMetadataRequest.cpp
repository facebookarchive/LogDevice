/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/DeleteLogMetadataRequest.h"

#include <algorithm>
#include <iterator>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"

namespace facebook { namespace logdevice {

DeleteLogMetadataRequest::DeleteLogMetadataRequest(
    logid_t first_log_id,
    logid_t last_log_id,
    LogMetadataType log_metadata_type,
    const std::set<ShardID>& shards,
    Callback* callback)
    : FireAndForgetRequest(RequestType::DELETE_LOG_METADATA),
      first_log_id_(first_log_id),
      last_log_id_(last_log_id),
      log_metadata_type_(log_metadata_type),
      callback_(callback),
      destShards_(shards),
      numPendingReplies_{0} {
  ld_check(first_log_id_ != LOGID_INVALID);
  ld_check(last_log_id_ != LOGID_INVALID);
  ld_check(callback_ != nullptr);
}

void DeleteLogMetadataRequest::initTimer() {
  request_timer_ = std::make_unique<Timer>([this] { this->onTimeout(); });

  std::chrono::milliseconds delay(
      Worker::settings().delete_log_metadata_request_timeout.count());
  request_timer_->activate(delay);
}

void DeleteLogMetadataRequest::executionBody() {
  initTimer();
  sendMessages();
}

void DeleteLogMetadataRequest::sendMessages() {
  Worker* w = Worker::onThisThread();

  numPendingReplies_ = destShards_.size();

  for (auto sid : destShards_) {
    const DELETE_LOG_METADATA_Header header{
        first_log_id_, last_log_id_, log_metadata_type_, sid.shard(), id_};
    auto msg = std::make_unique<DELETE_LOG_METADATA_Message>(header);
    int rv = w->sender().sendMessage(std::move(msg), sid.asNodeID());
    if (rv != 0) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          5,
          "Failed to send DELETE_LOG_METADATA message %s to %s: %s",
          header.toString().c_str(),
          Sender::describeConnection(Address(sid.asNodeID())).c_str(),
          error_description(err));
      if (--numPendingReplies_ == 0) {
        finalize();
        return;
      }
    }
  }
}

void DeleteLogMetadataRequest::onTimeout() {
  ld_warning("Timeout for replies on %s with rqid:%lu"
             ", expected from: [ %s ], received from: [ %s ], successful "
             "deletions in: [ %s ]",
             describe().c_str(),
             id_.val(),
             toString(destShards_).c_str(),
             toString(recvFrom_).c_str(),
             toString(successfulDels_).c_str());
  finalize();
}

void DeleteLogMetadataRequest::onReply(ShardID sid, Status status) {
  if (destShards_.find(sid) == destShards_.end()) {
    // not expected
    ld_warning("Unexpected shard %s responded to %s "
               "with rqid:%lu. Maybe confused client?",
               sid.toString().c_str(),
               describe().c_str(),
               id_.val());
  } else {
    // logging was done in DELETE_LOG_METADATA_REPLY_Message::onReceived
    recvFrom_.insert(sid);
    if (status == E::OK) {
      successfulDels_.insert(sid);
    }

    if (--numPendingReplies_ == 0) {
      finalize();
    }
  }
}

void DeleteLogMetadataRequest::finalize() {
  request_timer_->cancel();

  {
    std::set<ShardID> noAnswer, failed;

    std::set_difference(destShards_.begin(),
                        destShards_.end(),
                        recvFrom_.begin(),
                        recvFrom_.end(),
                        std::inserter(noAnswer, noAnswer.begin()));

    std::set_difference(recvFrom_.begin(),
                        recvFrom_.end(),
                        successfulDels_.begin(),
                        successfulDels_.end(),
                        std::inserter(failed, failed.begin()));

    (*callback_)(noAnswer, failed);
  }

  destroy();
}

}} // namespace facebook::logdevice
