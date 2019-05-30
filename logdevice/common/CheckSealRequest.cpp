/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/CheckSealRequest.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/CHECK_SEAL_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

CheckSealRequest::CheckSealRequest(
    std::unique_ptr<GET_SEQ_STATE_Message> message,
    Address from,
    logid_t log_id,
    std::vector<ShardID> copyset,
    epoch_t local_epoch)
    : Request(RequestType::CHECK_SEAL),
      gss_message_(std::move(message)),
      from_(from),
      log_id_(log_id),
      copyset_(std::move(copyset)),
      local_epoch_(local_epoch) {
  // This request should only be run for data logs.
  ld_check(!MetaDataLog::isMetaDataLog(log_id));
}

void CheckSealRequest::initTimer() {
  request_timer_ = std::make_unique<Timer>([this] { this->onTimeout(); });

  std::chrono::milliseconds delay(
      Worker::settings().check_seal_req_min_timeout.count());
  if ((delay.count() >
       Worker::settings().get_seq_state_reply_timeout.count() / 2) &&
      Worker::settings().get_seq_state_reply_timeout.count() / 2 > 0) {
    delay = Worker::settings().get_seq_state_reply_timeout / 2;
  }

  request_timer_->activate(delay);
}

Request::Execution CheckSealRequest::execute() {
  Worker* w = Worker::onThisThread();
  initTimer();

  auto& map = w->runningCheckSeals().map;
  map.insert(std::make_pair(id_, std::unique_ptr<CheckSealRequest>(this)));

  sendCheckSeals();
  return Execution::CONTINUE;
}

void CheckSealRequest::sendCheckSeals() {
  Worker* w = Worker::onThisThread();
  auto my_nodeid = w->processor_->getMyNodeID();

  ld_spew("Sending CHECK_SEAL_Message(rqid:%lu, gss-rqid:%lu, sender:%s) for "
          "log:%lu to shards:[%s]",
          id_.val(),
          gss_message_->request_id_.val(),
          from_.id_.node_.toString().c_str(),
          log_id_.val_,
          toString(copyset_).c_str());

  for (auto to : copyset_) {
    CHECK_SEAL_Header header{id_, wave_, log_id_, my_nodeid, to.shard()};
    auto msg = std::make_unique<CHECK_SEAL_Message>(header);
    int rv = w->sender().sendMessage(std::move(msg), to.asNodeID());
    if (rv != 0) {
      // wait for timeout to send retry to client
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Failed to send CHECK_SEAL message to %s"
                     " for log:%lu, rqid:%lu, gss-rqid:%lu, err:%s",
                     to.toString().c_str(),
                     log_id_.val_,
                     id_.val(),
                     gss_message_->request_id_.val(),
                     error_name(err));
    }
  }
}

void CheckSealRequest::onSent(ShardID shard, Status status) {
  ld_spew("log:%lu, (rqid:%lu, gss-rqid:%lu) to:%s, status:%s",
          log_id_.val_,
          id_.val(),
          gss_message_->request_id_.val(),
          shard.toString().c_str(),
          error_name(status));

  switch (status) {
    case E::OK:
      return;
      break;

    case E::CONNFAILED:
    case E::PEER_CLOSED:
    case E::PEER_UNAVAILABLE:
    case E::BADMSG:
    case E::PROTO:
    case E::ACCESS:
    case E::INVALID_CLUSTER:
    case E::NOTINCONFIG:
    case E::NOSSLCONFIG:
    case E::SSLREQUIRED:
    case E::DESTINATION_MISMATCH:
    case E::CANCELLED:
    case E::INTERNAL:
    case E::TIMEDOUT:
    case E::SHUTDOWN:
      recvd_from_.push_back(shard);
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "Sending a check-seal message to %s failed with err:%s"
                     " (log:%lu, rqid:%lu, gss-rqid:%lu)",
                     shard.toString().c_str(),
                     error_name(status),
                     log_id_.val_,
                     id_.val(),
                     gss_message_->request_id_.val());
      // wait for timeout to send retry to client.
      break;

    default:
      recvd_from_.push_back(shard);
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1),
          5,
          "An unexpected error:%s occured while sending check-seal to %s",
          error_name(status),
          shard.toString().c_str());
      ld_check(false);
      // wait for timeout to send retry to client.
      break;
  }
}

void CheckSealRequest::onReply(ShardID from,
                               const CHECK_SEAL_REPLY_Message& msg) {
  recvd_from_.push_back(from);

  Status st = msg.getHeader().status;
  ld_debug("Received CHECK_SEAL_REPLY for log:%lu from %s, with "
           "rqid:%lu, status (%s), local_epoch_=%u, seal info[epoch=%u, "
           "seal_type=%d, seq_node=%s], replies_got=%zu, expecting=%zu",
           msg.getHeader().log_id.val_,
           from.toString().c_str(),
           msg.getHeader().rqid.val(),
           error_name(st),
           local_epoch_.val_,
           msg.getHeader().sealed_epoch.val_,
           (int)msg.getHeader().seal_type,
           msg.getHeader().sequencer.toString().c_str(),
           recvd_from_.size(),
           copyset_.size());

  if (st == E::OK ||
      st == E::NOTSUPPORTED) { // if check-seal is disabled on destination
    ++replies_successful_;
  } else if (st == E::FAILED || st == E::NOTSTORAGE) {
    ld_debug("Blacklisting %s for log:%lu, rqid:%lu, gss-rqid:%lu",
             from.toString().c_str(),
             msg.getHeader().log_id.val_,
             id_.val(),
             gss_message_->request_id_.val());

    gss_message_->blacklistNodeInNodeset(
        from,
        Worker::settings().disabled_retry_interval,
        NodeSetState::NotAvailableReason::STORE_DISABLED);
  }

  // If preempted, finish this request immediately.
  if (st == E::OK && local_epoch_ <= msg.getHeader().sealed_epoch) {
    ld_debug("Got %u successful replies for log:%lu, rqid:%lu, gss-rqid:%lu",
             replies_successful_,
             msg.getHeader().log_id.val_,
             id_.val(),
             gss_message_->request_id_.val());
    gss_message_->notePreempted(
        msg.getHeader().sealed_epoch, msg.getHeader().sequencer);
    gss_message_->continueExecution(from_);
    finalize();
  } else if (replies_successful_ == copyset_.size()) {
    // If all nodes have replied successfully, finish request.
    // No retry will be sent to client
    gss_message_->continueExecution(from_);
    finalize();
  } else if (recvd_from_.size() == copyset_.size()) {
    // If an attempt to connect to all nodes was made
    // but not all could reply successfully, finalize the
    // request, it will internally send a retry to the client
    // in this case
    finalize();
  }
}

void CheckSealRequest::onTimeout() {
  WORKER_STAT_INCR(check_seal_req_timedout);

  std::sort(copyset_.begin(), copyset_.end());
  std::string copyset_str = "[";
  for (auto to : copyset_) {
    copyset_str += to.toString() + " ";
  }
  copyset_str += "]";

  std::sort(recvd_from_.begin(), recvd_from_.end());
  std::string recvd_str = "[";
  for (auto to : recvd_from_) {
    recvd_str += to.toString() + " ";
  }
  recvd_str += "]";

  if (facebook::logdevice::dbg::currentLevel <
      facebook::logdevice::dbg::Level::SPEW) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        2,
        "Timed out on replies for log:%lu for CheckSealRequest(rqid:%lu, "
        "gss-rqid:%lu), expected_from:%s, recvd_from:%s, replies_successful=%d",
        log_id_.val_,
        id_.val(),
        gss_message_->request_id_.val(),
        copyset_str.c_str(),
        recvd_str.c_str(),
        replies_successful_);
  } else {
    ld_spew(
        "Timed out on replies for log:%lu for CheckSealRequest(rqid:%lu, "
        "gss-rqid:%lu), expected_from:%s, recvd_from:%s, replies_successful=%d",
        log_id_.val_,
        id_.val(),
        gss_message_->request_id_.val(),
        copyset_str.c_str(),
        recvd_str.c_str(),
        replies_successful_);
  }

  finalize();
}

void CheckSealRequest::sendRetryToClient() {
  GET_SEQ_STATE_REPLY_Header reply_hdr = {
      log_id_,
      LSN_INVALID, // last_released_lsn
      LSN_INVALID  // next_lsn
  };
  gss_message_->sendReply(from_, reply_hdr, E::AGAIN, NodeID());
}

void CheckSealRequest::finalize() {
  request_timer_->cancel();

  if (replies_successful_ < copyset_.size()) {
    sendRetryToClient();
  }

  Worker* w = Worker::onThisThread();
  auto& map = w->runningCheckSeals().map;
  auto it = map.find(id_);
  ld_check(it != map.end());
  map.erase(it); // destroys unique_ptr which owns this
}

CheckSealRequest::~CheckSealRequest() {
  ld_spew("Destroying rqid:%lu (gss-rqid:%lu) for log:%lu",
          id_.val(),
          gss_message_->request_id_.val(),
          log_id_.val_);
}

}} // namespace facebook::logdevice
