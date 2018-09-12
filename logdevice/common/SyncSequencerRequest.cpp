/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "SyncSequencerRequest.h"

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

#include <folly/Optional.h>

namespace facebook { namespace logdevice {

const SyncSequencerRequest::flags_t SyncSequencerRequest::WAIT_RELEASED;
const SyncSequencerRequest::flags_t
    SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES;
const SyncSequencerRequest::flags_t
    SyncSequencerRequest::INCLUDE_HISTORICAL_METADATA;
const SyncSequencerRequest::flags_t SyncSequencerRequest::INCLUDE_TAIL_RECORD;

SyncSequencerRequest::SyncSequencerRequest(
    logid_t logid,
    flags_t flags,
    Callback cb,
    GetSeqStateRequest::Context ctx,
    std::chrono::milliseconds timeout,
    GetSeqStateRequest::MergeType merge_type,
    folly::Optional<epoch_t> min_epoch)
    : Request(RequestType::SYNC_SEQUENCER),
      logid_(logid),
      flags_(flags),
      cb_(std::move(cb)),
      ctx_(ctx),
      callbackHelper_(this),
      timeout_(timeout),
      merge_type_(merge_type),
      min_epoch_(min_epoch) {}

Request::Execution SyncSequencerRequest::execute() {
  Worker::onThisThread()->runningSyncSequencerRequests().getList().push_back(
      *this);

  retry_timer_ = std::make_unique<ExponentialBackoffTimer>(
      Worker::onThisThread()->getEventBase(),
      [this]() { tryAgain(); },
      Worker::settings().seq_state_backoff_time);

  if (timeout_.count() > 0) {
    timeout_timer_ = std::make_unique<LibeventTimer>(
        Worker::onThisThread()->getEventBase(), [this] { this->onTimeout(); });
    timeout_timer_->activate(timeout_);
  }

  tryAgain();

  return Execution::CONTINUE;
}

void SyncSequencerRequest::tryAgain() {
  if (isCanceled()) {
    complete(E::CANCELLED);
    return;
  }

  ld_check(!shouldComplete());

  // The reply should be passed back to this worker and to onGotSeqState().
  auto callback_ticket = callbackHelper_.ticket();
  GetSeqStateRequest::Options opts;
  opts.merge_type = merge_type_;
  opts.wait_for_recovery = false;
  opts.min_epoch = min_epoch_;

  if (flags_ & INCLUDE_TAIL_ATTRIBUTES) {
    opts.include_tail_attributes = true;
  }

  if (flags_ & INCLUDE_HISTORICAL_METADATA) {
    opts.include_historical_metadata = true;
  }

  if (flags_ & INCLUDE_TAIL_RECORD) {
    opts.include_tail_record = true;
  }

  opts.on_complete = [=](GetSeqStateRequest::Result res) {
    ld_debug("GetSeqStateRequest callback called for log:%lu,"
             " id:%lu, status:%s",
             res.log_id.val_,
             res.rqid.val(),
             error_description(res.status));
    callback_ticket.postCallbackRequest([=](SyncSequencerRequest* rq) {
      if (!rq) {
        RATELIMIT_INFO(std::chrono::seconds(10),
                       1,
                       "GetSeqStateRequest finished after "
                       "SyncSequencerRequest was destroyed. log:%lu, rqid:%lu",
                       res.log_id.val(),
                       res.rqid.val());
        return;
      }
      rq->onGotSeqState(res);
    });
  };

  std::unique_ptr<Request> rq =
      std::make_unique<GetSeqStateRequest>(logid_, ctx_, opts);
  ld_debug("Posting a new GetSeqStateRequest(id:%" PRIu64 ") for log:%lu",
           (uint64_t)rq->id_,
           logid_.val_);
  auto rv = Worker::onThisThread()->processor_->postRequest(rq);
  if (rv != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        1,
        "Failed to post GetSeqStateRequest for log:%lu with error:%s",
        logid_.val_,
        error_description(err));
    complete(E::FAILED);
    return;
  }
}

void SyncSequencerRequest::onGotSeqState(GetSeqStateRequest::Result res) {
  if (isCanceled()) {
    complete(E::CANCELLED);
    return;
  }

  ld_check(!shouldComplete());

  lastGetSeqStateStatus_ = res.status;

  const dbg::Level level =
      res.status == E::OK ? dbg::Level::DEBUG : dbg::Level::ERROR;
  RATELIMIT_LEVEL(level,
                  std::chrono::seconds(1),
                  5,
                  "Got sequencer state (id:%" PRIu64 ") for log:%lu, status:%s,"
                  "sequencer:%s, last_released_lsn:%s, next_lsn:%s",
                  (uint64_t)res.rqid,
                  res.log_id.val_,
                  error_description(res.status),
                  res.last_seq.toString().c_str(),
                  lsn_to_string(res.last_released_lsn).c_str(),
                  lsn_to_string(res.next_lsn).c_str());

  if (res.status == E::NOTFOUND && complete_if_log_not_found_) {
    complete(E::NOTFOUND);
    return;
  }

  if (res.status == E::ACCESS) {
    complete(E::ACCESS);
    return;
  }

  folly::Optional<lsn_t> next_lsn;
  if (MetaDataLog::isMetaDataLog(res.log_id)) {
    // LSN_INVALID means recovery is not complete.
    if (res.last_released_lsn != LSN_INVALID) {
      // Metadata logs don't have their own next_lsns. Using last_released_lsn
      // instead.
      // TODO(#9523145): If there's a metadata log Appender running for LSN x,
      // this will set next_lsn=x instead of x+1. This may cause rebuilding to
      // miss record x in this very unlikely situation: the running Appender has
      // stored a copy on a node from rebuilding set before the node lost its
      // data, then rebuilding got next_lsn=x, then the Appender succeeded
      // without sending more waves.
      next_lsn = res.last_released_lsn + 1;
    }
  } else {
    next_lsn = res.next_lsn;
  }

  if (res.status == E::OK) {
    if (!nextLsn_.hasValue() && next_lsn.hasValue()) {
      nextLsn_ = next_lsn.value();
    }
    lastReleased_ = res.last_released_lsn;
    last_seq_ = res.last_seq;
    if (res.attributes.hasValue()) {
      log_tail_attributes_ =
          std::make_unique<LogTailAttributes>(res.attributes.value());
    } else {
      log_tail_attributes_.reset();
    }

    metadata_map_ = res.metadata_map;

    tail_record_ = res.tail_record;
  }

  if (shouldComplete()) {
    complete(E::OK);
  } else {
    // When the timer triggers we will retry GetSeqStateRequest.
    retry_timer_->activate();
  }
}

bool SyncSequencerRequest::gotReleasedUntilLSN() const {
  if (flags_ & WAIT_RELEASED) {
    // Need to keep pinging sequencer until it releases all records that we're
    // going to rebuild.
    return nextLsn_.hasValue() && lastReleased_.hasValue() &&
        lastReleased_.value() + 1 >= nextLsn_.value();
  } else {
    return nextLsn_.hasValue();
  }
}

bool SyncSequencerRequest::gotHistoricalMetaData() const {
  return metadata_map_ != nullptr;
}

bool SyncSequencerRequest::gotTailRecord() const {
  return tail_record_ != nullptr;
}

bool SyncSequencerRequest::shouldComplete() const {
  return gotReleasedUntilLSN() &&
      (!(flags_ & INCLUDE_HISTORICAL_METADATA) || gotHistoricalMetaData()) &&
      (!(flags_ & INCLUDE_TAIL_RECORD) || gotTailRecord());
}

void SyncSequencerRequest::onTimeout() {
  if (!lastGetSeqStateStatus_.hasValue()) {
    complete(E::TIMEDOUT);
    return;
  }

  Status res = lastGetSeqStateStatus_.value();
  switch (res) {
    case E::FAILED:
    case E::CONNFAILED:
    case E::NOSEQUENCER:
      break;
    case E::UNROUTABLE:
    case E::PROTONOSUPPORT:
    case E::DESTINATION_MISMATCH:
    case E::INVALID_CLUSTER:
      // These connection-related errors are reported as CONNFAILED.
      res = E::CONNFAILED;
      break;
    case E::NOTINCONFIG:
    case E::NOTREADY:
    case E::REBUILDING:
      // If these ever make it to the client, return NOSEQUENCER instead.
      res = E::NOSEQUENCER;
      break;
    case E::OK:
    default:
      // If res==E::OK, it means we timed out waiting for lastReleased_ + 1 >=
      // untilLsn_.
      res = E::TIMEDOUT;
      break;
  }

  complete(res);
}

void SyncSequencerRequest::complete(Status status) {
  ld_check(cb_);
  ld_check(status != E::INTERNAL);
  cb_(status,
      getLastSequencer(),
      nextLsn_.hasValue() ? nextLsn_.value() : LSN_INVALID,
      std::move(log_tail_attributes_),
      std::move(metadata_map_),
      std::move(tail_record_));
  delete this;
}

}} // namespace facebook::logdevice
