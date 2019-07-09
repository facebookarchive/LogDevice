/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/common/GetSeqStateRequest.h"

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_Message.h"
#include "logdevice/common/protocol/GET_SEQ_STATE_REPLY_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

std::string
GetSeqStateRequest::getContextString(GetSeqStateRequest::Context ctx) {
  switch (ctx) {
    case GetSeqStateRequest::Context::CATCHUP_QUEUE:
      return "catchup-queue";
    case GetSeqStateRequest::Context::FINDKEY_MESSAGE:
      return "findkey";
    case GetSeqStateRequest::Context::IS_LOG_EMPTY_MESSAGE:
      return "islogempty";
    case GetSeqStateRequest::Context::REBUILDING_THREAD:
      return "rebuilding-thread";
    case GetSeqStateRequest::Context::ROCKSDB_CF:
      return "rocksdb-cf";
    case GetSeqStateRequest::Context::START_MESSAGE:
      return "start-message";
    case GetSeqStateRequest::Context::STORE_MESSAGE:
      return "store-message";
    case GetSeqStateRequest::Context::GET_TRIM_POINT:
      return "get-trimpoint";
    case GetSeqStateRequest::Context::SEAL_STORAGE_TASK:
      return "seal-storage-task";
    case GetSeqStateRequest::Context::GET_TAIL_LSN:
      return "get-tail-lsn";
    case GetSeqStateRequest::Context::REBUILDING_PLANNER:
      return "rebuilding-planner";
    case GetSeqStateRequest::Context::SYNC_SEQUENCER:
      return "sync-sequencer";
    case GetSeqStateRequest::Context::GET_TAIL_ATTRIBUTES:
      return "get-tail-attributes";
    case GetSeqStateRequest::Context::UNRELEASED_RECORD:
      return "unreleased-record";
    case GetSeqStateRequest::Context::METADATA_UTIL:
      return "metadata-util";
    case GetSeqStateRequest::Context::HISTORICAL_METADATA:
      return "historical-metadata";
    case GetSeqStateRequest::Context::GET_TAIL_RECORD:
      return "get-tail-record";
    case GetSeqStateRequest::Context::READER_MONITORING:
      return "reader-monitoring";
    case GetSeqStateRequest::Context::IS_LOG_EMPTY_V2:
      return "is-log-empty-v2";
    default:
      break;
  }

  static_assert(
      static_cast<int>(GetSeqStateRequest::Context::MAX) == 20,
      "Not in sync with GetSeqStateRequest::Context, and fix switch case "
      "above.");

  return "unknown";
}

void GetSeqStateRequest::bumpContextStatsAllAttempts(
    GetSeqStateRequest::Context ctx) {
  switch (ctx) {
    case GetSeqStateRequest::Context::CATCHUP_QUEUE:
      WORKER_STAT_INCR(get_seq_state_attempts_context_catchup_queue);
      break;
    case GetSeqStateRequest::Context::FINDKEY_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_attempts_context_findkey_message);
      break;
    case GetSeqStateRequest::Context::IS_LOG_EMPTY_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_attempts_context_islogempty_message);
      break;
    case GetSeqStateRequest::Context::REBUILDING_THREAD:
      WORKER_STAT_INCR(get_seq_state_attempts_context_rebuilding_thread);
      break;
    case GetSeqStateRequest::Context::ROCKSDB_CF:
      WORKER_STAT_INCR(get_seq_state_attempts_context_rocksdb_cf);
      break;
    case GetSeqStateRequest::Context::START_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_attempts_context_start_message);
      break;
    case GetSeqStateRequest::Context::STORE_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_attempts_context_store_message);
      break;
    case GetSeqStateRequest::Context::GET_TRIM_POINT:
      WORKER_STAT_INCR(get_seq_state_attempts_context_gettrimpoint_message);
      break;
    case GetSeqStateRequest::Context::SEAL_STORAGE_TASK:
      WORKER_STAT_INCR(get_seq_state_attempts_context_sealstoragetask);
      break;
    case GetSeqStateRequest::Context::GET_TAIL_LSN:
      WORKER_STAT_INCR(get_seq_state_attempts_context_get_tail_lsn);
      break;
    case GetSeqStateRequest::Context::REBUILDING_PLANNER:
      WORKER_STAT_INCR(get_seq_state_attempts_context_rebuilding_planner);
      break;
    case GetSeqStateRequest::Context::SYNC_SEQUENCER:
      WORKER_STAT_INCR(get_seq_state_attempts_context_sync_sequencer);
      break;
    case GetSeqStateRequest::Context::GET_TAIL_ATTRIBUTES:
      WORKER_STAT_INCR(get_seq_state_attempts_context_get_tail_attributes);
      break;
    case GetSeqStateRequest::Context::UNRELEASED_RECORD:
      WORKER_STAT_INCR(get_seq_state_attempts_context_unreleased_record);
      break;
    case GetSeqStateRequest::Context::METADATA_UTIL:
      WORKER_STAT_INCR(get_seq_state_attempts_context_metadata_util);
      break;
    case GetSeqStateRequest::Context::HISTORICAL_METADATA:
      WORKER_STAT_INCR(get_seq_state_attempts_context_historical_metadata);
      break;
    case GetSeqStateRequest::Context::GET_TAIL_RECORD:
      WORKER_STAT_INCR(get_seq_state_attempts_context_get_tail_record);
      break;
    case GetSeqStateRequest::Context::READER_MONITORING:
      WORKER_STAT_INCR(get_seq_state_attempts_context_reader_monitoring);
      break;
    case GetSeqStateRequest::Context::IS_LOG_EMPTY_V2:
      WORKER_STAT_INCR(get_seq_state_attempts_context_is_log_empty_v2);
      break;
    case GetSeqStateRequest::Context::UNKNOWN:
    default:
      WORKER_STAT_INCR(get_seq_state_attempts_context_unknown);
      break;
  }

  static_assert(
      static_cast<int>(GetSeqStateRequest::Context::MAX) == 20,
      "Not in sync with GetSeqStateRequest::Context, and fix switch case "
      "above.");
}

void GetSeqStateRequest::bumpContextStats(GetSeqStateRequest::Context ctx) {
  switch (ctx) {
    case Context::CATCHUP_QUEUE:
      WORKER_STAT_INCR(get_seq_state_unique_context_catchup_queue);
      break;
    case Context::FINDKEY_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_unique_context_findkey_message);
      break;
    case Context::IS_LOG_EMPTY_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_unique_context_islogempty_message);
      break;
    case Context::REBUILDING_THREAD:
      WORKER_STAT_INCR(get_seq_state_unique_context_rebuilding_thread);
      break;
    case Context::ROCKSDB_CF:
      WORKER_STAT_INCR(get_seq_state_unique_context_rocksdb_cf);
      break;
    case Context::START_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_unique_context_start_message);
      break;
    case Context::STORE_MESSAGE:
      WORKER_STAT_INCR(get_seq_state_unique_context_store_message);
      break;
    case Context::GET_TRIM_POINT:
      WORKER_STAT_INCR(get_seq_state_unique_context_gettrimpoint_message);
      break;
    case Context::SEAL_STORAGE_TASK:
      WORKER_STAT_INCR(get_seq_state_unique_context_sealstoragetask);
      break;
    case Context::GET_TAIL_LSN:
      WORKER_STAT_INCR(get_seq_state_unique_context_get_tail_lsn);
      break;
    case Context::REBUILDING_PLANNER:
      WORKER_STAT_INCR(get_seq_state_unique_context_rebuilding_planner);
      break;
    case Context::SYNC_SEQUENCER:
      WORKER_STAT_INCR(get_seq_state_unique_context_sync_sequencer);
      break;
    case Context::GET_TAIL_ATTRIBUTES:
      WORKER_STAT_INCR(get_seq_state_unique_context_get_tail_attributes);
      break;
    case Context::UNRELEASED_RECORD:
      WORKER_STAT_INCR(get_seq_state_unique_context_unreleased_record);
      break;
    case Context::METADATA_UTIL:
      WORKER_STAT_INCR(get_seq_state_unique_context_metadata_util);
      break;
    case Context::HISTORICAL_METADATA:
      WORKER_STAT_INCR(get_seq_state_unique_context_historical_metadata);
      break;
    case Context::GET_TAIL_RECORD:
      WORKER_STAT_INCR(get_seq_state_unique_context_get_tail_record);
      break;
    case Context::READER_MONITORING:
      WORKER_STAT_INCR(get_seq_state_unique_context_reader_monitoring);
      break;
    case Context::IS_LOG_EMPTY_V2:
      WORKER_STAT_INCR(get_seq_state_unique_context_is_log_empty_v2);
      break;
    case Context::UNKNOWN:
    default:
      WORKER_STAT_INCR(get_seq_state_unique_context_unknown);
      break;
  }

  static_assert(
      static_cast<int>(GetSeqStateRequest::Context::MAX) == 20,
      "Not in sync with GetSeqStateRequest::Context, and fix switch case "
      "above.");
}

Request::Execution GetSeqStateRequest::execute() {
  bumpContextStatsAllAttempts(ctx_);

  if (shouldExecute() == false) {
    ld_spew(
        "Not continuing with rqid:%lu for log:%lu", id_.val(), log_id_.val_);
    return Execution::COMPLETE;
  }

  WORKER_STAT_INCR(get_seq_state_unique);
  bumpContextStats(ctx_);

  setupTimers();
  ld_spew("Executing a GetSeqStateRequest (rqid:%lu, ctx:%s) for log %lu. ",
          id_.val(),
          getContextString(getContext()).c_str(),
          log_id_.val_);

  router_->start();
  return Execution::CONTINUE;
}

void GetSeqStateRequest::onReply(NodeID from,
                                 const GET_SEQ_STATE_REPLY_Message& msg) {
  // discard replies from nodes other than the latest one(to which
  // the request was sent).
  if (from.isNodeID() && dest_.isNodeID() && (from.index() != dest_.index())) {
    return;
  }

  ld_debug("Sequencer for log:%lu on %s replied with status=%s "
           "for rqid:%lu, ctx:%s, next_lsn:%s, last_released_lsn:%s,"
           "epoch_offsets:%s. Previous values "
           "[next_lsn:%s, last_released_lsn:%s]",
           log_id_.val_,
           from.toString().c_str(),
           error_name(msg.status_),
           id_.val(),
           getContextString(getContext()).c_str(),
           lsn_to_string(msg.header_.next_lsn).c_str(),
           lsn_to_string(msg.header_.last_released_lsn).c_str(),
           msg.epoch_offsets_.toString().c_str(),
           lsn_to_string(next_lsn_).c_str(),
           lsn_to_string(last_released_lsn_).c_str());

  cancelReplyTimer();
  status_ = msg.status_;

  if (status_ == E::AGAIN) {
    // if E::AGAIN, exponential backoff timer will resend the request to the
    // same node if timer hasn't expired. Timer doesn't need to be reset.
    backoff_timer_->activate();
    return;
  }

  next_lsn_ = std::max(next_lsn_, msg.header_.next_lsn);
  last_released_lsn_ =
      std::max(last_released_lsn_, msg.header_.last_released_lsn);
  if (msg.header_.flags &
      GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_ATTRIBUTES) {
    LogTailAttributes attributes;
    attributes.last_released_real_lsn =
        msg.tail_attributes_.last_released_real_lsn;
    attributes.last_timestamp = msg.tail_attributes_.last_timestamp;
    attributes.offsets = msg.tail_attributes_.offsets;
    log_tail_attributes_ = attributes;
  }

  if (msg.header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_EPOCH_OFFSET) {
    epoch_offsets_ = msg.epoch_offsets_;
  }

  if (msg.header_.flags &
      GET_SEQ_STATE_REPLY_Header::INCLUDES_HISTORICAL_METADATA) {
    metadata_map_ = msg.metadata_map_;
  }

  if (msg.header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_TAIL_RECORD) {
    tail_record_ = msg.tail_record_;
  }

  if (msg.header_.flags & GET_SEQ_STATE_REPLY_Header::INCLUDES_IS_LOG_EMPTY) {
    is_log_empty_ = msg.is_log_empty_;
  }

  switch (status_) {
    case E::REDIRECTED:
    case E::PREEMPTED:
      ld_check(msg.redirect_.isNodeID());
      resetBackoffTimer();
      router_->onRedirected(from, msg.redirect_, status_);
      break;
    case E::NOTREADY:
    case E::SHUTDOWN:
    case E::REBUILDING:
      resetBackoffTimer();
      router_->onNodeUnavailable(from, status_);
      break;
    case E::OK:
      last_seq_ = from;
      if (msg.header_.next_lsn < next_lsn_) {
        // unlikely
        RATELIMIT_WARNING(std::chrono::seconds(1),
                          1,
                          "GET_SEQ_STATE reply for log:%lu, rqid:%lu received "
                          "from:%s has lower next_lsn(%s) than already"
                          " known(%s)",
                          log_id_.val_,
                          id_.val(),
                          from.toString().c_str(),
                          lsn_to_string(msg.header_.next_lsn).c_str(),
                          lsn_to_string(next_lsn_).c_str());
      }
      finalize();
      break;
    case E::NOTFOUND:
    case E::FAILED:
      last_seq_ = from;
      finalize();
      break;
    default:
      RATELIMIT_WARNING(std::chrono::seconds(5),
                        5,
                        "ERROR: Got an unexpected error code %s from node:%s"
                        " for log:%lu",
                        error_name(status_),
                        (from.isNodeID() ? from.toString().c_str() : ""),
                        log_id_.val_);
      last_seq_ = from;
      finalize();
      break;
  }
}

void GetSeqStateRequest::onSent(NodeID to, Status status) {
  if (status == E::OK) {
    return;
  }

  if (to != dest_) {
    ld_check(dest_.isNodeID());
    ld_spew("Another GET_SEQ_STATE message was already sent to a different "
            "node, ignoring (log:%lu, to:%s, dest_:%s)",
            log_id_.val(),
            to.toString().c_str(),
            dest_.toString().c_str());
    return;
  }

  RATELIMIT_INFO(std::chrono::seconds(10),
                 10,
                 "Failed to send GET_SEQ_STATE message to %s for log:%lu."
                 " Reason: %s",
                 to.toString().c_str(),
                 log_id_.val(),
                 error_description(status));

  // The message was never sent, so there is no reply to wait for.
  // The timers will be restarted if/when we perform the next retry.
  cancelReplyTimer();
  cancelBackoffTimer();

  // If connecting to this node failed, consider picking a different one.
  if (status == E::CONNFAILED || status == E::TIMEDOUT ||
      status == E::DISABLED || status == E::UNROUTABLE ||
      status == E::NOTINCONFIG || status == E::PEER_UNAVAILABLE ||
      status == E::PEER_CLOSED) {
    // The next retry should be to a different node, so restart the
    // backoff progression assuming the new node will be responsive.
    resetBackoffTimer();
    router_->onNodeUnavailable(to, status);
    return;
  } else if (status == E::SSLREQUIRED) {
    retrySending();
    return;
  }

  status_ = status;
  finalize();
}

void GetSeqStateRequest::onSequencerKnown(NodeID dest,
                                          SequencerRouter::flags_t sr_flags) {
  last_flags_ = sr_flags;

  GET_SEQ_STATE_flags_t flags{0};
  if (sr_flags & SequencerRouter::REDIRECT_CYCLE) {
    // `dest' is part of a redirection cycle.
    // Include the NO_REDIRECT flags to break it.
    flags |= GET_SEQ_STATE_Message::NO_REDIRECT;
  }
  if (sr_flags & SequencerRouter::PREEMPTED) {
    // `dest' is known to be preempted for the epoch this record belongs to.
    // By setting this flag we're asking the node to reactivate the sequencer
    // regardless.
    flags |= GET_SEQ_STATE_Message::REACTIVATE_IF_PREEMPTED;
  }
  if (!options_.wait_for_recovery) {
    flags |= GET_SEQ_STATE_Message::DONT_WAIT_FOR_RECOVERY;
  }
  if (options_.include_tail_attributes) {
    flags |= GET_SEQ_STATE_Message::INCLUDE_TAIL_ATTRIBUTES;
  }
  if (options_.include_epoch_offset) {
    flags |= GET_SEQ_STATE_Message::INCLUDE_EPOCH_OFFSET;
  }
  if (options_.include_historical_metadata) {
    flags |= GET_SEQ_STATE_Message::INCLUDE_HISTORICAL_METADATA;
  }
  if (options_.min_epoch.hasValue()) {
    flags |= GET_SEQ_STATE_Message::MIN_EPOCH;
  }
  if (options_.include_tail_record) {
    flags |= GET_SEQ_STATE_Message::INCLUDE_TAIL_RECORD;
  }
  if (options_.include_is_log_empty) {
    flags |= GET_SEQ_STATE_Message::INCLUDE_IS_LOG_EMPTY;
  }

  // Keep track of the current destination node. In case of a timeout, we
  // might want to skip this one and let SequencerRouter pick a different
  // node.
  dest_ = dest;
  ld_debug("Sending a GSS(rqid:%lu, ctx:%s) for log %lu"
           " to node %s with flags=%u",
           id_.val(),
           getContextString(getContext()).c_str(),
           log_id_.val_,
           dest_.toString().c_str(),
           flags);

  auto* w = Worker::onThisThread();
  auto msg = std::make_unique<GET_SEQ_STATE_Message>(
      log_id_, id_, flags, ctx_, options_.min_epoch);
  /*
   * It is possible that we are attempting to send while the
   * bandwidth callback was already registered on a previous attempt
   * to send. This can happen e.g. if we received a late reply(e.g. REDIRECT)
   * from an earlier contacted node. But onReply() checks that the reply should
   * be from the last contacted node, therefore it should be ok to just
   * deactivate the callback to prevent the assert in Sender(t11848669).
   */
  on_bw_avail_cb_.deactivate();
  int rv = w->sender().sendMessage(std::move(msg), dest_, &on_bw_avail_cb_);
  if (rv != 0) {
    ld_debug("Failed to queue GET_SEQ_STATE message for log %lu for sending "
             "to %s: %s",
             log_id_.val_,
             dest_.toString().c_str(),
             error_description(err));
    if (err == E::NOTINCONFIG || err == E::UNROUTABLE || err == E::DISABLED) {
      router_->onNodeUnavailable(dest_, err);
    } else if (err == E::CBREGISTERED) {
      // onSequencerKnown() will be invoked again once bandwidth is available.
      ld_spew("Awaiting bandwidth to queue GET_SEQ_STATE message for log %lu "
              "for sending to %s: %s",
              log_id_.val_,
              dest_.toString().c_str(),
              error_description(err));
      ld_check(!backoff_timer_->isActive());
      ld_check(!reply_timer_.isActive());
    } else {
      finalize();
    }
    // `this' may already be destroyed here
    return;
  }

  activateReplyTimer();
}

void GetSeqStateRequest::onSequencerRoutingFailure(Status status) {
  RATELIMIT_WARNING(
      std::chrono::seconds(5),
      5,
      "Unable to find a sequencer node for log %lu, rqid:%lu, ctx:%s, err(%s)",
      log_id_.val_,
      id_.val(),
      getContextString(getContext()).c_str(),
      error_name(status));
  status_ = status;
  finalize();
}

void GetSeqStateRequest::onReplyTimeout(NodeID node) {
  ld_check(node.isNodeID());

  WORKER_STAT_INCR(get_seq_state_timedout);
  cancelReplyTimer();
  resetBackoffTimer();
  RATELIMIT_INFO(
      std::chrono::seconds(1),
      2,
      "Timed out waiting for GET_SEQ_STATE_REPLY for "
      "GetSeqStateRequest (rqid:%lu, ctx:%s) for log:%lu from node %s",
      id_.val(),
      getContextString(getContext()).c_str(),
      log_id_.val_,
      node.toString().c_str());

  // timeout expired before we got a reply from `node_', let's try another one
  router_->onNodeUnavailable(node, E::TIMEDOUT);
}

void GetSeqStateRequest::addCallback(
    GetSeqStateRequest::CompletionCallback cb) {
  callback_list_.push_back(cb);
}

void GetSeqStateRequest::moveCallbackList(GetSeqStateRequest* old_req) {
  callback_list_ = std::move(old_req->callback_list_);
}

bool GetSeqStateRequest::shouldExecute() {
  if (!mergeRequest()) {
    ld_spew("No merging done for rqid:%lu for log:%lu"
            ", it can continue executing",
            id_.val(),
            log_id_.val_);
    return true;
  }

  if (options_.merge_type == MergeType::GSS_MERGE_INTO_NEW) {
    ld_spew("Current request with rqid:%lu for log:%lu can continue executing"
            ", since merge type was set to GSS_MERGE_INTO_NEW",
            id_.val(),
            log_id_.val_);
    return true;
  }

  return false;
}

void GetSeqStateRequest::retrySending() {
  auto next_delay = backoff_timer_->getNextDelay().count();
  auto max_delay = Worker::settings().seq_state_backoff_time.max_delay.count();

  cancelReplyTimer();

  // If exponential timer is not expired, retry the request
  // to the same destination
  if (next_delay < max_delay) {
    // clear reactivation flag on retry to the same node
    last_flags_ &= ~GET_SEQ_STATE_Message::REACTIVATE_IF_PREEMPTED;
    WORKER_STAT_INCR(get_seq_state_resent);
    ld_spew("Retrying GetSeqStateRequest (log:%lu, rqid:%lu, ctx:%s,"
            " flags:%u) to %s. next:%lums, max:%lums",
            log_id_.val(),
            id_.val(),
            getContextString(getContext()).c_str(),
            last_flags_,
            dest_.toString().c_str(),
            next_delay,
            max_delay);
    onSequencerKnown(dest_, last_flags_); // will activate the backoff timer
  } else {
    ld_debug("BackoffTimer for GetSeqStateRequest (log:%lu, rqid:%lu,"
             " ctx:%s) is giving up on %s. next:%lums, max:%lums",
             log_id_.val(),
             id_.val(),
             getContextString(getContext()).c_str(),
             dest_.toString().c_str(),
             next_delay,
             max_delay);
    status_ = E::TIMEDOUT;
    finalize();
  }
}

void GetSeqStateRequest::setupTimers() {
  auto on_reply_timeout = [this] { onReplyTimeout(dest_); };
  reply_timer_.assign(on_reply_timeout);
  // reply_timer_ is activated after sending a message

  // setting up exponential backoff timer
  backoff_timer_ = std::make_unique<ExponentialBackoffTimer>(
      std::function<void()>(), Worker::settings().seq_state_backoff_time);
  ld_check(backoff_timer_ != nullptr);
  backoff_timer_->setCallback(
      std::bind(&GetSeqStateRequest::retrySending, this));
}

void GetSeqStateRequest::activateReplyTimer() {
  ld_assert(reply_timer_.isAssigned());
  reply_timer_.activate(Worker::settings().get_seq_state_reply_timeout);
}

void GetSeqStateRequest::cancelReplyTimer() {
  ld_assert(reply_timer_.isAssigned());
  reply_timer_.cancel();
}

void GetSeqStateRequest::cancelBackoffTimer() {
  backoff_timer_->cancel();
}

void GetSeqStateRequest::resetBackoffTimer() {
  backoff_timer_->reset();
}

void GetSeqStateRequest::destroy() {
  backoff_timer_->cancel();

  // destroy this GetSeqStateRequest object
  auto& requestMap = Worker::onThisThread()->runningGetSeqState();
  bool deleted = requestMap.deleteGssEntryWithRequestId(log_id_, id_);
  ld_check(deleted);
}

void GetSeqStateRequest::executeCallbacks() {
  uint64_t num_cbs = 0;
  if (callback_list_.size() > 1) {
    RATELIMIT_INFO(
        std::chrono::minutes(1),
        1,
        "Number of callbacks to execute=%lu for log:%lu, rqid:%lu, ctx:%s",
        callback_list_.size(),
        log_id_.val_,
        id_.val(),
        getContextString(getContext()).c_str());
  }

  // Since the request executes on a dedicated thread, but the callbacks can
  // be executed on the thread which created the request, we can have
  // situations where just passing the Request pointer to the calback is
  // wrong, since it can get destroyed before callback gets executed.
  // Instead, create a Result structure and pass that.
  GetSeqStateRequest::Result res = {id_,
                                    log_id_,
                                    status_,
                                    last_seq_,
                                    last_released_lsn_,
                                    next_lsn_,
                                    log_tail_attributes_,
                                    epoch_offsets_,
                                    metadata_map_,
                                    tail_record_,
                                    is_log_empty_};
  for (auto& cb : callback_list_) {
    ld_debug("Executing callback #%lu for log:%lu, rqid:%lu, ctx:%s",
             ++num_cbs,
             log_id_.val_,
             id_.val(),
             getContextString(getContext()).c_str());
    cb(res);
  }
}

GetSeqStateRequest::~GetSeqStateRequest() {
  ld_spew("Destroying GetSeqStateRequest rqid:%lu for log:%lu",
          id_.val(),
          log_id_.val_);
}

void GetSeqStateRequest::finalize() {
  SCOPE_EXIT {
    destroy();
  };

  if (status_ != E::OK) {
    RATELIMIT_WARNING(
        std::chrono::seconds(5),
        5,
        "GET_SEQ_STATE for log %lu (rqid:%lu, ctx:%s) returned %s",
        log_id_.val_,
        id_.val(),
        getContextString(getContext()).c_str(),
        error_description(status_));
  }

  executeCallbacks();
}

void GetSeqStateRequest::printOptions() {
  auto opts = getOptions();
  ld_info("log:%lu, rqid:%lu, wait_for_recovery:%s, merge_type:%s",
          log_id_.val_,
          id_.val(),
          opts.wait_for_recovery ? "true" : "false",
          (opts.merge_type == MergeType::GSS_MERGE_INTO_OLD)
              ? "merge-into-old"
              : "merge-into-new");
}

GetSeqStateRequestMap::GetSeqStateRequestEntry*
GetSeqStateRequestMap::getGssEntryFromRequestId(logid_t log_id,
                                                request_id_t rqid) {
  auto it = map_.find(log_id);
  if (it != map_.end()) {
    for (auto& gss_entry : it->second) {
      ld_check(gss_entry.request->getLogID() == log_id);
      if (rqid == gss_entry.request->id_) {
        return &gss_entry;
      }
    }
  }

  // no such request present
  ld_spew("Didn't find rqid:%lu for log:%lu", rqid.val(), log_id.val_);
  return nullptr;
}

bool GetSeqStateRequestMap::deleteGssEntryWithRequestId(logid_t log_id,
                                                        request_id_t rqid) {
  auto it = map_.find(log_id);
  if (it != map_.end()) {
    auto& list_gss_entries = it->second;
    std::list<GetSeqStateRequestEntry>::iterator gss_entry_it;

    for (gss_entry_it = list_gss_entries.begin();
         gss_entry_it != list_gss_entries.end();
         ++gss_entry_it) {
      ld_check(gss_entry_it->request->getLogID() == log_id);
      if (gss_entry_it->request->id_ == rqid) {
        ld_spew("Deleting GetSeqStateRequest rqid:%lu for log:%lu",
                gss_entry_it->request->id_.val(),
                log_id.val_);
        gss_entry_it = list_gss_entries.erase(gss_entry_it);
        return true;
      }
    }
  }

  ld_spew("Didn't find any GetSeqStateRequest with rqid:%lu for log:%lu",
          rqid.val(),
          log_id.val_);
  return false;
}

std::list<GetSeqStateRequestMap::GetSeqStateRequestEntry>&
GetSeqStateRequestMap::getListOfGssEntriesForLog(logid_t log_id) {
  auto it = map_.find(log_id);
  if (it != map_.end()) {
    return it->second;
  }

  std::list<GetSeqStateRequestMap::GetSeqStateRequestEntry> gss_list;
  auto res = map_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(log_id),
                          std::forward_as_tuple(std::move(gss_list)));
  ld_check(res.second);
  return res.first->second;
}

bool GetSeqStateRequest::matchOptions(
    const GetSeqStateRequest::Options& other_req_options) {
  // feeble attempt at detecting changes to Options
  static_assert(sizeof(GetSeqStateRequest::Options) == 48,
                "please makes sure matchOptions() includes comparisons for all "
                "relevant options");
#define OPTION_EQ(o) (options_.o == other_req_options.o)
  return OPTION_EQ(wait_for_recovery) && OPTION_EQ(include_tail_attributes) &&
      OPTION_EQ(include_epoch_offset) &&
      OPTION_EQ(include_historical_metadata) && OPTION_EQ(min_epoch);
#undef OPTION_EQ
}

bool GetSeqStateRequest::mergeRequest() {
  auto& map = Worker::onThisThread()->runningGetSeqState();
  auto& list_req = map.getListOfGssEntriesForLog(log_id_);

  for (auto& gss_rq_entry : list_req) {
    // call a matching function to check if we already have a
    // GetSeqStateRequest that matches `this` request's options
    bool merging_possible = gss_rq_entry.request->matchOptions(options_);
    if (merging_possible) {
      ld_spew("Request options of this rqid:%lu match with"
              " old rqid:%lu for log:%lu. Merging possible.",
              id_.val(),
              gss_rq_entry.request->id_.val(),
              log_id_.val_);

      if (options_.merge_type ==
          GetSeqStateRequest::MergeType::GSS_MERGE_INTO_NEW) {
        // request creator doesn't want to merge into existing request,
        // therefore moving old callbacks into the new request.
        std::unique_ptr<GetSeqStateRequest> new_request(this);
        ld_spew("Replacing request_id:%lu with request_id:%lu for log:%lu",
                gss_rq_entry.request->id_.val(),
                new_request->id_.val(),
                log_id_.val_);

        new_request->moveCallbackList(gss_rq_entry.request.get());
        // replace existing request
        gss_rq_entry.request = std::move(new_request);

        WORKER_STAT_INCR(get_seq_state_merge_into_new);
      } else {
        // there is already an existing request with same options running and
        // `this` request's creator is ok to piggyback on the existing request
        WORKER_STAT_INCR(get_seq_state_merge_into_existing);
      }

      if (options_.on_complete) {
        gss_rq_entry.request->addCallback(options_.on_complete);
      }

      WORKER_STAT_INCR(get_seq_state_merged_total);
      return true;
    }
  }

  // No existing request exists for this log for the same options.
  // Let's create a new request and insert in the map
  ld_spew("There is no existing request for log:%lu, creating one"
          " with rqid:%lu, ctx:%s",
          log_id_.val_,
          id_.val(),
          getContextString(getContext()).c_str());

  GetSeqStateRequestMap::GetSeqStateRequestEntry req_entry{
      std::unique_ptr<GetSeqStateRequest>(this)};

  if (options_.on_complete) {
    req_entry.request->addCallback(options_.on_complete);
  }

  list_req.push_back(std::move(req_entry));
  return false;
}

}} // namespace facebook::logdevice
