/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <functional>
#include <list>
#include <memory>
#include <unordered_map>
#include <vector>

#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/GetSeqStateRequest-fwd.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/SequencerRouter.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/LogTailAttributes.h"

namespace facebook { namespace logdevice {

/**
 * @file  GetSeqStateRequest queries a sequencer for the state (such as the
 *        last released LSN) of the log. It uses SequencerRouter internally to
 *        find a sequencer node.
 *        For a given log-id, there can be multiple GetSeqStateRequests,
 *        but these requests can't have same options.
 */

class EpochMetaDataMap;
class GET_SEQ_STATE_REPLY_Message;
class GetSeqStateRequest;
class TailRecord;

// A map that contains a list of GetSeqStateRequest for each log.
class GetSeqStateRequestMap {
 public:
  struct GetSeqStateRequestEntry {
    std::unique_ptr<GetSeqStateRequest> request;
  };

  explicit GetSeqStateRequestMap() {}

  /**
   * Returns list of GetSeqStateRequestEntry if present.
   * Otherwise creates an empty list and returns it.
   */
  std::list<GetSeqStateRequestEntry>& getListOfGssEntriesForLog(logid_t log_id);

  /**
   * Find the entry for request 'rqid' in map for log 'log_id'
   *
   * @return:
   *   pointer to GetSeqStateRequestEntry - if found
   *   nullptr - if not found
   */
  GetSeqStateRequestEntry* getGssEntryFromRequestId(logid_t log_id,
                                                    request_id_t rqid);

  /**
   * Delete request 'rqid' from the list corresponding
   * to 'log_id', present in the map.
   *
   * @return:
   *   true -  request was found and then deleted
   *   false - list or request was not found
   */
  bool deleteGssEntryWithRequestId(logid_t log_id, request_id_t rqid);

 private:
  // Each element of the list corresponds to a unique combination of
  // GetSeqStateRequest::Options
  std::unordered_map<logid_t, std::list<GetSeqStateRequestEntry>, logid_t::Hash>
      map_;
};

class GetSeqStateRequest : public Request, public SequencerRouter::Handler {
  class RetrySendTo : public BWAvailableCallback {
   public:
    explicit RetrySendTo(GetSeqStateRequest& request)
        : gss_request_(&request) {}

    void operator()(FlowGroup&, std::mutex&) override {
      gss_request_->onSequencerKnown(
          gss_request_->dest_, gss_request_->last_flags_);
    }

    void cancelled(Status /*st*/) override {
      gss_request_->onSequencerKnown(
          gss_request_->dest_, gss_request_->last_flags_);
    }

   private:
    GetSeqStateRequest* gss_request_;
  };

 public:
  using Result = GetSeqStateRequestResult;
  using CompletionCallback = std::function<void(Result)>;

  // Context in which GetSeqStateRequest is created
  enum class Context : uint8_t {
    // Same as context in LogStorageState.h
    // NOTE: Refer to sequence of context in LogStorageState.h before changing.
    UNKNOWN,
    CATCHUP_QUEUE,
    FINDKEY_MESSAGE,
    IS_LOG_EMPTY_MESSAGE,
    REBUILDING_THREAD,
    ROCKSDB_CF,
    START_MESSAGE,
    STORE_MESSAGE,
    GET_TRIM_POINT,
    SEAL_STORAGE_TASK,

    // Other contexts
    GET_TAIL_LSN,
    REBUILDING_PLANNER,
    SYNC_SEQUENCER,
    GET_TAIL_ATTRIBUTES,
    UNRELEASED_RECORD,
    METADATA_UTIL,
    HISTORICAL_METADATA,
    GET_TAIL_RECORD,
    READER_MONITORING,
    IS_LOG_EMPTY_V2,
    MAX,
  };

  enum class MergeType : uint8_t {
    GSS_MERGE_INTO_OLD,
    GSS_MERGE_INTO_NEW,
    GSS_MERGE_TYPE_MAX
  };

  // NOTE: if you add any options here, make sure to update
  // GetSeqStateRequest::matchOptions() to take these into account when deciding
  // which requests can be merged
  struct Options {
    Options() {}

    // If set, the request should succeed only after we were able to contact a
    // sequencer that finished recovery.
    bool wait_for_recovery{true};

    // If set, tail attributes of sequencer will be requested and passed back
    // with GET_SEQ_STATE_REPLY_Message.
    bool include_tail_attributes{false};

    // If set, epoch offset of active sequencer's epoch will be requested and
    // passed back with GET_SEQ_STATE_REPLY_Message.
    bool include_epoch_offset{false};

    // If set, sequencer will include historical epoch metadata of the log with
    // the reply
    bool include_historical_metadata{false};

    // If set, sequencer will include the tail record of the log with the
    // reply
    bool include_tail_record{false};

    // If set, sequencer will indicate whether the log is empty.
    bool include_is_log_empty{false};

    // If the caller is ok with piggy-backing on an existing request,
    // it can set this option to GSS_MERGE_INTO_OLD.
    // However, if 'GSS_MERGE_INTO_NEW' is set, the new request will be
    // executed and will replace the existing request, and the list of
    // callbacks will be reassigned to the new request.
    //
    // NOTE: GSS_MERGE_INTO_NEW is a requirement of Rebuilding, otherwise
    // other contexts don't really need to create a new request.
    MergeType merge_type{MergeType::GSS_MERGE_INTO_OLD};

    // An optional completion callback.
    // NOTE:
    // 1. This will be invoked on the posted thread and not on the calling
    //    thread. If this is unacceptable to the caller, caller should use
    //    something like WorkerCallbackHelper to post the request back to the
    //    calling thread.
    // 2. It is possible that when this callback is invoked, the object
    //    containing the callback already died. Caller is responsible for making
    //    sure that such scenarios are handled, again by using something like
    //    WorkerCallbackHelper.
    std::function<void(Result)> on_complete{};

    // The minimum epoch the sequencer should be in. If it is running in an
    // epoch below the one specified, it should reactivate
    folly::Optional<epoch_t> min_epoch;
  };

  GetSeqStateRequest(logid_t log_id, Context ctx, Options opts = Options())
      : GetSeqStateRequest(log_id,
                           ctx,
                           std::move(opts),
                           std::make_unique<SequencerRouter>(log_id, this)) {}

  ~GetSeqStateRequest() override;

  int getThreadAffinity(int nthreads) override {
    return folly::hash::twang_mix64(log_id_.val_) % nthreads;
  }

  Request::Execution execute() override;

  // Add to the list of callbacks that will hang off
  // the existing GetSeqStateRequest. These callbacks get
  // executed when the request completes.
  void addCallback(CompletionCallback cb);

  // Move list of callbacks from the 'old_req' (request which is going
  // to be replaced) to `this` request.
  void moveCallbackList(GetSeqStateRequest* old_req);

  // Called upon receiving a reply to the GET_SEQ_STATE message.
  void onReply(NodeID from, const GET_SEQ_STATE_REPLY_Message& msg);

  // Called after the message is put on the wire.
  void onSent(NodeID to, Status status);

  // Match `this` request's options with other request's options
  // This is needed to decide if requests should be merged or not.
  //
  // Currently relevant options to match are:
  // - wait_for_recovery
  //
  // @return
  // true  - if all relevant options match
  // false - otherwise
  bool matchOptions(const GetSeqStateRequest::Options& other_req_options);

  const Options& getOptions() const {
    return options_;
  }

  void printOptions();

  // implementation of the SequencerRouter::Handler interface
  void onSequencerKnown(NodeID dest, SequencerRouter::flags_t flags) override;
  void onSequencerRoutingFailure(Status status) override;

  logid_t getLogID() const {
    return log_id_;
  }
  GetSeqStateRequest::Context getContext() {
    return ctx_;
  }
  static std::string getContextString(GetSeqStateRequest::Context);

  /**
   * Bump context stats for all requests that were created. It
   * also includes requests that were not necessarily sent out,
   * because they got merged into existing requests.
   */
  void bumpContextStatsAllAttempts(GetSeqStateRequest::Context);

  /**
   * Bump context stats for requests that were created and actually sent out.
   */
  void bumpContextStats(GetSeqStateRequest::Context);

 private:
  GetSeqStateRequest(logid_t log_id,
                     Context ctx,
                     Options opts,
                     std::unique_ptr<SequencerRouter> router)
      : Request(RequestType::GET_SEQ_STATE),
        log_id_(log_id),
        ctx_(ctx),
        options_(std::move(opts)),
        router_(std::move(router)),
        on_bw_avail_cb_(*this) {
    setRequestType(SRRequestType::GET_SEQ_STATE_REQ_TYPE);
    ld_check(options_.merge_type < MergeType::GSS_MERGE_TYPE_MAX);
  }

  void finalize();
  void onReplyTimeout(NodeID node);
  void retrySending();

  virtual void setupTimers();
  virtual void cancelBackoffTimer();
  virtual void resetBackoffTimer();

  // Decide if `this` request is allowed to execute
  // or an existing request should continue execution.
  //
  // @return
  // true:  If this request is allowed to execute.
  //        This happens if no merging happened,
  //        OR 'merge_type=GSS_MERGE_INTO_NEW'
  // false: otherwise
  virtual bool shouldExecute();

  // Merges two requests for the same log, if they have the
  // same options:
  // - if merge_type=GSS_MERGE_INTO_OLD, new request is merged into old,
  //   and old state machine keeps running, whereas new request is destroyed.
  // - if merge_type=GSS_MERGE_INTO_NEW, old request is merged into new,
  //   and new state machine keeps running, whereas old request is destroyed.
  //
  //  If there is no existing request with same options,
  //  `this` request's state machine is allowed to execute()
  //
  // @return
  //   true  - if requests were merged either way.
  //   false - if no merging happened.
  bool mergeRequest();

  virtual void activateReplyTimer();
  virtual void cancelReplyTimer();
  virtual void destroy();
  void executeCallbacks();

  const logid_t log_id_;
  Context ctx_{Context::UNKNOWN};
  Options options_;
  std::unique_ptr<SequencerRouter> router_;

  // Timer used to select a different node if the current one takes too long to
  // respond.
  Timer reply_timer_;

  // Timer used to retry GET_SEQ_STATE request if sequencer bringup is
  // in progress. As long as the sequencer node replies with E:AGAIN until
  // timeout, this timer is not reset. Once max time limit is reached,
  // another node will be picked.
  std::unique_ptr<ExponentialBackoffTimer> backoff_timer_{nullptr};

  // flags used to resend message to last node.
  SequencerRouter::flags_t last_flags_{0};

  // status of this request
  Status status_{E::FAILED};
  // Which node will the next GET_SEQ_STATE message be sent.
  NodeID dest_{};
  // Last known sequencer for this node.
  // Getting a valid last released LSN through GET_SEQ_STATE is
  // treated the same as receiving a RELEASE from this node.
  NodeID last_seq_{};
  // LSN of the record most recently released by the sequencer.
  lsn_t last_released_lsn_{LSN_INVALID};
  // LSN that'll be assigned to the next record received by the sequencer.
  lsn_t next_lsn_{LSN_INVALID};

  RetrySendTo on_bw_avail_cb_;

  folly::Optional<LogTailAttributes> log_tail_attributes_ = folly::none;

  folly::Optional<OffsetMap> epoch_offsets_ = folly::none;

  std::shared_ptr<const EpochMetaDataMap> metadata_map_;

  std::shared_ptr<TailRecord> tail_record_;

  folly::Optional<bool> is_log_empty_ = folly::none;

  std::vector<CompletionCallback> callback_list_;

  friend class GetSeqStateRequestTest;
  friend class MockGetSeqStateRequest;
};

}} // namespace facebook::logdevice
