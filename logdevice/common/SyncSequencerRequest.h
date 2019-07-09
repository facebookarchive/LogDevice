/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <chrono>
#include <queue>
#include <unordered_map>
#include <vector>

#include <folly/IntrusiveList.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/LogTailAttributes.h"

/**
 * @file SyncSequencerRequest.h
 *
 * SyncSequencerRequest is a Request used to synchronize with the Sequencer of a
 * log. It's basically a utility that wraps GetSeqStateRequest with a retry
 * mechanism with exponential backoff.
 *
 * The completion callback provides a `next_lsn` which provides the following
 * guarantees if the GSS merge type is GSS_MERGE_INTO_NEW:
 *
 * 1/ `next_lsn` is greater than the LSN of any record written prior to issuing
 *    the request. As such SyncSequencerRequest can be used for causal
 *    consistency, ie if you start a reader to read up to lsn `next_lsn-1` you
 *    are guaranteed that this reader will read any record written prior to
 *    issuing the request and won't block waiting for new records to be
 *    appended to the log. The reader may still block while recovery releases
 *    all records up to `next_lsn-1` for delivery;
 * 2/ If flags::WAIT_RELASED is used, all records up to LSN `next_lsn-1` are
 *    guaranteed to be released for delivery at the time the callback is called.
 *    Note that it doesn't guarantee that a ClientReadStream will be able to
 *    read them immediately: if the sequencer crashes before propagating the
 *    release to all nodes, reader may need to wait for a new sequencer to
 *    finish recovery and do another release.
 */

namespace facebook { namespace logdevice {

class SyncSequencerRequest : public Request {
 public:
  /**
   * Callback called when this request completes.
   *
   * @param status
   *   - E::OK: On success.
   *   - E::CANCELED: If isCanceled() is implemeted by a base class and returned
   *     true before this request could complete.
   *   - E::ACCESS: If sequencer node denied us access.
   *     If complete_if_access_denied_ is true, ACCESS is returned right away.
   *     If complete_if_access_denied_ is false, SyncSequencerRequest keeps
   *     trying until timeout expires (and if timeout is inifinite, ACCESS error
   *     is never returned).
   *   - E::NOTFOUND: If complete_if_log_not_found_ is true, NOTFOUND is
   *     returned if a sequencer node says that the log is not in config.
   *     complete_if_log_not_found_ is false, we keep trying until timeout
   *     expires and then return FAILED.
   *   - E::INVALID_PARAM: If prevent_metadata_logs_ is true, and the provided
   *     log ID is a metadata log, immediately complete with this status.
   *   - The following errors can be returned after timeout expires. If timeout
   *     is infinite, these error codes are not used:
   *      -- E::TIMEDOUT: We could not get successful a reply from a sequencer
   *         in time. If WAIT_RELEASED is used, this can also mean that we timed
   *         out before the Sequencer finished recovering all records up to LSN
   *         `next_lsn`.
   *      -- E::CONNFAILED: Unable to reach a sequencer node.
   *      -- E::NOSEQUENCER: Failed to determine which node runs the sequencer.
   *      -- E::FAILED: Sequencer activation failed for some other reason e.g.
   *         due to E::SYSLIMIT, E::NOBUFS, E::TOOMANY(too many activations).
   *         If complete_if_log_not_found_ is false, FAILED is also returned if
   *         the log is not in config.
   *
   * param  seq      NodeID that runs the sequencer.
   * @param next_lsn All records up to next_lsn-1 are guaranteed to be released
   *                 for delivery. And any record that was written prior to
   *                 issuing the SyncSequencerRequest are guaranteed to have
   *                 LSN < next_lsn.
   * @param tail_attributes   set as pointer to sequencer's tail attributes if
   *                          sequencer finished recovery and
   *                          INCLUDE_TAIL_ATTRIBUTES was set. nullptr otherwise
   * @param metadata_map      Historical epoch metadata from the sequencer if
   *                          it is available on sequencer and
   *                          INCLUDE_HISTORICAL_METADATA is set.
   *                          nullptr otherwise.
   * @param tail_record       tail record of the log, for tail optimized logs
   *                          the record payload is also included
   */
  using Callback =
      std::function<void(Status status,
                         NodeID seq,
                         lsn_t next_lsn,
                         std::unique_ptr<LogTailAttributes> tail_attributes,
                         std::shared_ptr<const EpochMetaDataMap> metadata_map,
                         std::shared_ptr<TailRecord> tail_record,
                         folly::Optional<bool> is_log_empty)>;

  using flags_t = uint8_t;
  // The callback will only be called once all records up to `next_lsn` are
  // released for delivery.
  static const flags_t WAIT_RELEASED = 1 << 0;
  // If set, LogTailAttributes will be fetched from sequencer and send back
  // in GET_SEQ_STATE_REPLY_Message
  static const flags_t INCLUDE_TAIL_ATTRIBUTES = 1 << 2;
  // If set, historical epoch metadata will be fetched from sequencer and send
  // back in GET_SEQ_STATE_REPLY_Message
  static const flags_t INCLUDE_HISTORICAL_METADATA = 1 << 3;
  // If set, log tail record will be fetched from sequencer and send
  // back in GET_SEQ_STATE_REPLY_Message
  static const flags_t INCLUDE_TAIL_RECORD = 1 << 4;
  // If set, include is_log_empty in GSS response.
  static const flags_t INCLUDE_IS_LOG_EMPTY = 1u << 5;

  folly::IntrusiveListHook list_hook;

  /**
   * Create a SyncSequencerRequest.
   *
   * @param logid       Log id of the Sequencer we want to synchronize with;
   * @param flags       @see flags_t;
   * @param cb          Callback to be called on completion;
   * @param ctx         context in which SyncSequencerRequest will call
   *                    GetSeqStateRequest
   * @param timeout     If greater than 0, the callback will be called with
   *                    status=E::TIMEDOUT if we cannot synchronize with
   *                    sequencer after the timeout.
   * @param merge_type  Merge type to use for GSS request
   */
  explicit SyncSequencerRequest(
      logid_t logid,
      flags_t flags,
      Callback cb,
      GetSeqStateRequest::Context ctx =
          GetSeqStateRequest::Context::SYNC_SEQUENCER,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{0},
      GetSeqStateRequest::MergeType merge_type =
          GetSeqStateRequest::MergeType::GSS_MERGE_INTO_NEW,
      folly::Optional<epoch_t> min_epoch = folly::none);

  ~SyncSequencerRequest() override {}

  Request::Execution execute() override;

  // Can be overridden if base class wants to implement a cancellation
  // mechanism.
  virtual bool isCanceled() const {
    return false;
  }

  int getThreadAffinity(int nthreads) override {
    if (!override_thread_idx_.hasValue()) {
      return folly::hash::twang_mix64(logid_.val_) % nthreads;
    }
    ld_check(override_thread_idx_.value() < nthreads);
    return override_thread_idx_.value();
  }

  // overrides the hash-based worker thread selection
  void setThreadIdx(int idx) {
    override_thread_idx_.assign(idx);
  }

  logid_t getLogID() const {
    return logid_;
  }
  folly::Optional<lsn_t> getNextLSN() const {
    return nextLsn_;
  }
  folly::Optional<lsn_t> getLastReleasedLSN() const {
    return lastReleased_;
  }
  folly::Optional<Status> getLastStatus() const {
    return lastGetSeqStateStatus_;
  }

  void setCompleteIfLogNotFound(bool value) {
    complete_if_log_not_found_ = value;
  }

  void setCompleteIfAccessDenied(bool value) {
    complete_if_access_denied_ = value;
  }

  void setPreventMetadataLogs(bool value) {
    prevent_metadata_logs_ = value;
  }

 protected:
  NodeID getLastSequencer() const {
    return last_seq_;
  }
  /**
   * @return True if we retrieved a nextLsn_ from the sequencer and - if
   * WAIT_RELEASED is used - if we also have lastReleased_ + 1 >= nextLsn_.
   */
  bool gotReleasedUntilLSN() const;

  /**
   * @return  true if we retrieved historical metadata from the sequencer
   */
  bool gotHistoricalMetaData() const;

  /**
   * @return  true if we retrieved tail record from the sequencer
   */
  bool gotTailRecord() const;

  /**
   * @return  true if the sequencer told us whether the log is empty
   */
  bool gotIsLogEmpty() const;

  /**
   * @return  if we got all result we want and should conclude the state
   *          machine. Until this function returns true, we will keep on
   *          retrying GetSeqStateRequests.
   */
  bool shouldComplete() const;

  // If the log does not exist, SyncSequencerRequest should complete with
  // E::NOTFOUND
  bool complete_if_log_not_found_{false};

  // If sequencer node replied with E::ACCESS, complete with E::ACCESS right
  // away. If false, we'll keep trying until timeout expires.
  bool complete_if_access_denied_{true};

  // If the log is a metadata log, immediately return E::INVALID_PARAM.
  bool prevent_metadata_logs_{false};

 private:
  logid_t logid_;
  uint8_t flags_;
  Callback cb_;
  GetSeqStateRequest::Context ctx_;
  WorkerCallbackHelper<SyncSequencerRequest> callbackHelper_;
  std::chrono::milliseconds timeout_;
  GetSeqStateRequest::MergeType merge_type_;
  folly::Optional<epoch_t> min_epoch_;
  folly::Optional<int> override_thread_idx_;

  // Timer for retrying GetSeqStateRequests.
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;

  // Timer for giving up after the user provided timeout.
  std::unique_ptr<Timer> timeout_timer_;

  // Updated the first time we successfully complete a GetSeqStateRequest.
  folly::Optional<lsn_t> nextLsn_;

  // Updated each time we successfully complete a GetSeqStateRequest. The state
  // machine completes when this value + 1 >= nextLsn_.
  folly::Optional<lsn_t> lastReleased_;

  folly::Optional<Status> lastGetSeqStateStatus_;

  NodeID last_seq_;

  std::unique_ptr<LogTailAttributes> log_tail_attributes_;

  std::shared_ptr<const EpochMetaDataMap> metadata_map_;

  std::shared_ptr<TailRecord> tail_record_;

  folly::Optional<bool> is_log_empty_;

  void onTimeout();
  void complete(Status status, bool delete_this = true);

  void tryAgain();
  void onGotSeqState(GetSeqStateRequest::Result res);
};

// Wrapper instead of typedef to allow forward-declaring in Worker.h
class SyncSequencerRequestList {
 public:
  using intrusive_list_t =
      folly::IntrusiveList<SyncSequencerRequest,
                           &SyncSequencerRequest::list_hook>;

  void terminateRequests() {
    list_.clear_and_dispose(std::default_delete<SyncSequencerRequest>());
  }

  ~SyncSequencerRequestList() {
    terminateRequests();
  }

  intrusive_list_t& getList() {
    return list_;
  }

 private:
  intrusive_list_t list_;
};

}} // namespace facebook::logdevice
