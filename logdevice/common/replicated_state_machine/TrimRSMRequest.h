/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <functional>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"

/**
 * TrimRSMRequest is a Request for trimming the delta and snapshot logs of a RSM
 * state machine given a retention period. The user passes a retention (example:
 * 2d), the Request will then guarantee that we keep at least 2 days worth of
 * deltas. This state machine will always trim all the snapshots but the last
 * one.
 *
 * Steps are as follows:
 *
 * 1/ Find the last released lsn in the snapshot log.
 * 2/ Read the snapshot log up to the last released lsn to find the last
 *    snapshot record:
 *      - min_snapshot_lsn_ is the lsn of the last snapshot;
 *      - snapshot_read_ptr_ is the delta log read pointer at the time we took
 *        a snapshot;
 * 3/ trim snapshot log up to min_snapshot_lsn_ - 1
 * 4/ f=findTime(delta_log_id, NOW-retention)
 * 5/ trim delta log up to min(f - 1, snapshot_read_ptr_ - 1)
 */

namespace facebook { namespace logdevice {

class TrimRSMRequest : public Request {
 public:
  using Callback = std::function<void(Status)>;

  /**
   * Create a TrimRSMRequest.
   *
   * @param delta_log_id     Id of the delta log.
   * @param snapshot_log_id  Id of the snapshot log.
   * @param timestamp        Guarantee that we don't trim deltas newer than this
   *                         timestamp.
   * @param cb               Callback to call on completion. Status is set to
   *                         E::OK on success, E::PARTIAL if some data could not
   *                         be trimmed, E::BADMSG if we encountered an invalid
   *                         snapshot, or any error that can be returned by
   *                         findTime() or trim().
   * @param worker_id        Id of the worker to run this request on.
   * @param findtime_timeout Timeout for the findtime operation.
   * @param trim_timeout     Timeout for the trim operation.
   */
  TrimRSMRequest(
      logid_t delta_log_id,
      logid_t snapshot_log_id,
      std::chrono::milliseconds timestamp,
      Callback cb,
      worker_id_t worker_id,
      WorkerType worker_type,
      RSMType rsm_type,
      bool trim_everything,
      std::chrono::milliseconds findtime_timeout = std::chrono::seconds{10},
      std::chrono::milliseconds trim_timeout = std::chrono::seconds{10})
      : Request(RequestType::FIND_KEY),
        delta_log_id_(delta_log_id),
        snapshot_log_id_(snapshot_log_id),
        min_timestamp_(timestamp),
        cb_(cb),
        worker_id_(worker_id),
        worker_type_(worker_type),
        rsm_type_(rsm_type),
        trim_everything_(trim_everything),
        findtime_timeout_(findtime_timeout),
        trim_timeout_(trim_timeout),
        callbackHelper_(this) {}

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_id_.val_;
  }

  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

  Execution execute() override;
  void snapshotFindTimeCallback(Status st, lsn_t lsn);
  void trimSnapshotLog();
  void onSnapshotTrimmed(Status st);
  void deltaFindTimeCallback(Status st, lsn_t lsn);
  void onDeltaTrimmed(Status st);

 private:
  static void extractVersionAndReadPointerFromSnapshot(DataRecord& rec,
                                                       RSMType rsm_type,
                                                       lsn_t& version,
                                                       lsn_t& read_ptr);
  void completeAndDeleteThis(Status st);

  const logid_t delta_log_id_;
  const logid_t snapshot_log_id_;
  const std::chrono::milliseconds min_timestamp_;
  const Callback cb_;
  const worker_id_t worker_id_;
  WorkerType worker_type_;
  RSMType rsm_type_;
  // Do we trim even the last snapshot?
  bool trim_everything_;
  const std::chrono::milliseconds findtime_timeout_;
  const std::chrono::milliseconds trim_timeout_;

  // Oldest snapshot we want to keep.
  std::unique_ptr<DataRecord> last_seen_snapshot_;
  // Lsn of the oldest snapshot we want to keep.
  lsn_t min_snapshot_lsn_{LSN_INVALID};
  // Version of the oldest snapshot we want to keep.
  lsn_t min_snapshot_version_{LSN_INVALID};
  // Delta log read pointer recorded in the oldest snapshot.
  lsn_t snapshot_delta_read_ptr_{LSN_INVALID};

  bool is_partial_{false};
  read_stream_id_t snapshot_log_rsid_{READ_STREAM_ID_INVALID};

  WorkerCallbackHelper<TrimRSMRequest> callbackHelper_;
};

}} // namespace facebook::logdevice
