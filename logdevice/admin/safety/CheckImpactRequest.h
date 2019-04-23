/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Function.h>
#include <folly/Optional.h>

#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/configuration/Node.h"

namespace facebook { namespace logdevice {
class ReplicationProperty;
class Timer;

namespace configuration {
class InternalLogs;
}
/**
 * @file CheckImpactRequest is a safety checker request that validates
 * that it's safe to perform one or more operations on the cluster
 * without affecting read/write availability.
 *
 * There are important design decisions about this that you need to know about
 * when using it:
 *   - This safety checker will first check the metadata nodeset for safety
 *   issues. This will not continue to schedule checks for the logs unless
 *   metadata nodeset will not be affected.
 */

class CheckImpactRequest : public Request {
 public:
  using Callback = folly::Function<void(Status, Impact)>; // Result of the check
  enum class State {
    // The state machine is now checking the metadata and internal logs,
    CHECKING_INTERNAL_LOGS,
    // The state machine is now checking the data,
    CHECKING_DATA_LOGS,
  };

  CheckImpactRequest(
      ShardAuthoritativeStatusMap status_map,
      /* Can be empty, means check given current state of shards*/
      ShardSet shards,
      /* Can be READ_WRITE is shards is empty */
      configuration::StorageState target_storage_state,
      SafetyMargin safety_margin,
      /* Do we check the metadata logs too? */
      bool check_metadata_logs,
      /* Do we check the interal logs (see InternalLogs.h)? */
      bool check_internal_logs,
      /*
       * if folly::none we check all logs, if empty vector, we don't check any
       * logs, unless check_metadata_logs and/or check_internal_logs is/are set.
       */
      folly::Optional<std::vector<logid_t>> logids_to_check,
      size_t max_in_flight,
      bool abort_on_error,
      std::chrono::milliseconds timeout,
      size_t error_sample_size,
      bool read_epoch_metadata_from_sequencer,
      WorkerType worker_type,
      Callback cb);

  Request::Execution execute() override;

  /**
   * Use this function to decide whether the request will be scheduled on
   * GENERAL or BACKGROUND worker pool
   */
  static WorkerType workerType(Processor* processor);
  WorkerType getWorkerTypeAffinity() override;
  ~CheckImpactRequest() override;

 private:
  /**
   * Kicks off the safety check. This works in the following fashion:
   *   - We first submit a CheckImpactForLogRequest to process the metadata
   * logs.
   *   - Immediate after this, we submit the requests to validate the internal
   *   logs (small number of logs). Then we wait for responses to come in.
   *   - Once we validate that all internal logs are fine. We will submit a
   *   large batch of normal user logs until we fill up to the _max_in_flight_
   *   value.
   *   - On every response we receive, we will top-up the in-flight requests
   *   unless the processor is busy.
   */
  Request::Execution start();

  // Runs the processing logic of this state machine
  Request::Execution process();
  /**
   * Finalizes the request and destroy the object. This will ensure that the
   * callback is called.
   *
   * No code should be executed after calling complete()
   */
  void complete(Status st);
  /*
   * Destroys this object by removing it from the requests map on the worker
   */
  void deleteThis();
  /**
   * We send CheckImpactForLogRequest requests in batches to avoid overloading
   * the worker(s) with load that can be deferred.
   *
   * This will fill up to the _max_in_flight_ number of requests in flight. It
   * will top it up on every call.
   *
   * If we cannot schedule these requests, it's alright, we will stop trying
   * until we hear back from the inflight requests.
   *
   * If we don't have any in-flight requests and we cannot schedule a next
   * batch, then we fail the whole request and call the user callback.
   */
  void requestNextBatch();
  /**
   * This will submit the internal logs for processing. If we failed to
   * submit any logs in one go, we will fail the whole safety
   * check.
   */
  int requestInternalLogs();
  int requestSingleLog(logid_t log_id);
  void onCheckImpactForLogResponse(Status st, Impact::ImpactOnEpoch impact);
  // Starts the global timeout timer
  void activateTimeoutTimer();
  void onTimeout();

  // What's the current state in the state machine
  State state_;

  ShardAuthoritativeStatusMap status_map_;
  ShardSet shards_;
  configuration::StorageState target_storage_state_;
  SafetyMargin safety_margin_;
  bool check_metadata_logs_;
  bool check_internal_logs_;
  folly::Optional<std::vector<logid_t>> logids_to_check_;
  size_t max_in_flight_{1000};
  size_t logs_done_{0};

  int impact_result_all_{0};
  std::vector<Impact::ImpactOnEpoch> affected_logs_sample_;
  bool internal_logs_affected_{false};
  size_t in_flight_{0};

  bool abort_on_error_{true};
  std::chrono::milliseconds timeout_;
  std::chrono::milliseconds per_log_timeout_;
  size_t error_sample_size_{100};
  // TODO(T28386689): Set automatically to true once sequencers get the ability
  // to update their in-memory metadata cache after trims.
  bool read_epoch_metadata_from_sequencer_{false};
  WorkerType worker_type_;
  Callback callback_;
  WorkerCallbackHelper<CheckImpactRequest> metadata_check_callback_helper_;
  bool callback_called_{false};
  std::chrono::steady_clock::time_point start_time_;
  bool abort_processing_{false};
  // The final status of this request
  Status st_{E::OK};

  std::unique_ptr<Timer> timeout_timer_;
};
}} // namespace facebook::logdevice
