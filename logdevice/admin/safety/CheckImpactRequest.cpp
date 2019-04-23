/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/CheckImpactRequest.h"

#include "logdevice/admin/safety/CheckImpactForLogRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timer.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {
CheckImpactRequest::CheckImpactRequest(
    ShardAuthoritativeStatusMap status_map,
    ShardSet shards,
    configuration::StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_logs,
    bool check_internal_logs,
    folly::Optional<std::vector<logid_t>> logids_to_check,
    size_t max_in_flight,
    bool abort_on_error,
    std::chrono::milliseconds timeout,
    size_t error_sample_size,
    bool read_epoch_metadata_from_sequencer,
    WorkerType worker_type,
    Callback cb)
    : Request(RequestType::CHECK_IMPACT),
      state_(State::CHECKING_INTERNAL_LOGS),
      status_map_(std::move(status_map)),
      shards_(std::move(shards)),
      target_storage_state_(target_storage_state),
      safety_margin_(safety_margin),
      check_metadata_logs_(check_metadata_logs),
      check_internal_logs_(check_internal_logs),
      logids_to_check_(std::move(logids_to_check)),
      max_in_flight_(max_in_flight),
      abort_on_error_(abort_on_error),
      timeout_(timeout),
      per_log_timeout_(timeout / 2),
      error_sample_size_(error_sample_size),
      read_epoch_metadata_from_sequencer_(read_epoch_metadata_from_sequencer),
      worker_type_(worker_type),
      callback_(std::move(cb)),
      metadata_check_callback_helper_(this) {}

WorkerType CheckImpactRequest::workerType(Processor* processor) {
  // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
  // on whether we have background workers.
  if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
    return WorkerType::BACKGROUND;
  }
  return WorkerType::GENERAL;
}

WorkerType CheckImpactRequest::getWorkerTypeAffinity() {
  return worker_type_;
}

Request::Execution CheckImpactRequest::execute() {
  if (!shards_.empty() && target_storage_state_ == StorageState::READ_WRITE) {
    callback_(E::INVALID_PARAM, Impact(Impact::ImpactResult::INVALID, {}));
    callback_called_ = true;
    return Request::Execution::COMPLETE;
  }

  auto ret = start();
  if (ret == Request::Execution::CONTINUE) {
    timeout_timer_ = std::make_unique<Timer>(
        std::bind(&CheckImpactRequest::onTimeout, this));
    // Insert request into map for worker to track it
    auto insert_result =
        Worker::onThisThread()->runningCheckImpactRequests().map.insert(
            std::make_pair(id_, std::unique_ptr<CheckImpactRequest>(this)));
    ld_check(insert_result.second);
    activateTimeoutTimer();
  }
  return ret;
}

Request::Execution CheckImpactRequest::start() {
  Worker* worker = Worker::onThisThread(true);
  start_time_ = std::chrono::steady_clock::now();
  std::shared_ptr<Configuration> cfg = worker->getConfiguration();
  const auto& local_logs_config = cfg->getLocalLogsConfig();
  // User-logs only
  const logsconfig::LogsConfigTree& log_tree =
      local_logs_config.getLogsConfigTree();
  const InternalLogs& internal_logs = local_logs_config.getInternalLogs();
  // If no logids_to_check is none, we check all logs.
  if (!logids_to_check_) {
    logids_to_check_ = std::vector<logid_t>();
    for (auto it = log_tree.logsBegin(); it != log_tree.logsEnd(); ++it) {
      logids_to_check_->push_back(logid_t(it->first));
    }
  }

  return process();
}

int CheckImpactRequest::requestSingleLog(logid_t log_id) {
  Worker* worker = Worker::onThisThread(true);
  auto ticket = metadata_check_callback_helper_.ticket();
  auto cb = [ticket](Status st, Impact::ImpactOnEpoch impact) {
    ticket.postCallbackRequest([=](CheckImpactRequest* rq) {
      if (rq) {
        rq->onCheckImpactForLogResponse(st, impact);
      }
    });
  };
  bool is_metadata = false;
  if (log_id == LOGID_INVALID) {
    ld_debug("Checking metadata nodeset...");
    is_metadata = true;
  } else {
    ld_debug("Checking Impact on log %lu", log_id.val_);
  }
  auto req = std::make_unique<CheckImpactForLogRequest>(log_id,
                                                        per_log_timeout_,
                                                        status_map_,
                                                        shards_,
                                                        target_storage_state_,
                                                        safety_margin_,
                                                        is_metadata,
                                                        worker_type_,
                                                        cb);
  if (read_epoch_metadata_from_sequencer_) {
    req->readEpochMetaDataFromSequencer();
  }
  std::unique_ptr<Request> request = std::move(req);
  int rv = worker->processor_->postRequest(request);
  if (rv == 0) {
    in_flight_++;
  } else if (is_metadata) {
    ld_warning("Failed to post a CheckImpactForLogRequest to check the metadata"
               " log to the processor, aborting the safety check: %s",
               error_description(err));
  }
  return rv;
}

int CheckImpactRequest::requestInternalLogs() {
  Worker* worker = Worker::onThisThread(true);
  int current_in_flight = in_flight_;
  // Getting the configuration of the internal logs
  std::shared_ptr<Configuration> cfg = worker->getConfiguration();
  const auto& local_logs_config = cfg->getLocalLogsConfig();
  const auto& internal_logs = local_logs_config.getInternalLogs();

  for (auto it = internal_logs.logsBegin(); it != internal_logs.logsEnd();
       ++it) {
    logid_t log_id = logid_t(it->first);
    // We fail the safety check immediately if we cannot schedule internal logs
    // to be checked.
    int rv = requestSingleLog(log_id);
    if (rv != 0) {
      ld_warning("Failed to post a CheckImpactForLogRequest "
                 "(logid=%zu *Internal Log*) to check the metadata"
                 " to the processor, aborting the safety check: %s",
                 log_id.val(),
                 error_description(err));
      return -1;
    }
  }
  ld_check(in_flight_ == internal_logs.size() + current_in_flight);
  return 0;
}

Request::Execution CheckImpactRequest::process() {
  switch (state_) {
    case State::CHECKING_INTERNAL_LOGS:
      if (check_metadata_logs_) {
        // LOGID_INVALID means that we want to check the metadata logs nodeset.
        if (requestSingleLog(LOGID_INVALID) != 0) {
          complete(err);
          return Request::Execution::COMPLETE;
        }
      } else {
        ld_info("Not checking the metadata logs as per user request");
      }

      if (check_internal_logs_) {
        // requestInternalLogs will log if it failed.
        if (requestInternalLogs() != 0) {
          complete(err);
          return Request::Execution::COMPLETE;
        }
      } else {
        ld_info("Not checking the internal logs as per user request");
      }
      // Do we have logs in-flight?
      if (in_flight_ == 0) {
        state_ = State::CHECKING_DATA_LOGS;
        return process();
      }
      break;
    case State::CHECKING_DATA_LOGS:
      if (logids_to_check_->empty()) {
        ld_info("No data logs has been checked as per user request");
        complete(E::OK);
        return Request::Execution::COMPLETE;
      }
      requestNextBatch();
      // requestNextBatch has either failed or we don't have more work to do
      if (in_flight_ == 0) {
        complete(logids_to_check_->empty() ? E::OK : err);
        return Request::Execution::COMPLETE;
      }
      break;
    default:
      ld_check(false);
      complete(E::INTERNAL);
      return Request::Execution::COMPLETE;
  }
  return Request::Execution::CONTINUE;
}

void CheckImpactRequest::requestNextBatch() {
  // We don't have enough in-flight requests to fill up the max_in_flight
  // budget. And we still have logs to check left.
  //
  int submitted = 0;
  while (logids_to_check_ && !abort_processing_ &&
         (in_flight_ < max_in_flight_) && !logids_to_check_->empty()) {
    logid_t log_id = logids_to_check_->back();
    int rv = requestSingleLog(log_id);
    if (rv != 0) {
      // We couldn't schedule this request. Will wait until the next batch
      // as long as we have requests in-flight. If not, we fail.
      // We also fail if the problem is anything other than a buffering
      // problem.
      RATELIMIT_INFO(
          std::chrono::seconds{1},
          1,
          "We cannot submit CheckImpactForLogRequest due to (%s). Deferring "
          "this until we receive more responses from the in-flight requests.",
          error_description(err));
      // Stop here, there is no point of trying more.
      break;
    }
    // Pop it out of the vector since we successfully posted that one.
    logids_to_check_->pop_back();
    submitted++;
  }
  if (submitted > 0) {
    RATELIMIT_INFO(std::chrono::seconds{5},
                   1,
                   "Submitted a batch of %i logs to be processed",
                   submitted);
  }
}

void CheckImpactRequest::onCheckImpactForLogResponse(
    Status st,
    Impact::ImpactOnEpoch impact) {
  ld_debug("Response from %zu: %s", impact.log_id.val_, error_description(st));
  --in_flight_;
  ++logs_done_;
  // We will only submit new logs to be processed if the processing of this
  // current log is OK. In case of errors, we stop sending requests and we drain
  // what is in-flight already.
  // Treat as no-impact. We should submit more jobs in this case.
  if (st != E::OK || impact.impact_result > Impact::ImpactResult::NONE) {
    // We want to ensure that we do not submit further.
    abort_processing_ = abort_on_error_;
    st_ = st;
    // There is a negative impact on this log. Or we cannot establish this due
    // to an error.
    //
    // In this case we will wait until all requests submitted return with
    // results and then we complete the request. This is so we can build a
    // sample of the logs that will be affected by this.
    // This is a metadata log
    if (impact.log_id == LOGID_INVALID) {
      RATELIMIT_WARNING(
          std::chrono::seconds{5},
          1,
          "Operation is unsafe on the metadata log(s), status: %s, "
          "impact: %s.",
          error_name(st),
          Impact::toStringImpactResult(impact.impact_result).c_str());
    } else {
      RATELIMIT_WARNING(
          std::chrono::seconds{5},
          1,
          "Operation is unsafe on log %lu%s), status: %s, "
          "impact: %s.",
          impact.log_id.val_,
          InternalLogs::isInternal(impact.log_id) ? " *Internal Log*" : "",
          error_name(st),
          Impact::toStringImpactResult(impact.impact_result).c_str());
    }

    impact_result_all_ |= impact.impact_result;
    if (impact.log_id == LOGID_INVALID ||
        InternalLogs::isInternal(impact.log_id)) {
      internal_logs_affected_ = true;
    }

    if (affected_logs_sample_.size() < error_sample_size_) {
      affected_logs_sample_.push_back(std::move(impact));
    }
  }

  ld_debug("In-flight=%zu, abort_processing? %s",
           in_flight_,
           abort_processing_ ? "yes" : "no");
  // We need to figure whether we are going to request more logs to be checked
  // or not.
  // Let's start by checking if abort was requested
  if (abort_processing_) {
    if (in_flight_ == 0) {
      // All inflight requests completed, let's complete this request
      complete(st_);
    }
    // We will wait until we drain all in-flight requests
    return;
  }

  // Did we finish the internal logs?
  if (in_flight_ == 0 && state_ == State::CHECKING_INTERNAL_LOGS) {
    // All internal logs and metadata nodeset has been checked and passed
    // the safety check.
    ld_info("Internal logs and/or metadata logs has passed the safety check, "
            "checking the data logs next");
    state_ = State::CHECKING_DATA_LOGS;
    // process the data logs
    process();
  } else if (in_flight_ == 0 && state_ == State::CHECKING_DATA_LOGS) {
    if (logids_to_check_->empty()) {
      // We have processed everything.
      complete(st_);
      return;
    } else {
      // We still have logs to process
      process();
    }
  }
}

void CheckImpactRequest::complete(Status st) {
  std::chrono::seconds total_time =
      std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - start_time_);
  ld_info("CheckImpactRequest completed with %s in %lds, %zu logs checked",
          error_name(st),
          total_time.count(),
          logs_done_);

  if (st != E::OK) {
    // This is to ensure that we don't return ImpactResult::NONE if the
    // operation was not successful.
    callback_(st, Impact(Impact::ImpactResult::INVALID, {}, false));
  } else {
    callback_(st,
              Impact(impact_result_all_,
                     std::move(affected_logs_sample_),
                     internal_logs_affected_,
                     logs_done_,
                     total_time));
  }
  callback_called_ = true;
  deleteThis();
}

CheckImpactRequest::~CheckImpactRequest() {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("CheckImpactRequest destroyed while still processing");
    callback_(E::SHUTDOWN, Impact());
    // There is no point, but just for total correctness!
    callback_called_ = true;
  }
}

void CheckImpactRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningCheckImpactRequests().map;
  auto it = map.find(id_);
  if (it != map.end()) {
    map.erase(it); // destroys unique_ptr which owns this
  }
}

void CheckImpactRequest::activateTimeoutTimer() {
  ld_check(timeout_timer_);
  timeout_timer_->activate(timeout_);
}

void CheckImpactRequest::onTimeout() {
  ld_warning(
      "Timeout waiting for %zu in-flight CheckImpactForLogRequests to "
      "complete. We have processed %lu logs. Timeout is (%lus). Consider "
      "increasing the timeout if you "
      "have many logs and we can't process all of them fast enough.",
      in_flight_,
      logs_done_,
      std::chrono::duration_cast<std::chrono::seconds>(timeout_).count());
  complete(E::TIMEDOUT);
}

}} // namespace facebook::logdevice
