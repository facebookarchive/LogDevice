/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/CheckImpactRequest.h"

#include "logdevice/admin/safety/CheckMetaDataLogRequest.h"
#include "logdevice/common/Processor.h"

using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {
CheckImpactRequest::CheckImpactRequest(
    ShardAuthoritativeStatusMap status_map,
    ShardSet shards,
    int operations,
    SafetyMargin safety_margin,
    std::vector<logid_t> logids_to_check,
    size_t max_in_flight,
    bool abort_on_error,
    std::chrono::milliseconds per_log_timeout,
    size_t error_sample_size,
    WorkerType worker_type,
    Callback cb)
    : Request(RequestType::CHECK_IMPACT),
      status_map_(std::move(status_map)),
      shards_(std::move(shards)),
      operations_(operations),
      safety_margin_(safety_margin),
      logids_to_check_(std::move(logids_to_check)),
      max_in_flight_(max_in_flight),
      abort_on_error_(abort_on_error),
      per_log_timeout_(per_log_timeout),
      error_sample_size_(error_sample_size),
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
  if ((operations_ & (Operation::DISABLE_WRITES | Operation::DISABLE_READS)) ==
      0) {
    callback_(Impact(E::INVALID_PARAM, Impact::ImpactResult::INVALID, {}));
    callback_called_ = true;
    return Request::Execution::COMPLETE;
  }

  auto ret = start();
  if (ret == Request::Execution::CONTINUE) {
    // Insert request into map for worker to track it
    auto insert_result =
        Worker::onThisThread()->runningCheckImpactRequests().map.insert(
            std::make_pair(id_, std::unique_ptr<CheckImpactRequest>(this)));
    ld_check(insert_result.second);
  }
  return ret;
}

Request::Execution CheckImpactRequest::start() {
  Worker* worker = Worker::onThisThread(true);
  start_time_ = std::chrono::steady_clock::now();
  // LOGID_INVALID means that we want to check the metadata logs nodeset.
  if (requestSingleLog(LOGID_INVALID) != 0) {
    ld_warning("Failed to post a CheckMetadataLogRequest to check the metadata"
               " log to the processor, aborting the safety check: %s",
               error_description(err));
    complete(err);
    return Request::Execution::COMPLETE;
  }
  std::shared_ptr<Configuration> cfg = worker->getConfiguration();
  const auto& local_logs_config = cfg->getLocalLogsConfig();
  // User-logs only
  const logsconfig::LogsConfigTree& log_tree =
      local_logs_config.getLogsConfigTree();
  const InternalLogs& internal_logs = local_logs_config.getInternalLogs();
  // If no specific log-ids are requested, we check all logs.
  if (logids_to_check_.empty()) {
    for (auto it = log_tree.logsBegin(); it != log_tree.logsEnd(); ++it) {
      logids_to_check_.push_back(logid_t(it->first));
    }
  }
  // We process the internal logs first.
  return requestAllInternalLogs(internal_logs) == 0
      ? Request::Execution::CONTINUE
      : Request::Execution::COMPLETE;
}

int CheckImpactRequest::requestSingleLog(logid_t log_id) {
  Worker* worker = Worker::onThisThread(true);
  auto ticket = metadata_check_callback_helper_.ticket();
  auto cb = [ticket](Status st,
                     int impact_result,
                     logid_t completed_log_id,
                     epoch_t error_epoch,
                     StorageSet storage_set,
                     ReplicationProperty replication) {
    ticket.postCallbackRequest([=](CheckImpactRequest* rq) {
      if (rq) {
        rq->onCheckMetadataLogResponse(st,
                                       impact_result,
                                       completed_log_id,
                                       error_epoch,
                                       storage_set,
                                       std::move(replication));
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
  auto req = std::make_unique<CheckMetaDataLogRequest>(log_id,
                                                       per_log_timeout_,
                                                       status_map_,
                                                       shards_,
                                                       operations_,
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
  }
  return rv;
}

int CheckImpactRequest::requestAllInternalLogs(
    const InternalLogs& internal_logs) {
  // Internal Logs must always be checked
  for (auto it = internal_logs.logsBegin(); it != internal_logs.logsEnd();
       ++it) {
    logid_t log_id = logid_t(it->first);
    // We fail the safety check immediately if we cannot schedule internal logs
    // to be checked.
    int rv = requestSingleLog(log_id);
    if (rv != 0) {
      ld_warning("Failed to post a CheckMetadataLogRequest "
                 "(logid=%zu *Internal Log*) to check the metadata"
                 " to the processor, aborting the safety check: %s",
                 log_id.val(),
                 error_description(err));
      complete(err);
      return -1;
    }
  }
  // We add 1 to account for the metadata nodeset check.
  ld_check(in_flight_ == internal_logs.size() + 1);
  return 0;
}

void CheckImpactRequest::requestNextBatch() {
  // We don't have enough in-flight requests to fill up the max_in_flight
  // budget. And we still have logs to check left.
  int submitted = 0;
  while (!abort_processing_ && (in_flight_ < max_in_flight_) &&
         !logids_to_check_.empty()) {
    logid_t log_id = logids_to_check_.back();
    int rv = requestSingleLog(log_id);
    if (rv != 0) {
      // We couldn't schedule this request. Will wait until the next batch
      // as long as we have requests in-flight. If not, we fail.
      // We also fail if the problem is anything other than a buffering
      // problem.
      RATELIMIT_INFO(
          std::chrono::seconds{1},
          1,
          "We cannot submit CheckMetadataLogRequest due to (%s). Deferring "
          "this until we receive more responses from the in-flight requests.",
          error_description(err));
      // Stop here, there is no point of trying more.
      break;
    }
    // Pop it out of the vector since we successfully posted that one.
    logids_to_check_.pop_back();
    submitted++;
  }
  RATELIMIT_INFO(std::chrono::seconds{5},
                 1,
                 "Submitting a batch of %i logs to be processed",
                 submitted);
}

void CheckImpactRequest::onCheckMetadataLogResponse(
    Status st,
    int impact_result,
    logid_t log_id,
    epoch_t error_epoch,
    StorageSet storage_set,
    ReplicationProperty replication) {
  ld_debug("Response from %zu: %s", log_id.val_, error_description(st));
  --in_flight_;
  ++logs_done_;
  // We will only submit new logs to be processed if the processing of this
  // current log is OK. In case of errors, we stop sending requests and we drain
  // what is in-flight already.
  // Treat as no-impact. We should submit more jobs in this case.
  if (st != E::OK || impact_result > Impact::ImpactResult::NONE) {
    // We want to ensure that we do not submit further.
    if (abort_on_error_) {
      abort_processing_ = true;
    }
    st_ = st;
    // There is a negative impact on this log. Or we cannot establish this due
    // to an error.
    //
    // In this case we will wait until all requests submitted return with
    // results and then we complete the request. This is so we can build a
    // sample of the logs that will be affected by this.
    // This is a metadata log
    if (log_id == LOGID_INVALID) {
      RATELIMIT_WARNING(
          std::chrono::seconds{5},
          1,
          "Operation is unsafe on the metadata log(s), status: %s, "
          "impact: %s.",
          error_name(st),
          Impact::toStringImpactResult(impact_result).c_str());
    } else {
      RATELIMIT_WARNING(std::chrono::seconds{5},
                        1,
                        "Operation is unsafe on log %lu%s), status: %s, "
                        "impact: %s.",
                        log_id.val_,
                        internal_logs_complete_ ? "" : " *Internal Log*",
                        error_name(st),
                        Impact::toStringImpactResult(impact_result).c_str());
    }

    impact_result_all_ |= impact_result;
    if (affected_logs_sample_.size() < error_sample_size_) {
      affected_logs_sample_.push_back(Impact::ImpactOnEpoch(
          log_id, error_epoch, storage_set, replication, impact_result));
    }
    if (log_id == LOGID_INVALID || !internal_logs_complete_) {
      internal_logs_affected_ = true;
    }
  }

  ld_debug("In-flight=%zu, abort_processing? %s",
           in_flight_,
           abort_processing_ ? "yes" : "no");
  // We need to figure whether we are going to request more logs to be checked
  // or not.
  //
  // Let's start by checking if abort was requested
  if (abort_processing_) {
    if (in_flight_ == 0) {
      // All inflight requests completed, let's complete this request
      complete(st_);
    }
    // We will wait untill we drain all in-flight requests
    return;
  }

  // Did we finish the internal logs?
  if (internal_logs_complete_) {
    // Internal logs has been checked, we should request checks for normal
    // logs.
    requestNextBatch();
  } else if (in_flight_ == 0) {
    // All internal logs and metadata nodeset has been checked and passed
    // the safety check.
    ld_info("InternalLogs has passed the safety check, checking the normal "
            "logs next");
    internal_logs_complete_ = true;
    // request the first batch of the normal logs
    requestNextBatch();
  } else {
    // Internal logs are incomplete yet, and we have inflight request. Let's
    // just wait.
    return;
  }

  // Did we fail to schedule requests?
  if (in_flight_ == 0) {
    if (logids_to_check_.empty()) {
      // We have processed everything.
      complete(st_);
      return;
    } else {
      // err should be set by requestNextBatch()
      complete(err);
      return;
    }
  }
}

void CheckImpactRequest::complete(Status st) {
  double total_time = std::chrono::duration_cast<std::chrono::duration<double>>(
                          std::chrono::steady_clock::now() - start_time_)
                          .count();
  ld_info("CheckImpactRequest completed in %.1fs, %zu logs checked",
          total_time,
          logs_done_);

  callback_(Impact(st,
                   impact_result_all_,
                   std::move(affected_logs_sample_),
                   internal_logs_affected_));
  callback_called_ = true;
  deleteThis();
};

CheckImpactRequest::~CheckImpactRequest() {
  const Worker* worker = static_cast<Worker*>(EventLoop::onThisThread());
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!callback_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    ld_check(worker->shuttingDown());
    ld_warning("CheckImpactRequest destroyed while still processing");
    callback_(Impact(E::SHUTDOWN));
    // There is no point, but just for total correctness!
    callback_called_ = true;
  }
}

void CheckImpactRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningCheckImpactRequests().map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

}} // namespace facebook::logdevice
