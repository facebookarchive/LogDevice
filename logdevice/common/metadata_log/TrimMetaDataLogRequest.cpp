/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/metadata_log/TrimMetaDataLogRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Semaphore.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/metadata_log/TrimDataLogRequest.h"

namespace facebook { namespace logdevice {

TrimMetaDataLogRequest::TrimMetaDataLogRequest(
    logid_t log_id,
    std::shared_ptr<Configuration> config,
    std::shared_ptr<Processor> processor,
    std::chrono::milliseconds read_timeout,
    std::chrono::milliseconds trim_timeout,
    bool do_trim_data_log,
    Callback callback,
    std::chrono::seconds metadata_log_record_time_grace_period,
    bool trim_all_UNSAFE)
    : Request(RequestType::TRIM_METADATA_LOG),
      state_(State::INVALID),
      log_id_(log_id),
      config_(std::move(config)),
      processor_(std::move(processor)),
      callback_(std::move(callback)),
      read_timeout_(read_timeout),
      trim_timeout_(trim_timeout),
      do_trim_data_log_(do_trim_data_log),
      metadata_log_record_time_grace_period_(
          metadata_log_record_time_grace_period),
      trim_all_UNSAFE_(trim_all_UNSAFE) {
  ld_check(log_id != LOGID_INVALID);
  // must operate on a data log
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  ld_check(processor_ != nullptr);
  ld_check(callback_ != nullptr);
}

TrimMetaDataLogRequest::~TrimMetaDataLogRequest() {
  ld_check(current_worker_.val_ == -1 ||
           current_worker_ == Worker::onThisThread()->idx_);
  if (meta_reader_) {
    Worker::onThisThread()->disposeOfMetaReader(std::move(meta_reader_));
  }
}

Request::Execution TrimMetaDataLogRequest::execute() {
  ld_check(state_ == State::INVALID);
  // Worker thread on which the request is running
  current_worker_ = Worker::onThisThread()->idx_;

  // if start delay is specified, start a timer to defer starting the
  // entire operation
  if (start_delay_ > std::chrono::milliseconds::zero()) {
    start_delay_timer_ = std::make_unique<Timer>([this] { start(); });

    start_delay_timer_->activate(start_delay_);
    return Execution::CONTINUE;
  }

  start();
  return Execution::CONTINUE;
}

void TrimMetaDataLogRequest::start() {
  // get backlog duration of the data log
  const std::shared_ptr<LogsConfig::LogGroupNode> log =
      config_->getLogGroupByIDShared(log_id_);
  if (!log) {
    ld_warning("Cannot determine backlog duration, log %lu not found in "
               "config!",
               log_id_.val_);
    complete(E::NOTINCONFIG);
    return;
  }

  backlog_ = log->attrs().backlogDuration().value();
  // directly read metadata log in trim_all_UNSAFE mode
  if (!backlog_.hasValue() && !trim_all_UNSAFE_) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "There is no backlog duration found in log config for "
                   "log %lu. Fall back to trim by reading data log.",
                   log_id_.val_);

    state_ = State::READ_DATALOG_TRIMGAP;
    readTrimGapDataLog();
    return;
  }

  // The data log has backlog duration, trim by just reading metadata log
  state_ = State::READ_METADATA_LOG;
  readMetaDataLog();
}

void TrimMetaDataLogRequest::complete(Status st) {
  if (st == E::OK || st == E::UPTODATE) {
    // if the final status is E::OK or E::UPTODATE, the state machine must
    // end up in State::FINISHED state
    ld_check(state_ == State::FINISHED);
  } else if (st != E::NOTINCONFIG) {
    ld_error("TrimMetaDataLogRequest for log %lu failed with status "
             "(%s) in state %s.",
             log_id_.val_,
             error_name(st),
             getStateName(state_));
  }

  // call user provided callback
  callback_(st, log_id_);

  // destroy the request
  delete this;
}

void TrimMetaDataLogRequest::readTrimGapDataLog() {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_DATALOG_TRIMGAP);

  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;

  std::unique_ptr<Request> req = std::make_unique<TrimDataLogRequest>(
      log_id_,
      current_worker_,
      read_timeout_,
      trim_timeout_,
      do_trim_data_log_,
      std::bind(&TrimMetaDataLogRequest::onDataLogTrimComplete,
                this,
                std::placeholders::_1,
                std::placeholders::_2));
  int rv = processor->postImportant(req);
  ld_check(rv == 0);
}

void TrimMetaDataLogRequest::onDataLogTrimComplete(Status st,
                                                   lsn_t data_trim_point) {
  if (st != E::OK) {
    complete(st);
    return;
  }

  // must have a valid trim point
  ld_check(data_trim_point != LSN_INVALID);
  trim_point_datalog_ = data_trim_point;
  // advance to the next stage
  state_ = State::READ_METADATA_LOG;
  readMetaDataLog();
}

void TrimMetaDataLogRequest::readMetaDataLog() {
  ld_check(state_ == State::READ_METADATA_LOG);
  ld_check(meta_reader_ == nullptr);

  // read the whole metadata log
  meta_reader_ = std::make_unique<MetaDataLogReader>(
      log_id_,
      EPOCH_MIN,
      EPOCH_MAX,
      [this](Status st, MetaDataLogReader::Result result) {
        onEpochMetaData(st, std::move(result));
      });

  meta_reader_->start();

  // start a timer for reading the metadata log
  if (reader_timer_ == nullptr) {
    reader_timer_ = std::make_unique<Timer>([this] { onReadTimeout(); });
  } else {
    // we used the read_timer_ for data log reading
    ld_check(!backlog_.hasValue());
    ld_check(!reader_timer_->isActive());
  }
  reader_timer_->activate(read_timeout_);
}

void TrimMetaDataLogRequest::onReadTimeout() {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_DATALOG_TRIMGAP ||
           state_ == State::READ_METADATA_LOG);
  ld_check(!reader_timer_->isActive());

  switch (state_) {
    case State::READ_METADATA_LOG: {
      // we have timed out reading metadata log, but we can proceed to perform
      // trimming. the worst thing can happen is that we may trim less record,
      // which is acceptable.
      ld_info("Reading metadata log timed out for log %lu, proceed to trimming "
              "with the current metadata trim point: %s.",
              log_id_.val_,
              lsn_to_string(trim_target_metadatalog_).c_str());
      meta_reader_.reset();
      finalizeReadingMetaDataLog();
      return;
    }
    default:
      ld_critical("Unexpected state: %s.", getStateName(state_));
      ld_check(false);
      complete(E::INTERNAL);
      return;
  }
}

void TrimMetaDataLogRequest::onEpochMetaData(Status st,
                                             MetaDataLogReader::Result result) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_METADATA_LOG);
  ld_check(result.log_id == log_id_);

  if (st == E::NOTINCONFIG) {
    complete(st);
    return;
  }

  if (st != E::OK) {
    ld_warning("Fetch epoch metadata for epoch %u of log %lu FAILED: %s. "
               "Expect bad metadata until epoch %u",
               result.epoch_req.val_,
               log_id_.val_,
               error_description(st),
               result.epoch_until.val_);
    // we do not stop here but rather wait for metadata log reading to finish.
    // it is possible that these bad record can be trimmed away.
    if (trim_all_UNSAFE_) {
      trimAllUnsafe(std::move(result));
    }
    return;
  } else {
    ld_check(result.metadata->isValid());
    ld_spew("Got metadata for epoch %u for log %lu, timestamp: %s, "
            "effective until %u, metadata log lsn %s, metadata: %s",
            result.epoch_req.val_,
            log_id_.val_,
            format_time(result.timestamp).c_str(),
            result.epoch_until.val_,
            lsn_to_string(result.lsn).c_str(),
            result.metadata->toString().c_str());
  }

  if (trim_all_UNSAFE_) {
    trimAllUnsafe(std::move(result));
  } else if (backlog_.hasValue()) {
    trimWithMetaDataTimestamp(std::move(result));
  } else {
    trimWithDataLog(std::move(result));
  }
}

void TrimMetaDataLogRequest::trimWithMetaDataTimestamp(
    MetaDataLogReader::Result result) {
  ld_check(result.metadata);
  ld_check(backlog_.hasValue());
  ld_check(state_ == State::READ_METADATA_LOG);

  const epoch_t epoch = result.epoch_req;
  std::chrono::milliseconds timestamp = result.timestamp;
  EpochMetaData& info = *result.metadata;
  ld_check(info.isValid());

  // basic protection against errors / bugs such as zero timestamps
  if (timestamp == std::chrono::milliseconds(0)) {
    ld_error("Encountered a zero timestamp metadata log record for "
             "log %lu, epoch %u, record lsn %s, metadata %s.",
             log_id_.val_,
             epoch.val_,
             lsn_to_string(result.lsn).c_str(),
             info.toString().c_str());
    // skip this record
    return;
  }

  const auto cutoff_timestamp =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch()) -
      std::chrono::duration_cast<std::chrono::milliseconds>(backlog_.value()) -
      std::chrono::duration_cast<std::chrono::milliseconds>(
          metadata_log_record_time_grace_period_);

  bool should_stop = (result.source == MetaDataLogReader::RecordSource::LAST);

  if (timestamp < cutoff_timestamp) {
    trim_target_metadatalog_ =
        std::max(trim_target_metadatalog_, result.lsn - 1);
    ld_debug("Move metadata log trim point to %s for log %lu. Trim upto "
             "(exclusive): record timestamp: %s, record %s, "
             "datalog backlog duration %lu s, cutoff time: %s.",
             lsn_to_string(trim_target_metadatalog_).c_str(),
             log_id_.val_,
             format_time(timestamp).c_str(),
             info.toString().c_str(),
             backlog_.value().count(),
             format_time(cutoff_timestamp).c_str());
  } else {
    // we cannot trim this record, stop reading the metadata log further and
    // prepare to perform the trim operation
    should_stop = true;
  }

  if (should_stop) {
    finalizeReadingMetaDataLog();
  }
}

void TrimMetaDataLogRequest::trimWithDataLog(MetaDataLogReader::Result result) {
  ld_check(result.metadata);
  ld_check(state_ == State::READ_METADATA_LOG);

  ld_check(!backlog_.hasValue());
  ld_check(trim_point_datalog_.hasValue());

  const epoch_t epoch = result.epoch_req;
  epoch_t until = result.epoch_until;
  EpochMetaData& info = *result.metadata;
  ld_check(info.isValid());

  // got a metadata log record whose effective epoch range is [epoch, until]
  // only trim this record if the `until' epoch is strictly smaller than the
  // epoch of data log's trim point
  if (until < lsn_to_epoch(trim_point_datalog_.value())) {
    // safe to trim this metadata log record
    if (result.lsn <= trim_target_metadatalog_) {
      ld_critical("Got out of order metadata log record of lsn %s "
                  "for log %lu, already got %s",
                  lsn_to_string(result.lsn).c_str(),
                  log_id_.val_,
                  lsn_to_string(trim_target_metadatalog_).c_str());
      complete(E::INTERNAL);
      return;
    }

    ld_debug("Prepare to trim metadata for log %lu effective from [%u, %u]."
             "\n MetaData: %s",
             log_id_.val_,
             epoch.val_,
             until.val_,
             info.toString().c_str());

    // advance the trim target
    trim_target_metadatalog_ = result.lsn;
  } else {
    // we cannot trim this record, stop reading the metadata log and
    // prepare to perform the trim operation
    finalizeReadingMetaDataLog();
  }
}

void TrimMetaDataLogRequest::trimAllUnsafe(MetaDataLogReader::Result result) {
  ld_check(trim_all_UNSAFE_);
  ld_check(state_ == State::READ_METADATA_LOG);

  // Trim every single record encountered
  trim_target_metadatalog_ = std::max(trim_target_metadatalog_, result.lsn);
  if (result.source == MetaDataLogReader::RecordSource::LAST) {
    finalizeReadingMetaDataLog();
  }
}

void TrimMetaDataLogRequest::finalizeReadingMetaDataLog() {
  ld_check(state_ == State::READ_METADATA_LOG);

  if (meta_reader_) {
    Worker::onThisThread()->disposeOfMetaReader(std::move(meta_reader_));
  }

  // cancel the reader timer
  reader_timer_.reset();

  if (trim_target_metadatalog_ == LSN_INVALID) {
    // nothing needs to be trimmed, conclude the operation with E::UPTODATE
    state_ = State::FINISHED;
    complete(E::UPTODATE);
    return;
  }

  // advance to the next stage
  state_ = State::TRIM_METADATA_LOG;
  trimMetaDataLog();
}

void TrimMetaDataLogRequest::trimMetaDataLog() {
  ld_check(state_ == State::TRIM_METADATA_LOG);
  // must have a valid trim point
  ld_check(trim_target_metadatalog_ > LSN_INVALID);

  ld_info("Trimming MetaDataLog to %s for data log %lu. Mode %s.",
          lsn_to_string(trim_target_metadatalog_).c_str(),
          log_id_.val_,
          trim_all_UNSAFE_ ? "TRIM_ALL_UNSAFE" : "NORMAL");

  auto request =
      std::make_unique<TrimRequest>(nullptr,
                                    MetaDataLog::metaDataLogID(log_id_),
                                    trim_target_metadatalog_,
                                    trim_timeout_,
                                    [this](Status st) { onTrimComplete(st); });

  // set the request affinity to the same worker thread
  request->setTargetWorker(current_worker_);
  request->bypassWriteTokenCheck();
  // This is a hack to work around SyncSequencerRequest returning underestimated
  // next_lsn for metadata logs.
  request->bypassTailLSNCheck();

  std::unique_ptr<Request> req(std::move(request));
  int rv = processor_->postWithRetrying(req);

  if (rv != 0) {
    ld_check(err == E::SHUTDOWN);
    complete(err);
    return;
  }
}

void TrimMetaDataLogRequest::onTrimComplete(Status st) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::TRIM_METADATA_LOG);

  switch (st) {
    case E::OK:
      // all done
      state_ = State::FINISHED;
      break;
    case E::NOTFOUND:
      st = E::NOTINCONFIG;
      break;
    case E::PARTIAL:
    case E::FAILED:
    case E::TIMEDOUT:
    case E::ACCESS:
      break;
    case E::TOOBIG:
      ld_error("TrimRequest said that the target lsn %s for log %lu is above "
               "log's tail lsn. This should be impossible.",
               lsn_to_string(trim_target_metadatalog_).c_str(),
               MetaDataLog::metaDataLogID(log_id_).val());
      st = E::FAILED;
      break;
    default:
      ld_critical(
          "Unexpected status returned from TrimRequest: %s", error_name(st));
      ld_check(false);
      st = E::INTERNAL;
  }

  complete(st);
}

const char* TrimMetaDataLogRequest::getStateName(State state) {
  switch (state) {
    case State::INVALID:
      return "INVALID";
    case State::READ_DATALOG_TRIMGAP:
      return "READ_DATALOG_TRIMGAP";
    case State::READ_METADATA_LOG:
      return "READ_METADATA_LOG";
    case State::TRIM_METADATA_LOG:
      return "TRIM_METADATA_LOG";
    case State::FINISHED:
      return "FINISHED";
  }

  ld_check(false);
  return "UNKNOWN";
}

}} // namespace facebook::logdevice
