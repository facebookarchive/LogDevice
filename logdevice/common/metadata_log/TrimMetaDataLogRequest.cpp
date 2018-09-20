/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TrimMetaDataLogRequest.h"

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

namespace facebook { namespace logdevice {

TrimMetaDataLogRequest::TrimMetaDataLogRequest(
    logid_t log_id,
    std::shared_ptr<Configuration> config,
    std::shared_ptr<Processor> processor,
    std::chrono::milliseconds read_timeout,
    std::chrono::milliseconds trim_timeout,
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
    start_delay_timer_ = std::make_unique<LibeventTimer>(
        Worker::onThisThread()->getEventBase(), [this] { start(); });

    start_delay_timer_->activate(
        start_delay_, &Worker::onThisThread()->commonTimeouts());
    return Execution::CONTINUE;
  }

  start();
  return Execution::CONTINUE;
}

void TrimMetaDataLogRequest::start() {
  // get backlog duration of the data log
  const LogsConfig::LogGroupNode* log = config_->getLogGroupByIDRaw(log_id_);
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
  rsid_ = processor->issueReadStreamID();

  // reference of `this' gets passed to the internal read stream, this
  // is OK as it guarantees to outlive the read stream
  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid_,
      log_id_,
      "",
      std::bind(
          &TrimMetaDataLogRequest::onDataRecord, this, std::placeholders::_1),
      std::bind(
          &TrimMetaDataLogRequest::onGapRecord, this, std::placeholders::_1),
      std::function<void(logid_t)>(),
      nullptr, // metadata cache
      nullptr);

  // Create a client read stream to read the data log from LSN_OLDEST
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid_,
      log_id_,
      // ClientReadStream will issue a BRIDGE gap [e0n0, e1n0] since sequencers
      // never write records in epoch 0. This behavior was introduced in
      // D6924080. Because this state machine stops reading the first time it
      // encounters a gap that's not a trim gap, let's start reading from epoch
      // 1 instead of LSN_OLDEST.
      compose_lsn(epoch_t(1), esn_t(1)),
      LSN_MAX - 1, // greatest lsn possible
      Worker::settings().client_read_flow_control_threshold,
      ClientReadStreamBufferType::CIRCULAR,
      16,
      std::move(deps),
      processor->config_);

  // disable single copy delivery as we only care about the first gap
  read_stream->forceNoSingleCopyDelivery();
  // transfer ownership of the ClientReadStream to the worker and kick it
  // start
  w->clientReadStreams().insertAndStart(std::move(read_stream));

  // start a timer for reading the data log
  ld_check(reader_timer_ == nullptr);
  reader_timer_ = std::make_unique<LibeventTimer>(
      Worker::onThisThread()->getEventBase(), [this] { onReadTimeout(); });

  reader_timer_->activate(
      read_timeout_, &Worker::onThisThread()->commonTimeouts());
}

bool TrimMetaDataLogRequest::onDataRecord(std::unique_ptr<DataRecord>& record) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(record->logid.val_ == log_id_.val_);
  ld_spew("Data record received for data log %lu, lsn %s.",
          record->logid.val_,
          lsn_to_string(record->attrs.lsn).c_str());

  if (state_ != State::READ_DATALOG_TRIMGAP) {
    ld_critical("Got data record in unexpected state: %s for log %lu",
                getStateName(state_),
                log_id_.val_);
    ld_check(false);
    finalizeReadingDataLog(E::INTERNAL);
    return true;
  }

  // since we are reading from LSN_OLDEST, which does not belong to a valid
  // epoch, we must get a gap before this record and must have acted on it
  if (!reading_datalog_finalized_) {
    ld_critical("Got record of lsn %s before any gap for log %lu!",
                lsn_to_string(record->attrs.lsn).c_str(),
                log_id_.val_);
    ld_check(false);
    finalizeReadingDataLog(E::INTERNAL);
  } else {
    // ignore the record
  }
  return true;
}

bool TrimMetaDataLogRequest::onGapRecord(const GapRecord& gap) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(gap.logid.val_ == log_id_.val_);
  ld_spew("Gap record received for data log %lu "
          "range [%s, %s], type %d",
          gap.logid.val_,
          lsn_to_string(gap.lo).c_str(),
          lsn_to_string(gap.hi).c_str(),
          static_cast<int>(gap.type));

  if (state_ != State::READ_DATALOG_TRIMGAP) {
    ld_critical("Got gap record in unexpected state: %s for log %lu",
                getStateName(state_),
                log_id_.val_);
    ld_check(false);
    finalizeReadingDataLog(E::INTERNAL);
    return true;
  }

  if (reading_datalog_finalized_) {
    return true;
  }

  ld_check(!trim_point_datalog_.hasValue());
  if (gap.type == GapType::TRIM || gap.type == GapType::BRIDGE) {
    // use the right end of the first trim or bridge gap as the trim point
    trim_point_datalog_.assign(gap.hi);
    ld_debug("Trim point for data log %lu: %s",
             log_id_.val_,
             lsn_to_string(trim_point_datalog_.value()).c_str());

    // some extra protection for misbehaving client / storage nodes
    // that report a trim point to LSN_MAX
    if (lsn_to_epoch(trim_point_datalog_.value()).val_ >=
        lsn_to_epoch(LSN_MAX).val_) {
      ld_error("Invalid trim point %s got from reading data log %lu!",
               lsn_to_string(trim_point_datalog_.value()).c_str(),
               log_id_.val_);
      finalizeReadingDataLog(E::INVALID_PARAM);
      return true;
    }

    // get what we want, schedule to destroy the data log readstream
    finalizeReadingDataLog(E::OK);
  } else {
    // the first gap we got is not a trim gap, it is possible that trimming
    // has not yet happen on storage nodes yet, abort the entire operation
    // with E::UPTODATE
    ld_info("The first gap received for data log %lu "
            "range [%s, %s] is not a TRIM|BRIDGE gap but type %d. "
            "Trim will NOT be performed for its metadata log.",
            gap.logid.val_,
            lsn_to_string(gap.lo).c_str(),
            lsn_to_string(gap.hi).c_str(),
            static_cast<int>(gap.type));
    finalizeReadingDataLog(E::UPTODATE);
  }

  ld_check(reading_datalog_finalized_);
  return true;
}

void TrimMetaDataLogRequest::finalizeReadingDataLog(Status st) {
  ld_check(!reading_datalog_finalized_);
  ld_check(destroy_readstream_timer_ == nullptr);

  if (reader_timer_) {
    reader_timer_->cancel();
  }

  // use a zero timer to schedule the destruction of the read stream
  // in the next event loop iteration
  destroy_readstream_timer_ = std::make_unique<LibeventTimer>(
      Worker::onThisThread()->getEventBase(),
      [this, st] { onDestroyReadStreamTimedout(st); });
  destroy_readstream_timer_->activate(
      std::chrono::milliseconds::zero(),
      &Worker::onThisThread()->commonTimeouts());

  reading_datalog_finalized_ = true;
}

void TrimMetaDataLogRequest::onDestroyReadStreamTimedout(Status st) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_DATALOG_TRIMGAP);
  ld_check(reading_datalog_finalized_);

  destroy_readstream_timer_.reset();
  // destroy the data log readstream
  stopReadingDataLog();

  if (st != E::OK) {
    if (st == E::UPTODATE) {
      state_ = State::FINISHED;
    }
    complete(st);
    return;
  }

  // must have a valid trim point
  ld_check(trim_point_datalog_.hasValue());
  // advance to the next stage
  state_ = State::READ_METADATA_LOG;
  readMetaDataLog();
}

void TrimMetaDataLogRequest::stopReadingDataLog() {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_DATALOG_TRIMGAP);

  // destroy the client readstream reading the data log
  ld_check(rsid_ != READ_STREAM_ID_INVALID);
  Worker::onThisThread()->clientReadStreams().erase(rsid_);
  rsid_ = READ_STREAM_ID_INVALID;
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
    reader_timer_ = std::make_unique<LibeventTimer>(
        Worker::onThisThread()->getEventBase(), [this] { onReadTimeout(); });
  } else {
    // we used the read_timer_ for data log reading
    ld_check(!backlog_.hasValue());
    ld_check(!reader_timer_->isActive());
  }
  reader_timer_->activate(
      read_timeout_, &Worker::onThisThread()->commonTimeouts());
}

void TrimMetaDataLogRequest::onReadTimeout() {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(state_ == State::READ_DATALOG_TRIMGAP ||
           state_ == State::READ_METADATA_LOG);
  ld_check(!reader_timer_->isActive());

  switch (state_) {
    case State::READ_DATALOG_TRIMGAP: {
      // we have timed out reading data log, but haven't got the first gap yet,
      // safe to stop reading since we are not in the readstream callback
      stopReadingDataLog();
      complete(E::TIMEDOUT);
      return;
    }
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
    case E::PARTIAL:
    case E::FAILED:
      break;
    default:
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
