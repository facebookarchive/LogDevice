/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/metadata_log/TrimDataLogRequest.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"

namespace facebook { namespace logdevice {

TrimDataLogRequest::TrimDataLogRequest(logid_t log_id,
                                       worker_id_t worker_id,
                                       std::chrono::milliseconds read_timeout,
                                       std::chrono::milliseconds trim_timeout,
                                       bool do_trim,
                                       Callback callback)
    : Request(RequestType::TRIM_DATA_LOG),
      log_id_(log_id),
      callback_(std::move(callback)),
      read_timeout_(read_timeout),
      trim_timeout_(trim_timeout),
      do_trim_(do_trim),
      current_worker_(worker_id) {}

void TrimDataLogRequest::verifyWorker() {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
}

Request::Execution TrimDataLogRequest::execute() {
  auto ssr_cb = [this](Status status,
                       NodeID,
                       lsn_t next_lsn,
                       std::unique_ptr<LogTailAttributes>,
                       std ::shared_ptr<const EpochMetaDataMap>,
                       std::shared_ptr<TailRecord>,
                       folly::Optional<bool>) {
    onTailReceived(status, (next_lsn == 0 ? 0 : next_lsn - 1));
  };

  getTailLSN(std::move(ssr_cb));
  return Request::Execution::CONTINUE;
}

void TrimDataLogRequest::getTailLSN(SyncSequencerRequest::Callback cb) {
  verifyWorker();

  // get tail lsn of the log
  auto ssr = std::make_unique<SyncSequencerRequest>(
      log_id_,
      /* flags */ 0,
      std::move(cb),
      GetSeqStateRequest::Context::METADATA_UTIL,
      read_timeout_);
  ssr->setThreadIdx(Worker::onThisThread()->idx_.val());
  std::unique_ptr<Request> rq(std::move(ssr));
  int rv = Worker::onThisThread()->processor_->postImportant(rq);
  ld_check(rv == 0);
}

void TrimDataLogRequest::onTailReceived(Status st, lsn_t tail_lsn) {
  if (st != E::OK) {
    ld_error("Unable to get tail LSN for log %lu: %s",
             log_id_.val(),
             error_description(st));
    finalizeReading(st);
    return;
  }
  tail_lsn_ = tail_lsn;

  startReader(compose_lsn(epoch_t(1), esn_t(1)), tail_lsn_);
}

void TrimDataLogRequest::startReader(lsn_t from_lsn, lsn_t until_lsn) {
  verifyWorker();
  // starting reader for the data log
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  rsid_ = processor->issueReadStreamID();

  // reference of `this' gets passed to the internal read stream, this
  // is OK as this request will outlive the read stream
  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid_,
      log_id_,
      "",
      std::bind(&TrimDataLogRequest::onDataRecord, this, std::placeholders::_1),
      std::bind(&TrimDataLogRequest::onGapRecord, this, std::placeholders::_1),
      std::bind(
          &TrimDataLogRequest::onDoneReading, this, std::placeholders::_1),
      nullptr, // metadata cache
      nullptr);

  // Create a client read stream to read the data log from LSN_OLDEST
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid_,
      log_id_,
      from_lsn,
      until_lsn,
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
  reader_timer_ = std::make_unique<Timer>([this] { onReadTimeout(); });

  reader_timer_->activate(read_timeout_);
}

void TrimDataLogRequest::onReadTimeout() {
  verifyWorker();
  ld_check(!reader_timer_->isActive());

  stopReading();
  finalize(E::TIMEDOUT, LSN_INVALID);
}

bool TrimDataLogRequest::onDataRecord(std::unique_ptr<DataRecord>& record) {
  ld_check(record->logid.val_ == log_id_.val_);
  ld_spew("Data record received for data log %lu, lsn %s.",
          record->logid.val_,
          lsn_to_string(record->attrs.lsn).c_str());

  if (reading_finalized_) {
    return true;
  }

  first_record_lsn_ = std::min(first_record_lsn_, record->attrs.lsn);

  ld_check(new_trim_point_ < record->attrs.lsn);
  new_trim_point_ = record->attrs.lsn - 1;

  finalizeReading(E::OK);
  return true;
}

bool TrimDataLogRequest::onGapRecord(const GapRecord& gap) {
  ld_check(gap.logid.val_ == log_id_.val_);
  ld_spew("Gap record received for data log %lu "
          "range [%s, %s], type %d",
          gap.logid.val_,
          lsn_to_string(gap.lo).c_str(),
          lsn_to_string(gap.hi).c_str(),
          static_cast<int>(gap.type));

  if (reading_finalized_) {
    return true;
  }

  switch (gap.type) {
    case GapType::UNKNOWN:
    case GapType::DATALOSS:
    case GapType::ACCESS:
    case GapType::NOTINCONFIG:
    case GapType::FILTERED_OUT:
      // Can't trim anything with these gaps
      ld_error("Unexpected gap type received for data log %lu "
               "range [%s, %s], type %d, unable to trim metadata log",
               gap.logid.val_,
               lsn_to_string(gap.lo).c_str(),
               lsn_to_string(gap.hi).c_str(),
               static_cast<int>(gap.type));
      finalizeReading(E::FAILED);
      break;
    case GapType::BRIDGE:
    case GapType::HOLE:
      // No data - no action necessary
      break;
    case GapType::TRIM:
      // Updating known trim point
      ld_check(current_trim_point_ < gap.hi);
      current_trim_point_ = gap.hi;
      break;
    case GapType::MAX:
      ld_check(false);
      finalizeReading(E::FAILED);
      break;
  }
  return true;
}

void TrimDataLogRequest::onDoneReading(logid_t log_id) {
  if (reading_finalized_) {
    return;
  }
  ld_check(log_id == log_id_);
  new_trim_point_ = tail_lsn_;
  finalizeReading(E::OK);
}

void TrimDataLogRequest::finalizeReading(Status st) {
  verifyWorker();

  ld_check(!reading_finalized_);
  ld_check(destroy_readstream_timer_ == nullptr);

  if (reader_timer_) {
    reader_timer_->cancel();
  }

  // use a zero timer to schedule the destruction of the read stream
  // in the next event loop iteration
  destroy_readstream_timer_ =
      std::make_unique<Timer>([this, st] { onDestroyReadStreamTimer(st); });
  destroy_readstream_timer_->activate(std::chrono::milliseconds::zero());

  reading_finalized_ = true;
}

void TrimDataLogRequest::onDestroyReadStreamTimer(Status st) {
  ld_check(reading_finalized_);

  destroy_readstream_timer_.reset();
  // destroy the data log readstream
  stopReading();

  if (st != E::OK) {
    finalize(st, LSN_INVALID);
    return;
  }
  // some extra protection for misbehaving client / storage nodes
  // that report a trim point to LSN_MAX
  if (lsn_to_epoch(new_trim_point_).val_ >= lsn_to_epoch(LSN_MAX).val_) {
    ld_error("Invalid trim point %s calculated from reading data log %lu!",
             lsn_to_string(new_trim_point_).c_str(),
             log_id_.val_);
    finalize(E::FAILED, LSN_INVALID);
    return;
  }

  if (st != E::OK) {
    finalize(st, LSN_INVALID);
    return;
  }

  considerTrimming();
}

void TrimDataLogRequest::stopReading() {
  verifyWorker();

  // destroy the client readstream reading the data log
  ld_check(rsid_ != READ_STREAM_ID_INVALID);
  Worker::onThisThread()->clientReadStreams().erase(rsid_);
  rsid_ = READ_STREAM_ID_INVALID;
}

void TrimDataLogRequest::considerTrimming() {
  if (!do_trim_ || new_trim_point_ == current_trim_point_) {
    finalize(E::OK, current_trim_point_);
    return;
  }
  ld_info("Trimming data log %lu to trim point %s (existing trim point %s, "
          "tail LSN %s)",
          log_id_.val(),
          lsn_to_string(new_trim_point_).c_str(),
          lsn_to_string(current_trim_point_).c_str(),
          lsn_to_string(tail_lsn_).c_str());

  ld_check(do_trim_);
  ld_check(new_trim_point_ <= tail_lsn_);
  ld_check(new_trim_point_ < first_record_lsn_);

  runTrim();
}

void TrimDataLogRequest::runTrim() {
  verifyWorker();
  auto request = std::make_unique<TrimRequest>(
      nullptr, log_id_, new_trim_point_, trim_timeout_, [this](Status st) {
        onTrimComplete(st);
      });

  // set the request affinity to the same worker thread
  request->setTargetWorker(current_worker_);
  request->bypassWriteTokenCheck();

  std::unique_ptr<Request> req(std::move(request));
  int rv = Worker::onThisThread()->processor_->postImportant(req);

  if (rv != 0) {
    ld_check(err == E::SHUTDOWN);
    finalize(err, LSN_INVALID);
    return;
  }
}

void TrimDataLogRequest::onTrimComplete(Status st) {
  if (st != E::OK) {
    ld_error("Failed to trim data log %lu to %s: %s",
             log_id_.val(),
             lsn_to_string(new_trim_point_).c_str(),
             error_description(st));
    finalize(E::FAILED, LSN_INVALID);
    return;
  }
  finalize(E::OK, new_trim_point_);
}

void TrimDataLogRequest::finalize(Status st, lsn_t new_trim_point) {
  ld_debug("Calling the callback for log %lu with status %s, trim point %s",
           log_id_.val(),
           error_description(st),
           lsn_to_string(new_trim_point).c_str());
  callback_(st, new_trim_point);
  delete this;
}

}} // namespace facebook::logdevice
