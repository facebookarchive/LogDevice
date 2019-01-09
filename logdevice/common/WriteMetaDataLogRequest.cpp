/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/WriteMetaDataLogRequest.h"

#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/InternalAppendRequest.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

WriteMetaDataLogRequest::WriteMetaDataLogRequest(
    logid_t log_id,
    std::shared_ptr<const EpochMetaData> epoch_store_metadata,
    callback_t callback)
    : FireAndForgetRequest(RequestType::WRITE_METADATA_LOG),
      log_id_(log_id),
      ref_holder_(this),
      epoch_store_metadata_(std::move(epoch_store_metadata)),
      callback_(std::move(callback)),
      tracer_(Worker::onThisThread()->getTraceLogger(),
              log_id_,
              MetaDataTracer::Action::WRITE_METADATA_LOG) {
  ld_check(epoch_store_metadata_);
  ld_check(epoch_store_metadata_->isValid());
  ld_check(!epoch_store_metadata_->disabled());
  ld_check(!epoch_store_metadata_->writtenInMetaDataLog());
}

void WriteMetaDataLogRequest::executionBody() {
  // serializing the payload
  checksum_bits_ = Worker::settings().checksum_bits;
  if (checksum_bits_ == 0) {
    // There's no good reason to leave metadata log records without a checksum
    checksum_bits_ = 32;
  }
  EpochMetaData metadata = *epoch_store_metadata_;

  // epoch field should contain the same value as `effective_since` in the
  // metadata log record
  metadata.h.epoch = metadata.h.effective_since;
  ld_check(!metadata.writtenInMetaDataLog());
  ld_check(!metadata.disabled());
  ld_check(metadata.isValid());
  size_t cs_size_bytes = checksum_bits_ / 8;
  size_t md_size_bytes = metadata.sizeInPayload();
  serialized_payload_.resize(cs_size_bytes + md_size_bytes);

  // serialize payload
  int rv = metadata.toPayload(
      const_cast<char*>(serialized_payload_.data()) + cs_size_bytes,
      md_size_bytes);
  ld_check(rv != -1);

  // checksum payload
  Slice checksummed(serialized_payload_.data() + cs_size_bytes, md_size_bytes);
  checksum_bytes(checksummed,
                 checksum_bits_,
                 const_cast<char*>(serialized_payload_.data()));

  tracer_.setNewMetaData(metadata);
  writeRecord();
  // the request will control its own lifetime
}

void WriteMetaDataLogRequest::writeRecord() {
  auto cfg = Worker::getConfig();
  auto& ml_conf = cfg->serverConfig()->getMetaDataLogsConfig();
  if (!ml_conf.sequencers_write_metadata_logs) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Sequencer writing of metadata logs disabled, aborting "
                    "writing to metadata log %lu",
                    MetaDataLog::metaDataLogID(log_id_).val());
    destroyRequest(E::DISABLED);
    return;
  }
  ld_debug("Writing metadata log record for log %lu: %s",
           log_id_.val(),
           epoch_store_metadata_->toString().c_str());

  // duplicate the payload, and pass the ownership to the
  // Appender eventually
  auto payload_dup =
      Payload(serialized_payload_.data(), serialized_payload_.size()).dup();
  WeakRef<WriteMetaDataLogRequest> ref = ref_holder_.ref();
  auto reply = runInternalAppend(
      MetaDataLog::metaDataLogID(log_id_),
      AppendAttributes(),
      PayloadHolder(payload_dup.data(), payload_dup.size()),
      [ref, this](Status st, lsn_t lsn, NodeID, RecordTimestamp) {
        if (!ref) {
          // WriteMetaDataLogRequest was already destroyed
          return;
        }
        onAppendResult(st, lsn);
      },
      APPEND_Header::NO_REDIRECT | APPEND_Header::NO_ACTIVATION, // flags
      checksum_bits_,
      10000, /* timeout, 10sec*/
      1,     /* append_message_count */
      epoch_store_metadata_->h.epoch /* acceptable_epoch */);
  if (reply.hasValue()) {
    // got a synchronous error
    onAppendResult(reply.value().status, LSN_INVALID);
  }
}

void WriteMetaDataLogRequest::onAppendResult(Status st, lsn_t lsn) {
  ld_info("Got metadata log write result for log %lu: %s. LSN: %s",
          log_id_.val(),
          error_description(st),
          lsn_to_string(lsn).c_str());
  tracer_.trace(st, lsn);
  switch (st) {
    case E::OK:
      // Written successfully, now we have to update the epoch store to set the
      // written bit
      setMetaDataWrittenES();
      return;
    case E::PREEMPTED:
    case E::SHUTDOWN:
    case E::ABORTED:
    case E::NOTFOUND:
    case E::NOTINSERVERCONFIG:
    case E::NOSEQUENCER:
    case E::NOTREADY:
      // another sequencer should take care of this
      destroyRequest(st);
      return;
    case E::NOSPC:
    case E::OVERLOADED:
    case E::SEQNOBUFS:
    case E::SEQSYSLIMIT:
    case E::REBUILDING:
    case E::DISABLED:
    case E::ISOLATED:
      // transient error, retry
      if (!append_retry_timer_) {
        append_retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

            [this]() { writeRecord(); },
            Worker::settings().sequencer_metadata_log_write_retry_delay);
        append_retry_timer_->randomize();
      }
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Append to metadata log %lu failed with error %s, "
                      "retrying in %lums",
                      MetaDataLog::metaDataLogID(log_id_).val(),
                      error_description(st),
                      std::chrono::duration_cast<std::chrono::milliseconds>(
                          append_retry_timer_->getNextDelay())
                          .count());
      append_retry_timer_->activate();
      return;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "unexpected error when appending to metadata log %lu: %s",
                      MetaDataLog::metaDataLogID(log_id_).val(),
                      error_description(st));
      dd_assert(false, "unexpected error on append");
      destroyRequest(st);
      return;
  }
  ld_check(false);
}

void WriteMetaDataLogRequest::setMetaDataWrittenES() {
  MetaDataTracer tracer(Worker::onThisThread()->getTraceLogger(),
                        log_id_,
                        MetaDataTracer::Action::SET_WRITTEN_BIT);
  WeakRef<WriteMetaDataLogRequest> ref = ref_holder_.ref();
  int rv = getEpochStore().createOrUpdateMetaData(
      log_id_,
      std::make_shared<EpochMetaDataUpdateToWritten>(
          epoch_store_metadata_ /*compare_equality*/),
      [ref, this](Status st,
                  logid_t log_id,
                  std::unique_ptr<const EpochMetaData> metadata,
                  std::unique_ptr<EpochStore::MetaProperties> /*unused*/) {
        if (!ref) {
          // WriteMetaDataLogRequest was already destroyed
          return;
        }
        onEpochStoreUpdated(st, log_id, std::move(metadata));
      },
      std::move(tracer),
      EpochStore::WriteNodeID::KEEP_LAST);

  if (rv == -1) {
    onEpochStoreUpdated(err, log_id_, nullptr);
  }
}

void WriteMetaDataLogRequest::onEpochStoreUpdated(
    Status st,
    logid_t log_id,
    std::unique_ptr<const EpochMetaData>) {
  ld_info("Epoch store update for log %lu completed with status: %s",
          log_id.val(),
          error_description(st));
  auto scheduleRetry = [this]() {
    if (!epoch_store_retry_timer_) {
      epoch_store_retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

          [this]() { setMetaDataWrittenES(); },
          Worker::settings().sequencer_epoch_store_write_retry_delay);
      epoch_store_retry_timer_->randomize();
    }
    auto res = epoch_store_retry_timer_->getNextDelay();
    epoch_store_retry_timer_->activate();
    return res;
  };
  auto log = [this, st](folly::Optional<std::chrono::milliseconds> delay,
                        bool unexpected = false) {
    static const char* unexpected_str = "Unexpected ";
    std::string retry_str;
    if (delay.hasValue()) {
      retry_str =
          " Retrying in " + std::to_string(delay.value().count()) + "ms.";
    }
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "%serror when updating metadata in epoch store for log "
                    "%lu: %s.%s",
                    (unexpected ? unexpected_str : ""),
                    log_id_.val(),
                    error_description(st),
                    retry_str.c_str());
  };

  std::chrono::milliseconds delay;

  switch (st) {
    case E::OK:
    case E::UPTODATE:
      // success
      destroyRequest(E::OK);
      return;
    case E::ACCESS:
    case E::CONNFAILED:
    case E::NOTCONN:
    case E::SYSLIMIT:
      // transient errors - retry and log
      delay = scheduleRetry();
      log(delay);
      return;
    case E::AGAIN:
      // someone modified the ZK entry - potentially another sequencer that was
      // activated. This is benign, we should retry.
      scheduleRetry();
      return;
    case E::STALE:
    case E::NOTFOUND:
    case E::EMPTY:
      // 3 errors above: contents of the ZK node don't contain metadata that we
      // wrote anymore, log the error and destroy the request
    case E::TOOBIG:
    case E::INTERNAL:
      // internal errors, no point in retrying. But we should log the attempt.
      log(folly::none);
      FOLLY_FALLTHROUGH;
    case E::SHUTDOWN:
      // shutting down, forget about it
      destroyRequest(st);
      return;
    default:
      log(folly::none, true);
      dd_assert(false, "unexpected error on append");
      destroyRequest(st);
      return;
  }
  ld_check(false);
}

void WriteMetaDataLogRequest::destroyRequest(Status st) {
  // No need in calling the callback when shutting down
  if (st != E::SHUTDOWN) {
    callback_(st, epoch_store_metadata_);
  }
  destroy();
  // `this' is destroyed
}

EpochStore& WriteMetaDataLogRequest::getEpochStore() {
  Worker* w = Worker::onThisThread();
  ld_check(w);
  return w->processor_->allSequencers().getEpochStore();
}

}} // namespace facebook::logdevice
