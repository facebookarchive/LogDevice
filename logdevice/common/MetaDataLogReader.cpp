/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/MetaDataLogReader.h"

#include <folly/Memory.h>

#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/StopReadingRequest.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

void MetaDataLogReader::start() {
  startReading();
  started_ = true;
}

void MetaDataLogReader::startReading() {
  STAT_INCR(getStats(), metadata_log_readers_started);
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  ld_check(!started_);
  rsid_ = processor->issueReadStreamID();

  const logid_t meta_logid = MetaDataLog::metaDataLogID(log_id_);

  // reference of `this' gets passed to the internal read stream, this
  // is OK as it erases the read stream in the destructor
  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid_,
      meta_logid,
      "",
      [this](std::unique_ptr<DataRecord>& record) {
        onDataRecord(std::move(record));
        return true;
      },
      [this](const GapRecord& gap) {
        onGapRecord(gap);
        return true;
      },
      std::function<void(logid_t)>(),
      nullptr, // metadata cache
      nullptr  // health cb
  );

  // Create a client read stream to read the metadata log.
  // The read stream reads all records in the log with lsn range
  // [e1n1, LSN_MAX-1]. The buffer size for the readstream is set to 1
  // since each metadata log record has its distinct epoch.
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid_,
      meta_logid,
      compose_lsn(EPOCH_MIN, ESN_MIN), // first valid lsn possibly written
      LSN_MAX - 1,                     // greatest lsn possible
      Worker::settings().client_read_flow_control_threshold,
      // Note: using std::map based buffer implementation since LSNs of
      // a metadata log records are guaranteed to be sparse (one record per
      // epoch)
      ClientReadStreamBufferType::ORDERED_MAP,
      // the capacity of the buffer is set to be 128 epochs, which means a
      // metadata storage node can send 128 metadata log records in the window
      128ull * (static_cast<size_t>(ESN_MAX.val_) + 1),
      std::move(deps),
      processor->config_);

  // use ignoreReleasedStatus() by default, but not in WAIT_UNTIL_RELEASED mode
  if (mode_ != Mode::WAIT_UNTIL_RELEASED) {
    read_stream->ignoreReleasedStatus();
  }

  // disable single copy delivery when reading metadata logs
  read_stream->forceNoSingleCopyDelivery();

  ld_spew("Attempting to fetch metadata for epoch [%u, %u] for log %lu... "
          "start reading its metadata log %lu with rsid %lu.",
          epoch_start_.val_,
          epoch_end_.val_,
          log_id_.val_,
          meta_logid.val_,
          rsid_.val_);

  // transfer ownership of the ClientReadStream to the worker
  w->clientReadStreams().insertAndStart(std::move(read_stream));
}

void MetaDataLogReader::onDataRecord(std::unique_ptr<DataRecord> record) {
  if (!started_) {
    // ignore when the reader is already finialized
    return;
  }

  ld_check(record->logid == MetaDataLog::metaDataLogID(log_id_));
  ld_spew("Data record received for metadata log %lu of log %lu, lsn %s.",
          record->logid.val_,
          log_id_.val_,
          lsn_to_string(record->attrs.lsn).c_str());

  const epoch_t last_read_epoch = lastReadEpoch();

  // read epoch metadata from the payload of the record
  auto metadata_read = std::make_unique<EpochMetaData>();
  const lsn_t metadata_read_lsn = record->attrs.lsn;
  const std::chrono::milliseconds metadata_read_timestamp =
      record->attrs.timestamp;
  int rv = metadata_read->fromPayload(
      record->payload, log_id_, *getNodesConfiguration());
  if (rv != 0) {
    // we encountered a malformed record, which may indicate potential loss
    // of metadata for any epoch larger than last_read_epoch, set the
    // bad_epoch_ flag and return
    ld_error("Encountered a metadata log %lu record for log %lu with "
             "timestamp %s which does not have valid epoch metadata in "
             "payload! last valid metadata epoch read: %u",
             record->logid.val_,
             log_id_.val_,
             format_time(record->attrs.timestamp).c_str(),
             last_read_epoch.val_);
    bad_epoch_ = true;
    error_reason_ = E::BADMSG;
    offending_lsns_.insert(metadata_read_lsn);
    return;
  }

  ld_check(metadata_read->isValid());
  const epoch_t epoch_read = metadata_read->h.epoch;
  if (last_read_epoch > epoch_read) {
    ld_error("Metadata log %lu records for log %lu have metadata epochs "
             "out-of-order! current %u, already got %u. Check the utility "
             "that writes metadata log!",
             record->logid.val_,
             log_id_.val_,
             epoch_read.val_,
             last_read_epoch.val_);
    bad_epoch_ = true;
    error_reason_ = E::BADMSG;
    offending_lsns_.insert(metadata_read_lsn);
    return;
  }

  if (last_read_epoch == epoch_read) {
    // it is possible to read two records with the same epoch metadata,
    // in such case, just ignore the the duplicated metadata
    ld_debug("Got duplicate metadata log %lu records for log %lu on "
             "epoch %u",
             record->logid.val_,
             log_id_.val_,
             last_read_epoch.val_);
    ld_check(metadata_);
    if (!metadata_->identicalInMetaDataLog(*metadata_read)) {
      ld_error("Got metadata log %lu records for log %lu with the same "
               "epoch %u but different metadata! Check the utility that "
               "writes metadata log! Current: %s, got: %s",
               record->logid.val_,
               log_id_.val_,
               metadata_->h.epoch.val_,
               metadata_->toString().c_str(),
               metadata_read->toString().c_str());
      bad_epoch_ = true;
      error_reason_ = E::BADMSG;
      offending_lsns_.insert(metadata_read_lsn);
    }
    return;
  }

  if (first_metadata_epoch_ == EPOCH_INVALID) {
    first_metadata_epoch_ = epoch_read;
  }

  // at this time we are sure that current epoch metadata is effective with
  // epoch range [last_read_epoch, epoch_read - 1], conclude the current epoch
  // interval and deliver the epoch metadata if necessary
  concludeEpochInterval(epoch_t(epoch_read.val_ - 1));

  // update current metadata and starts the new interval
  metadata_ = std::move(metadata_read);
  record_lsn_ = metadata_read_lsn;
  record_timestamp_ = metadata_read_timestamp;

  if (epoch_to_deliver_ > epoch_end_) {
    finalize();
  } else if (mode_ == Mode::WAIT_UNTIL_RELEASED && epoch_read >= epoch_start_) {
    // if WAIT_UNTIL_RELEASED mode is set, conclude when first see a record with
    // metadata epoch >= epoch_start_
    deliverMetaData(E::OK, epoch_read, /*last_record*/ false);
    finalize();
  }
}

void MetaDataLogReader::onGapRecord(const GapRecord& gap) {
  if (!started_) {
    return;
  }

  ld_check(gap.logid == MetaDataLog::metaDataLogID(log_id_));
  ld_spew("Gap record received for metadata log %lu of log %lu, "
          "range [%s, %s], type %d",
          gap.logid.val_,
          log_id_.val_,
          lsn_to_string(gap.lo).c_str(),
          lsn_to_string(gap.hi).c_str(),
          static_cast<int>(gap.type));

  if (gap.type == GapType::DATALOSS) {
    ld_error("DataLoss reported for metadata log %lu of log %lu, "
             "range [%s,%s], last valid metadata epoch read: %u",
             gap.logid.val_,
             log_id_.val_,
             lsn_to_string(gap.lo).c_str(),
             lsn_to_string(gap.hi).c_str(),
             lastReadEpoch().val_);
    bad_epoch_ = true;
    error_reason_ = E::DATALOSS;
  }

  if (gap.type == GapType::NOTINCONFIG) {
    ld_info("Cannot read metadata log %lu because its corresponding data log "
            "%lu is not in the config",
            gap.logid.val_,
            log_id_.val_);
    deliverMetaData(E::NOTINCONFIG, epoch_end_, true);
    finalize();
  } else if (gap.type == GapType::ACCESS) {
    ld_info("Invalid Access for metadata log %lu of log %lu",
            gap.logid.val_,
            log_id_.val_);

    // Client does not have correct permissions to access log
    deliverMetaData(E::ACCESS, epoch_end_, true);
    finalize();
  } else if (gap.hi >= LSN_MAX - 1) {
    // might be a DATALOSS gap if all nodes are in rebuilding
    ld_check(gap.type == GapType::BRIDGE || gap.type == GapType::DATALOSS);

    // we received the special gap indicating that there is no more
    // released records in metadata storage nodes at the current time
    // conclude the reader
    concludeEpochInterval(EPOCH_MAX);

    ld_check(epoch_to_deliver_ > epoch_end_);
    finalize();
  }
}

void MetaDataLogReader::concludeEpochInterval(epoch_t interval_until) {
  const epoch_t interval_start = lastReadEpoch();
  ld_check(interval_until >= interval_start && interval_until <= EPOCH_MAX);

  // if there are requested epochs that belong to previous interval but
  // not delivered, then the current interval must be the first valid metadata
  // interval
  ld_check(epoch_to_deliver_ >= interval_start ||
           interval_start == first_metadata_epoch_);

  const bool last_record = interval_until >= EPOCH_MAX;
  if (last_record && interval_start == EPOCH_INVALID) {
    // we haven't read anything valid but the readstream is concluded.
    // This indicates that there is no valid epoch metadata records in the
    // metadata log. return the failure reason to the requester (this is
    // initialized to E::NOTFOUND if no records were encountered.
    deliverMetaData(error_reason_, epoch_end_, last_record);
  } else if (epoch_to_deliver_ <= interval_until) {
    // if this is the last interval, until should be no larger than
    // epoch_end_
    epoch_t until =
        last_record ? std::max(interval_start, epoch_end_) : interval_until;

    // deliver the metadata to the user if the current epoch is bad, or it is
    // not the initial interval before the first valid record
    if (bad_epoch_) {
      // the bad epoch interval may start with a valid epoch metadata
      // deliver it first followed by the bad interval
      if (interval_start > EPOCH_INVALID &&
          epoch_to_deliver_ <= interval_start) {
        deliverMetaData(E::OK, interval_start, false);
      }
      if (epoch_to_deliver_ <= epoch_end_) {
        ld_check(error_reason_ != E::OK);
        deliverMetaData(error_reason_, until, last_record);
      }
    } else if (interval_start > EPOCH_INVALID) {
      deliverMetaData(E::OK, until, last_record);
    }
  } else {
    // not interested in the current interval, do nothing
  }

  // clear bad_epoch_ flag since the next interval is considered healthy
  bad_epoch_ = false;
  return;
}

void MetaDataLogReader::deliverMetaData(Status st,
                                        epoch_t until,
                                        bool last_record) {
  if (!started_) {
    // this is the only function that accesses the metadata_cb_, which may
    // reference other objects, if `this' is deactivated (finalize() is called),
    // metadata_cb_ may no longer be valid, and we should not proceed.

    // expect caller of this function to ensure such property
    ld_check(false);
    return;
  }

  ld_check(until >= epoch_to_deliver_);
  ld_check(st != E::OK || (metadata_ && record_lsn_ != LSN_INVALID));

  // we need to make sure that the call chain that involves user provided
  // callback won't trigger a new metadata to be delivered. This is true
  // since it needs to return to the libevent loop to deliver the next message
  // (record or gap).
  ld_check(!inside_callback_);

  if (st == E::OK) {
    ld_spew("Got metadata for epoch %u for log %lu. metadata epoch %u, "
            "effective until %u, metadata: %s",
            epoch_to_deliver_.val_,
            log_id_.val_,
            metadata_->h.epoch.val_,
            until.val_,
            metadata_->toString().c_str());
  } else if (st != E::NOTFOUND || warn_if_notfound_) {
    LOG_STAT_INCR(
        getStats(), getClusterConfig(), log_id_, metadata_log_read_failed);

    if (st == E::BADMSG) {
      STAT_INCR(getStats(), metadata_log_read_failed_corruption);
    } else if (st == E::DATALOSS) {
      STAT_INCR(getStats(), metadata_log_read_dataloss);
    } else {
      STAT_INCR(getStats(), metadata_log_read_failed_other);
    }

    if (st == E::NOTFOUND) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "No metadata for epoch %u of log %lu in metadata log %lu. "
                     "Maybe the log is new and hasn't been fully provisioned "
                     "yet. This should be rare.",
                     epoch_to_deliver_.val(),
                     log_id_.val(),
                     MetaDataLog::metaDataLogID(log_id_).val());
    } else {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Failed to get metadata for epoch %u for log %lu: %s ",
                      epoch_to_deliver_.val_,
                      log_id_.val_,
                      error_name(st));
    }
  }

  if (metadata_cb_) {
    inside_callback_ = true;

    // call the user provided callback, it is possible that inside the
    // callback `this' got deactivated by calling finalize(), but `this'
    // should not be destroyed until the next libevent iteration
    metadata_cb_(st,
                 {log_id_,
                  epoch_to_deliver_,
                  until,
                  last_record ? RecordSource::LAST : RecordSource::NOT_LAST,
                  record_lsn_,
                  record_timestamp_,
                  st == E::OK ? std::move(metadata_) : nullptr});

    record_lsn_ = LSN_INVALID;
    record_timestamp_ = std::chrono::milliseconds(0);
    inside_callback_ = false;
  }

  ld_check(until <= EPOCH_MAX);
  epoch_to_deliver_.val_ = until.val_ + 1;
}

void MetaDataLogReader::finalize() {
  if (!started_) {
    return;
  }
  // ignore further records/gaps and stop delivering epoch metadata
  started_ = false;
  STAT_INCR(getStats(), metadata_log_readers_finalized);
}

void MetaDataLogReader::stopReading() {
  // cannot be inside the user provided metadata callback
  ld_check(!inside_callback_);

  if (rsid_ != READ_STREAM_ID_INVALID) {
    // cannot be inside the callback of the read stream either
    // destructor of the read stream asserts
    Worker* w = Worker::onThisThread();
    if (!w->shuttingDown()) {
      w->clientReadStreams().erase(rsid_);
    }
    rsid_ = READ_STREAM_ID_INVALID;
  }
}

MetaDataLogReader::~MetaDataLogReader() {
  ld_check(!inside_callback_);
  ld_spew("Destroying metadata reader reading epoch [%u, %u] for log %lu, "
          "also destroying its readstream rsid %lu...",
          epoch_start_.val_,
          epoch_end_.val_,
          log_id_.val_,
          rsid_.val_);
  stopReading();
}

StatsHolder* MetaDataLogReader::getStats() {
  return Worker::stats();
}

std::shared_ptr<Configuration> MetaDataLogReader::getClusterConfig() const {
  return Worker::getConfig();
}

std::shared_ptr<const NodesConfiguration>
MetaDataLogReader::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

}} // namespace facebook::logdevice
