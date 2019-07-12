/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/replicated_state_machine/TrimRSMRequest.h"

#include <chrono>

#include "logdevice/common/FindKeyRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/TrimRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/replicated_state_machine/RSMSnapshotHeader.h"
#include "logdevice/common/replicated_state_machine/logging.h"

namespace facebook { namespace logdevice {

Request::Execution TrimRSMRequest::execute() {
  // Step 1: find the last released lsn in the snapshot log.

  auto ticket = callbackHelper_.ticket();

  std::unique_ptr<Request> req;

  if (trim_everything_) {
    auto cb = [ticket](Status st,
                       NodeID /*seq*/,
                       lsn_t next_lsn,
                       std::unique_ptr<LogTailAttributes> /* unused */,
                       std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
                       std::shared_ptr<TailRecord> /*tail_record*/,
                       folly::Optional<bool> /*is_log_empty*/) {
      const lsn_t tail_lsn = next_lsn <= LSN_OLDEST ? LSN_OLDEST : next_lsn - 1;
      ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
        if (rq) {
          rq->snapshotFindTimeCallback(st, tail_lsn);
        }
      });
    };
    req = std::make_unique<SyncSequencerRequest>(
        snapshot_log_id_, SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES, cb);

  } else {
    auto cb = [ticket](Status st, lsn_t lsn) {
      ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
        if (rq) {
          rq->snapshotFindTimeCallback(st, lsn);
        }
      });
    };

    req = std::make_unique<FindKeyRequest>(
        snapshot_log_id_,
        RecordTimestamp::duration::max(), // max() gives us last released lsn
        folly::none,
        findtime_timeout_,
        cb,
        find_key_callback_t(),
        FindKeyAccuracy::STRICT);
  }
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  processor->postWithRetrying(req);

  return Execution::CONTINUE;
}

void TrimRSMRequest::extractVersionAndReadPointerFromSnapshot(DataRecord& rec,
                                                              RSMType rsm_type,
                                                              lsn_t& version,
                                                              lsn_t& read_ptr) {
  RSMSnapshotHeader hdr;

  auto sz = RSMSnapshotHeader::deserialize(rec.payload, hdr);
  if (sz < 0) {
    rsm_error(rsm_type,
              "Failed to deserialize header of snapshot record with lsn %s",
              lsn_to_string(rec.attrs.lsn).c_str());
    read_ptr = version = LSN_INVALID;
    return;
  }

  if (hdr.base_version == LSN_INVALID) {
    rsm_error(rsm_type,
              "Unexpected version LSN_INVALID seen in snapshot with lsn %s",
              lsn_to_string(rec.attrs.lsn).c_str());
    err = E::BADMSG;
    read_ptr = version = LSN_INVALID;
    return;
  }

  version = hdr.base_version;
  if (hdr.format_version >=
      RSMSnapshotHeader::CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH) {
    read_ptr = hdr.delta_log_read_ptr;
  } else {
    // We can estimate it to be base_version + 1
    read_ptr = std::min(hdr.base_version, LSN_MAX - 1) + 1;
  }
}

void TrimRSMRequest::snapshotFindTimeCallback(Status st, lsn_t lsn) {
  if (st != E::OK && st != E::PARTIAL) {
    rsm_error(rsm_type_,
              "findTime completed with st=%s for snapshot log %lu",
              error_name(st),
              snapshot_log_id_.val_);
    completeAndDeleteThis(st);
    return;
  }

  if (lsn == LSN_OLDEST) {
    // Snapshot log is empty, nothing to trim.
    rsm_info(rsm_type_,
             "findTime completed with st=%s, lsn=LSN_OLDEST for "
             "snapshot log %lu, not trimming anything.",
             error_name(st),
             snapshot_log_id_.val_);
    completeAndDeleteThis(E::OK);
    return;
  }

  rsm_info(rsm_type_,
           "findTime completed with st=%s, lsn=%s for snapshot log %lu",
           error_name(st),
           lsn_to_string(lsn).c_str(),
           snapshot_log_id_.val_);

  // Step 2: read the snapshot log up to the last released lsn to find the last
  // snapshot record.

  auto record_callback = [&](std::unique_ptr<DataRecord>& record) {
    // We want to find the last snapshot record in the snapshot log.
    // We will keep that last snapshot and trim everything that is older than
    // it.
    last_seen_snapshot_ = std::move(record);
    return true;
  };

  auto done_callback = [this, lsn](logid_t) {
    if (!last_seen_snapshot_) {
      if (trim_everything_) {
        rsm_info(rsm_type_,
                 "Trimming up to next_lsn=%s; trim_everything=true",
                 lsn_to_string(lsn).c_str());
        last_seen_snapshot_ =
            std::make_unique<DataRecord>(snapshot_log_id_, Payload(), lsn);
      } else {
        // We did not read any snapshot, there is nothing to trim.
        rsm_info(rsm_type_,
                 "There are no snapshots with a timestamp older than %s.",
                 format_time(min_timestamp_).c_str());
        completeAndDeleteThis(E::OK);
        return;
      }
    }

    trimSnapshotLog();
  };

  // If we want to trim everything, there is no point in starting a readstream
  // for the latest snapshot. We can trim up to next_lsn
  if (trim_everything_) {
    done_callback(logid_t(0));
    return;
  }

  rsm_info(rsm_type_,
           "Creating a read stream to read snapshot log %lu up to lsn %s",
           snapshot_log_id_.val_,
           lsn_to_string(lsn).c_str());

  // Read up to lsn - 1. We are guaranteed the read stream will stop since lsn
  // may be at most last_released_lsn + 1.
  const lsn_t until_lsn = lsn - 1;

  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  const auto rsid = processor->issueReadStreamID();
  auto deps = std::make_unique<ClientReadStreamDependencies>(rsid,
                                                             snapshot_log_id_,
                                                             "",
                                                             record_callback,
                                                             nullptr,
                                                             done_callback,
                                                             nullptr,
                                                             nullptr);

  // We could use findKey which returns a [lo,hi] interval instead of findTime
  // which returns only one LSN, this would help make a point query read instead
  // of reading from LSN_OLDEST. However, we expect to read very few snapshots
  // (if not one) when reading from LSN_OLDEST if we assume this request will be
  // run periodically (after creating a snapshot for instance).
  auto read_stream = std::make_unique<ClientReadStream>(
      rsid,
      snapshot_log_id_,
      LSN_OLDEST,
      until_lsn,
      Worker::settings().client_read_flow_control_threshold,
      ClientReadStreamBufferType::CIRCULAR,
      100,
      std::move(deps),
      processor->config_);

  // SCD adds complexity and may incur latency on storage node failures. Since
  // replicated state machines should be low volume logs, we can afford to not
  // use that optimization.
  read_stream->forceNoSingleCopyDelivery();
  w->clientReadStreams().insertAndStart(std::move(read_stream));
}

void TrimRSMRequest::trimSnapshotLog() {
  ld_check(last_seen_snapshot_);
  min_snapshot_lsn_ = last_seen_snapshot_->attrs.lsn;
  if (trim_everything_) {
    min_snapshot_version_ = LSN_MAX;
    snapshot_delta_read_ptr_ = LSN_MAX;
  } else {
    extractVersionAndReadPointerFromSnapshot(*last_seen_snapshot_,
                                             rsm_type_,
                                             min_snapshot_version_,
                                             snapshot_delta_read_ptr_);
  }

  if (min_snapshot_version_ == LSN_INVALID) {
    completeAndDeleteThis(err);
    return;
  }

  if (min_snapshot_version_ == LSN_OLDEST) {
    // Snapshot has version LSN_OLDEST, nothing more to do.
    completeAndDeleteThis(E::OK);
    return;
  }

  rsm_info(
      rsm_type_,
      "Found snapshot with lsn=%s, version=%s, ts=%s, delta_log_read_ptr=%s. "
      "Will trim every snapshot prior to that snapshot.",
      lsn_to_string(min_snapshot_lsn_).c_str(),
      lsn_to_string(min_snapshot_version_).c_str(),
      format_time(last_seen_snapshot_->attrs.timestamp).c_str(),
      lsn_to_string(snapshot_delta_read_ptr_).c_str());

  // Step 3: trim the snapshot log up to `min_snapshot_lsn_-1`.

  auto ticket = callbackHelper_.ticket();
  auto on_trimmed = [ticket](Status trim_status) {
    ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
      if (rq) {
        rq->onSnapshotTrimmed(trim_status);
      }
    });
  };

  lsn_t trim_to;
  if (trim_everything_) {
    trim_to = min_snapshot_lsn_;
    rsm_info(rsm_type_,
             "Trimming (all) snapshot log %lu to lsn %s",
             snapshot_log_id_.val_,
             lsn_to_string(trim_to).c_str());

  } else {
    trim_to = min_snapshot_lsn_ - 1;
    rsm_info(rsm_type_,
             "Trimming snapshot log %lu up to lsn %s",
             snapshot_log_id_.val_,
             lsn_to_string(min_snapshot_lsn_ - 1).c_str());
  }

  auto trimreq = std::make_unique<TrimRequest>(
      nullptr, snapshot_log_id_, trim_to, trim_timeout_, on_trimmed);
  trimreq->bypassWriteTokenCheck();
  std::unique_ptr<Request> req(std::move(trimreq));

  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  processor->postWithRetrying(req);
}

void TrimRSMRequest::onSnapshotTrimmed(Status st) {
  if (st != E::OK && st != E::PARTIAL) {
    rsm_error(rsm_type_,
              "Could not trim snapshot log %lu: %s",
              snapshot_log_id_.val_,
              error_name(st));
  } else {
    rsm_info(rsm_type_,
             "Successfully trimmed snapshot log %lu",
             snapshot_log_id_.val_);
  }

  // Step 4: f=findTime(delta_log_id, NOW-retention)
  // unless we are trimming everything, we will then get use the tail lsn
  // instead of using findTime
  std::unique_ptr<Request> req;
  auto ticket = callbackHelper_.ticket();
  if (trim_everything_) {
    auto cb = [ticket](Status status,
                       NodeID /*seq*/,
                       lsn_t next_lsn,
                       std::unique_ptr<LogTailAttributes> /* unused */,
                       std::shared_ptr<const EpochMetaDataMap> /* unused */,
                       std::shared_ptr<TailRecord> /* unused */,
                       folly::Optional<bool> /* unused */) {
      const lsn_t tail_lsn = next_lsn <= LSN_OLDEST ? LSN_OLDEST : next_lsn - 1;
      ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
        if (rq) {
          rq->deltaFindTimeCallback(status, tail_lsn);
        }
      });
    };
    req = std::make_unique<SyncSequencerRequest>(
        delta_log_id_, SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES, cb);
  } else {
    rsm_info(
        rsm_type_,
        "Starting FindTime request to find the delta record lsn to trim up "
        "to in delta log %lu. Will keep delta records up to timestamp %s",
        delta_log_id_.val_,
        format_time(min_timestamp_).c_str());

    auto cb = [ticket](Status status, lsn_t lsn) {
      ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
        if (rq) {
          rq->deltaFindTimeCallback(status, lsn);
        }
      });
    };

    req = std::make_unique<FindKeyRequest>(delta_log_id_,
                                           min_timestamp_,
                                           folly::none,
                                           findtime_timeout_,
                                           cb,
                                           find_key_callback_t(),
                                           FindKeyAccuracy::STRICT);
  }
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  processor->postWithRetrying(req);
}

void TrimRSMRequest::deltaFindTimeCallback(Status st, lsn_t lsn) {
  if (st != E::OK && st != E::PARTIAL) {
    rsm_error(rsm_type_,
              "findTime completed with st=%s for delta log %lu",
              error_name(st),
              delta_log_id_.val_);
    completeAndDeleteThis(st);
    return;
  }

  if (lsn == LSN_OLDEST) {
    // Nothing to trim in delta log.
    rsm_info(rsm_type_,
             "findTime completed with st=%s, lsn=LSN_OLDEST for delta "
             "log %lu, not trimming anything.",
             error_name(st),
             delta_log_id_.val_);
    completeAndDeleteThis(E::OK);
    return;
  }

  rsm_info(rsm_type_,
           "findTime completed with st=%s, lsn=%s for delta log %lu",
           error_name(st),
           lsn_to_string(lsn).c_str(),
           delta_log_id_.val_);

  ld_check(lsn > LSN_OLDEST);
  // on trim_everything_ the snapshot_delta_read_ptr_ is LSN_MAX
  const lsn_t trim_up_to =
      std::max(std::min(lsn, snapshot_delta_read_ptr_), LSN_OLDEST + 1) - 1;

  // Step 5: trim delta log up to min(f - 1, snapshot_delta_read_ptr_ - 1)

  rsm_info(rsm_type_,
           "Trimming delta log %lu up to lsn %s.",
           delta_log_id_.val_,
           lsn_to_string(trim_up_to).c_str());

  auto ticket = callbackHelper_.ticket();
  auto on_trimmed = [ticket](Status status) {
    ticket.postCallbackRequest([=](TrimRSMRequest* rq) {
      if (rq) {
        rq->onDeltaTrimmed(status);
      }
    });
  };

  auto trimreq = std::make_unique<TrimRequest>(
      nullptr, delta_log_id_, trim_up_to, trim_timeout_, on_trimmed);
  trimreq->bypassWriteTokenCheck();
  std::unique_ptr<Request> req(std::move(trimreq));

  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  processor->postWithRetrying(req);
}

void TrimRSMRequest::onDeltaTrimmed(Status st) {
  if (st != E::OK && st != E::PARTIAL) {
    rsm_error(rsm_type_,
              "Could not trim delta log %lu: %s",
              delta_log_id_.val_,
              error_name(st));
    completeAndDeleteThis(st);
    return;
  }

  rsm_info(rsm_type_, "Successfully trimmed delta log %lu", delta_log_id_.val_);

  if (st == E::PARTIAL) {
    is_partial_ = true;
  }

  completeAndDeleteThis(st);
}

void TrimRSMRequest::completeAndDeleteThis(Status st) {
  ld_check(cb_);
  if (st == E::OK && is_partial_) {
    st = E::PARTIAL;
  }
  cb_(st);
  delete this;
}

}} // namespace facebook::logdevice
