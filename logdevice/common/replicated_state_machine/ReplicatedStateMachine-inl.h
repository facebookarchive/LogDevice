/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/Checksum.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine-enum.h"
#include "logdevice/common/replicated_state_machine/logging.h"

namespace facebook { namespace logdevice {

template <typename T, typename D>
ReplicatedStateMachine<T, D>::ReplicatedStateMachine(RSMType rsm_type,
                                                     logid_t delta_log_id,
                                                     logid_t snapshot_log_id)
    : rsm_type_(rsm_type),
      delta_log_id_(delta_log_id),
      snapshot_log_id_(snapshot_log_id),
      callbackHelper_(this) {
  ld_check(delta_log_id_ != LOGID_INVALID);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::start() {
  // Initialize `data_` with a default value that we'll use if the snapshot
  // log is empty.
  data_ = makeDefaultState(version_);

  if (snapshot_log_id_ == LOGID_INVALID) {
    onBaseSnapshotRetrieved();
  } else {
    getSnapshotLogTailLSN();
  }
  stopped_ = false;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::stop() {
  ld_check(!stopped_);

  auto stop_read_stream = [](read_stream_id_t& rsid) {
    if (rsid != READ_STREAM_ID_INVALID) {
      Worker::onThisThread()->clientReadStreams().erase(rsid);
      rsid = READ_STREAM_ID_INVALID;
    }
  };

  stop_read_stream(snapshot_log_rsid_);
  stop_read_stream(delta_log_rsid_);

  stopped_ = true;
  cancelGracePeriodForSnapshotting();
  // This will unblock anyone that called wait().
  sem_.post();
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::wait(std::chrono::milliseconds timeout) {
  const int rv = sem_.timedwait(timeout, false);
  if (rv == 0) {
    return true;
  } else {
    ld_check(err == E::TIMEDOUT);
    return false;
  }
}

template <typename T, typename D>
read_stream_id_t ReplicatedStateMachine<T, D>::createBasicReadStream(
    logid_t logid,
    lsn_t start_lsn,
    lsn_t until_lsn,
    ClientReadStreamDependencies::record_cb_t on_record,
    ClientReadStreamDependencies::gap_cb_t on_gap,
    ClientReadStreamDependencies::health_cb_t health_cb) {
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;

  const auto rsid = processor->issueReadStreamID();

  auto deps = std::make_unique<ClientReadStreamDependencies>(
      rsid, logid, "", on_record, on_gap, nullptr, nullptr, health_cb);

  auto read_stream = std::make_unique<ClientReadStream>(
      rsid,
      logid,
      start_lsn,
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

  return rsid;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::resumeReadStream(read_stream_id_t id) {
  Worker* w = Worker::onThisThread();
  AllClientReadStreams& streams = w->clientReadStreams();
  ClientReadStream* reader = streams.getStream(id);
  if (reader) {
    reader->resumeReading();
  }
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::getSnapshotLogTailLSN() {
  rsm_info(rsm_type_, "Retrieving tail lsn of snapshot log...");

  auto ticket = callbackHelper_.ticket();
  auto cb_wrapper =
      [ticket](Status st,
               NodeID /*seq*/,
               lsn_t next_lsn,
               std::unique_ptr<LogTailAttributes> /*tail*/,
               std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
               std::shared_ptr<TailRecord> /*tail_record*/,
               folly::Optional<bool> /*is_log_empty*/) {
        const lsn_t tail_lsn =
            next_lsn <= LSN_OLDEST ? LSN_OLDEST : next_lsn - 1;
        ticket.postCallbackRequest(
            [st, tail_lsn](ReplicatedStateMachine<T, D>* s) {
              if (s) {
                s->onGotSnapshotLogTailLSN(st, LSN_OLDEST, tail_lsn);
              }
            });
      };

  std::unique_ptr<Request> req = std::make_unique<SyncSequencerRequest>(
      snapshot_log_id_,
      SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES,
      cb_wrapper);
  postRequestWithRetrying(req);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onGotSnapshotLogTailLSN(Status st,
                                                           lsn_t start,
                                                           lsn_t lsn) {
  // Because the SyncSequencerRequest is called without a timeout, and because
  // we never cancel the request, its completion callback must always succeed.
  ld_check(st == E::OK);

  rsm_info(
      rsm_type_, "Tail lsn of snapshot log is %s", lsn_to_string(lsn).c_str());

  ld_check(lsn != LSN_INVALID);
  snapshot_sync_ = lsn;

  // If stop_at_tail_ is used, we don't care about reading past the tail of the
  // snapshot log.
  const lsn_t until_lsn = stop_at_tail_ ? lsn : LSN_MAX;

  snapshot_log_rsid_ = createBasicReadStream(
      snapshot_log_id_,
      start,
      until_lsn,
      [this](std::unique_ptr<DataRecord>& record) {
        return onSnapshotRecord(record);
      },
      [this](const GapRecord& gap) { return onSnapshotGap(gap); },
      nullptr);
}

template <typename T, typename D>
int ReplicatedStateMachine<T, D>::deserializeSnapshot(
    const DataRecord& record,
    std::unique_ptr<T>& out,
    RSMSnapshotHeader& header_out) const {
  const auto header_sz =
      RSMSnapshotHeader::deserialize(record.payload, header_out);
  if (header_sz < 0) {
    rsm_error(rsm_type_,
              "Failed to deserialize header of snapshot record with lsn %s",
              lsn_to_string(record.attrs.lsn).c_str());
    err = E::BADMSG;
    return -1;
  }

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(record.payload.data());
  ptr += header_sz;

  std::unique_ptr<uint8_t[]> buf_decompressed;
  Payload p(ptr, record.payload.size() - header_sz);

  if (header_out.flags & RSMSnapshotHeader::ZSTD_COMPRESSION) {
    size_t uncompressed_size = ZSTD_getDecompressedSize(p.data(), p.size());
    buf_decompressed = std::make_unique<uint8_t[]>(uncompressed_size);
    size_t rv = ZSTD_decompress(buf_decompressed.get(), // dst
                                uncompressed_size,      // dstCapacity
                                p.data(),               // src
                                p.size());              // compressedSize
    if (ZSTD_isError(rv)) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "ZSTD_decompress() failed: %s",
                      ZSTD_getErrorName(rv));
      ld_check(false);
      err = E::BADMSG;
      return -1;
    }
    if (rv != uncompressed_size) {
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Zstd decompression length %zu does not match %lu found"
                      "in header",
                      rv,
                      uncompressed_size);
      ld_check(false);
      err = E::BADMSG;
      return -1;
    }
    p = Payload(buf_decompressed.get(), uncompressed_size);
  }

  std::chrono::milliseconds timestamp = record.attrs.timestamp;
  auto new_val = deserializeState(p, header_out.base_version, timestamp);
  if (new_val) {
    out = std::move(new_val);
    return 0;
  } else {
    // err set by `deserializeState`.
    return -1;
  }
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::canFastForward(lsn_t lsn) {
  if (isGracePeriodForFastForwardActive()) {
    return false;
  }

  if (allow_fast_forward_up_to_ < lsn) {
    allow_fast_forward_up_to_ = lsn;
    activateGracePeriodForFastForward();
    return false;
  }

  // The grace period timer expired. We can now fast forward.
  rsm_info(rsm_type_,
           "Fast forwarding this state machine currently at version %s "
           "with snapshot at version %s",
           lsn_to_string(version_).c_str(),
           lsn_to_string(lsn).c_str());
  return true;
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::onSnapshotRecord(
    std::unique_ptr<DataRecord>& record) {
  if (sync_state_ == SyncState::SYNC_SNAPSHOT &&
      record->attrs.lsn < snapshot_sync_) {
    // Do not deserialize this snapshot just yet. We'll look at it only when we
    // know that it was the last snapshot, inside ::onSnapshotGap().
    last_snapshot_record_ = std::move(record);
    return true;
  }

  last_snapshot_record_.reset();
  return processSnapshot(record);
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::processSnapshot(
    std::unique_ptr<DataRecord>& record) {
  std::unique_ptr<T> data;
  RSMSnapshotHeader header;
  const int rv = deserializeSnapshot(*record, data, header);

  if (rv != 0) {
    // NOTE: We cannot make progress if this is the last snapshot and it's bad,
    // this means that the RSM will stall unless a newer snapshot is written.
    rsm_critical(rsm_type_,
                 "Could not deserialize snapshot record with lsn %s: %s",
                 lsn_to_string(record->attrs.lsn).c_str(),
                 error_name(err));
    if (!can_skip_bad_snapshot_) {
      return false;
    }
  } else if (header.base_version > version_) {
    // Return false if we should not be fast forwarding right now, in that case
    // the grace period timer is activated. @see canFastForward().
    if (sync_state_ == SyncState::TAILING &&
        waiting_for_snapshot_ == LSN_INVALID &&
        !canFastForward(header.base_version)) {
      return false;
    }

    ld_check(data);
    data_ = std::move(data);
    version_ = header.base_version;
    last_snapshot_version_ = header.base_version;
    if (header.format_version >=
        RSMSnapshotHeader::CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH) {
      last_snapshot_last_read_ptr_ = header.delta_log_read_ptr;
    } else {
      last_snapshot_last_read_ptr_ = LSN_OLDEST;
    }
    delta_log_byte_offset_ = header.byte_offset;
    delta_log_offset_ = header.offset;
    snapshot_log_timestamp_ = record->attrs.timestamp;

    if (sync_state_ == SyncState::TAILING || deliver_while_replaying_) {
      notifySubscribers();
    }

    rsm_info(rsm_type_,
             "Applied snapshot record with lsn %s timestamp %lu, "
             "base version %s, delta_log_read_ptr %s (serialization format "
             "version was %d)",
             lsn_to_string(record->attrs.lsn).c_str(),
             record->attrs.timestamp.count(),
             lsn_to_string(header.base_version).c_str(),
             (header.format_version >=
              RSMSnapshotHeader::CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH)
                 ? lsn_to_string(last_snapshot_last_read_ptr_).c_str()
                 : "disabled",
             header.format_version);
  }

  if (rv == 0) {
    // Using max() here because these values may already be higher as they can
    // be set by the snapshot() function, and also because snapshots can be
    // unordered.
    last_snapshot_byte_offset_ =
        std::max(header.byte_offset, last_snapshot_byte_offset_);
    last_snapshot_offset_ = std::max(header.offset, last_snapshot_offset_);
  }

  if (sync_state_ == SyncState::SYNC_SNAPSHOT &&
      record->attrs.lsn >= snapshot_sync_) {
    onBaseSnapshotRetrieved();
  }

  if (version_ >= waiting_for_snapshot_) {
    // We were stalling reading the delta log because we saw a TRIM or
    // DATALOSS gap in it, but now we have a snapshot that accounts for the data
    // we missed, so we can resume reading the delta log.
    waiting_for_snapshot_ = LSN_INVALID;
    resumeReadStream(delta_log_rsid_);
    cancelStallGracePeriod();
    if (bumped_stalled_stat_) {
      WORKER_STAT_DECR(num_replicated_state_machines_stalled);
      bumped_stalled_stat_ = false;
    }
  }

  // If we fast forwarded, this may cause some entries in
  // `pending_confirmation_` to be discarded.
  discardSkippedPendingDeltas();

  cancelGracePeriodForFastForward();
  return true;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::discardSkippedPendingDeltas() {
  auto& pending = pending_confirmation_;
  while (!pending.empty() && pending.front().lsn != LSN_INVALID &&
         pending.front().lsn <= version_) {
    pending.front().cb(
        E::FAILED, pending.front().lsn, "Cannot confirm operation");
    pending_confirmation_by_uuid_.erase(pending.front().uuid);
    pending.erase(pending.begin());
  }
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::onSnapshotGap(const GapRecord& gap) {
  if (gap.type == GapType::DATALOSS) {
    rsm_info(rsm_type_,
             "Receiving a DATALOSS gap [%s, %s] on snapshot log %lu. This "
             "state machine won't stall if all deltas that were accounted for "
             "by this lost snapshot are still in the delta log. If that's not "
             "the case, this state machine will stall until a snapshot with "
             "high enough base version appears.",
             lsn_to_string(gap.lo).c_str(),
             lsn_to_string(gap.hi).c_str(),
             snapshot_log_id_.val_);
  }

  if (sync_state_ == SyncState::SYNC_SNAPSHOT && gap.hi >= snapshot_sync_) {
    if (last_snapshot_record_) {
      // We found a snapshot record and deferred its serialization until we know
      // it's the last one. Do it now.
      if (!processSnapshot(last_snapshot_record_)) {
        return false;
      }
      last_snapshot_record_.reset();
    }
    onBaseSnapshotRetrieved();
  }

  return true;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onBaseSnapshotRetrieved() {
  rsm_info(rsm_type_,
           "Base snapshot has version %s, delta_log_read_ptr %s",
           lsn_to_string(version_).c_str(),
           lsn_to_string(last_snapshot_last_read_ptr_).c_str());
  activateGracePeriodForSnapshotting();
  gotInitialState(*data_);
  sync_state_ = SyncState::SYNC_DELTAS;
  getDeltaLogTailLSN();
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::getDeltaLogTailLSN() {
  ld_check(version_ != LSN_INVALID);
  ld_check(data_);

  rsm_info(rsm_type_, "Retrieving tail lsn of delta log...");

  auto callback_ticket = callbackHelper_.ticket();
  auto cb = [=](Status st,
                NodeID /*seq*/,
                lsn_t next_lsn,
                std::unique_ptr<LogTailAttributes> /* tail_attributes */,
                std::shared_ptr<const EpochMetaDataMap> /*metadata_map*/,
                std::shared_ptr<TailRecord> /*tail_record*/,
                folly::Optional<bool> /*is_log_empty*/) {
    callback_ticket.postCallbackRequest([=](ReplicatedStateMachine<T, D>* s) {
      if (s) {
        lsn_t tail_lsn = next_lsn <= LSN_OLDEST ? LSN_OLDEST : next_lsn - 1;
        s->onGotDeltaLogTailLSN(st, tail_lsn);
      }
    });
  };

  std::unique_ptr<Request> req =
      std::make_unique<SyncSequencerRequest>(delta_log_id_, 0, cb);
  postRequestWithRetrying(req);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onGotDeltaLogTailLSN(Status st, lsn_t lsn) {
  // Because we use SyncSequencerRequest without a timeout and don't cancel that
  // request, the request has to complete Successfully.
  ld_check(st == E::OK);

  rsm_info(
      rsm_type_, "Tail lsn of delta log is %s", lsn_to_string(lsn).c_str());

  // We will notifier subscribers of the initial state machine's state only
  // after we sync up to that lsn.
  ld_check(lsn != LSN_INVALID);

  delta_sync_ = lsn;

  const lsn_t start_lsn = std::max(version_ + 1, last_snapshot_last_read_ptr_);
  // If stop_at_tail_ is true, we don't care about reading deltas past the tail
  // lsn.
  const lsn_t until_lsn = stop_at_tail_ ? delta_sync_ : LSN_MAX;

  if (version_ >= delta_sync_ || delta_read_ptr_ >= delta_sync_) {
    // The last snapshot we got already accounts for all the deltas. Or we've
    // already read up to the tail.
    // We can notify subscribers of the initial state immediately.
    onReachedDeltaLogTailLSN();
  }

  // It is possible to have start_lsn > until_lsn if stop_at_tail_ was used.
  // also it is possible that the readstream was already created
  if (delta_log_rsid_ == READ_STREAM_ID_INVALID && start_lsn <= until_lsn) {
    delta_log_rsid_ = createBasicReadStream(
        delta_log_id_,
        start_lsn,
        until_lsn,
        [this](std::unique_ptr<DataRecord>& record) {
          return onDeltaRecord(record);
        },
        [this](const GapRecord& gap) { return onDeltaGap(gap); },
        [this](bool is_healthy) {
          onDeltaLogReadStreamHealthChange(is_healthy);
        });
  }
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onDeltaLogReadStreamHealthChange(
    bool is_healthy) {
  if (delta_read_stream_is_healthy_ != is_healthy) {
    rsm_info(rsm_type_,
             "Delta log %lu read stream is now %s",
             delta_log_id_.val(),
             is_healthy ? "healthy" : "unhealthy");
    if (is_healthy && sync_state_ == SyncState::TAILING) {
      // the read stream was unhealthy while we were tailing but is now healthy.
      // fetch the tail lsn and sync deltas that we may have missed
      sync_state_ = SyncState::SYNC_DELTAS;
      getDeltaLogTailLSN();
    }
  }
  delta_read_stream_is_healthy_ = is_healthy;
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::onDeltaRecord(
    std::unique_ptr<DataRecord>& record) {
  if (waiting_for_snapshot_ != LSN_INVALID) {
    // We are stalling reading the delta log because we missed some data and are
    // waiting for a snapshot.
    return false;
  }

  // keep track of the last records received
  ld_check(record->attrs.lsn > delta_read_ptr_);
  delta_read_ptr_ = record->attrs.lsn;

  // If the timer for fast forwarding with a snapshot is active, let's restart
  // it.
  if (isGracePeriodForFastForwardActive()) {
    activateGracePeriodForFastForward();
  }

  if (record->attrs.lsn <= version_) {
    // We already have a higher version because we read a more recent snapshot,
    // skip this delta.
    return true;
  }

  Status st = E::OK;

  DeltaHeader header;
  std::unique_ptr<D> delta;
  int rv = deserializeDelta(*record, delta, header);
  // A string to be filled by the delta application in case of failure.
  std::string failure_reason;

  if (rv != 0) {
    rsm_info(rsm_type_,
             "Could not deserialize delta record with lsn=%s ts=%s: %s",
             lsn_to_string(record->attrs.lsn).c_str(),
             format_time(record->attrs.timestamp).c_str(),
             error_name(err));
    st = err;
  } else {
    ld_check(data_);
    rv = applyDelta(*delta,
                    *data_,
                    record->attrs.lsn,
                    record->attrs.timestamp,
                    failure_reason);
    if (rv != 0) {
      rsm_info(rsm_type_,
               "Could not apply delta record with lsn=%s ts=%s on base with "
               "version %s: %s, %s",
               lsn_to_string(record->attrs.lsn).c_str(),
               format_time(record->attrs.timestamp).c_str(),
               lsn_to_string(version_).c_str(),
               error_name(err),
               failure_reason.c_str());
      st = err;
    } else {
      rsm_info(rsm_type_,
               "Applied delta record with lsn=%s ts=%s",
               lsn_to_string(record->attrs.lsn).c_str(),
               format_time(record->attrs.timestamp).c_str());

      // Only update the version if the delta was successfully applied.
      // This ensures that the replicated state machine version is the version
      // of the last delta (or snapshot) seen by subscribers. Indeed, if a
      // delta cannot be applied, it won't be passed to subscribers.
      // See T21314227.
      version_ = record->attrs.lsn;
    }
  }

  delta_log_byte_offset_ += record->payload.size();
  ++delta_log_offset_;

  if (!header.uuid.is_nil()) {
    auto it = pending_confirmation_by_uuid_.find(header.uuid);
    if (it != pending_confirmation_by_uuid_.end()) {
      // Either the append was not confirmed yet (lsn == LSN_INVALID) or the
      // lsns match.
      ld_check(it->second->lsn == LSN_INVALID ||
               it->second->lsn == record->attrs.lsn);
      it->second->cb(st, record->attrs.lsn, failure_reason);
      pending_confirmation_.erase(it->second);
      pending_confirmation_by_uuid_.erase(it);
    }
  }

  // This call catches the case where we could not parse the deltas's header and
  // thus its uuid.
  discardSkippedPendingDeltas();

  if (st == E::OK &&
      (sync_state_ == SyncState::TAILING || deliver_while_replaying_)) {
    ld_check(delta);
    notifySubscribers(delta.get());
  }

  if (sync_state_ == SyncState::SYNC_DELTAS &&
      record->attrs.lsn >= delta_sync_) {
    // We finished reading the backlog and reached the tail. This function will
    // inform all subscribers of the initial state.
    onReachedDeltaLogTailLSN();
  }

  return true;
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::deserializeDeltaHeader(
    const Payload& payload,
    DeltaHeader& header) {
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(payload.data());
  /**
   * 1. Read the minimum required header, which should include the real size
   *    of the header as well as a checksum. If the payload is too small,
   *    consider there is no header.;
   * 2. Check if the real size of the header is greater than the payload size,
   *    in which case consider there is no header;
   * 3. Validate the checksum of the whole header, if the checksum does not
   *    match, consider there is no header.
   *
   * If the real header size is smaller than sizeof(DeltaHeader), we will read
   * as much as we can and leave the rest default initialized. if the real
   * header size is greater, we will discard the part we don't understand.
   */
  if (payload.size() >= MIN_DELTA_HEADER_SZ) {
    memcpy(&header, ptr, MIN_DELTA_HEADER_SZ);
    const size_t header_sz = header.header_sz;
    if (header_sz <= payload.size() && header_sz >= MIN_DELTA_HEADER_SZ) {
      const auto header_sz_offset = offsetof(DeltaHeader, header_sz);
      const uint32_t checksum = checksum_32bit(
          Slice(ptr + header_sz_offset, header_sz - header_sz_offset));
      if (checksum == header.checksum) {
        header = DeltaHeader{};
        memcpy(&header, ptr, std::min(header_sz, sizeof(DeltaHeader)));
        return true;
      }
    }
  }

  return false;
}

template <typename T, typename D>
int ReplicatedStateMachine<T, D>::deserializeDelta(const DataRecord& record,
                                                   std::unique_ptr<D>& out,
                                                   DeltaHeader& header) {
  bool use_header = deserializeDeltaHeader(record.payload, header);
  size_t payload_sz = record.payload.size();
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(record.payload.data());

  if (use_header) {
    payload_sz -= header.header_sz;
    ptr += header.header_sz;
  } else {
    // Make sure we leave the header default initilized.
    header = DeltaHeader{};
  }

  out = deserializeDelta(Payload(ptr, payload_sz));
  if (!out) {
    return -1;
  }

  return 0;
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::onDeltaGap(const GapRecord& gap) {
  if (waiting_for_snapshot_ != LSN_INVALID) {
    // We are stalling reading the delta log because we missed some data and are
    // waiting for a snapshot.
    return false;
  }

  // keep track of latest gap received
  ld_check(gap.hi > delta_read_ptr_);
  delta_read_ptr_ = gap.hi;

  if (gap.hi <= version_) {
    // We already have a higher version because we read a more recent snapshot,
    // skip this delta gap.
    return true;
  }

  if (snapshot_log_id_ == LOGID_INVALID) {
    if (gap.type == GapType::DATALOSS) {
      rsm_critical(rsm_type_,
                   "Receiving a DATALOSS gap [%s, %s] on delta log %lu.",
                   lsn_to_string(gap.lo).c_str(),
                   lsn_to_string(gap.hi).c_str(),
                   delta_log_id_.val_);
    } else if (gap.type == GapType::TRIM) {
      // When there is no snapshot log configured, the log being trimmed means
      // the state needs to be reset to its defaults.
      version_ = gap.hi;
      data_ = makeDefaultState(version_);
      if (sync_state_ == SyncState::TAILING || deliver_while_replaying_) {
        notifySubscribers();
      }
    }
  } else {
    // If this condition is true, this means we lost data in the delta log. In
    // that case, after we swallow this gap, we should not make any progress in
    // the delta log until we read a snapshot record with a version >= gap.hi,
    // ie that accounted for the missing data.
    //
    // The check for version_ != LSN_OLDEST ensures that we do not hit this code
    // path because of the initial TRIM gap in the delta log and when the
    // snapshot log is empty, which is the case when we migrate existing
    // clusters to using a snapshot log for the event log.
    const bool skipping_data =
        ((gap.type == GapType::DATALOSS && stall_if_data_loss_) ||
         (gap.type == GapType::TRIM && version_ != LSN_OLDEST));

    if (skipping_data) {
      rsm_info(rsm_type_,
               "Receiving a %s gap [%s, %s] on delta log %lu. Stalling "
               "reading the delta log until we receive a snapshot with higher "
               "version.",
               gap.type == GapType::TRIM ? "TRIM" : "DATALOSS",
               lsn_to_string(gap.lo).c_str(),
               lsn_to_string(gap.hi).c_str(),
               delta_log_id_.val_);
      waiting_for_snapshot_ = gap.hi;
      // If this does not get resolved in a timely manner, we'll bump a stat so
      // that an oncall can be notified and manually write a snapshot.
      activateStallGracePeriod();
    }
  }

  if (sync_state_ == SyncState::SYNC_DELTAS && gap.hi >= delta_sync_) {
    onReachedDeltaLogTailLSN();
  }

  return true;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onReachedDeltaLogTailLSN() {
  sync_state_ = SyncState::TAILING;

  rsm_info(rsm_type_, "Reached tail of delta log");

  // If we were not already delivering updates while we were replaying the
  // backlog, now is the time to deliver the first update to subscribers.
  if (!deliver_while_replaying_) {
    notifySubscribers();
  }

  if (stop_at_tail_) {
    // This will unblock anyone that called wait().
    sem_.post();
  }
}

template <typename T, typename D>
std::string
ReplicatedStateMachine<T, D>::createDeltaPayload(std::string user_payload,
                                                 DeltaHeader header) {
  if (!write_delta_header_) {
    return user_payload;
  }

  header.header_sz = sizeof(header);
  const auto past_checksum_offset = offsetof(DeltaHeader, header_sz);
  const auto past_checksum_sz = sizeof(header) - past_checksum_offset;
  const uint32_t checksum = checksum_32bit(
      Slice(reinterpret_cast<uint8_t*>(&header) + past_checksum_offset,
            past_checksum_sz));
  header.checksum = checksum;

  std::string buf;
  buf.resize(user_payload.size() + sizeof(header));

  uint8_t* ptr = reinterpret_cast<uint8_t*>(&buf[0]);
  memcpy(ptr, &header, sizeof(header));
  memcpy(ptr + sizeof(header), &user_payload[0], user_payload.size());

  return buf;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::writeDelta(
    std::string payload,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  ld_check(!stopped_);

  if (mode == WriteMode::CONFIRM_APPLIED) {
    if (sync_state_ != SyncState::TAILING) {
      // We cannot write a delta with CONFIRM_APPLIED flag while we are
      // replaying the backlog.
      cb(E::AGAIN, LSN_INVALID, "Operation is now allowed!");
      return;
    }
    if (!delta_read_stream_is_healthy_) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Cannot write delta to log %lu with CONFIRM_APPLIED "
                     "because the read stream is unhealthy",
                     delta_log_id_.val());
      cb(E::AGAIN, LSN_INVALID, "Cannot perform operation: Please try again!");
      return;
    }
    if (pending_confirmation_.size() > max_pending_confirmation_) {
      // We cannot write a delta with CONFIRM_APPLIED if too many deltas are
      // already pending confirmation.
      cb(E::NOBUFS,
         LSN_INVALID,
         "Cannot perform operation: Too many messages queued already.");
      return;
    }
    if (!write_delta_header_) {
      // If the user decided to not include the header in delta record, deltas
      // written with CONFIRM_APPLIED mode cannot be confirmed this the header
      // is used for that purpose.
      cb(E::NOTSUPPORTED, LSN_INVALID, "Operation Not Supported");
    }
  }

  if (base_version.hasValue()) {
    // The caller asked to write that delta only if the state is at a specific
    // version. Do the check here and fail if they don't match.

    if (base_version.value() < version_) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Cannot write delta to log %lu because the base version "
                     "is too old (%s < %s)",
                     delta_log_id_.val(),
                     lsn_to_string(base_version.value()).c_str(),
                     lsn_to_string(version_).c_str());
      cb(E::STALE, LSN_INVALID, "Cannot perform operation: Version conflict!");
      return;
    }
    // base_version should not be ahead of version_. Make sure they are equal.
    ld_check(base_version.value() == version_);
  }

  DeltaHeader header{};
  header.uuid = uuid_gen_();
  last_uuid_ = header.uuid;

  std::string buf = createDeltaPayload(std::move(payload), header);

  if (mode == WriteMode::CONFIRM_APPLIED) {
    DeltaPendingConfirmation a = {};
    a.uuid = header.uuid;
    a.cb = cb;
    auto it = pending_confirmation_.emplace(
        pending_confirmation_.end(), std::move(a));
    pending_confirmation_by_uuid_[header.uuid] = it;
  }

  auto append_cb = [=](Status st, lsn_t lsn) {
    ld_check(delta_appends_in_flight_ > 0);
    --delta_appends_in_flight_;

    if (st != E::OK) {
      rsm_error(rsm_type_, "Could not write delta: %s.", error_description(st));
    } else {
      rsm_info(rsm_type_,
               "Successfully wrote delta with lsn %s",
               lsn_to_string(lsn).c_str());
    }
    if (mode == WriteMode::CONFIRM_APPLIED) {
      auto it = pending_confirmation_by_uuid_.find(header.uuid);
      if (it != pending_confirmation_by_uuid_.end()) {
        if (st == E::OK) {
          it->second->lsn = lsn;
          activateConfirmTimer(header.uuid);
          // may be we fast forwarded with a snapshot past that lsn.
          discardSkippedPendingDeltas();
        } else {
          it->second->cb(
              st,
              LSN_INVALID,
              "Cannot perform operation: cannot enqueue the message!");
          pending_confirmation_.erase(it->second);
          pending_confirmation_by_uuid_.erase(it);
        }
      }
    } else {
      // We don't pass the failure reason in the case of
      // WriteMode::CONFIRM_APPEND_ONLY because we don't have any!
      cb(st, st == E::OK ? lsn : LSN_INVALID, "");
    }
  };

  ++delta_appends_in_flight_;
  postAppendRequest(
      delta_log_id_, std::move(buf), delta_append_timeout_, append_cb);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::postAppendRequest(
    logid_t logid,
    std::string payload,
    std::chrono::milliseconds timeout,
    std::function<void(Status st, lsn_t lsn)> cb) {
  auto callback_ticket = callbackHelper_.ticket();
  auto cb_wrapper = [=](Status st, const DataRecord& r) {
    const lsn_t lsn = r.attrs.lsn;
    callback_ticket.postCallbackRequest([=](ReplicatedStateMachine<T, D>* p) {
      if (p) {
        cb(st, lsn);
      }
    });
  };

  std::unique_ptr<AppendRequest> req =
      std::make_unique<AppendRequest>(nullptr,
                                      logid,
                                      AppendAttributes(),
                                      std::move(payload),
                                      timeout,
                                      cb_wrapper);

  req->bypassWriteTokenCheck();
  std::unique_ptr<Request> base_req = std::move(req);

  postRequestWithRetrying(base_req);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::activateGracePeriodForFastForward() {
  Worker* w = Worker::onThisThread(false);
  ld_check(w);
  if (!fastForwardGracePeriodTimer_.isAssigned()) {
    fastForwardGracePeriodTimer_.assign(
        [this] { resumeReadStream(snapshot_log_rsid_); });
  }
  fastForwardGracePeriodTimer_.activate(fast_forward_grace_period_);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::cancelGracePeriodForFastForward() {
  fastForwardGracePeriodTimer_.cancel();
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::isGracePeriodForFastForwardActive() {
  return fastForwardGracePeriodTimer_.isActive();
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::activateStallGracePeriod() {
  Worker* w = Worker::onThisThread(false);
  ld_check(w);
  if (!stallGracePeriodTimer_.isAssigned()) {
    stallGracePeriodTimer_.assign([this] {
      if (waiting_for_snapshot_ != LSN_INVALID) {
        WORKER_STAT_INCR(num_replicated_state_machines_stalled);
        bumped_stalled_stat_ = true;
      }
    });
  }
  stallGracePeriodTimer_.activate(fast_forward_grace_period_);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::cancelStallGracePeriod() {
  stallGracePeriodTimer_.cancel();
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::activateGracePeriodForSnapshotting() {
  if (!snapshotting_timer_.isAssigned()) {
    snapshotting_timer_.assign([this] {
      if (canSnapshot()) {
        // Create a snapshot if:
        // 1. We are not already snapshotting;
        // 2. Snapshotting is enabled in the settings;
        // 3. This node is resposnible for snapshots
        // (first node alive according to the FD);
        //
        // We always take a node regardless whether there are new deltas or not.
        rsm_info(rsm_type_, "Taking a new time-based snapshot");
        auto cb = [&](Status st) {
          if (st != E::OK) {
            rsm_error(rsm_type_,
                      "Could not take a time-based snapshot: %s",
                      error_name(st));
          } else {
            rsm_info(rsm_type_, "Time based snapshot was successful");
          }
        };
        snapshot(std::move(cb));
      } else {
        rsm_info(rsm_type_,
                 "Not taking a time-based snapshot on this node now because "
                 "it's not the node responsible for snapshots!");
      }
      // Scheduling the next run.
      snapshotting_timer_.activate(snapshotting_grace_period_);
    });
  }
  snapshotting_timer_.activate(snapshotting_grace_period_);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::cancelGracePeriodForSnapshotting() {
  snapshotting_timer_.cancel();
}

template <typename T, typename D>
bool ReplicatedStateMachine<T, D>::isGracePeriodForSnapshottingActive() {
  return snapshotting_timer_.isActive();
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::activateConfirmTimer(
    boost::uuids::uuid uuid) {
  auto it = pending_confirmation_by_uuid_.find(uuid);
  if (it == pending_confirmation_by_uuid_.end()) {
    return;
  }

  Worker* w = Worker::onThisThread(false);
  ld_check(w);
  ld_check(!it->second->timer);
  it->second->timer = std::make_unique<Timer>();
  it->second->timer->assign([this, uuid] { onDeltaConfirmationTimeout(uuid); });
  it->second->timer->activate(confirm_timeout_);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::onDeltaConfirmationTimeout(
    boost::uuids::uuid uuid) {
  auto it = pending_confirmation_by_uuid_.find(uuid);
  // The timer would have been destroyed if the entry was removed.
  ld_check(it != pending_confirmation_by_uuid_.end());
  // The timer should have been activated after a successful append.
  ld_check(it->second->lsn != LSN_INVALID);
  rsm_error(rsm_type_,
            "Timed out synchronizing the state machine past delta with lsn %s",
            lsn_to_string(it->second->lsn).c_str());
  it->second->cb(E::TIMEDOUT, it->second->lsn, "Operation timed out!");
  pending_confirmation_.erase(it->second);
  pending_confirmation_by_uuid_.erase(it);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::postRequestWithRetrying(
    std::unique_ptr<Request>& rq) {
  Worker* w = Worker::onThisThread();
  Processor* processor = w->processor_;
  processor->postWithRetrying(rq);
}

template <typename T, typename D>
std::unique_ptr<typename ReplicatedStateMachine<T, D>::SubscriptionHandle>
ReplicatedStateMachine<T, D>::subscribe(update_cb_t cb) {
  // If we are tailing, deliver the initial state to this subscriber now,
  // otherwise this will be done when we are done replaying.
  if (sync_state_ == SyncState::TAILING) {
    ld_check(data_);
    cb(*data_, nullptr, version_);
  }

  auto it = subscribers_.emplace(subscribers_.end(), std::move(cb));

  // Cannot use std::make_unique because constructor is private.
  auto* h = new SubscriptionHandle(this, it);
  return std::unique_ptr<SubscriptionHandle>(h);
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::unsubscribe(SubscriptionHandle& h) {
  subscribers_.erase(h.it_);
  h.owner_ = nullptr;
}

template <typename T, typename D>
ReplicatedStateMachine<T, D>::SubscriptionHandle::SubscriptionHandle(
    ReplicatedStateMachine<T, D>* owner,
    typename std::list<update_cb_t>::iterator it)
    : owner_(owner), it_(it) {}

template <typename T, typename D>
ReplicatedStateMachine<T, D>::SubscriptionHandle::~SubscriptionHandle() {
  if (owner_) {
    owner_->unsubscribe(*this);
  }
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::notifySubscribers(const D* delta) {
  for (auto& cb : subscribers_) {
    cb(*data_, delta, version_);
  }
}

template <typename T, typename D>
std::string ReplicatedStateMachine<T, D>::createSnapshotPayload(
    const T& data,
    lsn_t version,
    bool rsm_include_read_pointer_in_snapshot) {
  RSMSnapshotHeader header{
      /*format_version=*/rsm_include_read_pointer_in_snapshot
          ? RSMSnapshotHeader::CONTAINS_DELTA_LOG_READ_PTR_AND_LENGTH
          : RSMSnapshotHeader::BASE_VERSION,
      /*flags=*/0,
      /*byte_offset=*/delta_log_byte_offset_,
      /*offset=*/delta_log_offset_,
      /*base_version=*/version,
      /*delta_log_read_ptr=*/delta_read_ptr_};

  // Determine the size of the header.
  const size_t header_sz = RSMSnapshotHeader::computeLengthInBytes(header);
  ld_check(header_sz > 0);

  // Determine the size of the uncompressed payload.
  const size_t uncompressed_payload_size = serializeState(data, nullptr, 0);

  // Serialize both header and uncompressed payload onto a buffer.
  std::string buf;
  {
    buf.resize(header_sz + uncompressed_payload_size);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&buf[0]);
    auto rv = RSMSnapshotHeader::serialize(header, ptr, header_sz);
    ld_check(rv == header_sz);
    ptr += header_sz;
    rv = serializeState(data, ptr, uncompressed_payload_size);
    ld_check(rv == uncompressed_payload_size);
  }

  if (snapshot_compression_) {
    header.flags |= RSMSnapshotHeader::ZSTD_COMPRESSION;

    // Allocate a new buffer to hold the header and compressed payload.
    const size_t compressed_data_bound =
        ZSTD_compressBound(uncompressed_payload_size);
    ld_check(compressed_data_bound > 0);
    std::string compressed_buf;
    compressed_buf.resize(header_sz + compressed_data_bound);

    // Serialize the header.
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&compressed_buf[0]);
    auto rv = RSMSnapshotHeader::serialize(header, ptr, header_sz);
    ld_check(rv == header_sz);
    ptr += header_sz;

    // Compress the paylaod.
    const uint8_t* ptr_src = reinterpret_cast<uint8_t*>(&buf[0]) + header_sz;
    const int ZSTD_LEVEL = 5;
    auto compressed_size = ZSTD_compress(ptr,                   // dst
                                         compressed_data_bound, // dstCapacity
                                         ptr_src,               // src
                                         uncompressed_payload_size, // srcSize
                                         ZSTD_LEVEL);               // level
    if (ZSTD_isError(compressed_size)) {
      rsm_error(rsm_type_,
                "ZSTD_compress() failed: %s",
                ZSTD_getErrorName(compressed_size));
      ld_check(false);
      return std::string();
    }
    compressed_buf.resize(header_sz + compressed_size);
    return compressed_buf;
  }

  return buf;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::snapshot(std::function<void(Status st)> cb) {
  auto cb_or_noop = [=](Status st) {
    if (cb) {
      cb(st);
    }
  };

  if (snapshot_log_id_ == LOGID_INVALID) {
    rsm_error(rsm_type_,
              "Cannot create snapshot because this replicated state machine "
              "is not configured to use a snapshot log");
    cb_or_noop(E::NOTSUPPORTED);
    return;
  }

  if (snapshot_in_flight_) {
    // We are already writing a snapshot.
    cb_or_noop(E::INPROGRESS);
    return;
  }

  if (sync_state_ != SyncState::TAILING) {
    // call the callback with E::AGAIN if we are still replaying the backlog.
    cb_or_noop(E::AGAIN);
    return;
  }

  bool include_read_ptr =
      Worker::settings().rsm_include_read_pointer_in_snapshot;
  rsm_info(
      rsm_type_,
      "Creating snapshot with version %s, delta_log_read_ptr %s, compression "
      "%s",
      lsn_to_string(version_).c_str(),
      include_read_ptr ? lsn_to_string(delta_read_ptr_).c_str() : "disabled",
      snapshot_compression_ ? "enabled" : "disabled");
  std::string payload =
      createSnapshotPayload(*data_, version_, include_read_ptr);

  // We'll capture these in the lambda below.
  const size_t byte_offset_at_time_of_snapshot = delta_log_byte_offset_;
  const size_t offset_at_time_of_snapshot = delta_log_offset_;

  auto append_cb = [=](Status st, lsn_t lsn) {
    if (st == E::OK) {
      // We don't want to wait for the snapshot to be read before
      // last_snapshot_* members are modified otherwise
      // numDeltaRecordsSinceLastSnapshot() and numBytesSinceLastSnapshot() may
      // report stale values and the user may want to create a snapshot again.
      // We may have read other snapshots in between so make sure we use max().
      last_snapshot_byte_offset_ =
          std::max(byte_offset_at_time_of_snapshot, last_snapshot_byte_offset_);
      last_snapshot_offset_ =
          std::max(offset_at_time_of_snapshot, last_snapshot_offset_);
      ld_info("Snapshot was assigned LSN %s", lsn_to_string(lsn).c_str());
    }
    snapshot_in_flight_ = false;

    onSnapshotCreated(st, payload.size());

    cb_or_noop(st);
  };

  postAppendRequest(
      snapshot_log_id_, payload, snapshot_append_timeout_, append_cb);

  snapshot_in_flight_ = true;
}

template <typename T, typename D>
void ReplicatedStateMachine<T, D>::getDebugInfo(
    InfoReplicatedStateMachineTable& table) const {
  Worker* w = Worker::onThisThread();
  AllClientReadStreams& streams = w->clientReadStreams();
  ClientReadStream* delta_reader = streams.getStream(delta_log_rsid_);
  ClientReadStream* snapshot_reader = streams.getStream(snapshot_log_rsid_);

  table.next();
  table.set<0>(delta_log_id_);
  table.set<1>(snapshot_log_id_);
  table.set<2>(version_);
  if (delta_reader) {
    table.set<3>(delta_reader->getNextLSNToDeliver());
  }
  table.set<4>(delta_sync_);
  if (snapshot_reader) {
    table.set<5>(snapshot_reader->getNextLSNToDeliver());
  }
  table.set<6>(snapshot_sync_);
  table.set<7>(waiting_for_snapshot_);
  table.set<8>(delta_appends_in_flight_);
  table.set<9>(pending_confirmation_.size());
  table.set<10>(snapshot_in_flight_);
  table.set<11>(numBytesSinceLastSnapshot());
  table.set<12>(numDeltaRecordsSinceLastSnapshot());
  table.set<13>(delta_read_stream_is_healthy_);
}

}} // namespace facebook::logdevice
