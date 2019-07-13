/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/CatchupOneStream.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <utility>

#include <folly/CppAttributes.h>
#include <folly/Optional.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/Checksum.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/ServerRecordFilter.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/protocol/GAP_Message.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Record.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerRecordFilterFactory.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/CatchupOneStream.h"
#include "logdevice/server/read_path/IteratorCache.h"
#include "logdevice/server/storage_tasks/EpochOffsetStorageTask.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

// Macros for covering rare branches during testing. Ugly, but better than a
// bunch of command-line parameters just for testing. It would be even better
// to have a more flexible framework for parametrizing integration tests.
//#define LNG_METADATA_READ_BLOCKS 1
//#define LNG_METADATA_READ_ERROR NOTFOUND
//#define LNG_DATA_READ_BLOCKS 1
//#define LNG_DATA_READ_ERROR NOTFOUND

namespace facebook { namespace logdevice {

using namespace std::literals::chrono_literals;

namespace {

/**
 * Storage task for reading the last known good.
 */
class ReadLngTask final : public StorageTask {
 public:
  ReadLngTask(ServerReadStream* stream,
              WeakRef<CatchupQueue> catchup_queue,
              epoch_t epoch,
              StatsHolder* stats,
              ThreadType thread_type,
              StorageTaskPriority priority);
  void execute() override;
  void onDone() override;
  void onDropped() override;
  ThreadType getThreadType() const override {
    return thread_type_;
  }
  StorageTaskPriority getPriority() const override {
    return priority_;
  }
  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

 private:
  WeakRef<ServerReadStream> stream_;
  WeakRef<CatchupQueue> catchup_queue_;
  StatsHolder* const stats_;
  ThreadType thread_type_;
  StorageTaskPriority priority_;
  const epoch_t epoch_;
  Status status_;
  lsn_t last_known_good_;
};

/**
 * Read last known good from last record in epoch.
 *
 * @return Returns 0 on success and sets last_known_good_out to result.
 *         Returns -1 on error, sets last_known_good_out to LSN_INVALID, and
 *         leaves err at whatever LocalLogStoreReader::getLastKnownGood() set
 *         it to.
 */
int read_last_known_good_from_data_record(lsn_t& last_known_good_out,
                                          const ServerReadStream& stream,
                                          LocalLogStore& store,
                                          bool allow_blocking_io,
                                          StatsHolder* stats);

/**
 * Get LocalLogStore for a given shard.
 *
 * @param shard   Shard for which to look up LocalLogStore.
 * @param worker  ServerWorker of this thread, if known. If not provided, will
 *                call ServerWorker::onThisThread(false).
 * @return        LocalLogStore for the given shard. nullptr when not in a
 *                worker thread.
 */
LocalLogStore* FOLLY_NULLABLE
get_local_log_store(shard_index_t shard, ServerWorker* worker = nullptr);

} // anonymous namespace

template <>
/* static */
const std::string&
EnumMap<CatchupOneStream::Action, std::string>::invalidValue() {
  static const std::string invalidActionName("");
  return invalidActionName;
}

template <>
void EnumMap<CatchupOneStream::Action, std::string>::setValues() {
  static_assert(static_cast<int>(CatchupOneStream::Action::MAX) == 12,
                "Please update CatchupOneStream.cpp after modifying "
                "CatchupOneStream::Action");
  set(CatchupOneStream::Action::TRANSIENT_ERROR, "TRANSIENT_ERROR");
  set(CatchupOneStream::Action::PERMANENT_ERROR, "PERMANENT_ERROR");
  set(CatchupOneStream::Action::WAIT_FOR_BANDWIDTH, "WAIT_FOR_BANDWIDTH");
  set(CatchupOneStream::Action::WAIT_FOR_READ_BANDWIDTH,
      "WAIT_FOR_READ_BANDWIDTH");
  set(CatchupOneStream::Action::WAIT_FOR_STORAGE_TASK, "WAIT_FOR_STORAGE_TASK");
  set(CatchupOneStream::Action::WAIT_FOR_LNG, "WAIT_FOR_LNG");
  set(CatchupOneStream::Action::DEQUEUE_AND_CONTINUE, "DEQUEUE_AND_CONTINUE");
  set(CatchupOneStream::Action::ERASE_AND_CONTINUE, "ERASE_AND_CONTINUE");
  set(CatchupOneStream::Action::REQUEUE_AND_CONTINUE, "REQUEUE_AND_CONTINUE");
  set(CatchupOneStream::Action::REQUEUE_AND_DRAIN, "REQUEUE_AND_DRAIN");
  set(CatchupOneStream::Action::WOULDBLOCK, "WOULDBLOCK");
  set(CatchupOneStream::Action::NOT_IN_REAL_TIME_BUFFER,
      "NOT_IN_REAL_TIME_BUFFER");
}

EnumMap<CatchupOneStream::Action, std::string> CatchupOneStream::action_names;

/**
 * Implementation of LocalLogStoreReader::Callback that ships records to the
 * client.
 *
 * When we are doing nonblocking local log store reads on the worker thread,
 * this callback is passed to LocalLogStoreReader.  Records get copied
 * directly from local log store memory into the output evbuffer, avoiding
 * intermediate buffering inside LogDevice.
 *
 * When we are doing blocking local log store reads on a storage thread, a
 * different Callback buffers the records, making copies of all data and
 * passes them to the worker thread via the ReadStorageTask.  Once on a worker
 * thread, all records from the task are run through an instance of this class
 * and sent to the client.
 */
class ReadingCallback : public LocalLogStoreReader::Callback {
 public:
  ReadingCallback(CatchupOneStream* catchup,
                  ServerReadStream* stream,
                  ServerReadStream::RecordSource source,
                  CatchupEventTrigger reason = CatchupEventTrigger::OTHER)
      : catchup_(catchup),
        stream_(stream),
        store_(get_local_log_store(stream->shard_)),
        source_(source),
        catchup_reason_(reason) {}

  int processRecord(const RawRecord& record) override;

  int nrecords_ = 0;

  int processRecord(const lsn_t lsn,
                    const std::chrono::milliseconds timestamp,
                    const LocalLogStoreRecordFormat::flags_t flags,
                    const std::map<KeyType, std::string>& optional_keys,
                    const Payload& payload,
                    const uint32_t wave,
                    const esn_t last_known_good,
                    const copyset_size_t copyset_size,
                    const ShardID* const copyset,
                    const OffsetMap& offsets_within_epoch);

 private:
  // Sends a RECORD_Message for the given record over the wire
  int shipRecord(lsn_t lsn,
                 std::chrono::milliseconds timestamp,
                 LocalLogStoreRecordFormat::flags_t disk_flags,
                 Payload payload,
                 std::unique_ptr<ExtraMetadata> extra_metadata,
                 OffsetMap offsets);

  std::unique_ptr<ExtraMetadata>
  prepareExtraMetadata(esn_t last_known_good,
                       uint32_t wave,
                       const ShardID copyset[],
                       copyset_size_t copyset_size,
                       OffsetMap offsets_within_epoch);
  /**
   * @return    OffsetMap of epoch record_epoch.
   *            Empty OffsetMap if it is unavailable at this moment.
   */
  OffsetMap getEpochOffsets(epoch_t record_epoch, LogStorageState& log_state);

  CatchupOneStream* catchup_;
  ServerReadStream* stream_;
  LocalLogStore* store_;
  ServerReadStream::RecordSource source_;
  CatchupEventTrigger catchup_reason_;
};

int ReadingCallback::processRecord(const RawRecord& record) {
  const lsn_t lsn = record.lsn;

  // Parse the local log store blob
  std::chrono::milliseconds timestamp;
  Payload payload;
  LocalLogStoreRecordFormat::flags_t flags;
  std::map<KeyType, std::string> optional_keys;
  copyset_size_t copyset_size;
  esn_t last_known_good;
  ShardID* copyset = nullptr;
  uint32_t wave;
  OffsetMap offsets_within_epoch;
  // Parse the copyset only if necessary
  if (stream_->include_extra_metadata_) {
    copyset = (ShardID*)alloca(COPYSET_SIZE_MAX * sizeof(ShardID));
  }
  int rv = LocalLogStoreRecordFormat::parse(
      record.blob,
      &timestamp,
      &last_known_good,
      &flags,
      &wave,
      &copyset_size,
      stream_->include_extra_metadata_ ? copyset : nullptr,
      COPYSET_SIZE_MAX,
      &offsets_within_epoch,
      stream_->filter_pred_ != nullptr ? &optional_keys : nullptr,
      &payload,
      stream_->shard_);

  if (rv != 0) {
    ld_check(false);
    return -1;
  }

  // NOTE: record.from_under_replicated_region reflects the sticky state
  //       of our read iterator, not the absolute state of the partition
  //       the record came from. This ensures that any implied gaps in
  //       the record stream properly reflect any under-replication.
  //
  //       However, this isn't quite enough to ensure implied gaps are
  //       properly classified. Consider two back to back reads, the
  //       first ending at the boundary of an under-replicated region
  //       and the next read starting in a fully-replicated region. Any
  //       implied gap between these two reads must be reported as
  //       under-replicated. In most cases, CatchupOneStream will
  //       issue explicit gaps between reads when necessary. But it doesn't
  //       do this if the reads are for the same "logical read": the first
  //       read attempt being non-blocking, the second blocking. To ensure
  //       under-replication is never under-estimated in this scenario,
  //       we treat the stream under-replication state as sticky too. It
  //       is only reset at the end of each read if the read only accessed
  //       fully replicated portions of the LocalLogStore.
  stream_->in_under_replicated_region_ |= record.from_under_replicated_region;
  return processRecord(lsn,
                       timestamp,
                       flags,
                       optional_keys,
                       payload,
                       wave,
                       last_known_good,
                       copyset_size,
                       copyset,
                       offsets_within_epoch);
}

int ReadingCallback::processRecord(
    const lsn_t lsn,
    const std::chrono::milliseconds timestamp,
    const LocalLogStoreRecordFormat::flags_t flags,
    const std::map<KeyType, std::string>& optional_keys,
    const Payload& payload,
    const uint32_t wave,
    const esn_t last_known_good,
    const copyset_size_t copyset_size,
    const ShardID* const copyset,
    const OffsetMap& offsets_within_epoch) {
  ld_check(lsn > stream_->last_delivered_lsn_);

  // [Experimental Feature] If server-side filtering is enabled, we should
  // do filtering here. If record key can not pass record filter,
  // filtered_out will be set to be true. A gap message with reason
  // FILTERED_OUT will be sent to client-side.
  bool filtered_out = false;

  if (stream_->filter_pred_ != nullptr &&
      (flags & LocalLogStoreRecordFormat::FLAG_OPTIONAL_KEYS)) {
    const auto it = optional_keys.find(KeyType::FILTERABLE);
    if (it != optional_keys.end()) {
      if (!(*stream_->filter_pred_)(it->second)) {
        filtered_out = true;
      }
    }
  }

  // Insert a TRIM gap for any records before this one that have been trimmed
  // between the time the read for this record was scheduled (pushRecords()
  // with its trim check) and read completed. This ensures that the client can
  // proceed and does not declare any data loss for records below the new trim
  // point.
  //
  // NOTE: Trim is advisory and we err on the side of over delivery. If the
  //       record we are processing now is at or below the new trim point, we
  //       deliver the record anyway.
  //
  // Example:
  //   1) Record with lsn=1 was read, and read_ptr_.lsn=2
  //   2) Trimming occured with trim_point=5, and all previous records wiped
  //      out from the log store
  //   3) Next record we read and deliver has lsn=6
  // We don't want the client to detect this 1->6 jump as data loss (assuming
  // other storage nodes don't deliver records in [2, 5]).

  LogStorageState& log_state = catchup_->deps_.getLogStorageStateMap().get(
      stream_->log_id_, stream_->shard_);
  folly::Optional<lsn_t> trim_point = log_state.getTrimPoint();
  if (trim_point.hasValue() &&
      stream_->last_delivered_lsn_ < trim_point.value() &&
      lsn > stream_->last_delivered_lsn_ + 1) {
    int rv = catchup_->sendGAP(
        std::min(trim_point.value(), lsn - 1), GapReason::TRIM);

    if (rv != 0) {
      if (err == E::NOBUFS || err == E::SHUTDOWN) {
        return -1;
      }
      // see below
      ld_error("Got unexpected error from Sender::sendMessage(): %s",
               error_description(err));
      ld_check(false);
      return -1;
    }
  }

  // Iterators are expected to skip amends that don't correspond to any record.
  ld_check(!(flags & LocalLogStoreRecordFormat::FLAG_AMEND));

  std::unique_ptr<ExtraMetadata> extra_metadata;
  if (stream_->include_extra_metadata_) {
    DCHECK_NOTNULL(copyset);
    extra_metadata = this->prepareExtraMetadata(
        last_known_good, wave, copyset, copyset_size, offsets_within_epoch);
  }

  OffsetMap offsets;
  if (stream_->include_byte_offset_ && offsets_within_epoch.isValid()) {
    // epoch OffsetMap value has to be known to determine global OffsetMap.
    OffsetMap epoch_offsets = getEpochOffsets(lsn_to_epoch(lsn), log_state);
    if (epoch_offsets.isValid()) {
      offsets = OffsetMap::mergeOffsets(
          std::move(epoch_offsets), offsets_within_epoch);
    }
  }

  if (filtered_out) {
    // If filtered_out_end_lsn_ is not lsn - 1, FILTERED_OUT gap is not
    // continuous between filtered_out_end_lsn_ and lsn. We deliver last
    // FILTERED_OUT gap and set filtered_out_end_lsn_ to current lsn.
    if (stream_->filtered_out_end_lsn_ != lsn - 1 &&
        (catchup_->sendGapFilteredOutIfNeeded(trim_point) != 0 ||
         (lsn - 1 > stream_->last_delivered_lsn_ &&
          catchup_->sendGAP(lsn - 1, GapReason::NO_RECORDS) != 0))) {
      return -1;
    }

    stream_->filtered_out_end_lsn_ = lsn;
    if (stream_->filtered_out_end_lsn_ >= stream_->getWindowHigh()) {
      if (catchup_->sendGapFilteredOutIfNeeded(trim_point) != 0) {
        return -1;
      }
      stream_->filtered_out_end_lsn_ = LSN_INVALID;
    }

  } else {
    if (catchup_->sendGapFilteredOutIfNeeded(trim_point) != 0) {
      // There is a filtered out gap that has not been shipped.
      return -1;
    }

    int rv = shipRecord(lsn,
                        timestamp,
                        flags,
                        payload,
                        std::move(extra_metadata),
                        std::move(offsets));
    if (rv != 0) {
      return -1;
    }

    // Record has been queued for transmission.  It will either get
    // transmitted to the client or the message will get dropped later.  In
    // the success case we obviously need to update the read stream state.  As
    // for the failure case (RECORD message gets dropped), the messaging layer
    // guarantees this only happens when there is miscommunication or the
    // client disconnects, and the Socket is getting closed.  Since the
    // ClientID is tied to the Socket, it means the read stream will also get
    // destroyed and it doesn't matter what we do.

    ld_check(!stream_->storage_task_in_flight_);
    stream_->last_delivered_lsn_ = lsn;
    stream_->last_delivered_record_ = lsn;
  }

  // NOTE: the outer if() here ensures we don't roll back the stream if a WINDOW
  // message fast-forwarded it since the storage task was created.  #2723465
  if (lsn >= stream_->getReadPtr().lsn) {
    stream_->setReadPtr(lsn + 1);
  }

  stream_->noteSent(catchup_->deps_.getStatsHolder(),
                    source_,
                    RECORD_Message::expectedSize(payload.size()));

  return 0;
}

OffsetMap ReadingCallback::getEpochOffsets(epoch_t record_epoch,
                                           LogStorageState& log_state) {
  if (store_ == nullptr) {
    ld_error("Store is not initialized in ReadingCallback::getEpochOffsets");
    return OffsetMap();
  }
  LogStorageState::LastReleasedLSN last_released_lsn =
      log_state.getLastReleasedLSN();

  // last_released_lsn was known before start reading so it should be known
  // now.
  ld_check(last_released_lsn.hasValue());
  epoch_t last_epoch = lsn_to_epoch(last_released_lsn.value());

  const folly::Optional<std::pair<epoch_t, OffsetMap>>&
      epoch_offsets_from_log_state = log_state.getEpochOffsetMap();

  const folly::Optional<std::pair<epoch_t, OffsetMap>>&
      epoch_offsets_from_metadata = stream_->epoch_offsets_;

  // First check if right epoch offsets are already available from
  // LogStorageState or cached value from PerEpochLogMetadata in
  // ServerReadStream.
  if (epoch_offsets_from_log_state.hasValue() &&
      epoch_offsets_from_log_state.value().first == record_epoch) {
    return epoch_offsets_from_log_state.value().second;
  } else if (epoch_offsets_from_metadata.hasValue() &&
             epoch_offsets_from_metadata.value().first == record_epoch) {
    return epoch_offsets_from_metadata.value().second;
  }

  // PerEpochLogMetadata is written on node after epoch recovery.
  // So, PerEpochLogMetadata for record_epoch is already stored on node if
  // record_epoch is not active epoch which sequencer use.
  // Optimization: PerEpochLogMetadata for epoch (record_epoch.val_ - 1) will be
  // available on node if nodeset for record_epoch epoch still includes this
  // node. In this case epoch_end_offsets of epoch (record_epoch.val_ - 1) can
  // be used as epoch_offsets of record_epoch.

  bool allow_reads_on_workers =
      catchup_->deps_.getSettings().allow_reads_on_workers;

  if (allow_reads_on_workers) {
    EpochRecoveryMetadata metadata;
    int rv = store_->readPerEpochLogMetadata(stream_->log_id_,
                                             epoch_t(record_epoch.val_ - 1),
                                             &metadata,
                                             false,  // find_last_available
                                             false); // allow_blocking_io
    if (rv == 0) {
      ld_check(metadata.header_.epoch_end_offset ==
               metadata.epoch_end_offsets_.getCounter(BYTE_OFFSET));
      return metadata.epoch_end_offsets_;
    } else if (rv != 0 && err == E::LOCAL_LOG_STORE_READ) {
      ld_error("Error while reading PerEpochLogMetadata for epoch %u",
               record_epoch.val_ - 1);
      return OffsetMap();
    } else {
      ld_check(err == E::NOTFOUND || err == E::WOULDBLOCK);
      // Optimization failed. Continue looking for epoch offset.
    }
  }

  if (record_epoch < last_epoch) {
    // Try to get epoch_offsets from PerEpochLogMetadata.
    // First try to read on worker thread without blocking.
    if (allow_reads_on_workers) {
      EpochRecoveryMetadata metadata_;
      int rv = store_->readPerEpochLogMetadata(stream_->log_id_,
                                               record_epoch,
                                               &metadata_,
                                               false,  // find_last_available
                                               false); // allow_blocking_io
      if (rv == 0) {
        ld_check(metadata_.header_.epoch_end_offset ==
                 metadata_.epoch_end_offsets_.getCounter(BYTE_OFFSET));
        return OffsetMap::getOffsetsDifference(
            std::move(metadata_.epoch_end_offsets_), metadata_.epoch_size_map_);
      }
      ld_check(rv == -1);
      if (err == E::NOTFOUND) {
        // for the following conditions, EpochRecoveryMetadata is not present:
        // 1) the cluster just updated from a version running purging v1; OR
        // 2) currently rebuilding does not restore or re-replicate any
        //    metadata, so the metadata might be lost after rebuilding
        RATELIMIT_WARNING(std::chrono::seconds(10),
                          3,
                          "Non empty epoch %u is smaller than active epoch %u "
                          "but missing PerEpochLogMetadata",
                          record_epoch.val_,
                          last_epoch.val_);
        return OffsetMap();
      } else if (err != E::WOULDBLOCK) {
        ld_check(err == E::LOCAL_LOG_STORE_READ);
        ld_error("Error while reading PerEpochLogMetadata for epoch %u",
                 record_epoch.val_);
        return OffsetMap();
      }
      ld_check(err == E::WOULDBLOCK);
      // Proceed to reading on storage thread.
    }

    auto prio = catchup_->getPriorityForStorageTasks();
    auto task = std::make_unique<EpochOffsetStorageTask>(stream_->createRef(),
                                                         stream_->log_id_,
                                                         record_epoch,
                                                         std::get<1>(prio),
                                                         std::get<2>(prio));

    stream_->epoch_task_in_flight = true;
    ServerWorker::onThisThread()
        ->getStorageTaskQueueForShard(stream_->shard_)
        ->putTask(std::move(task));
    STAT_INCR(catchup_->deps_.getStatsHolder(), epoch_offset_to_storage);
  } else {
    // epoch_offsets_from_log_state is not updated or was not fetched yet
    // as checked above.
    // Try to get epoch_offsets from contacting sequencer. The result of log
    // state recovery may be available next times when record have
    // offsets_within_epoch and epoch_offsets has to be found. Request will be
    // rejected if previous one was sent recently.
    catchup_->deps_.recoverLogState(stream_->log_id_,
                                    stream_->shard_,
                                    true // force to send request to sequencer
    );
    return OffsetMap();
  }
  return OffsetMap();
}

int ReadingCallback::shipRecord(lsn_t lsn,
                                std::chrono::milliseconds timestamp,
                                LocalLogStoreRecordFormat::flags_t disk_flags,
                                Payload payload,
                                std::unique_ptr<ExtraMetadata> extra_metadata,
                                OffsetMap offsets) {
  ++nrecords_;

  RECORD_flags_t wire_flags = 0;
  if (extra_metadata) {
    wire_flags |= RECORD_Header::INCLUDES_EXTRA_METADATA;
  }

  // LocalLogStoreRecordFormat.h has asserts that the checksum flag bits match
  // between disk flags and RECORD headers, then copy them from the disk header
  wire_flags |= disk_flags & LocalLogStoreRecordFormat::FLAG_MASK;
  if (stream_->digest_) {
    wire_flags |= RECORD_Header::DIGEST;
  }

  if (stream_->in_under_replicated_region_) {
    wire_flags |= RECORD_Header::UNDER_REPLICATED_REGION;
  }

  RECORD_Header header = {stream_->log_id_,
                          stream_->id_,
                          lsn,
                          static_cast<uint64_t>(timestamp.count()),
                          wire_flags,
                          stream_->shard_};

  if (stream_->no_payload_ || stream_->csi_data_only_) {
    payload = Payload(nullptr, 0);
    // Clear checksum flags if we don't ship payload
    header.flags &= ~(RECORD_Header::CHECKSUM | RECORD_Header::CHECKSUM_64BIT);
    header.flags |= RECORD_Header::CHECKSUM_PARITY;
  } else if (stream_->payload_hash_only_) {
    // Strip checksum from the payload.
    if (header.flags & RECORD_Header::CHECKSUM) {
      size_t checksum_sz = header.flags & RECORD_Header::CHECKSUM_64BIT ? 8 : 4;
      if (!dd_assert(
              payload.size() >= checksum_sz,
              "Malformed record: expected %lu checksum bytes, found %lu bytes; "
              "log: %lu, lsn: %s",
              checksum_sz,
              payload.size(),
              stream_->log_id_.val_,
              lsn_to_string(lsn).c_str())) {
        checksum_sz = payload.size();
      }
      payload = Payload(
          payload.data() ? (const char*)payload.data() + checksum_sz : nullptr,
          payload.size() - checksum_sz);
      header.flags &=
          ~(RECORD_Header::CHECKSUM | RECORD_Header::CHECKSUM_64BIT);
      header.flags |= RECORD_Header::CHECKSUM_PARITY;
    }

    struct {
      uint32_t length;
      uint32_t hash;
    } __attribute__((__packed__)) h;

    h.length = static_cast<uint32_t>(payload.size());
    h.hash = checksum_32bit(Slice(payload));
    payload = Payload(&h, sizeof(h)).dup();
  } else {
    // Make private copy of the data so it is stable for the lifetime of
    // the, possibly deferred on transmission, RECORD message.
    payload = payload.dup();
  }

  if (stream_->include_byte_offset_ && offsets.isValid()) {
    header.flags |= RECORD_Header::INCLUDE_BYTE_OFFSET;
  }

  if (extra_metadata && extra_metadata->offsets_within_epoch.isValid()) {
    header.flags |= RECORD_Header::INCLUDE_OFFSET_WITHIN_EPOCH;
  }

  auto msg =
      std::make_unique<RECORD_Message>(header,
                                       stream_->trafficClass(),
                                       std::move(payload),
                                       std::move(extra_metadata),
                                       RECORD_Message::Source::LOCAL_LOG_STORE,
                                       std::move(offsets),
                                       stream_->log_group_path_);

  if (lsn <= stream_->last_delivered_lsn_) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "Stream will go backwards: "
                       "min_next_lsn(%s), RECORD(%s), Stream(%s)",
                       lsn_to_string(stream_->last_delivered_lsn_ + 1).c_str(),
                       msg->identify().c_str(),
                       toString(*stream_).c_str());
  }

  // Remember how much space we will take in the output evbuffer
  const auto msg_size = msg->size();

  // Let the Sender deal with any traffic shaping induced deferral.
  // We don't want to have to read the data again just because traffic
  // shaping is pacing data.
  int rv =
      catchup_->deps_.sender_->sendMessage(std::move(msg), stream_->client_id_);

  if (rv != 0) {
    ld_check(err != E::CBREGISTERED);
    if (err == E::NOBUFS || err == E::SHUTDOWN) {
      return -1;
    }
    // None of the other error conditions are expected or recoverable when
    // the target is a ClientID:
    //
    // - UNREACHABLE means the client went away, but then this object should
    //   not exist
    // - some of the codes apply only when the target is a NodeID (ours is a
    //   ClientID)
    // - the rest are unrecoverable

    ld_check(false);
    ld_error("got unexpected error from sender::sendMessage(): %s",
             error_description(err));
    return -1;
  }

  if (catchup_reason_ == CatchupEventTrigger::RELEASE) {
    uint64_t latency = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() -
        header.timestamp;
    latency *= 1000;

    HISTOGRAM_ADD(Worker::stats(), write_to_read_latency, latency);
  }

  size_t& bytes_queued = catchup_->record_bytes_queued_;
  ld_check(bytes_queued <= std::numeric_limits<size_t>::max() - msg_size);
  bytes_queued += msg_size;
  ld_spew("record %lu%s queued, msg_size:%zu, record_bytes_queued_ = %zu",
          header.log_id.val_,
          lsn_to_string(header.lsn).c_str(),
          msg_size,
          bytes_queued);
  return 0;
}

std::unique_ptr<ExtraMetadata>
ReadingCallback::prepareExtraMetadata(esn_t last_known_good,
                                      uint32_t wave,
                                      const ShardID copyset[],
                                      copyset_size_t copyset_size,
                                      OffsetMap offsets_within_epoch) {
  auto result = std::make_unique<ExtraMetadata>();
  result->header = {last_known_good, wave, copyset_size};
  result->copyset.assign(copyset, copyset + copyset_size);
  result->offsets_within_epoch = std::move(offsets_within_epoch);
  return result;
}

CatchupOneStream::CatchupOneStream(CatchupQueueDependencies& deps,
                                   ServerReadStream* stream,
                                   BWAvailableCallback& resume_cb)
    : deps_(deps),
      stream_(stream),
      resume_cb_(resume_cb),
      record_bytes_queued_(0) {}

CatchupOneStream::Action
CatchupOneStream::startRead(WeakRef<CatchupQueue> catchup_queue,
                            bool try_non_blocking_read,
                            size_t max_record_bytes_queued,
                            bool first_record_any_size,
                            bool allow_storage_task,
                            CatchupEventTrigger catchup_reason,
                            ReadIoShapingCallback& read_shaping_cb) {
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    const auto& log_map = Worker::settings().dont_serve_reads_logs;
    if (log_map.find(stream_->log_id_) != log_map.end()) {
      Status status = Worker::settings().dont_serve_reads_status;
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Failing reads for log %lu based on settings with status %s",
          stream_->log_id_.val(),
          error_description(status));
      return handleBatchEnd(stream_->version_, status, stream_->getReadPtr());
    }
  }

  LogStorageState& log_state =
      deps_.getLogStorageStateMap().get(stream_->log_id_, stream_->shard_);
  LogStorageState::LastReleasedLSN last_released_lsn =
      log_state.getLastReleasedLSN();
  folly::Optional<lsn_t> trim_point = log_state.getTrimPoint();

  if (sendStarted(last_released_lsn) != 0) {
    ld_check(err != E::CBREGISTERED);
    stream_->last_batch_status_ = "failed to send STARTED";
    return Action::TRANSIENT_ERROR;
  }

  bool needs_recover =
      (!last_released_lsn.hasValue() && !stream_->ignore_released_status_) ||
      !trim_point.hasValue();

  if (needs_recover) {
    // If either trim_point or last_released_lsn (in case we care about it)
    // is not initialized, try to recover it first.
    int rv = deps_.recoverLogState(stream_->log_id_, stream_->shard_);
    if (rv == 0) {
      stream_ld_debug(
          *stream_, "Dequeuing stream because log state needs to be recovered");
      stream_->last_batch_status_ = "recover log state";
      return Action::DEQUEUE_AND_CONTINUE;
    } else {
      stream_->last_batch_status_ = "permanent error in recoverLogState";
      return Action::PERMANENT_ERROR;
    }
  }

  if (stream_->rebuilding_) {
    // We will wake up this stream once the shard finishes rebuilding.
    stream_ld_debug(*stream_, "Dequeuing stream because shard is rebuilding");
    stream_->last_batch_status_ = "rebuilding";
    return Action::DEQUEUE_AND_CONTINUE;
  }

  if (stream_->getReadPtr().lsn <= trim_point.value() ||
      stream_->need_to_deliver_lsn_zero_) {
    // Next requested LSN is not past the trim point. We'll inform the
    // client and fast-forward the stream (we'll keep reading if trim_point+1
    // is still inside client's window).
    int rv = sendGAP(trim_point.value(), GapReason::TRIM);
    if (rv != 0) {
      ld_check(err != E::CBREGISTERED);
      stream_ld_debug(*stream_,
                      "Failed to send a gap up to %s",
                      lsn_to_string(trim_point.value()).c_str());
      stream_->last_batch_status_ = "failed to send gap";
      return Action::TRANSIENT_ERROR;
    }

    if (trim_point.value() >= LSN_MAX) {
      // log was fully trimmed, there's nothing more to read
      stream_ld_debug(*stream_, "trim_point >= LSN_MAX. Erasing stream.");
      stream_->last_batch_status_ = "trim_point >= LSN_MAX";
      return Action::ERASE_AND_CONTINUE;
    }

    // fast forward the stream....
    if (stream_->getReadPtr().lsn <= trim_point.value()) {
      stream_->setReadPtr(trim_point.value() + 1);
    }
  }

  ld_check(!stream_->need_to_deliver_lsn_zero_);

  // We don't care about released records in recovery mode; sequencer is
  // interested in all records we have.
  ld_check(stream_->ignore_released_status_ || last_released_lsn.hasValue());
  lsn_t last_released =
      stream_->ignore_released_status_ ? LSN_MAX : last_released_lsn.value();

  // If the last_released lsn is too low to allow the reader to make progress,
  // bump it up to the highest lsn that is safe to read; that is, the minimum
  // of the last per-epoch released LSN and the last known good (lng) of the
  // epoch.
  //
  // This allows readers to force progress if recovery is stuck in an earlier
  // epoch (T13693894).
  //
  // NOTE: We never bump up the epoch number in last_released, so readers will
  // get stuck at the end of every epoch. This is by design, but we may lift
  // this restriction in the future.
  //
  if (stream_->last_delivered_lsn_ >= last_released) {
    // last_delivered_lsn_ is beyond last_released, but client allows us to read
    // past it, up to the last per-epoch released. But we also cannot read past
    // the lng of the epoch (that would risk reading records that later do not
    // become released). First get the last per-epoch released.
    lsn_t last_per_epoch_released = log_state.getLastPerEpochReleasedLSN();
    ld_check(last_per_epoch_released >= last_released);

    // Refresh last_released. The underlying atomic may have increased in
    // the meantime. Refreshing last_released avoids false positives where
    // stream_->last_delivered_lsn_ would fall between last_released and
    // last_per_epoch_released just because getLastPerEpochReleasedLSN() is
    // called (much) later than getLastReleasedLSN().
    last_released_lsn = log_state.getLastReleasedLSN();
    ld_check(last_released_lsn.hasValue());
    ld_check(last_released <= last_released_lsn.value());
    last_released = last_released_lsn.value();

    if (stream_->last_delivered_lsn_ >= last_released &&
        stream_->last_delivered_lsn_ < last_per_epoch_released) {
      // There is something to read between the last_delivered_lsn and the
      // last_per_epoch_released. We need to read the lng of the epoch to make
      // progress.

      if (lsn_to_epoch(stream_->last_delivered_lsn_) ==
          lsn_to_epoch(last_per_epoch_released)) {
        // By definition, the last per-epoch released and the lng of an epoch
        // are the same if the last per-epoch released falls into that epoch.
        // So we can just use the last per-epoch released here.
        stream_->last_known_good_ = last_per_epoch_released;
      } else if (stream_->last_known_good_ == LSN_INVALID ||
                 lsn_to_epoch(stream_->last_known_good_) !=
                     lsn_to_epoch(stream_->last_delivered_lsn_) ||
                 stream_->last_known_good_ <= stream_->last_delivered_lsn_) {
        // No cached lng or cached lng too low to make progress. Try to read
        // from record cache or store.
        if (readLastKnownGood(catchup_queue, allow_storage_task) != 0) {
          // A storage task is needed and, if allow_storage_task is true, was
          // created.
          if (allow_storage_task) {
            stream_->last_batch_status_ = "sent LNG storage task";
            return Action::WAIT_FOR_LNG;
          } else {
            stream_->last_batch_status_ = "WOULDBLOCK for LNG";
            return Action::WOULDBLOCK;
          }
        }
      }

      if (stream_->last_known_good_ != LSN_INVALID &&
          lsn_to_epoch(stream_->last_known_good_) ==
              lsn_to_epoch(stream_->last_delivered_lsn_) &&
          stream_->last_known_good_ > last_released) {
        stream_ld_debug(
            *stream_,
            "Reading past the global last-released lsn. last_released=%s, "
            "last_per_epoch_released=%s, last_known_good=%s",
            lsn_to_string(last_released).c_str(),
            lsn_to_string(last_per_epoch_released).c_str(),
            lsn_to_string(stream_->last_known_good_).c_str());

        // Bump up last_released to last_known_good_.
        last_released = stream_->last_known_good_;
      }

      // Increment success/fail counters. We succeeded if last_released has
      // been bumped up high enough that we can deliver something.
      if (stream_->last_delivered_lsn_ < last_released) {
        STAT_INCR(deps_.getStatsHolder(), read_past_last_released_success);
      } else {
        STAT_INCR(deps_.getStatsHolder(), read_past_last_released_failed);
      }
    }
  }

  // If `read_ptr' is beyond the lsn that the client expects us to
  // deliver next, notify the client with a gap record so it can use this
  // information to detect potential gaps.
  if (stream_->getReadPtr().lsn > stream_->last_delivered_lsn_ + 1 &&
      stream_->last_delivered_lsn_ + 1 <= stream_->until_lsn_ &&
      last_released > stream_->last_delivered_lsn_) {
    lsn_t end_lsn = std::min(stream_->getReadPtr().lsn - 1, last_released);
    if (sendGapNoRecords(end_lsn) != 0) {
      ld_check(err != E::CBREGISTERED);
      stream_->last_batch_status_ =
          "failed to send no records gap in startRead()";
      return Action::TRANSIENT_ERROR;
    }
  }

  // Delivery of gaps may have completed this stream.
  if (stream_->last_delivered_lsn_ >= stream_->until_lsn_) {
    stream_ld_debug(*stream_,
                    "Stream hit until_lsn(%s), erasing",
                    lsn_to_string(stream_->until_lsn_).c_str());
    stream_->last_batch_status_ = "reached until lsn in startRead()";
    return Action::ERASE_AND_CONTINUE;
  }

  // Or placed us at the end of the WINDOW.
  if (stream_->isPastWindow()) {
    stream_ld_debug(*stream_, "Stream waiting on WINDOW update, dequeuing");
    stream_->last_batch_status_ = "waiting for WINDOW";
    return Action::DEQUEUE_AND_CONTINUE;
  }

  // Or the client has issued a WINDOW update that has moved us into an
  // unreleased range of records.
  if (stream_->getReadPtr().lsn > last_released &&
      stream_->getReadPtr().lsn <= stream_->until_lsn_) {
    if (log_state.hasPermanentError()) {
      // ... but last released lsn will never move forward because of a
      // permanent error.
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Permanent error on read stream %" PRIu64 " for log %lu "
                      "on client %s: stream is caught up but last released lsn "
                      "will never move because purging hit a permanent error.",
                      stream_->id_.val_,
                      stream_->log_id_.val_,
                      Sender::describeConnection(stream_->client_id_).c_str());
      stream_->last_batch_status_ = "permanent error in log_state";
      return Action::PERMANENT_ERROR;
    } else {
      stream_ld_debug(
          *stream_,
          "Dequeing stream because it reached past last_released=%s",
          lsn_to_string(last_released).c_str());
      stream_->last_batch_status_ = "reached last released in startRead()";
      return Action::DEQUEUE_AND_CONTINUE;
    }
  }

  // We must have something to read or we shouldn't be here.
  ld_check(stream_->last_delivered_lsn_ < last_released);
  ld_check(stream_->getReadPtr().lsn <= last_released);

  STAT_INCR(deps_.getStatsHolder(), read_requests);
  ld_check_eq(record_bytes_queued_, 0);

  // Create the read context.
  LocalLogStoreReader::ReadContext read_ctx =
      createReadContext(last_released,
                        max_record_bytes_queued,
                        first_record_any_size,
                        catchup_reason);
  auto& io_fault_injection = IOFaultInjection::instance();
  auto fault =
      io_fault_injection.getInjectedFault(stream_->shard_,
                                          IOFaultInjection::IOType::READ,
                                          IOFaultInjection::FaultType::LATENCY);
  bool inject_latency{fault == IOFaultInjection::FaultType::LATENCY};
  // Injecting latency when reading from the cache means blocking the worker
  // thread which is undesirable, hence read latency injection will happen
  // in the blocking reads. If we want to inject latency we will also bypass
  // non-blocking reads, otherwise most reads will be served from the cache
  // and we won't see the effects of latency injection.

  // We have to do this AFTER computing the last_released, to ensure that any
  // released records <= last_released will be processed below.
  deps_.distributeNewlyReleasedRecords();
  auto released_records = stream_->giveReleasedRecords();

  ld_spew("Real time reads: log id %s read ptr %s first real time record %s",
          toString(stream_->log_id_).c_str(),
          lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
          released_records.empty()
              ? "NONE"
              : lsn_to_string(released_records.front()->begin_lsn_).c_str());

  if (released_records.empty()) {
    if (MetaDataLog::isMetaDataLog(stream_->log_id_)) {
      STAT_INCR(deps_.getStatsHolder(), real_time_no_released_metadata);
    } else {
      STAT_INCR(deps_.getStatsHolder(), real_time_no_released_regular);
    }
  }

  // If we can push realtime records, lets do it!
  if (!released_records.empty() && !inject_latency) {
    Action action = pushReleasedRecords(released_records, read_ctx);
    if (action != Action::NOT_IN_REAL_TIME_BUFFER) {
      deps_.used(stream_->log_id_);
      return action;
    }
  }

  if (try_non_blocking_read && !inject_latency) {
    // First try an immediate non-blocking read on the current worker
    // thread.  If we can get data from the local log store without going to
    // disk, we'll avoid involving a storage thread and the cost of
    // inter-thread communication.
    Action action = readNonBlocking(catchup_queue, read_ctx);
    if (action != Action::WOULDBLOCK) {
      return action;
    }
  }

  if (!allow_storage_task) {
    stream_->last_batch_status_ = "WOULDBLOCK";
    return Action::WOULDBLOCK;
  }

  // We can deliver a little more than max_bytes_to_deliver, because the test we
  // do uses an underestimate of the true value.
  read_ctx.max_bytes_to_deliver_ -=
      std::min(record_bytes_queued_, read_ctx.max_bytes_to_deliver_);
  read_ctx.first_record_any_size_ &= (record_bytes_queued_ == 0);

  bool throttle = deps_.getSettings().enable_read_throttling &&
      !(MetaDataLog::isMetaDataLog(stream_->log_id_) ||
        configuration::InternalLogs::isInternal(stream_->log_id_));

  size_t cost_estimate = 0;
  if (throttle) {
    if (!deps_.canIssueReadIO(read_shaping_cb, stream_)) {
      return Action::WAIT_FOR_READ_BANDWIDTH;
    }
    if (w) {
      cost_estimate = w->settings().max_record_bytes_read_at_once;
    }
    STAT_INCR(deps_.getStatsHolder(), read_throttling_num_storage_tasks_issued);
  }

  readOnStorageThread(
      std::move(catchup_queue), read_ctx, cost_estimate, inject_latency);
  stream_->last_batch_status_ = "sent storage task";
  return Action::WAIT_FOR_STORAGE_TASK;
}

CatchupOneStream::Action CatchupOneStream::pushReleasedRecords(
    std::vector<std::shared_ptr<ReleasedRecords>>& released_records,
    LocalLogStoreReader::ReadContext& read_ctx) {
  ReadingCallback callback(this,
                           stream_,
                           ServerReadStream::RecordSource::REAL_TIME,
                           read_ctx.catchup_reason_);

  int nrecords = 0;
  size_t bytes_delivered = 0;
  bool seen_our_epoch = false;

  // By definition, every record in released_records has been released.  So it's
  // safe to increase last_released_lsn_ to the max in released_records for this
  // epoch.  Also, if we don't update it, we'll drop the associated real time
  // records and have to go to RocksDB next time to get them.
  for (const auto& rec : released_records) {
    if (!same_epoch(rec->end_lsn_, read_ctx.read_ptr_.lsn)) {
      ld_spew("Real time reads: logid %s read_ptr %s different epoch than "
              "released %s",
              toString(stream_->log_id_).c_str(),
              lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
              lsn_to_string(rec->end_lsn_).c_str());

      STAT_INCR(deps_.getStatsHolder(), real_time_records_from_wrong_epoch);
      continue;
    }

    seen_our_epoch = true;

    if (rec->end_lsn_ > read_ctx.last_released_lsn_) {
      // If the below assert fails, it means read_ctx.last_released_lsn_ is from
      // an earlier epoch than the read pointer.  Can this happen during
      // per-epoch releases??  In that case, did we grab the wrong
      // last_released_lsn when we created the read context?
      ld_check(same_epoch(rec->end_lsn_, read_ctx.last_released_lsn_));
      if (same_epoch(rec->end_lsn_, read_ctx.last_released_lsn_)) {
        stream_ld_debug(
            *stream_,
            "Advancing log_id %s's read_ctx.last_released_lsn_ from %s to %s",
            toString(stream_->log_id_).c_str(),
            lsn_to_string(read_ctx.last_released_lsn_).c_str(),
            lsn_to_string(rec->end_lsn_).c_str());

        read_ctx.last_released_lsn_ = rec->end_lsn_;
      } else {
        RATELIMIT_ERROR(1s,
                        1,
                        "read_ptr_ and last_released_lsn_ are from different "
                        "epochs!  Log %s, read_ptr_ %s, last_released_lsn %s",
                        toString(stream_->log_id_).c_str(),
                        lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
                        lsn_to_string(rec->end_lsn_).c_str());
      }
    }
  }

  if (!seen_our_epoch) {
    if (MetaDataLog::isMetaDataLog(stream_->log_id_)) {
      STAT_INCR(deps_.getStatsHolder(), real_time_no_released_metadata);
    } else {
      STAT_INCR(deps_.getStatsHolder(), real_time_no_released_regular);
    }

    ld_spew("Real time reads: logid %s released records from wrong epoch. "
            "read_ptr %s",
            toString(stream_->log_id_).c_str(),
            lsn_to_string(read_ctx.read_ptr_.lsn).c_str());

    // We didn't see any records from our epoch.
    return Action::NOT_IN_REAL_TIME_BUFFER;
  }

  bool found_records_ge_read_ptr{false};
  Status status = E::OK;
  for (const auto& rec : released_records) {
    // Because of per-epoch releases, and maybe even without it, its possible to
    // get interleaved records from different epochs.  For now, ignore the
    // records from a different epoch.
    if (!same_epoch(rec->end_lsn_, read_ctx.read_ptr_.lsn)) {
      continue;
    }

    // Within an epoch, all the records are mostly in-order.  But under memory
    // pressure, we can get multiple overlapping or disjoint ReleasedRecords.
    // Just ignore the out-of-order ones.
    if (*rec > read_ctx.read_ptr_.lsn) {
      if (found_records_ge_read_ptr) {
        if (MetaDataLog::isMetaDataLog(stream_->log_id_)) {
          STAT_INCR(deps_.getStatsHolder(), real_time_out_of_order_metadata);
        } else {
          STAT_INCR(deps_.getStatsHolder(), real_time_out_of_order_regular);
        }

        RATELIMIT_WARNING(1s,
                          1,
                          "Got records from the same epoch out of order.  "
                          "Logid %s read_ptr_ %s released records %s",
                          toString(stream_->log_id_).c_str(),
                          lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
                          toString(*rec).c_str());

        ld_spew("Real time reads: logid %s records too new read_ptr %s first "
                "released %s",
                toString(stream_->log_id_).c_str(),
                lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
                toString(*rec).c_str());
      } else {
        if (MetaDataLog::isMetaDataLog(stream_->log_id_)) {
          STAT_INCR(deps_.getStatsHolder(), real_time_too_new_metadata);
        } else {
          STAT_INCR(deps_.getStatsHolder(), real_time_too_new_regular);
        }

        ld_spew("Real time reads: logid %s records too new read_ptr %s first "
                "released %s",
                toString(stream_->log_id_).c_str(),
                lsn_to_string(read_ctx.read_ptr_.lsn).c_str(),
                toString(*rec).c_str());
      }
      return Action::NOT_IN_REAL_TIME_BUFFER;
    }

    found_records_ge_read_ptr = true;
    // Have we already sent all these records?
    if (*rec < read_ctx.read_ptr_.lsn) {
      continue;
    }

    // The read ptr is within rec.  Consider each entry!
    for (ZeroCopiedRecord* entry = rec->entries_.get(); entry != nullptr;
         entry = entry->next_.get()) {
      // Skip over already-delivered records
      if (entry->lsn < read_ctx.read_ptr_.lsn) {
        continue;
      }

      // SCD filtering
      if (!(*read_ctx.lls_filter_)(
              stream_->log_id_,
              entry->lsn,
              entry->copyset.data(),
              entry->copyset.size(),
              entry->flags & STORE_Header::HOLE,
              RecordTimestamp(std::chrono::milliseconds(entry->timestamp)),
              RecordTimestamp(std::chrono::milliseconds(entry->timestamp)))) {
        continue;
      }

      read_ctx.read_ptr_ = {entry->lsn};
      status = read_ctx.checkBatchComplete(IteratorState::AT_RECORD);
      if (status != E::OK) {
        break;
      }

      size_t msg_size = RECORD_Message::expectedSize(entry->payload_raw.size);

      if (read_ctx.byteLimitReached(nrecords, bytes_delivered, msg_size)) {
        status = E::BYTE_LIMIT_REACHED;
        break;
      }

      nrecords++;

      int rv = callback.processRecord(
          entry->lsn,
          std::chrono::milliseconds(entry->timestamp),
          entry->flags,
          entry->keys,
          Payload(entry->payload_raw.data, entry->payload_raw.size),
          entry->wave_or_recovery_epoch,
          entry->last_known_good,
          entry->copyset.size(),
          entry->copyset.data(),
          entry->offsets_within_epoch);
      if (rv != 0) {
        ld_check_ne(err, E::CBREGISTERED);
        status = E::ABORTED;
        break;
      }
      bytes_delivered += msg_size;
      read_ctx.read_ptr_ = {entry->lsn + 1};
    }

    if (status == E::OK) {
      read_ctx.read_ptr_ = {rec->end_lsn_ + 1};
    }
  }

  if (read_ctx.read_ptr_.lsn > stream_->getReadPtr().lsn) {
    stream_->setReadPtr(read_ctx.read_ptr_.lsn);
  }

  if (status == E::OK) {
    status = read_ctx.read_ptr_.lsn <= stream_->until_lsn_
        ? E::CAUGHT_UP
        : E::UNTIL_LSN_REACHED;
  }

  return handleBatchEnd(stream_->version_, status, read_ctx.read_ptr_);
}

CatchupOneStream::Action
CatchupOneStream::readNonBlocking(WeakRef<CatchupQueue> /*catchup_queue*/,
                                  LocalLogStoreReader::ReadContext& read_ctx) {
  stream_ld_debug(*stream_,
                  "Reading from worker thread. "
                  "last_released=%s, max_record_bytes_queued=%lu, "
                  "first_record_any_size=%s",
                  lsn_to_string(read_ctx.last_released_lsn_).c_str(),
                  read_ctx.max_bytes_to_deliver_,
                  read_ctx.first_record_any_size_ ? "true" : "false");

  LocalLogStore::ReadOptions options("catchup-nonblocking");
  options.allow_blocking_io = false; // on worker thread, disallow blocking I/O
  options.tailing = true;
  options.fill_cache = stream_->fill_cache_;
  // Use the copyset for filtering out records. Even though we can't always make
  // use of the copyset index (e.g. we don't if we fall back to ALL_SEND_ALL
  // from SCD, we choose to enable it uniformly so we don't have to hold
  // separate iterator cache instances for two types of iterators (with CSI
  // enabled and disabled).
  options.allow_copyset_index = true;
  options.csi_data_only = stream_->csi_data_only_;

  // Cache can be nullptr in tests.
  std::shared_ptr<LocalLogStore::ReadIterator> read_iterator;
  if (stream_->iterator_cache_) {
    read_iterator = stream_->iterator_cache_->createOrGet(options);
    ld_check(read_iterator);
  }

  ReadingCallback callback(this,
                           stream_,
                           ServerReadStream::RecordSource::NON_BLOCKING,
                           read_ctx.catchup_reason_);
  Status status = deps_.read(read_iterator.get(), callback, &read_ctx);

  stream_ld_debug(*stream_,
                  "got %d records without blocking, status=%s",
                  callback.nrecords_,
                  error_description(status));

  WORKER_STAT_INCR(non_blocking_reads);
  if (callback.nrecords_ == 0 && status != E::WOULDBLOCK) {
    WORKER_STAT_INCR(non_blocking_reads_empty);
  }

  stream_->in_under_replicated_region_ =
      read_iterator && // May be null in tests.
      read_iterator->accessedUnderReplicatedRegion();

  ld_check(status != E::CBREGISTERED);
  ld_check(status != E::NOBUFS);

  if (status == E::WOULDBLOCK) {
    // E::WOULDBLOCK should not be returned if we already shipped more than the
    // limit.
    //
    // Note that this can fail, because record_bytes_queued_ counts the *actual*
    // bytes queued, whereas all our tests of whether or not to send depend on
    // RECORD_Message::expectedSize(), which doesn't include rebuilding metadata
    // or byte_offset and is therefore an underestimate.
    ld_check(read_ctx.max_bytes_to_deliver_ > record_bytes_queued_);

    // setting the read pointer from the read_ctx, so we don't read the same
    // data twice
    stream_->setReadPtr(read_ctx.read_ptr_);

    stream_ld_debug(
        *stream_, "got WOULDBLOCK from ReadStorageTask::executeNow(), ");

    return Action::WOULDBLOCK;
  }

  return handleBatchEnd(stream_->version_, status, read_ctx.read_ptr_);
}

void CatchupOneStream::readOnStorageThread(
    WeakRef<CatchupQueue> catchup_queue,
    LocalLogStoreReader::ReadContext& read_ctx,
    size_t cost_estimate,
    bool inject_latency) {
  stream_ld_debug(*stream_,
                  "Reading from storage thread. "
                  "last_released=%s, max_record_bytes_queued=%lu, "
                  "first_record_any_size=%s",
                  lsn_to_string(read_ctx.last_released_lsn_).c_str(),
                  read_ctx.max_bytes_to_deliver_,
                  read_ctx.first_record_any_size_ ? "true" : "false");

  LocalLogStore::ReadOptions options("catchup");
  options.allow_blocking_io = true;
  options.tailing = true;
  options.fill_cache = stream_->fill_cache_;
  // Use the copyset for filtering out records. Even though we can't always make
  // use of the copyset index (e.g. we don't if we fall back to ALL_SEND_ALL
  // from SCD, we choose to enable it uniformly so we don't have to hold
  // separate iterator cache instances for two types of iterators (with CSI
  // enabled and disabled).
  options.allow_copyset_index = true;
  options.csi_data_only = stream_->csi_data_only_;
  options.inject_latency = inject_latency;

  std::weak_ptr<LocalLogStore::ReadIterator> read_iterator;
  if (stream_->iterator_cache_ && stream_->iterator_cache_->valid(options)) {
    // Creating an iterator may be an expensive operation, so fetch it here only
    // if an existing one is found in the cache. Otherwise, ReadStorageTask will
    // create a new iterator, while onReadTaskDone() will update
    // iterator_cache_.
    read_iterator = stream_->iterator_cache_->createOrGet(options);
  }

  // Get worker. May be nullptr in unit tests.
  ServerWorker* w = ServerWorker::onThisThread(false);
  Sockaddr client_address;
  if (w) {
    client_address = w->sender().getSockaddr(Address(stream_->client_id_));
  }
  auto prio = getPriorityForStorageTasks();
  auto task_uniq = std::make_unique<ReadStorageTask>(stream_->createRef(),
                                                     std::move(catchup_queue),
                                                     stream_->version_,
                                                     stream_->filter_version_,
                                                     read_ctx,
                                                     options,
                                                     read_iterator,
                                                     cost_estimate,
                                                     stream_->getReadPriority(),
                                                     std::get<0>(prio),
                                                     std::get<1>(prio),
                                                     std::get<2>(prio),
                                                     std::get<3>(prio),
                                                     client_address);
  deps_.putStorageTask(std::move(task_uniq), stream_->shard_);
  STAT_INCR(deps_.getStatsHolder(), read_requests_to_storage);

  ld_check_gt(read_ctx.read_ptr_.lsn, stream_->last_delivered_lsn_);
  ld_check(!stream_->storage_task_in_flight_);
  stream_->storage_task_in_flight_ = true;
}

std::pair<CatchupOneStream::Action, size_t>
CatchupOneStream::read(CatchupQueueDependencies& deps,
                       ServerReadStream* stream,
                       WeakRef<CatchupQueue> catchup_queue,
                       bool try_non_blocking_read,
                       size_t max_record_bytes_queued,
                       bool first_record_any_size,
                       bool allow_storage_task,
                       CatchupEventTrigger catchup_reason) {
  CatchupOneStream catchup(deps, stream, catchup_queue->resumeCallback());
  Action action = catchup.startRead(std::move(catchup_queue),
                                    try_non_blocking_read,
                                    max_record_bytes_queued,
                                    first_record_any_size,
                                    allow_storage_task,
                                    catchup_reason,
                                    stream->read_shaping_cb_);
  return std::make_pair(action, catchup.record_bytes_queued_);
}

std::pair<CatchupOneStream::Action, size_t>
CatchupOneStream::onReadTaskDone(CatchupQueueDependencies& deps,
                                 ServerReadStream* stream,
                                 const ReadStorageTask& task) {
  auto& resume_cb = task.catchup_queue_->resumeCallback();
  CatchupOneStream catchup(deps, stream, resume_cb);
  Action action = catchup.processTask(task);
  return std::make_pair(action, catchup.record_bytes_queued_);
}

CatchupOneStream::Action
CatchupOneStream::processTask(const ReadStorageTask& task) {
  stream_ld_debug(*stream_,
                  "got %zu records, status=%s",
                  task.records_.size(),
                  error_description(task.status_));

  ld_check(task.status_ != E::UNKNOWN);

  bool accessed_under_replicated_region =
      task.owned_iterator_ && // May be null in tests.
      task.owned_iterator_->accessedUnderReplicatedRegion();

  if (stream_->iterator_cache_ &&
      !stream_->iterator_cache_->valid(task.options_)) {
    // We don't have an iterator in cache, either because it was the first
    // batch, or because we invalidated the iterator while the storage task was
    // in flight.
    // Store the iterator that the ReadStorageTask created in cache.
    stream_->iterator_cache_->set(
        task.options_, std::move(task.owned_iterator_));
  }

  // The filter version at the time the task was created is not the same as the
  // current filter version. We must not send the records and do nothing
  // because the stream has rewinded.
  if (stream_->filter_version_ != task.filter_version_) {
    stream_ld_debug(*stream_,
                    "Filter version mismatch: %lu != %lu. Requeuing.",
                    stream_->filter_version_.val_,
                    task.filter_version_.val_);
    return Action::REQUEUE_AND_CONTINUE;
  }

  LocalLogStoreReader::ReadPointer read_ptr = task.read_ctx_.read_ptr_;

  return processRecords(task.records_,
                        task.server_read_stream_version_,
                        read_ptr,
                        accessed_under_replicated_region,
                        task.status_,
                        task.read_ctx_.catchup_reason_);
}

int CatchupOneStream::sendStarted(
    LogStorageState::LastReleasedLSN last_released) {
  ld_check(!resume_cb_.active());

  if (!stream_->needs_started_message_) {
    return 0;
  }

  // to not include last released if the read stream is used for rebuilding
  // or recovery (digest)
  lsn_t last_released_to_include =
      ((stream_->rebuilding_ || stream_->digest_ || !last_released.hasValue())
           ? LSN_INVALID
           : last_released.value());

  STARTED_Header header = {stream_->log_id_,
                           stream_->id_,
                           stream_->rebuilding_ ? E::REBUILDING : E::OK,
                           stream_->filter_version_,
                           last_released_to_include,
                           stream_->shard_};

  auto reply = std::make_unique<STARTED_Message>(
      header,
      stream_->trafficClass(),
      STARTED_Message::Source::LOCAL_LOG_STORE,
      stream_->getReadPtr().lsn);

  StatsHolder* stats = deps_.getStatsHolder();
  if (!stream_->sent_state.empty()) {
    auto& last_sent = stream_->sent_state.back();
    if (last_sent.filter_version > stream_->filter_version_) {
      STAT_INCR(stats, read_stream_start_violations);
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Client filter version went backwards "
                      "for %s stream to client %s "
                      "for log:%lu and read stream:%" PRIu64 ": "
                      "expected filter version >= %" PRIu64 ", saw %" PRIu64,
                      trafficClasses()[stream_->trafficClass()].c_str(),
                      Sender::describeConnection(stream_->client_id_).c_str(),
                      stream_->log_id_.val_,
                      stream_->id_.val_,
                      last_sent.filter_version.val_,
                      stream_->filter_version_.val_);
    }

    // The Client can ask us to send STARTED again, but can't change
    // any characteristics for the stream.
    if (last_sent.filter_version == stream_->filter_version_) {
      if (last_sent.start_lsn != stream_->start_lsn_) {
        STAT_INCR(stats, read_stream_start_violations);
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        1,
                        "Duplicate START request has changed start lsn "
                        "in %s stream to client %s "
                        "for log:%lu, read stream:%" PRIu64
                        ", and filter version %" PRIu64
                        ": expected start lsn %s, saw %s",
                        trafficClasses()[stream_->trafficClass()].c_str(),
                        Sender::describeConnection(stream_->client_id_).c_str(),
                        stream_->log_id_.val_,
                        stream_->id_.val_,
                        stream_->filter_version_.val_,
                        logdevice::toString(last_sent.start_lsn).c_str(),
                        logdevice::toString(stream_->start_lsn_).c_str());
      }
    }
  }

  ld_debug("Read Stream(%" PRIu64 ") Recording sent_state: fv = %" PRIu64
           ", start_lsn = %s",
           stream_->id_.val_,
           stream_->filter_version_.val_,
           logdevice::toString(stream_->start_lsn_).c_str());
  stream_->sent_state.emplace_back(
      stream_->filter_version_, stream_->start_lsn_);

  if (deps_.sender_->sendMessage(std::move(reply), stream_->client_id_) != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "error sending STARTED message for %s to client %s "
                    "for log %lu and read stream %" PRIu64 ": %s",
                    trafficClasses()[stream_->trafficClass()].c_str(),
                    Sender::describeConnection(stream_->client_id_).c_str(),
                    stream_->log_id_.val_,
                    stream_->id_.val_,
                    error_name(err));
    stream_->sent_state.pop_back();
    return -1;
  }

  stream_->needs_started_message_ = false;

  return 0;
}

int CatchupOneStream::sendGapNoRecords(lsn_t no_records_upto) {
  ld_check(no_records_upto != LSN_INVALID);

  LogStorageState& log_state =
      deps_.getLogStorageStateMap().get(stream_->log_id_, stream_->shard_);
  folly::Optional<lsn_t> trim_point = log_state.getTrimPoint();

  int rv = 0;

  // First send a TRIM gap (if any).
  if (trim_point.hasValue() &&
      stream_->last_delivered_lsn_ < trim_point.value()) {
    // Trim point was set after the check in pushRecords().
    rv =
        sendGAP(std::min(trim_point.value(), no_records_upto), GapReason::TRIM);
    if (rv != 0) {
      return rv;
    }
  }

  // If there is a filtered out gap, we need to send it out first.
  if (trim_point.value_or(LSN_INVALID) < stream_->filtered_out_end_lsn_ &&
      sendGapFilteredOutIfNeeded(trim_point) != 0) {
    return rv;
  }
  stream_->filtered_out_end_lsn_ = LSN_INVALID;

  // Then send a NO_RECORDS gap (if any).
  if (stream_->last_delivered_lsn_ < no_records_upto) {
    rv = sendGAP(no_records_upto, GapReason::NO_RECORDS);
  }

  return rv;
}

int CatchupOneStream::sendGapFilteredOutIfNeeded(
    folly::Optional<lsn_t> trim_point) {
  if (trim_point.value_or(LSN_INVALID) < stream_->filtered_out_end_lsn_ &&
      sendGAP(stream_->filtered_out_end_lsn_, GapReason::FILTERED_OUT) != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    3,
                    "Error happend when sending out filtered out gap.start"
                    " lsn:%s, end lsn:%s",
                    lsn_to_string(stream_->last_delivered_lsn_ + 1).c_str(),
                    lsn_to_string(stream_->filtered_out_end_lsn_).c_str());
    return -1;
  }

  stream_->filtered_out_end_lsn_ = LSN_INVALID; // reset filtered out end
  return 0;
}

CatchupOneStream::Action CatchupOneStream::processRecords(
    const std::vector<RawRecord>& records,
    server_read_stream_version_t version,
    const LocalLogStoreReader::ReadPointer& read_ptr,
    bool accessed_under_replicated_region,
    Status status,
    CatchupEventTrigger catchup_reason) {
  // ReadingCallback will actually ship records to the client, as
  // in the non-blocking read path.
  ReadingCallback callback(
      this, stream_, ServerReadStream::RecordSource::BLOCKING, catchup_reason);
  for (const RawRecord& record : records) {
    if (callback.processRecord(record) != 0) {
      ld_check(err != E::CBREGISTERED);
      stream_ld_debug(*stream_,
                      "Could not process record with lsn %s. Aborting.",
                      lsn_to_string(record.lsn).c_str());
      status = E::ABORTED;
      break;
    }
  }

  stream_->in_under_replicated_region_ = accessed_under_replicated_region;

  return handleBatchEnd(version, status, read_ptr);
}

CatchupOneStream::Action CatchupOneStream::handleBatchEnd(
    server_read_stream_version_t stream_version_before,
    Status status,
    const LocalLogStoreReader::ReadPointer& read_ptr) {
  ld_check(status != E::CBREGISTERED);
  if (status != E::ABORTED && status != E::CBREGISTERED &&
      stream_->getReadPtr().lsn <= read_ptr.lsn) {
    // Update the read pointer here to account for skipped records.
    //
    // The read pointer is advanced any time we successfully process a record.
    // However, due to filtering, not all records that are read are processed,
    // allowing our saved read pointer to lag the highest location read.
    //
    // If the status is not E::ABORTED, we were able to read, filter, and
    // selectively process the whole batch returned by the storage task.
    // read_ptr is the pointer that LocalLogStoreReader was able to advance to.
    // It is now safe to advance the stream's read pointer to that value.
    //
    // Note: Many records with the same lsn may be read. The '<= read_ptr.lsn'
    //       check ensures that we do this update even if the skipped records
    //       only differ in wave number from the last saved position.
    stream_->setReadPtr(read_ptr);
  }

  if (status == E::CAUGHT_UP || status == E::WINDOW_END_REACHED) {
    // There were no more records to deliver; stream is most likely caught up.
    // However, if reading was done on a storage thread, only mark it as
    // caught up if the ServerReadStream version has not changed since the
    // task was created.

    // If `read_ptr' is beyond the lsn that the client expects us to
    // deliver next, notify the client with a gap record so it can use this
    // information to detect potential gaps.
    if (read_ptr.lsn > stream_->last_delivered_lsn_ + 1) {
      if (sendGapNoRecords(read_ptr.lsn - 1) != 0) {
        ld_check(err != E::CBREGISTERED);
        stream_->last_batch_status_ =
            "failed to send no records gap up to read_ptr";
        return Action::TRANSIENT_ERROR;
      }
    }

    // Check isPastWindow() again even though status might be
    // E::WINDOW_END_REACHED, because window_high may have been updated before
    // the storage task came back.
    if (stream_->isPastWindow()) {
      stream_ld_debug(*stream_, "Read ptr is past window_high. Dequeuing.");
      stream_->last_batch_status_ = "reached window high";
      return Action::DEQUEUE_AND_CONTINUE;
    } else if (stream_version_before == stream_->version_) {
      // We know last_released_lsn was not updated so it's safe to mark the
      // stream caught up.
      stream_ld_debug(*stream_, "Read ptr is past last_released. Dequeuing.");
      stream_->last_batch_status_ = "reached last released";
      return Action::DEQUEUE_AND_CONTINUE;
    } else {
      // Version changed, so we don't want to call the stream caught up because
      // last_released_lsn may have changed after new records were released.
      // CatchupQueue will re-enqueue it for processing.
      stream_ld_debug(*stream_, "Version changed. Requeuing.");
      stream_->last_batch_status_ = "version changed";
      return Action::REQUEUE_AND_CONTINUE;
    }
  } else if (status == E::UNTIL_LSN_REACHED) {
    if (stream_->last_delivered_lsn_ < stream_->until_lsn_) {
      // We reached the client-supplied until_lsn while reading through the
      // local log store but there is a small range at the end within which we
      // don't have records.  Tell the client about the range so that it knows
      // not to expect anything more from us.
      if (sendGapNoRecords(stream_->until_lsn_) != 0) {
        ld_check(err != E::CBREGISTERED);
        stream_->last_batch_status_ =
            "failed to send no records gap up to until_lsn";
        return Action::TRANSIENT_ERROR;
      }
    }
    stream_ld_debug(*stream_, "Reached until lsn. Erasing.");
    stream_->last_batch_status_ = "reached until lsn";
    return Action::ERASE_AND_CONTINUE;
  }

  // This is just a char* assignment, no string copying.
  stream_->last_batch_status_ = error_name(status);

  switch (status) {
    case E::ABORTED:
      return Action::TRANSIENT_ERROR;
    case E::CBREGISTERED:
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          1,
          "CBREGISTERED returned while reading from local log store(log:%lu)."
          " This should not occur. Returning WAIT_FOR_BANDWIDTH.",
          stream_->log_id_.val());
      return Action::WAIT_FOR_BANDWIDTH;
    case E::FAILED:
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Error reading from local log store(log:%lu),"
                      " returning PERMANENT_ERROR.",
                      stream_->log_id_.val());
      return Action::PERMANENT_ERROR;
    case E::BYTE_LIMIT_REACHED:
      stream_ld_debug(*stream_, "Byte limit reached. Requeuing and draining.");
      return Action::REQUEUE_AND_DRAIN;
    case E::PARTIAL:
      // E::PARTIAL means that LocalLogStoreReader reached the limit of bytes to
      // read from the local log store in one batch, which generally happens if
      // most of the records seen were filtered out. In that case, it is better
      // to update the client with a gap so that it can make progress.
      if (read_ptr.lsn > stream_->last_delivered_lsn_ + 1) {
        if (sendGapNoRecords(read_ptr.lsn - 1) != 0) {
          ld_check(err != E::CBREGISTERED);
          return Action::TRANSIENT_ERROR;
        }
      }
      stream_ld_debug(*stream_, "Reached read limit in one batch. Requeuing.");
      return Action::REQUEUE_AND_CONTINUE;
    default: // Remaining cases should be handled above.
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      3,
                      "Got unknown status: %s.",
                      error_name(status));
      ld_check(false);
      return Action::ERASE_AND_CONTINUE;
  }
}

int CatchupOneStream::sendGAP(lsn_t end_lsn,
                              GapReason reason,
                              lsn_t start_lsn) {
  ld_check(stream_);
  if (start_lsn == LSN_INVALID) {
    start_lsn = stream_->need_to_deliver_lsn_zero_
        ? LSN_INVALID
        : stream_->last_delivered_lsn_ + 1;
  }

  // Readers don't care about anything after until_lsn. Make sure the right
  // endpoint is within the range.
  end_lsn = std::min(end_lsn, stream_->until_lsn_);

  stream_ld_debug(*stream_,
                  "Sending GAP [%s, %s], reason=%s",
                  lsn_to_string(start_lsn).c_str(),
                  lsn_to_string(end_lsn).c_str(),
                  gap_reason_names[reason].c_str());

  GAP_flags_t wire_flags = 0;
  if (stream_->digest_) {
    wire_flags |= GAP_Header::DIGEST;
  }

  if (reason == GapReason::NO_RECORDS && stream_->in_under_replicated_region_) {
    reason = GapReason::UNDER_REPLICATED;
  }

  auto message = std::make_unique<GAP_Message>(GAP_Header{stream_->log_id_,
                                                          stream_->id_,
                                                          start_lsn,
                                                          end_lsn,
                                                          reason,
                                                          wire_flags,
                                                          stream_->shard_},
                                               stream_->trafficClass());

  if (start_lsn <= stream_->last_delivered_lsn_) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       1,
                       "Stream will go backwards: "
                       "min_next_lsn(%s), GAP(%s), Stream(%s)",
                       lsn_to_string(stream_->last_delivered_lsn_ + 1).c_str(),
                       message->identify().c_str(),
                       toString(*stream_).c_str());
  }

  const int rv =
      deps_.sender_->sendMessage(std::move(message), stream_->client_id_);
  if (rv == 0) {
    ld_check(end_lsn > stream_->last_delivered_lsn_ ||
             stream_->need_to_deliver_lsn_zero_);
    ld_check(!stream_->storage_task_in_flight_);
    stream_->last_delivered_lsn_ = end_lsn;
    stream_->need_to_deliver_lsn_zero_ = false;
  } else {
    stream_ld_debug(*stream_,
                    "Failed to send GAP [%s, %s], err=%s",
                    lsn_to_string(start_lsn).c_str(),
                    lsn_to_string(end_lsn).c_str(),
                    error_description(err));
  }
  return rv;
}

LocalLogStoreReader::ReadContext
CatchupOneStream::createReadContext(lsn_t last_released_lsn,
                                    size_t max_record_bytes_queued,
                                    bool first_record_any_size,
                                    CatchupEventTrigger catchup_reason) {
  auto filter = std::make_shared<LocalLogStoreReadFilter>();
  if (stream_->scdEnabled()) {
    ShardID shard(deps_.getMyNodeID().index(), stream_->shard_);
    filter->scd_my_shard_id_ = shard;
    filter->scd_replication_ = stream_->replication_;
    filter->scd_copyset_reordering_ = stream_->scdCopysetReordering();
    stream_->csidHash(filter->csid_hash_pt1, filter->csid_hash_pt2);
    filter->scd_known_down_ = stream_->getKnownDown();

    if (stream_->localScdEnabled()) {
      auto client_location = std::make_unique<NodeLocation>();
      if (!client_location->fromDomainString(stream_->client_location_)) {
        filter->client_location_ = std::move(client_location);
        filter->setUpdateableConfig(
            ServerWorker::onThisThread()->getUpdateableConfig());
      } else {
        filter->client_location_ = nullptr;
      }
    }
  }

  LocalLogStoreReader::ReadContext read_ctx(stream_->log_id_,
                                            stream_->getReadPtr(),
                                            stream_->until_lsn_,
                                            stream_->getWindowHigh(),
                                            std::chrono::milliseconds::max(),
                                            last_released_lsn,
                                            max_record_bytes_queued,
                                            first_record_any_size,
                                            false, // is_rebuilding
                                            std::move(filter),
                                            catchup_reason);

  return read_ctx;
}

std::tuple<StorageTaskType,
           StorageTaskThreadType,
           StorageTaskPriority,
           StorageTaskPrincipal>
CatchupOneStream::getPriorityForStorageTasks() {
  bool metadata = MetaDataLog::isMetaDataLog(stream_->log_id_) ||
      configuration::InternalLogs::isInternal(stream_->log_id_);
  bool internal = stream_->is_internal_;
  bool tail = stream_->trafficClass() != TrafficClass::READ_BACKLOG;

  StorageTaskType type;
  StorageTaskPriority priority;
  StorageTaskPrincipal principal;
  if (metadata) {
    if (internal) {
      return std::make_tuple(StorageTaskType::READ_METADATA_INTERNAL,
                             StorageTaskThreadType::DEFAULT,
                             StorageTaskPriority::HIGH,
                             StorageTaskPrincipal::READ_INTERNAL);
    } else {
      return std::make_tuple(StorageTaskType::READ_METADATA_NORMAL,
                             StorageTaskThreadType::DEFAULT,
                             StorageTaskPriority::MID,
                             StorageTaskPrincipal::READ_TAIL);
    }
  } else {
    if (internal) {
      return std::make_tuple(StorageTaskType::READ_INTERNAL,
                             StorageTaskThreadType::SLOW,
                             StorageTaskPriority::VERY_HIGH,
                             StorageTaskPrincipal::READ_INTERNAL);
    } else if (tail) {
      return std::make_tuple(StorageTaskType::READ_TAIL,
                             StorageTaskThreadType::SLOW,
                             StorageTaskPriority::HIGH,
                             StorageTaskPrincipal::READ_TAIL);
    } else {
      return std::make_tuple(StorageTaskType::READ_BACKLOG,
                             StorageTaskThreadType::SLOW,
                             StorageTaskPriority::MID,
                             StorageTaskPrincipal::READ_BACKLOG);
    }
  }
}

int CatchupOneStream::readLastKnownGood(WeakRef<CatchupQueue> catchup_queue,
                                        bool allow_storage_task) {
  ServerReadStream& stream = *stream_;
  epoch_t epoch = lsn_to_epoch(stream.last_delivered_lsn_);

  // First, check the RecordCache. It keeps track of the lng of every unclean
  // epoch, which is exactly the information we need. (We do not need LNGs of
  // clean epochs, since the global last released LSN already covers those.)
  if (const auto& record_cache = deps_.getLogStorageStateMap()
                                     .get(stream.log_id_, stream.shard_)
                                     .record_cache_) {
    const auto cache_result = record_cache->getEpochRecordCache(epoch);
    switch (cache_result.first) {
      case RecordCache::Result::HIT:
        // Cache hit (typical case).
        stream.last_known_good_ =
            compose_lsn(epoch, cache_result.second->getLNG());
        return 0;
      case RecordCache::Result::NO_RECORD:
        // Guaranteed no records for desired epoch. Cannot possibly have
        // metadata either.
        stream.last_known_good_ = LSN_INVALID;
        return 0;
      case RecordCache::Result::MISS:
        // Cache miss. This can happen if the storage node was restarted,
        // because the RecordCache is not rebuilt. We have to read the metadata
        // from the log store.
        break;
    }
  }

  // Get worker. May be nullptr in unit tests.
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w == nullptr) {
    stream_->last_known_good_ = LSN_INVALID;
    return 0;
  }

  // Next, try a non-blocking read of per-epoch metadata.
  stream_ld_debug(
      stream, "Reading MutablePerEpochLogMetadata for epoch=%u", epoch.val_);
  StatsHolder* stats = deps_.getStatsHolder();
  MutablePerEpochLogMetadata metadata;
  LocalLogStore* store = get_local_log_store(stream.shard_, w);
  ld_check(store != nullptr);

  bool success;
#if LNG_METADATA_READ_BLOCKS
  success = false;
  err = E::WOULDBLOCK;
#elif defined(LNG_METADATA_READ_ERROR)
  success = false;
  err = E::LNG_METADATA_READ_ERROR;
#else
  if (deps_.getSettings().allow_reads_on_workers) {
    STAT_INCR(stats, last_known_good_from_metadata_reads);
    success = store->readPerEpochLogMetadata(
                  stream.log_id_, epoch, &metadata, false, false) == 0;
  } else {
    success = false;
    err = E::WOULDBLOCK;
  }
#endif

  if (success) {
    stream_ld_debug(stream,
                    "Non-blocking read of MutablePerEpochLogMetadata completed "
                    "for epoch=%u, "
                    "lng=%u",
                    epoch.val_,
                    metadata.data_.last_known_good.val_);
    stream.last_known_good_ =
        compose_lsn(epoch, metadata.data_.last_known_good);
    return 0;
  }

  // Non-blocking read of per-epoch metadata failed. Check why.
  switch (err) {
    default:
      // Unexpected error code.
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          3,
          "Error while reading MutablePerEpochLogMetadata for epoch=%u, "
          "status=%s",
          epoch.val_,
          error_description(err));
      // Do not fail yet. We may still be able to read the LNG from a data
      // record.
      FOLLY_FALLTHROUGH;
    case E::NOTFOUND:
      // Failed to read the metadata for some other reason than WOULDBLOCK.
      // There is no point in trying a blocking read of the metadata, it would
      // most likely fail as well. Try non-blocking read of last data record in
      // epoch instead. If that fails, never mind. (A blocking read would be a
      // performance hazard, so we do not allow that for now.)
      if (deps_.getSettings().allow_reads_on_workers &&
          read_last_known_good_from_data_record(
              stream.last_known_good_, stream, *store, false, stats) != 0 &&
          err != E::NOTFOUND && err != E::WOULDBLOCK) {
        // Unexpected error code.
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        3,
                        "Error while reading data record for epoch=%u, "
                        "status=%s",
                        epoch.val_,
                        error_description(err));
      }
      err = E::OK;
      return 0;
    case E::WOULDBLOCK:
      // Need to create a storage task for blocking IO. Will call
      // catchup_queue->resumeCallback() when done or dropped.
      if (allow_storage_task) {
        ld_check(!stream.storage_task_in_flight_);
        stream.storage_task_in_flight_ = true;
        auto prio = getPriorityForStorageTasks();
        w->getStorageTaskQueueForShard(stream.shard_)
            ->putTask(std::make_unique<ReadLngTask>(stream_,
                                                    std::move(catchup_queue),
                                                    epoch,
                                                    stats,
                                                    std::get<1>(prio),
                                                    std::get<2>(prio)));
      }
      return -1;
  }
}

namespace {

ReadLngTask::ReadLngTask(ServerReadStream* stream,
                         WeakRef<CatchupQueue> catchup_queue,
                         epoch_t epoch,
                         StatsHolder* stats,
                         ThreadType thread_type,
                         StorageTaskPriority priority)
    : StorageTask(StorageTask::Type::READ_LNG),
      stream_(stream->createRef()),
      catchup_queue_(std::move(catchup_queue)),
      stats_(stats),
      thread_type_(thread_type),
      priority_(priority),
      epoch_(epoch),
      status_(E::UNKNOWN),
      last_known_good_(LSN_INVALID) {}

void ReadLngTask::execute() {
  if (ServerReadStream* stream = stream_.get()) {
    LocalLogStore& store = storageThreadPool_->getLocalLogStore();

#ifndef LNG_METADATA_READ_ERROR
    // Try a blocking read of the per-epoch metadata.
    STAT_INCR(stats_, last_known_good_from_metadata_reads_to_storage);
    MutablePerEpochLogMetadata metadata;
    if (store.readPerEpochLogMetadata(
            stream_->log_id_, epoch_, &metadata, false, true) == 0) {
      // Successful read of metadata.
      last_known_good_ = compose_lsn(epoch_, metadata.data_.last_known_good);
      status_ = E::OK;
      return;
    }
#else
    err = E::LNG_METADATA_READ_ERROR
#endif

    // Metadata read failed. Try non-blocking read of last data record in epoch.
    // This is our last chance. (A blocking read would be a performance hazard,
    // so we do not allow that for now.)
    if (read_last_known_good_from_data_record(
            last_known_good_, *stream, store, false, stats_) == 0) {
      status_ = E::OK;
      return;
    }

    // Failure.
    status_ = err;
  }
}

void ReadLngTask::onDone() {
  if (ServerReadStream* stream = stream_.get()) {
    switch (status_) {
      case E::NOTFOUND:
      case E::OK:
        stream_ld_debug(*stream,
                        "ReadLngTask completed for epoch=%u, lng=%u, status=%s",
                        epoch_.val_,
                        lsn_to_esn(last_known_good_).val_,
                        error_description(status_));
        break;
      default:
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        3,
                        "Error in ReadLngTask for log=%ld, epoch=%u, status=%s",
                        stream->log_id_.val_,
                        epoch_.val_,
                        error_description(status_));
        break;
    }
    stream->last_known_good_ = last_known_good_;
  }
  if (CatchupQueue* catchup_queue = catchup_queue_.get()) {
    catchup_queue->onReadLngTaskDone(stream_.get());
  }
}

void ReadLngTask::onDropped() {
  if (ServerReadStream* stream = stream_.get()) {
    stream->last_known_good_ = LSN_INVALID;
  }
  if (CatchupQueue* catchup_queue = catchup_queue_.get()) {
    catchup_queue->onStorageTaskDropped(stream_.get());
  }
}

int read_last_known_good_from_data_record(lsn_t& last_known_good_out,
                                          const ServerReadStream& stream,
                                          LocalLogStore& store,
                                          bool allow_blocking_io,
                                          StatsHolder* stats) {
#if LNG_DATA_READ_BLOCKS
  if (!allow_blocking_io) {
    err = E::WOULDBLOCK;
    return -1;
  }
#endif
#ifdef LNG_DATA_READ_ERROR
  err = E::LNG_DATA_READ_ERROR;
  return -1;
#endif

  if (!allow_blocking_io) {
    err = E::WOULDBLOCK;
    return -1;
  }

  // Create log store iterator. Cannot use cached iterator, because we need to
  // perform a random access (no tail read).
  epoch_t epoch = lsn_to_epoch(stream.last_delivered_lsn_);
  LocalLogStore::ReadOptions read_options("lng-from-record");
  read_options.allow_blocking_io = allow_blocking_io;
  read_options.fill_cache = false;
  auto iterator = store.read(stream.log_id_, read_options);

  // Update stats.
  if (allow_blocking_io) {
    STAT_INCR(stats, last_known_good_from_record_reads_to_storage);
  } else {
    STAT_INCR(stats, last_known_good_from_record_reads);
  }

  // Use LocalLogStoreReader::getLastKnownGood() utility function. It will
  // seek to the beginning of the next epoch and iterate one record back.
  esn_t lng;
  int rv = LocalLogStoreReader::getLastKnownGood(
      *iterator, epoch, &lng, nullptr, nullptr);
  last_known_good_out = rv == 0 ? compose_lsn(epoch, lng) : LSN_INVALID;
  return rv;
}

LocalLogStore* FOLLY_NULLABLE get_local_log_store(shard_index_t shard,
                                                  ServerWorker* worker) {
  // Get worker if not already provided.
  worker = worker ?: ServerWorker::onThisThread(false);
  if (worker == nullptr) {
    // May happen in unit tests.
    return nullptr;
  } else {
    ld_check(worker->processor_ != nullptr);
    ld_check(worker->processor_->sharded_storage_thread_pool_ != nullptr);
    return &worker->processor_->sharded_storage_thread_pool_->getByIndex(shard)
                .getLocalLogStore();
  }
}

} // anonymous namespace

}} // namespace facebook::logdevice
