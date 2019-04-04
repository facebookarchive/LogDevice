/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/StoreStorageTask.h"

#include <initializer_list>

#include <folly/Format.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/protocol/MUTATED_Message.h"
#include "logdevice/common/protocol/STORED_Message.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/RecordCacheDisposal.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

StoreStorageTask::StoreStorageTask(
    const STORE_Header& store_header,
    const StoreChainLink* copyset,
    folly::Optional<lsn_t> block_starting_lsn,
    std::map<KeyType, std::string> optional_keys,
    const std::shared_ptr<PayloadHolder>& payload_holder,
    STORE_Extra extra,
    ClientID reply_to,
    std::chrono::steady_clock::time_point start_time,
    Durability durability,
    bool write_find_time_index,
    bool merge_mutable_per_epoch_log_metadata,
    bool write_shard_id_in_copyset)
    : WriteStorageTask(StorageTask::Type::STORE),
      payload_holder_(payload_holder),
      timestamp_(store_header.timestamp),
      lng_(store_header.last_known_good),
      flags_(store_header.flags),
      payload_raw_(payload_holder->getPayload()),
      rid_(store_header.rid),
      wave_(store_header.wave),
      reply_to_(reply_to),
      recovery_(store_header.flags & STORE_Header::RECOVERY),
      rebuilding_(store_header.flags & STORE_Header::REBUILDING),
      amend_copyset_(store_header.flags & STORE_Header::AMEND),
      drain_(store_header.flags & STORE_Header::DRAINING),
      extra_(extra),
      start_time_(start_time),
      record_header_buf_({}),
      write_op_(
          store_header.rid.logid,
          compose_lsn(store_header.rid.epoch, store_header.rid.esn),
          LocalLogStoreRecordFormat::formRecordHeader(store_header,
                                                      copyset,
                                                      &record_header_buf_,
                                                      write_shard_id_in_copyset,
                                                      optional_keys,
                                                      extra_),
          payload_raw_,
          rebuilding_ ? copyset[0].destination.node()
                      : (store_header.sequencer_node_id.isNodeID()
                             ? folly::make_optional(
                                   store_header.sequencer_node_id.index())
                             : folly::none),
          block_starting_lsn,
          LocalLogStoreRecordFormat::formCopySetIndexEntry(
              store_header,
              extra_,
              copyset,
              block_starting_lsn,
              write_shard_id_in_copyset,
              &copyset_index_entry_buf_),
          {},
          durability < Durability::NUM_DURABILITIES
              ? durability
              : (store_header.flags & STORE_Header::SYNC
                     ? Durability::SYNC_WRITE
                     : Durability::ASYNC_WRITE),
          rebuilding_) {
  task_deadline_ = store_header.timeout_ms > 0
      ? start_time + std::chrono::milliseconds(store_header.timeout_ms)
      : std::chrono::steady_clock::time_point::max();

  if (write_find_time_index) {
    uint64_t timestamp_big_endian = htobe64(store_header.timestamp);
    std::string timestamp_key(
        reinterpret_cast<const char*>(&timestamp_big_endian), sizeof(uint64_t));
    write_op_.index_key_list.emplace_back(
        FIND_TIME_INDEX, std::move(timestamp_key));
  }

  if (optional_keys.find(KeyType::FINDKEY) != optional_keys.end()) {
    write_op_.index_key_list.emplace_back(
        FIND_KEY_INDEX, std::move(optional_keys[KeyType::FINDKEY]));
  }

  for (int i = 0; i < store_header.copyset_size; ++i) {
    copyset_.push_back(copyset[i].destination);
  }

  if (merge_mutable_per_epoch_log_metadata) {
    uint16_t flags = 0;
    if (store_header.flags & STORE_Header::OFFSET_MAP) {
      flags |= MutablePerEpochLogMetadata::Data::SUPPORT_OFFSET_MAP;
    }
    OffsetMap epoch_size_map =
        (extra.offsets_within_epoch.isValid() ? extra.offsets_within_epoch
                                              : OffsetMap::fromLegacy(0));
    metadata_.emplace(
        flags, store_header.last_known_good, std::move(epoch_size_map));
    metadata_write_op_.emplace(store_header.rid.logid,
                               store_header.rid.epoch,
                               metadata_.get_pointer());
  }
}

StoreStorageTask::~StoreStorageTask() = default;

LogStorageStateMap* StoreStorageTask::getLogStateMap() const {
  return &storageThreadPool_->getProcessor().getLogStorageStateMap();
}

LogStorageState& StoreStorageTask::getLogStorageState() {
  return getLogStateMap()->get(rid_.logid, getShardIdx());
}

bool StoreStorageTask::isPreempted(Seal* preempted_by) {
  if (rebuilding_) {
    // Rebuilding doesn't care about seals.
    return false;
  }

  ld_check(preempted_by);

  const auto& log_state = getLogStorageState();
  folly::Optional<Seal> normal_seal =
      log_state.getSeal(LogStorageState::SealType::NORMAL);
  folly::Optional<Seal> soft_seal =
      log_state.getSeal(LogStorageState::SealType::SOFT);
  // both normal and soft seals must have been recovered by StoreStateMachine
  ld_check(normal_seal.hasValue());
  ld_check(soft_seal.hasValue());

  auto result = STORE_Message::checkIfPreempted(rid_,
                                                extra_.recovery_epoch,
                                                normal_seal.value(),
                                                soft_seal.value(),
                                                drain_);
  switch (result.first) {
    case STORE_Message::PreemptResult::PREEMPTED_SOFT_ONLY:
      soft_preempted_only_ = true;
    case STORE_Message::PreemptResult::PREEMPTED_NORMAL:
      ld_check(result.second.valid());
      *preempted_by = result.second;
      return true;
    case STORE_Message::PreemptResult::NOT_PREEMPTED:
      break;
  };

  return false;
}

bool StoreStorageTask::isLsnBeforeTrimPoint() {
  const auto& log_state = getLogStorageState();
  auto trim_point = log_state.getTrimPoint();
  // err on the side of caution
  if (!trim_point.hasValue()) {
    return false;
  }
  if (rid_.lsn() <= trim_point.value()) {
    return true;
  }
  return false;
}

bool StoreStorageTask::isTimedout() const {
  return std::chrono::steady_clock::now() > task_deadline_;
}

size_t StoreStorageTask::getPayloadSize() const {
  return payload_holder_ ? payload_holder_->size() : 0;
}

size_t StoreStorageTask::getNumWriteOps() const {
  return 1 + metadata_write_op_.hasValue();
}

size_t StoreStorageTask::getWriteOps(const WriteOp** write_ops,
                                     size_t write_ops_len) const {
  size_t write_ops_written = 0;
  for (const WriteOp* op : std::initializer_list<const WriteOp*>{
           &write_op_, metadata_write_op_.get_pointer()}) {
    if (op != nullptr && write_ops_written < write_ops_len) {
      write_ops[write_ops_written++] = op;
    } else {
      break;
    }
  }
  return write_ops_written;
}

void StoreStorageTask::onDone() {
  ServerWorker* worker = ServerWorker::onThisThread();
  if (status_ == E::TIMEDOUT) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Skip reply to STORE_Message request %s "
                   "as StoreStorageTask timed out",
                   rid_.toString().c_str());
    STAT_INCR(worker->stats(), store_storage_task_timedout);
    return;
  }

  // Coalesce all local log store failures into E::DISABLED to conform to
  // STORED_Header interface
  if (status_ != E::OK && status_ != E::PREEMPTED && status_ != E::NOSPC &&
      status_ != E::DISABLED && status_ != E::DROPPED &&
      status_ != E::CHECKSUM_MISMATCH) {
    ld_check(status_ == E::LOCAL_LOG_STORE_WRITE);
    // If we failed to write, the store will most likely become read-only;
    // even if not, subsequent writes are unlikely to succeed.
    status_ = E::DISABLED;
  }

  // Bump metadata write stats if metadata has been written successfully
  if (status_ == E::OK && metadata_write_op_.hasValue()) {
    STAT_INCR(worker->stats(), mutable_per_epoch_log_metadata_writes);
  }

  // Bump stat to trigger alarm if corruption was detected
  if (status_ == E::CHECKSUM_MISMATCH) {
    STAT_INCR(worker->stats(), payload_corruption);
  }

  // Record the store latency
  if (rebuilding_) {
    PER_SHARD_HISTOGRAM_ADD(Worker::stats(),
                            store_latency_rebuilding,
                            getShardIdx(),
                            usec_since(start_time_));
  } else {
    PER_SHARD_HISTOGRAM_ADD(
        Worker::stats(), store_latency, getShardIdx(), usec_since(start_time_));
  }

  sendReply(status_);
}

void StoreStorageTask::onDropped() {
  sendReply(E::DROPPED);
}

shard_index_t StoreStorageTask::getShardIdx() const {
  return static_cast<shard_index_t>(storageThreadPool_->getShardIdx());
}

void StoreStorageTask::sendReply(Status status) const {
  if (recovery_) {
    // STORE was for a mutation, reply with MUTATED
    MUTATED_Message::createAndSend(
        MUTATED_Header{
            extra_.recovery_id, rid_, status, seal_, getShardIdx(), wave_},
        reply_to_);
    return;
  }

  STORED_flags_t flags = 0;
  if (synced_) {
    flags |= STORED_Header::SYNCED;
  }
  if (status == E::OK) {
    // TODO (#T10357210): delet dis
    flags |= STORED_Header::AMENDABLE_DEPRECATED;
  }
  if (rebuilding_) {
    flags |= STORED_Header::REBUILDING;
    if (status == E::OK) {
      if (amend_copyset_) {
        WORKER_STAT_INCR(rebuilding_recipient_amended_ok);
      } else {
        WORKER_STAT_INCR(rebuilding_recipient_stored_ok);
      }
    }
  }

  if (soft_preempted_only_) {
    flags |= STORED_Header::PREMPTED_BY_SOFT_SEAL_ONLY;
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  if (worker->getStorageTaskQueueForShard(reply_shard_idx_)->isOverloaded()) {
    flags |= STORED_Header::OVERLOADED;
    WORKER_STAT_INCR(node_overloaded_sent);
  }

  const ShardedStorageThreadPool* sharded_pool =
      worker->processor_->sharded_storage_thread_pool_;
  Status st = sharded_pool->getByIndex(reply_shard_idx_)
                  .getLocalLogStore()
                  .acceptingWrites();
  if (st == E::LOW_ON_SPC) {
    flags |= STORED_Header::LOW_WATERMARK_NOSPC;
    WORKER_STAT_INCR(node_stored_low_on_space_sent);
  }
  // TODO (T35832374) : remove if condition when all servers support OffsetMap
  if (worker->settings().enable_offset_map) {
    flags |= STORE_Header::OFFSET_MAP;
  }

  STORED_Message::createAndSend(
      STORED_Header{rid_, wave_, status, seal_.seq_node, flags, getShardIdx()},
      reply_to_,
      extra_.rebuilding_version,
      extra_.rebuilding_wave,
      extra_.rebuilding_id,
      flushToken_);
}

int StoreStorageTask::putCache() {
  // the write must have been succeeded
  ld_check(status_ == E::OK);
  const auto& log_state = getLogStorageState();
  RecordCache* cache = log_state.record_cache_.get();
  if (cache == nullptr) {
    // caching not enabled
    return -1;
  }

  uint32_t wave_or_recovery_epoch =
      (flags_ & STORE_Header::WRITTEN_BY_RECOVERY ? extra_.recovery_epoch.val_
                                                  : wave_);

  int rv = cache->putRecord(rid_,
                            timestamp_,
                            lng_,
                            wave_or_recovery_epoch,
                            copyset_,
                            flags_,
                            write_op_.getKeys(),
                            payload_raw_,
                            payload_holder_,
                            extra_.offsets_within_epoch);
  if (rv == 0) {
    STAT_ADD(stats(),
             record_cache_bytes_cached_estimate,
             EpochRecordCacheEntry::getBytesEstimate(payload_raw_));
  }
  return rv;
}

StatsHolder* StoreStorageTask::stats() {
  return storageThreadPool_->stats();
}

void StoreStorageTask::getDebugInfoDetailed(StorageTaskDebugInfo& info) const {
  info.log_id = rid_.logid;
  info.lsn = rid_.lsn();
  info.client_id = reply_to_;
  info.extra_info = folly::sformat(
      "wave: {}, timestamp: {}, copyset: {}, recovery: {}, amend: {}",
      wave_,
      timestamp_,
      toString(copyset_.data(), copyset_.size()),
      recovery_,
      amend_copyset_);
}
}} // namespace facebook::logdevice
