/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage/SealStorageTask.h"

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/SEALED_Message.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/RecordCache.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

SealStorageTask::SealStorageTask(logid_t log_id,
                                 epoch_t last_clean,
                                 Seal seal,
                                 const Address& reply_to,
                                 bool tail_optimized)
    : StorageTask(StorageTask::Type::SEAL),
      log_id_(log_id),
      last_clean_(last_clean),
      seal_epoch_(seal.epoch),
      context_(Context::RECOVERY),
      reply_to_(reply_to),
      tail_optimized_(tail_optimized),
      seal_(seal),
      status_(E::OK) {
  ld_check(seal_.validOrEmpty());
}

SealStorageTask::SealStorageTask(logid_t log_id,
                                 epoch_t last_clean,
                                 Seal seal,
                                 WeakRef<PurgeUncleanEpochs> driver)
    : StorageTask(StorageTask::Type::SEAL),
      log_id_(log_id),
      last_clean_(last_clean),
      seal_epoch_(seal.epoch),
      context_(Context::PURGING),
      reply_to_(Address(NodeID())), // not used
      tail_optimized_(false),       // not used
      seal_(seal),
      status_(E::OK),
      purge_driver_(std::move(driver)) {
  ld_check(seal_.validOrEmpty());
  ld_check(!reply_to_.valid());
}

void SealStorageTask::execute() {
  status_ =
      executeImpl(storageThreadPool_->getLocalLogStore(),
                  storageThreadPool_->getProcessor().getLogStorageStateMap(),
                  storageThreadPool_->stats());
}

shard_index_t SealStorageTask::getShardIdx() const {
  ld_check(storageThreadPool_);
  return storageThreadPool_->getShardIdx();
}

Status SealStorageTask::executeImpl(LocalLogStore& store,
                                    LogStorageStateMap& state_map,
                                    StatsHolder* stats) {
  // quickly check the current value of the seal to avoid reading LNGs if
  // E::PREEMPTED would be returned

  LogStorageState* log_state = state_map.insertOrGet(log_id_, getShardIdx());
  if (log_state == nullptr) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Unable to update seal for log %lu: %s",
                    log_id_.val_,
                    error_description(err));
    return E::FAILED;
  }

  if (context_ == Context::PURGING) {
    // take a shortcut if we are in the purging context
    return sealForPurging(store, log_state, stats);
  }

  folly::Optional<Seal> current_seal =
      log_state->getSeal(LogStorageState::SealType::NORMAL);

  if (current_seal.hasValue() && current_seal.value() > seal_) {
    // return the current value to the sequencer
    seal_ = current_seal.value();
    return E::PREEMPTED;
  }

  // recover soft seal metadata before accessing the record cache, this could
  // increase the cache hit rate since the cache may have its last
  // nonauthoritative epoch initialized.
  recoverSoftSeals(store, log_state);

  Status status = E::OK;
  SealMetadata seal_metadata{seal_};

  LocalLogStore::WriteOptions write_options;
  int rv = store.updateLogMetadata(log_id_, seal_metadata, write_options);
  if (rv != 0) {
    ld_check(durability_ == Durability::INVALID);

    if (err != E::UPTODATE) {
      return E::FAILED;
    }

    if (seal_metadata.seal_ > seal_) {
      // the local log store previous had a larger Seal, this request is
      // preempted
      status = E::PREEMPTED;
      // even if updateLogMetadata() returns UPTODATE, we still want to update
      // the state map in case seal record hasn't found its way there yet
    } else {
      // the local log store already have the seal with the same Seal;
      // this could be the retries from the sequencer, so consider it as
      // success
      ld_check(seal_metadata.seal_.epoch == seal_.epoch);
      dd_assert(seal_metadata.seal_.seq_node.index() == seal_.seq_node.index(),
                "Seal of log %lu with same epoch (%u) but different seq_node "
                "(%s != %s).",
                log_id_.val(),
                seal_.epoch.val(),
                seal_metadata.seal_.seq_node.toString().c_str(),
                seal_.seq_node.toString().c_str());
    }
  } else {
    // update successful, sync is needed
    durability_ = Durability::SYNC_WRITE;
  }

  // If status is E::PREEMPTED, this'll update seal_ to point to the value
  // stored in the local log store.
  seal_ = seal_metadata.seal_;

  rv = log_state->updateSeal(
      seal_metadata.seal_, LogStorageState::SealType::NORMAL);
  if (rv != 0) {
    // update seal_ to the updated value
    current_seal = log_state->getSeal(LogStorageState::SealType::NORMAL);
    ld_check(current_seal.hasValue());
    seal_ = current_seal.value();
    status = E::PREEMPTED;
  }

  // as for the final step, check for soft seals for preemption, this must be
  // done AFTER normal seal is updated in local log store.
  Status soft_seal_status = checkSoftSeals(store, log_state);
  if (soft_seal_status != E::OK) {
    return soft_seal_status;
  }

  if (status != E::OK) {
    // For log recovery, do not try to get epoch info (which might be expensive)
    // if seal status is not E::OK (e.g., PREEMPTED). Epoch recovery state
    // machine doesn't care about epoch info in these cases
    return status;
  }

  Status epoch_info_status = getEpochInfo(store, log_state, stats);
  // must populate epoch_info_ on E::OK
  ld_check(epoch_info_status != E::OK || epoch_info_ != nullptr);
  return epoch_info_status;
}

Status SealStorageTask::recoverSoftSeals(LocalLogStore& store,
                                         LogStorageState* log_state) {
  ld_check(log_state != nullptr);
  folly::Optional<Seal> soft_seal =
      log_state->getSeal(LogStorageState::SealType::SOFT);

  if (!soft_seal.hasValue()) {
    // read soft seal from local log store if absent in memory
    SoftSealMetadata softseal_metadata;
    int rv = store.readLogMetadata(log_id_, &softseal_metadata);
    if (rv == 0) {
      ld_check(softseal_metadata.seal_.validOrEmpty());
    } else if (err != E::NOTFOUND) {
      return E::FAILED;
    }

    log_state->updateSeal(
        softseal_metadata.seal_, LogStorageState::SealType::SOFT);
  }

  // if record cache is enabled, update its metadata
  RecordCache* cache = log_state->record_cache_.get();
  if (cache != nullptr) {
    cache->updateLastNonAuthoritativeEpoch(log_id_);
  }

  return E::OK;
}

Status SealStorageTask::checkSoftSeals(LocalLogStore& store,
                                       LogStorageState* log_state) {
  ld_check(log_state != nullptr);
  folly::Optional<Seal> soft_seal =
      log_state->getSeal(LogStorageState::SealType::SOFT);

  if (!soft_seal.hasValue()) {
    Status recover_status = recoverSoftSeals(store, log_state);
    if (recover_status != E::OK) {
      return recover_status;
    }
    soft_seal = log_state->getSeal(LogStorageState::SealType::SOFT);
  }

  ld_check(soft_seal.hasValue());
  if (soft_seal.value() > seal_) {
    // If we already have a soft seal exceeding the seal just received from
    // a sequencer, it means the sequencer that tried to seal us is outdated
    // (there is/was a newer one in the cluster, and we've heard from it) and
    // should be preempted. Update seal_ to reflect the update-to-date value.
    seal_ = soft_seal.value();
    return E::PREEMPTED;
  }

  return E::OK;
}

bool SealStorageTask::getEpochInfoFromCache(LocalLogStore& /*store*/,
                                            LogStorageState* log_state,
                                            StatsHolder* stats) {
  ld_check(log_state != nullptr);
  ld_check(epoch_info_ != nullptr);

  RecordCache* cache = log_state->record_cache_.get();
  bool from_cache = false;
  if (cache) {
    from_cache = true;
    auto epoch_info_from_cache = std::make_unique<EpochInfoMap>();
    std::vector<TailRecord> tail_records;

    for (int64_t epoch = (int64_t)last_clean_.val_ + 1;
         from_cache && epoch <= (int64_t)seal_epoch_.val_;
         ++epoch) {
      auto result = cache->getEpochRecordCache(epoch_t(epoch));
      switch (result.first) {
        case RecordCache::Result::HIT: {
          ld_check(result.second != nullptr);
          auto insert_result = epoch_info_from_cache->insert(std::make_pair(
              epoch,
              EpochInfo{
                  result.second->getLNG(),                // lng
                  result.second->getMaxSeenESN(),         // last record
                  result.second->getOffsetsWithinEpoch(), // epoch_offset_map
                  result.second->getMaxSeenTimestamp()    // timestamp
              }));

          ld_check(insert_result.second);
          if (context_ == Context::RECOVERY) {
            auto tail = result.second->getTailRecord();
            if (tail.isValid()) {
              ld_check(tail.containOffsetWithinEpoch());
              if (tail.hasPayload() &&
                  (!tail_optimized_ ||
                   tail.getPayloadSlice().size > TAIL_RECORD_INLINE_LIMIT)) {
                // do not send payload if the log is not a tail optimized log,
                // or the payload is too larged to be included in the reply
                tail.removePayload();
              }
              tail_records.push_back(std::move(tail));
            }
          }
        } break;
        case RecordCache::Result::NO_RECORD:
          // TODO 9828808: notify EpochRecovery epoch is empty
          // skip this epoch in the map
          break;
        case RecordCache::Result::MISS:
          // if there is a miss, fallback to read from local log store
          from_cache = false;
          break;
      }
    }

    if (from_cache) {
      STAT_INCR(stats, record_cache_seal_hit);
      if (!MetaDataLog::isMetaDataLog(log_id_)) {
        STAT_INCR(stats, record_cache_seal_hit_datalog);
      }
      epoch_info_ = std::move(epoch_info_from_cache);
      epoch_info_source_ = EpochInfoSource::CACHE;
      tail_records_ = std::move(tail_records);
    } else {
      STAT_INCR(stats, record_cache_seal_miss);
      if (!MetaDataLog::isMetaDataLog(log_id_)) {
        STAT_INCR(stats, record_cache_seal_miss_datalog);
      }
    }
  }

  return from_cache;
}

Status SealStorageTask::getEpochInfo(LocalLogStore& store,
                                     LogStorageState* log_state,
                                     StatsHolder* stats) {
  ld_check(log_state != nullptr);
  epoch_info_ = std::make_unique<EpochInfoMap>();
  std::vector<TailRecord> tail_records;

  STAT_INCR(stats, log_recovery_seal_received);
  // try consulting the record cache first
  if (getEpochInfoFromCache(store, log_state, stats)) {
    // cache hit
    ld_check(epoch_info_->size() <= seal_epoch_.val_ - last_clean_.val_);
    return E::OK;
  }

  LocalLogStore::ReadOptions read_options("SealFindLNG");
  read_options.allow_blocking_io = true;
  read_options.tailing = false;
  auto iterator = store.read(log_id_, read_options);
  ld_check(iterator != nullptr);

  ld_check(epoch_info_->empty());
  for (int64_t epoch = (int64_t)last_clean_.val_ + 1;
       epoch <= (int64_t)seal_epoch_.val_;) {
    esn_t lng = ESN_INVALID;
    esn_t last_record = ESN_INVALID;
    epoch_t next_epoch = EPOCH_MIN;
    std::chrono::milliseconds last_record_timestamp =
        std::chrono::milliseconds{0};
    int rv = LocalLogStoreReader::getLastKnownGood(*iterator,
                                                   epoch_t(epoch),
                                                   &lng,
                                                   &last_record,
                                                   &last_record_timestamp,
                                                   &next_epoch);
    if (rv != 0) {
      if (err == E::NOTFOUND) {
        // skip this empty epoch
        ++epoch;
        continue;
      } else {
        RATELIMIT_ERROR(std::chrono::seconds(5),
                        1,
                        "Unable to find LNG for log %lu, epoch %ld",
                        log_id_.val_,
                        epoch);
        return E::FAILED;
      }
    }

    OffsetMap epoch_offset_map;
    // read potentially more accurate LNG and epoch size from
    // the MutablePerEpochLogMetadata.

    // TODO T17536971: use MutablePerEpochMetaData so that we don't need to call
    // LocalLogStoreReader::getLastKnownGood
    MutablePerEpochLogMetadata metadata;
    rv = store.readPerEpochLogMetadata(log_id_, epoch_t(epoch), &metadata);
    if (rv == 0) {
      lng = std::max(lng, metadata.data_.last_known_good);
      epoch_offset_map = std::move(metadata.epoch_size_map_);
    }

    auto result = epoch_info_->insert(std::make_pair(
        epoch,
        EpochInfo{lng,
                  last_record,
                  epoch_offset_map,
                  static_cast<uint64_t>(last_record_timestamp.count())}));
    ld_check(result.second);

    if (context_ != Context::PURGING) {
      TailRecord tail;
      // get the tail record in recovery context
      rv = LocalLogStoreReader::getTailRecord(
          *iterator,
          log_id_,
          epoch_t(epoch),
          lng,
          &tail,
          tail_optimized_); // read payload for tail optimized logs
      if (rv != 0) {
        if (err != E::NOTFOUND) {
          RATELIMIT_ERROR(std::chrono::seconds(5),
                          1,
                          "Unable to find tail record for log %lu, epoch %ld",
                          log_id_.val_,
                          epoch);
          return E::FAILED;
        }
      } else { // tail record found
        ld_check(tail.containOffsetWithinEpoch());
        ld_check(lsn_to_epoch(tail.header.lsn) == epoch_t(epoch));
        if (!tail.offsets_map_.isValid()) {
          // the tail record does not include epoch offset information,
          // use the `epoch_offset_map` as an approximation
          tail.offsets_map_ = std::move(epoch_offset_map);
        }

        if (tail.hasPayload() &&
            tail.getPayloadSlice().size > TAIL_RECORD_INLINE_LIMIT) {
          // payload is too larged to be included in the reply
          tail.removePayload();
        }

        tail_records.push_back(std::move(tail));
      }
    }

    // next_epoch must have been populated
    ld_check(next_epoch == EPOCH_INVALID || next_epoch.val_ > epoch);
    const int64_t empty_until =
        (next_epoch == EPOCH_INVALID
             ? seal_epoch_.val_
             : std::min(seal_epoch_.val_, next_epoch.val_ - 1));

    epoch = std::max(epoch, empty_until) + 1;
  }

  tail_records_ = std::move(tail_records);
  epoch_info_source_ = EpochInfoSource::LOCAL_LOG_STORE;
  ld_check(epoch_info_->size() <= seal_epoch_.val_ - last_clean_.val_);
  return E::OK;
}

Status SealStorageTask::sealForPurging(LocalLogStore& store,
                                       LogStorageState* log_state,
                                       StatsHolder* stats) {
  ld_check(log_state != nullptr);
  // recover soft seal first to increase its hit rate
  recoverSoftSeals(store, log_state);

  Status status = E::OK;
  SealMetadata seal_metadata{seal_};

  LocalLogStore::WriteOptions write_options;
  int rv = store.updateLogMetadata(log_id_, seal_metadata, write_options);
  if (rv != 0) {
    if (err != E::UPTODATE) {
      return E::FAILED;
    }
    status = E::PREEMPTED;
  }

  seal_ = seal_metadata.seal_;
  log_state->updateSeal(seal_metadata.seal_, LogStorageState::SealType::NORMAL);

  // unlike log recovery, PurgeUncleanEpochs state machine needs epoch
  // info even if the seal is preempted
  Status epoch_info_status = getEpochInfo(store, log_state, stats);
  if (epoch_info_status != E::OK) {
    return epoch_info_status;
  }

  ld_check(epoch_info_ != nullptr);
  return status;
}

void SealStorageTask::getAllEpochInfo(std::vector<lsn_t>& epoch_lng,
                                      std::vector<OffsetMap>& epoch_offset_map,
                                      std::vector<uint64_t>& last_timestamp,
                                      std::vector<lsn_t>& max_seen_lsn) const {
  epoch_lng.clear();
  epoch_offset_map.clear();
  last_timestamp.clear();
  max_seen_lsn.clear();

  if (epoch_info_ == nullptr) {
    // can happen if the seal is preempted during recovery context
    return;
  }

  int64_t epoch = (int64_t)last_clean_.val_ + 1;
  auto it = epoch_info_->cbegin();
  for (; epoch <= (int64_t)seal_epoch_.val_; ++epoch) {
    if (it == epoch_info_->cend() || epoch < it->first) {
      // empty epoch
      epoch_lng.push_back(compose_lsn(epoch_t(epoch), ESN_INVALID));
      epoch_offset_map.push_back(OffsetMap::fromLegacy(0));
      last_timestamp.push_back(0);
      max_seen_lsn.push_back(compose_lsn(epoch_t(epoch), ESN_INVALID));
    } else {
      ld_check(epoch == it->first);
      epoch_lng.push_back(compose_lsn(epoch_t(epoch), it->second.lng));
      epoch_offset_map.push_back(it->second.epoch_offset_map);
      last_timestamp.push_back(it->second.last_timestamp);
      max_seen_lsn.push_back(
          compose_lsn(epoch_t(epoch), it->second.last_record));
      ++it;
    }
  }

  // we must have exhausted the map
  ld_check(it == epoch_info_->cend());
  ld_check(epoch_lng.size() == seal_epoch_.val_ - last_clean_.val_);
  ld_check(epoch_offset_map.size() == epoch_lng.size());
  ld_check(last_timestamp.size() == epoch_lng.size());
  ld_check(max_seen_lsn.size() == epoch_lng.size());
}

void SealStorageTask::onDone() {
  switch (context_) {
    case Context::RECOVERY: {
      Seal seal = status_ == E::PREEMPTED ? seal_ : Seal();
      std::vector<lsn_t> epoch_lng;
      std::vector<OffsetMap> epoch_offset_map;
      std::vector<uint64_t> last_timestamp;
      std::vector<lsn_t> max_seen_lsn;

      auto& map = storageThreadPool_->getProcessor().getLogStorageStateMap();
      auto log_state = map.find(log_id_, getShardIdx());
      ld_check(log_state != nullptr);

      folly::Optional<lsn_t> trim_point = log_state->getTrimPoint();
      if (!trim_point.hasValue()) {
        int rv = map.recoverLogState(
            log_id_,
            getShardIdx(),
            LogStorageState::RecoverContext::SEAL_STORAGE_TASK);
        std::ignore = rv;

        // In the manwhile, return LSN_INVALID
        trim_point = LSN_INVALID;
      }
      getAllEpochInfo(
          epoch_lng, epoch_offset_map, last_timestamp, max_seen_lsn);
      SEALED_Message::createAndSend(reply_to_,
                                    log_id_,
                                    getShardIdx(),
                                    seal_epoch_,
                                    status_,
                                    trim_point.value(),
                                    epoch_lng,
                                    seal,
                                    epoch_offset_map,
                                    last_timestamp,
                                    max_seen_lsn,
                                    tail_records_);
    } break;
    case Context::PURGING: {
      PurgeUncleanEpochs* driver = purge_driver_.get();
      if (driver != nullptr) {
        checked_downcast<PurgeUncleanEpochs*>(driver)->onGetPurgeEpochsDone(
            status_, std::move(epoch_info_), epoch_info_source_);
      }
    } break;
  }
}

void SealStorageTask::onDropped() {
  switch (context_) {
    case Context::RECOVERY:
      SEALED_Message::createAndSend(reply_to_,
                                    log_id_,
                                    storageThreadPool_->getShardIdx(),
                                    seal_epoch_,
                                    E::FAILED);
      break;
    case Context::PURGING: {
      PurgeUncleanEpochs* driver = purge_driver_.get();
      if (driver != nullptr) {
        checked_downcast<PurgeUncleanEpochs*>(driver)->onGetPurgeEpochsDone(
            E::DROPPED, nullptr, EpochInfoSource::INVALID);
      }
    } break;
  }
}

template <>
const std::string&
EnumMap<SealStorageTask::EpochInfoSource, std::string>::invalidValue() {
  static const std::string invalidSourceName("");
  return invalidSourceName;
}

template <>
void EnumMap<SealStorageTask::EpochInfoSource, std::string>::setValues() {
  static_assert(static_cast<int>(SealStorageTask::EpochInfoSource::MAX) == 3,
                "EpochSourceInfo count does not match");
  set(SealStorageTask::EpochInfoSource::INVALID, "invalid");
  set(SealStorageTask::EpochInfoSource::CACHE, "cache");
  set(SealStorageTask::EpochInfoSource::LOCAL_LOG_STORE, "local_log_store");
}

EnumMap<SealStorageTask::EpochInfoSource, std::string>&
SealStorageTask::EpochInfoSourceNames() {
  static EnumMap<SealStorageTask::EpochInfoSource, std::string> map;
  return map;
}

}} // namespace facebook::logdevice
