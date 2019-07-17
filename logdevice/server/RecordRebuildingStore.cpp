/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordRebuildingStore.h"

#include <folly/Random.h>

#include "logdevice/common/CopySetSelector.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

RecordRebuildingStore::RecordRebuildingStore(
    size_t block_id,
    shard_index_t shard,
    RawRecord record,
    RecordRebuildingOwner* owner,
    std::shared_ptr<ReplicationScheme> replication,
    std::shared_ptr<PayloadHolder> scratch_payload_holder,
    const NodeAvailabilityChecker* node_availability)
    : RecordRebuildingBase(record.lsn,
                           shard,
                           owner,
                           replication,
                           node_availability),
      blockID_(block_id),
      record_(std::move(record)),
      payloadHolder_(std::move(scratch_payload_holder)) {
  // Record bytes need to be owned either by record_ or scratch_payload_holder,
  // but not both.
  ld_check(record_.owned != (payloadHolder_ != nullptr));
}

RecordRebuildingStore::~RecordRebuildingStore() {
  if (started_) {
    WORKER_STAT_DECR(record_rebuilding_in_progress);
  }
}

void RecordRebuildingStore::start(bool read_only) {
  ld_check(!started_);
  started_ = true;
  WORKER_STAT_INCR(record_rebuilding_in_progress);

  if (read_only) {
    deferredComplete();
    return;
  }

  int rv = parseRecord();
  if (rv != 0) {
    WORKER_STAT_INCR(rebuilding_malformed_records);
    deferredComplete();
    return;
  }
  sendCurrentStage();
}

int RecordRebuildingStore::parseRecord() {
  int rv;

  bool verify_checksum = getSettings().verify_checksum_before_replicating;

  if (verify_checksum) {
    rv = LocalLogStoreRecordFormat::checkWellFormed(record_.blob);
    if (rv != 0) {
      ld_error("Refusing to rebuild malformed record %lu%s. "
               "Blob: %s",
               owner_->getLogID().val_,
               lsn_to_string(record_.lsn).c_str(),
               hexdump_buf(record_.blob, 500).c_str());
      ld_check(err == E::MALFORMED_RECORD);
      return -1;
    }
  }

  storeHeader_.rid = RecordID(record_.lsn, owner_->getLogID());
  storeHeader_.nsync = 0;
  storeHeader_.copyset_offset = -1;
  copyset_size_t copyset_size;
  std::chrono::milliseconds timestamp;
  Payload payload;
  uint32_t wave;
  std::map<KeyType, std::string> optional_keys_read;

  // Call parse() twice: first time to get copyset size and everything except
  // copyset, second time to get copyset (after allocating memory for it).
  rv = LocalLogStoreRecordFormat::parse(record_.blob,
                                        &timestamp,
                                        &storeHeader_.last_known_good,
                                        &recordFlags_,
                                        &wave,
                                        &copyset_size,
                                        nullptr,
                                        0,
                                        &offsets_within_epoch_,
                                        &optional_keys_read,
                                        &payload,
                                        getMyShardID().shard());
  // The record was successfully parsed before creation of RecordRebuilding.
  ld_check(rv == 0);

  if ((recordFlags_ & LocalLogStoreRecordFormat::FLAG_CUSTOM_KEY) ||
      (recordFlags_ & LocalLogStoreRecordFormat::FLAG_OPTIONAL_KEYS)) {
    optional_keys_.insert(optional_keys_read.begin(), optional_keys_read.end());
  }

  storeHeader_.timestamp = timestamp.count();
  storeHeader_.wave = wave;

  storeHeader_.flags = recordFlags_ & LocalLogStoreRecordFormat::FLAG_MASK;
  if (offsets_within_epoch_.isValid()) {
    storeHeader_.flags |= STORE_Header::OFFSET_WITHIN_EPOCH;
    // TODO (T35832374) : remove if condition when all servers support OffsetMap
    if (Worker::settings().enable_offset_map) {
      storeHeader_.flags |= STORE_Header::OFFSET_MAP;
    }
  }
  storeHeader_.flags |= STORE_Header::REBUILDING;
  // For the local amend, we retain the FLAG_WRITTEN_BY_REBUILDING as
  // it currently exists in the record. This ensures that filtering
  // during future rebuildings treats the record appropriately (e.g.
  // as an append if the flag is clear).

  existingCopyset_.resize(copyset_size);
  rv = LocalLogStoreRecordFormat::parse(record_.blob,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        &existingCopyset_[0],
                                        copyset_size,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        getMyShardID().shard());
  ld_check(rv == 0);

  if (payloadHolder_ != nullptr) {
    ld_check(!record_.owned);
    *payloadHolder_ = PayloadHolder(payload, PayloadHolder::UNOWNED);
  } else {
    // We want to give STORE_Message a shared_ptr<PayloadHolder> that shares
    // ownership of the payload. The payload is a part of record_.blob,
    // owned by record_. Move the blob into CustomPayloadHolder and use
    // shared_ptr aliasing constructor to make shared_ptr<PayloadHolder> own it.
    struct CustomPayloadHolder {
      Slice blob;
      PayloadHolder holder;

      CustomPayloadHolder(Slice blob, Payload payload)
          : blob(blob), holder(payload, PayloadHolder::UNOWNED) {}
      ~CustomPayloadHolder() {
        std::free(const_cast<void*>(blob.data));
      }
    };
    ld_check(record_.owned);
    auto custom_ptr =
        std::make_shared<CustomPayloadHolder>(record_.blob, payload);
    record_.owned = false;
    payloadHolder_ =
        std::shared_ptr<PayloadHolder>(custom_ptr, &custom_ptr->holder);
  }

  return 0;
}

std::shared_ptr<PayloadHolder> RecordRebuildingStore::getPayloadHolder() const {
  return payloadHolder_;
}

void RecordRebuildingStore::traceEvent(const char* event_type,
                                       const char* status) {
  tracer_.traceRecordRebuild(owner_->getLogID(),
                             record_.lsn,
                             owner_->getRebuildingVersion(),
                             usec_since(creation_time_),
                             owner_->getRebuildingSet(),
                             existingCopyset_,
                             newCopyset_,
                             rebuildingWave_,
                             getPayloadSize(),
                             curStage_,
                             event_type,
                             status);
}

size_t RecordRebuildingStore::getPayloadSize() const {
  if (payloadHolder_) {
    return payloadHolder_->size();
  }
  // parseRecord() failed, or read-only mode.
  return 0;
}

void RecordRebuildingStore::buildNewCopysetBase() {
  auto& rebuilding_set = owner_->getRebuildingSet().shards;
  newCopyset_.clear();
  newCopyset_.reserve(
      replication_->epoch_metadata.replication.getReplicationFactor());

  const auto& storage_set = replication_->epoch_metadata.shards;

  std::chrono::milliseconds timestamp;
  LocalLogStoreRecordFormat::flags_t flags;
  int rv = LocalLogStoreRecordFormat::parse(record_.blob,
                                            &timestamp,
                                            nullptr,
                                            &flags,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            0,
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            getMyShardID().shard());
  // The record was successfully parsed before.
  ld_check(rv == 0);
  const DataClass dc =
      (flags & LocalLogStoreRecordFormat::FLAG_WRITTEN_BY_REBUILDING)
      ? DataClass::REBUILD
      : DataClass::APPEND;

  auto should_keep_recipient = [&](ShardID shard) {
    if (std::find(storage_set.begin(), storage_set.end(), shard) ==
        storage_set.end()) {
      // One of the recipients in the existing copyset of this record does not
      // belong to the storage set. This can either be because the cluster was
      // shrunk while we were rebuilding data that was stored on the removed
      // nodes (not a good idea...), or because there is a nasty bug somewhere.
      if (!isStorageShardInConfig(shard)) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Encountered a record whose copyset contains recipient(s) that do "
            "not"
            " belong to the config. This can happen if this cluster was shrunk"
            " while data on removed node was still being rebuilt. Log: %lu "
            "LSN: "
            "%s copyset: %s rebuilding set: %s, storage set: %s",
            owner_->getLogID().val_,
            lsn_to_string(record_.lsn).c_str(),
            toString(existingCopyset_.data(), existingCopyset_.size()).c_str(),
            owner_->getRebuildingSet().describe().c_str(),
            toString(storage_set.data(), storage_set.size()).c_str());
      } else {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Encountered a record whose copyset contains recipient(s) that do "
            "not"
            " belong to the storage set. Log: %lu LSN: %s copyset: %s "
            "rebuilding set: %s, storage set: %s",
            owner_->getLogID().val_,
            lsn_to_string(record_.lsn).c_str(),
            toString(existingCopyset_.data(), existingCopyset_.size()).c_str(),
            owner_->getRebuildingSet().describe().c_str(),
            toString(storage_set.data(), storage_set.size()).c_str());
      }
      return false;
    }

    if (std::find(newCopyset_.begin(), newCopyset_.end(), shard) !=
        newCopyset_.end()) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Existing copyset contains duplicates. Log: %lu LSN: %s "
                      "copyset: %s new copyset so far: %s duplicate shard: %s "
                      "record: %s",
                      owner_->getLogID().val_,
                      lsn_to_string(record_.lsn).c_str(),
                      toString(existingCopyset_).c_str(),
                      toString(newCopyset_).c_str(),
                      shard.toString().c_str(),
                      hexdump_buf(record_.blob, 100).c_str());
      return false;
    }

    auto shard_kv = rebuilding_set.find(shard);
    return shard_kv == rebuilding_set.end() ||
        !shard_kv->second.isUnderReplicated(dc, RecordTimestamp(timestamp)) ||
        (shard_kv->first == getMyShardID() &&
         !replication_->relocate_local_records);
  };

  bool have_extras = existingCopyset_.size() >
      replication_->epoch_metadata.replication.getReplicationFactor();
  if (have_extras) {
    // The copyset has extras. We don't know which R recipients in this copyset
    // do have a copy. The only thing we know for sure is that this node does.
    if (should_keep_recipient(getMyShardID())) {
      newCopyset_.push_back(getMyShardID());
    }
  } else {
    std::copy_if(existingCopyset_.begin(),
                 existingCopyset_.end(),
                 std::back_inserter(newCopyset_),
                 should_keep_recipient);
  }
}

int RecordRebuildingStore::pickCopyset() {
  // Populates newCopyset_ with recipients in existingCopyset_ that we want to
  // keep.
  buildNewCopysetBase();

  auto replication_factor =
      replication_->epoch_metadata.replication.getReplicationFactor();
  ld_check(newCopyset_.size() <= replication_factor);
  if (newCopyset_.size() == replication_factor) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        10,
        "Encountered a record without any rebuilding shards in copyset. "
        "Log: %lu LSN: %s copyset: %s rebuilding set: %s",
        owner_->getLogID().val_,
        lsn_to_string(record_.lsn).c_str(),
        toString(newCopyset_.data(), newCopyset_.size()).c_str(),
        owner_->getRebuildingSet().describe().c_str());
    WORKER_STAT_INCR(rebuilding_bad_copysets);
    deferredComplete();
    err = E::EXISTS;
    return -1;
  }

  copyset_t amendable_copies = newCopyset_;

  int rv = pickCopysetImpl();
  if (rv != 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Failed to pick copyset to re-replicate record %lu%s, will retry. "
        "existing copies: %s, rebuilding set: %s",
        owner_->getLogID().val_,
        lsn_to_string(record_.lsn).c_str(),
        toString(amendable_copies).c_str(),
        owner_->getRebuildingSet().describe().c_str());
    err = E::FAILED;
    return -1;
  }
  ld_check(newCopyset_.size() == replication_factor);
  storeHeader_.copyset_size = newCopyset_.size();

  ld_debug("Record %lu%s - old copyset: %s - new copyset: %s",
           owner_->getLogID().val_,
           lsn_to_string(record_.lsn).c_str(),
           toString(existingCopyset_.data(), existingCopyset_.size()).c_str(),
           toString(newCopyset_.data(), newCopyset_.size()).c_str());

  ld_check(stages_.empty());
  stages_.clear();
  ld_check(amendRecipients_.empty());
  bool allowAmends = !getSettings().rebuild_without_amends;
  auto addRecipient = [&](auto target_shard) {
    if (allowAmends &&
        (target_shard == getMyShardID() ||
         std::count(
             amendable_copies.begin(), amendable_copies.end(), target_shard))) {
      // We know that the record exists on this node. AMEND even if we would
      // otherwise do a STORE if record has extras.
      amendRecipients_.push_back(target_shard);
    } else {
      RecipientNode r(*this, target_shard);
      stages_.emplace_back(StageRecipients::Type::STORE);
      stages_.back().recipients.push_back(std::move(r));
    }
  };

  for (int i = (int)newCopyset_.size() - 1; i >= 0; --i) {
    addRecipient(newCopyset_[i]);
  }

  if (findCopysetOffset(newCopyset_, getMyShardID()) == -1) {
    // If we aren't in the copyset, this should be a drain. Ensure
    // we amend our local copy so that a restart of rebuilding with
    // our node still marked for drain will not reprocess this record.
    ld_check(replication_->relocate_local_records);
    if (replication_->relocate_local_records && allowAmends) {
      addRecipient(getMyShardID());
    }
  }

  if (stages_.empty()) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "No new nodes were picked for Record %lu%s - "
        "old copyset: %s - new copyset: %s",
        owner_->getLogID().val_,
        lsn_to_string(record_.lsn).c_str(),
        toString(existingCopyset_.data(), existingCopyset_.size()).c_str(),
        toString(newCopyset_.data(), newCopyset_.size()).c_str());
  }

  return 0;
}

int RecordRebuildingStore::pickCopysetImpl() {
  // Seed the RNG with the hash of the old copyset for deterministic copyset
  // selection to preserve sticky copyset blocks
  uint32_t rng_seed[4];
  getRNGSeedFromRecord(
      rng_seed, existingCopyset_.data(), existingCopyset_.size(), blockID_);
  XorShift128PRNG rng;
  rng.seed(rng_seed);

  size_t existing_copies = newCopyset_.size();
  auto replication_factor =
      replication_->epoch_metadata.replication.getReplicationFactor();
  newCopyset_.resize(existing_copies + replication_factor);

  copyset_size_t full_size;
  auto rv = replication_->copysetSelector->augment(
      newCopyset_.data(), existing_copies, &full_size, rng);
  if (rv != CopySetSelector::Result::SUCCESS) {
    return -1;
  }

  ld_check(full_size >= replication_factor);
  if (full_size > replication_factor) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Existing copyset doesn't satisfy replication property. Log %lu, LSN "
        "%s, "
        "replication %s, existing copyset %s, new copyset [%s], truncated to "
        "[%s].",
        owner_->getLogID().val_,
        lsn_to_string(lsn_).c_str(),
        replication_->epoch_metadata.replication.toString().c_str(),
        toString(existingCopyset_).c_str(),
        rangeToString(newCopyset_.begin(), newCopyset_.begin() + full_size)
            .c_str(),
        rangeToString(
            newCopyset_.begin(), newCopyset_.begin() + replication_factor)
            .c_str());
  }
  newCopyset_.resize(replication_factor);

  // Assert that the new copyset doesn't contain duplicates.
  for (size_t i = 1; i < newCopyset_.size(); ++i) {
    if (std::find(newCopyset_.begin(),
                  newCopyset_.begin() + i,
                  newCopyset_[i]) != newCopyset_.begin() + i) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          2,
          "Copyset selector returned a copyset with duplicates. Log %lu, LSN "
          "%s, existing copyset %s, new copyset %s, copyset selector type: %s",
          owner_->getLogID().val_,
          lsn_to_string(lsn_).c_str(),
          toString(existingCopyset_).c_str(),
          toString(newCopyset_).c_str(),
          replication_->copysetSelector->getName().c_str());
      ld_check(false);
      return -1;
    }
  }

  return 0;
}

void RecordRebuildingStore::sendCurrentStage(bool resend_inflight_stores) {
  if (curStage_ == (int)stages_.size()) {
    ld_check(curStageRepliesExpected_ == 0);
    deferredComplete();
    return;
  }

  if (curStage_ == -1) {
    ld_check(!resend_inflight_stores);
    resetStoreTimer();
    if (pickCopyset() == 0) {
      nextStage();
    } else if (err != E::EXISTS) {
      activateRetryTimer();
      traceEvent("CANNOT_PICK_COPYSET", error_name(err));
    }
    return;
  }

  int rv = sendStage(&stages_[curStage_], resend_inflight_stores);
  if (rv != 0) {
    startNewWave(/*immediate*/ false);
  }
}

void RecordRebuildingStore::startNewWave(bool immediate) {
  ++rebuildingWave_;
  stages_.clear();
  amendRecipients_.clear();
  curStage_ = -1;
  curStageRepliesExpected_ = 0;
  deferredStores_ = 0;
  resetRetryTimer();
  resetStoreTimer();
  if (immediate) {
    sendCurrentStage();
  } else {
    activateRetryTimer();
  }
}

void RecordRebuildingStore::nextStage() {
  ld_check(curStageRepliesExpected_ == 0);
  ld_check(deferredStores_ == 0);
  resetRetryTimer();
  resetStoreTimer();
  ++curStage_;
  sendCurrentStage();
}

void RecordRebuildingStore::onStageComplete() {
  nextStage();
}

void RecordRebuildingStore::onRetryTimeout() {
  WORKER_STAT_INCR(record_rebuilding_retries);

  if (curStage_ >= 0) {
    // `curStage_ >= 0` means we are keeping the same copyset. Check if all
    // recipients are still in the config. If that's not the case, try to pick a
    // new copyset otherwise we will get stuck.
    if (!checkEveryoneStillInConfig()) {
      startNewWave(/* immediate */ true);
      return;
    }
  }

  if (curStage_ == -1) {
    ld_check(curStageRepliesExpected_ == 0);
    RATELIMIT_INFO(std::chrono::seconds(10),
                   2,
                   "Retrying picking copyset for %lu%s",
                   owner_->getLogID().val_,
                   lsn_to_string(record_.lsn).c_str());
    sendCurrentStage();
    return;
  }

  ld_check(curStageRepliesExpected_ > 0);
  RATELIMIT_INFO(
      std::chrono::seconds(1),
      1,
      "Retrying RecordRebuildingStore stage. Log %lu, LSN %s, stage: %s",
      owner_->getLogID().val_,
      lsn_to_string(lsn_).c_str(),
      stageDebugInfo(&stages_[curStage_]).c_str());
  sendCurrentStage();
}

void RecordRebuildingStore::onStoreTimeout() {
  ld_check(curStage_ >= 0);
  ld_check(curStageRepliesExpected_ > 0);

  WORKER_STAT_INCR(record_rebuilding_store_timeouts);

  if (!checkEveryoneStillInConfig()) {
    startNewWave(/* immediate */ true);
    return;
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 1,
                 "Rebuilding store timed out. Log %lu, LSN %s, stage: %s",
                 owner_->getLogID().val_,
                 lsn_to_string(lsn_).c_str(),
                 stageDebugInfo(&stages_[curStage_]).c_str());
  sendCurrentStage(/* resend_inflight_stores */ true);
}

void RecordRebuildingStore::onComplete() {
  ServerWorker* w = ServerWorker::onThisThread(false);
  if (w) {
    // TODO(T15517759): When Flexible Log Sharding is fully deployed, it will
    // not make much sense to keep histograms of RecordRebuilding per shard.
    auto shard = getMyShardID().shard();
    PER_SHARD_HISTOGRAM_ADD(
        Worker::stats(), record_rebuilding, shard, usec_since(creation_time_));
  }

  ld_spew("All stores received for %lu%s",
          owner_->getLogID().val_,
          lsn_to_string(record_.lsn).c_str());
  ld_check(owner_);
  traceEvent("STORES RECEIVED", nullptr);

  auto flushTokenMap = std::make_unique<FlushTokenMap>();
  for (int i = 0; i < stages_.size(); i++) {
    for (auto& r : stages_[i].recipients) {
      ld_check(r.succeeded);
      if (r.flush_token != FlushToken_INVALID) {
        auto key = std::make_pair(r.shard_.node(), r.server_instance_id);
        flushTokenMap->insert(std::make_pair(key, r.flush_token));
      }
    }
  }

  owner_->onAllStoresReceived(record_.lsn, std::move(flushTokenMap));
  // `this` is destroyed here.
}

void RecordRebuildingStore::onStoreFailed() {
  startNewWave(/*immediate*/ true);
}

std::unique_ptr<RecordRebuildingAmendState>
RecordRebuildingStore::getRecordRebuildingAmendState() {
  return std::make_unique<RecordRebuildingAmendState>(lsn_,
                                                      replication_,
                                                      storeHeader_,
                                                      recordFlags_,
                                                      newCopyset_,
                                                      amendRecipients_,
                                                      rebuildingWave_);
}

}} // namespace facebook::logdevice
