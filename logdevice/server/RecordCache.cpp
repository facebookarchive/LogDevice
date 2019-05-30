/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordCache.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/EpochRecordCache.h"
#include "logdevice/server/RecordCacheDependencies.h"

namespace facebook { namespace logdevice {

using namespace RecordCacheSerializer;

RecordCache::RecordCache(logid_t log_id,
                         shard_index_t shard,
                         RecordCacheDependencies* deps)
    : log_id_(log_id), shard_(shard), deps_(deps) {
  ld_check(deps_ != nullptr);
}

RecordCache::RecordCache(logid_t log_id,
                         shard_index_t shard,
                         RecordCacheDependencies* deps,
                         epoch_t::raw_type head_epoch_cached,
                         epoch_t::raw_type next_epoch_to_cache,
                         epoch_t::raw_type last_nonauthoritative_epoch)
    : log_id_(log_id),
      shard_(shard),
      deps_(deps),
      epoch_caches_(head_epoch_cached),
      head_epoch_cached_(head_epoch_cached),
      next_epoch_to_cache_(next_epoch_to_cache),
      last_nonauthoritative_epoch_(last_nonauthoritative_epoch) {
  ld_check(deps_ != nullptr);
  ld_check(head_epoch_cached != EPOCH_INVALID.val());
  if (next_epoch_to_cache > head_epoch_cached) {
    epoch_caches_.push(next_epoch_to_cache - 1, nullptr);
  }
}

int RecordCache::putRecord(RecordID rid,
                           uint64_t timestamp,
                           esn_t lng,
                           uint32_t wave_or_recovery_epoch,
                           const copyset_t& copyset,
                           STORE_flags_t flags,
                           std::map<KeyType, std::string>&& optional_keys,
                           Slice payload_raw,
                           const std::shared_ptr<PayloadHolder>& payload_holder,
                           OffsetMap offsets_within_epoch) {
  ld_check(rid.logid == log_id_);

  if (folly::kIsDebug) {
    if (shutdown_.load()) {
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "INTERNAL ERROR: Insert record of log %lu after "
                         "shutdown of record cache.",
                         rid.logid.val_);
      ld_assert(false);
      return -1;
    }
  }

  if (flags & STORE_Header::REBUILDING) {
    // do not cache rebuilding stores as rebuilding stores are for clean epochs
    // and bypass seals
    return -1;
  }

  std::shared_ptr<EpochRecordCache> epoch_cache =
      epoch_caches_.get(rid.epoch.val_);

  if (epoch_cache != nullptr) {
    // common path
    return epoch_cache->putRecord(rid,
                                  timestamp,
                                  lng,
                                  wave_or_recovery_epoch,
                                  copyset,
                                  flags,
                                  std::move(optional_keys),
                                  payload_raw,
                                  payload_holder,
                                  std::move(offsets_within_epoch));
  }

  epoch_t head_epoch_cached = epoch_t(head_epoch_cached_.load());
  if (head_epoch_cached != EPOCH_INVALID && rid.epoch < head_epoch_cached) {
    // we may get records stored with their epoch smaller than first seen epoch
    // and even smaller than the soft seals, e.g.,
    //   (1) sequencer draining during migration
    //   (2) recovery mutation of previous epochs
    // we are unable to cache these records and the epoch will be a cache miss
    // to readers
    return -1;
  }

  auto epoch_stored_before =
      [this](epoch_t epoch) -> EpochRecordCache::StoredBefore {
    return (epoch >= next_epoch(epoch_t(last_nonauthoritative_epoch_.load()))
                ? EpochRecordCache::StoredBefore::NEVER
                : EpochRecordCache::StoredBefore::MAYBE);
  };

  const size_t epoch_cache_capacity = deps_->getEpochRecordCacheSize(rid.logid);
  const EpochRecordCache::TailOptimized tail_optimized =
      deps_->tailOptimized(rid.logid) ? EpochRecordCache::TailOptimized::YES
                                      : EpochRecordCache::TailOptimized::NO;

  while (epoch_cache == nullptr) {
    // it is likely we need to create a new EpochRecordCache entry, acquire
    // the lock
    std::lock_guard<std::mutex> cache_lock(epoch_cache_lock_);

    // try checking again with lock held
    head_epoch_cached = epoch_t(head_epoch_cached_.load());
    if (head_epoch_cached != EPOCH_INVALID && rid.epoch < head_epoch_cached) {
      return -1;
    }

    epoch_cache = epoch_caches_.get(rid.epoch.val_);
    if (epoch_cache != nullptr) {
      break;
    }

    if (head_epoch_cached == EPOCH_INVALID) {
      // the cache is never stored, initialize last_nonauthoritative_epoch from
      // soft seal values
      ld_check(cacheEmpty());
      updateLastNonAuthoritativeEpoch(rid.logid);

      // unless the record being written is in EPOCH_MAX,
      // last_nonauthoritative_epoch_ should be initialized to a non-default
      // value since soft seal is recovered before the STORE
      if (rid.epoch < EPOCH_MAX &&
          last_nonauthoritative_epoch_.load() == EPOCH_MAX.val_) {
        ld_critical("Failed to get last nonauthoritative epoch from "
                    " seal metadata after a successful non rebuilding store "
                    "for log %lu. This should never happen.",
                    rid.logid.val_);
        ld_check(false);
        return -1;
      }
    }

    const epoch_t next_epoch_to_cache = epoch_t(next_epoch_to_cache_.load());
    // existing cache
    if (rid.epoch.val_ < next_epoch_to_cache.val_) {
      // epoch is in the range of [head_epoch_cached_, next_epoch_to_cache_) and
      // head_epoch_cached_ != next_epoch_to_cache_ (not empty).
      // The store might be a draining store or recovery mutation that bypassed
      // soft seals, we can create a EpochRecordCache entry in the existing
      // range and cache it
      ld_check(!cacheEmpty());
      epoch_cache = epoch_caches_.put(
          rid.epoch.val_,
          std::make_shared<EpochRecordCache>(log_id_,
                                             shard_,
                                             rid.epoch,
                                             deps_,
                                             epoch_cache_capacity,
                                             tail_optimized,
                                             epoch_stored_before(rid.epoch)));
      STAT_INCR(deps_->getStatsHolder(), record_cache_epoch_created);
      break;
    }

    // epoch >= next_epoch_to_cache. This is the highest epoch seen so far and
    // the cache has not stored this epoch before.
    // Create an EpochRecordCache object for rid.epoch, moreover,
    // RandomAccessQueue will create entries of
    // [next_epoch_to_cache, rid.epoch) (if any) with padded nullptrs
    epoch_caches_.push(
        rid.epoch.val_,
        std::make_shared<EpochRecordCache>(log_id_,
                                           shard_,
                                           rid.epoch,
                                           deps_,
                                           epoch_cache_capacity,
                                           tail_optimized,
                                           epoch_stored_before(rid.epoch)));
    STAT_INCR(deps_->getStatsHolder(), record_cache_epoch_created);

    // update head_epoch_cached_ and next_epoch_to_cache_
    if (head_epoch_cached == EPOCH_INVALID) {
      head_epoch_cached_.store(rid.epoch.val_);
    }
    next_epoch_to_cache_.store(next_epoch(rid.epoch).val_);

    epoch_cache = epoch_caches_.get(rid.epoch.val_);
    ld_check(epoch_cache != nullptr);
  }

  return epoch_cache->putRecord(rid,
                                timestamp,
                                lng,
                                wave_or_recovery_epoch,
                                copyset,
                                flags,
                                std::move(optional_keys),
                                payload_raw,
                                payload_holder,
                                std::move(offsets_within_epoch));
}

std::pair<RecordCache::Result, std::shared_ptr<EpochRecordCache>>
RecordCache::getEpochRecordCache(epoch_t epoch) const {
  std::shared_ptr<EpochRecordCache> epoch_cache = epoch_caches_.get(epoch.val_);

  if (epoch_cache != nullptr) {
    if (!epoch_cache->isConsistent()) {
      RATELIMIT_INFO(std::chrono::seconds(20),
                     3,
                     "RecordCache miss for log %lu epoch %u: cache found "
                     "but not consistent.",
                     log_id_.val_,
                     epoch.val_);
      // epoch cache is disabled or may not be correct. consider this as a miss.
      return std::make_pair(Result::MISS, nullptr);
    }

    return std::make_pair(Result::HIT, std::move(epoch_cache));
  }

  // the epoch has no records if it is strictly larger than
  // last_nonauthoritative_epoch, no matter if there has ever been a record of
  // the epoch stored in the cache.
  // However, we cannot declare NO_RECORD for epochs that are already evicted
  // upon LCE advancements. Declare cache miss instead.
  //
  // TODO: currently we use head_epoch_cached_ to indicate the highest epoch
  // (exclusive) evicted upto upon LCE advances. This is not accurate as
  // head_epoch_cached_ can also be the first cached epoch before eviction
  // happens. This may negatively impact the cache hit rate.
  // Ideally we should have a separate variable (e.g., evicted_upto_)
  // to keep track of LCE advancements and use it to determine
  // MISS vs NO_RECORD.
  if (epoch.val_ >= head_epoch_cached_.load() &&
      epoch.val_ > last_nonauthoritative_epoch_.load()) {
    return std::make_pair(Result::NO_RECORD, nullptr);
  }

  RATELIMIT_INFO(std::chrono::seconds(20),
                 3,
                 "RecordCache miss for log %lu epoch %u: cache not found "
                 "but cannot declare no data. head epoch : %u, "
                 "last_nonauthoritative_epoch: %u.",
                 log_id_.val_,
                 epoch.val_,
                 head_epoch_cached_.load(),
                 last_nonauthoritative_epoch_.load());

  // cache miss for all other cases
  return std::make_pair(Result::MISS, nullptr);
}

void RecordCache::onLastCleanEpochAdvanced(epoch_t lce) {
  epoch_t head_epoch_cached = epoch_t(head_epoch_cached_.load());
  if (head_epoch_cached == EPOCH_INVALID || lce < head_epoch_cached) {
    // cache is empty or already discarded beyond lce; nothing to do
    return;
  }

  std::vector<std::shared_ptr<EpochRecordCache>> discarded_caches;
  {
    std::lock_guard<std::mutex> cache_lock(epoch_cache_lock_);
    if (lce < epoch_t(head_epoch_cached_.load())) {
      return;
    }

    const epoch_t next_epoch_to_cache = epoch_t(next_epoch_to_cache_.load());

    // do not discard beyond next_epoch_to_cache
    epoch_t discard_upto = std::min(next_epoch(lce), next_epoch_to_cache);
    discarded_caches = epoch_caches_.popUpTo(discard_upto.val_);

    head_epoch_cached_.store(discard_upto.val_);

    if (discard_upto == next_epoch_to_cache) {
      // the whole cache is cleared
      ld_check(cacheEmpty());
    }
  }

  const size_t num_evicted = std::count_if(
      discarded_caches.begin(),
      discarded_caches.end(),
      [](const std::shared_ptr<EpochRecordCache>& c) { return c != nullptr; });

  STAT_ADD(deps_->getStatsHolder(), record_cache_epoch_evicted, num_evicted);
  // epoch caches will get dropped outside of the lock
}

void RecordCache::onRelease(lsn_t last_released) {
  const esn_t release_esn = lsn_to_esn(last_released);
  if (release_esn > ESN_INVALID) {
    // find the epoch cache of the epoch being release. If found, try to evict
    // records from it by advancing LNG to last_released. We can do this because
    // last release lsn must behind LNG of its epoch. This holds even if this
    // is a per-epoch release.

    // However, we don't want to evict records from the epoch upon release
    // if the epoch has been **hard-sealed** (sealed by log recovery). The
    // reason is to avoid the race condition in the following:
    // 1) epoch recovery got epoch information (e.g., LNG, last_record) in
    //    sealing epoch _e_
    // 2) records after LNG may still be evicted as releases in _e_ are received
    // 3) Digest started by epoch recovery, however evicted records are not
    //    sent from the record cache, which gives epoch recovery inconsistent
    //    information
    const epoch_t release_epoch = lsn_to_epoch(last_released);
    if (auto epoch_cache = epoch_caches_.get(release_epoch.val_)) {
      folly::Optional<Seal> seal =
          deps_->getSeal(log_id_, shard_, /*soft=*/false);
      if (!seal.hasValue() || seal.value().epoch < release_epoch) {
        epoch_cache->advanceLNG(release_esn);
      }
    }
  }
}

void RecordCache::updateLastNonAuthoritativeEpoch(logid_t logid) {
  // no-op if last_nonauthoritative_epoch_ already has non-default value
  if (last_nonauthoritative_epoch_.load() != EPOCH_MAX.val_) {
    return;
  }

  // Determine whether there were records previously stored in an epoch by
  // leveraging the soft seal metadata value.
  // Example: at the time of the first store, soft seal value for the epoch
  // is epoch 5. We are sure that the storage node never stored records on epoch
  // 7 or higher before - otherwise the soft seal would have been at least
  // epoch 6 instead of epoch 5. But we cannot draw any conclusion for epoch
  // <= 6.
  folly::Optional<Seal> soft_seal =
      deps_->getSeal(logid, shard_, /*soft=*/true);
  if (soft_seal.hasValue()) {
    atomic_fetch_min(last_nonauthoritative_epoch_,
                     next_epoch(epoch_t(soft_seal.value().epoch.val_)).val_);
  }

  // Moreover, if highest inserted lsn is available,
  // last_nonauthoritative_epoch_ should simply be the epoch of highest
  // inserted lsn, since we know that the local log store never stored any
  // record beyond that
  lsn_t highest_lsn;
  int rv = deps_->getHighestInsertedLSN(logid, shard_, &highest_lsn);
  if (rv == 0) {
    atomic_fetch_min(
        last_nonauthoritative_epoch_, lsn_to_epoch(highest_lsn).val_);
  }

  if (folly::kIsDebug) {
    if (last_nonauthoritative_epoch_.load() != EPOCH_MAX.val_) {
      ld_debug(
          "Last non-authoritative epoch has been updated to %u for log %lu."
          "soft seal: %s, highest lsn: %s",
          last_nonauthoritative_epoch_.load(),
          logid.val_,
          (soft_seal.hasValue() ? soft_seal.value().toString().c_str() : "n/a"),
          rv == 0 ? lsn_to_string(highest_lsn).c_str() : "n/a");
    }
  }
}

void RecordCache::neverStored() {
  last_nonauthoritative_epoch_.store(EPOCH_INVALID.val_);
}

void RecordCache::shutdown() {
  // clearing the cache by evicting all epochs, this is done when the cache is
  // no longer used, and nothing got persisted

  // Note: logdeviced stops writing and persists record cache before calling
  // this function on graceful shutdown
  bool prev = shutdown_.exchange(true);
  ld_check(!prev);
  evictResetAllEpochs();
}

void RecordCache::evictResetEpoch(epoch_t epoch) {
  std::shared_ptr<EpochRecordCache> epoch_cache = epoch_caches_.get(epoch.val_);
  if (epoch_cache == nullptr || epoch_cache->emptyWithoutTailPayload()) {
    // epoch cache does not exist or empty (does not store any record payload),
    // nothing to do
    return;
  }

  std::lock_guard<std::mutex> cache_lock(epoch_cache_lock_);
  evictResetEpochImpl(epoch);
}

void RecordCache::evictResetEpochImpl(epoch_t epoch) {
  // recheck the existence under the lock, since the epoch might get
  // poped before we acquire the lock
  std::shared_ptr<EpochRecordCache> epoch_cache = epoch_caches_.get(epoch.val_);
  if (epoch_cache == nullptr || epoch_cache->emptyWithoutTailPayload()) {
    return;
  }

  // re-create a new epoch cache with StoredBefore::MAYBE, which makes it
  // inconsistent at the beginning
  epoch_cache = epoch_caches_.put(
      epoch.val_,
      std::make_shared<EpochRecordCache>(
          log_id_,
          shard_,
          epoch,
          deps_,
          deps_->getEpochRecordCacheSize(log_id_),
          (deps_->tailOptimized(log_id_) ? EpochRecordCache::TailOptimized::YES
                                         : EpochRecordCache::TailOptimized::NO),
          EpochRecordCache::StoredBefore::MAYBE));

  ld_check(epoch_cache != nullptr);
  STAT_INCR(deps_->getStatsHolder(), record_cache_epoch_evicted_by_reset);
}

void RecordCache::evictResetAllEpochs() {
  std::lock_guard<std::mutex> cache_lock(epoch_cache_lock_);
  accessAllEpochCaches([this](EpochRecordCache& epoch_cache) {
    evictResetEpochImpl(epoch_cache.getEpoch());
  });
}

size_t RecordCache::getPayloadSizeEstimate() const {
  size_t total_size = 0;
  accessAllEpochCaches([&](const EpochRecordCache& epoch_cache) {
    total_size += epoch_cache.bufferedPayloadBytes();
  });
  return total_size;
}

void RecordCache::getDebugInfo(InfoRecordCacheTable& table) const {
  accessAllEpochCaches([&table](const EpochRecordCache& epoch_cache) {
    epoch_cache.getDebugInfo(table);
  });
}

ssize_t RecordCache::sizeInLinearBuffer() const {
  size_t result = sizeof(CacheHeader);
  auto epoch_record_caches_snapshot = epoch_caches_.getVersion();
  if (epoch_record_caches_snapshot) {
    for (const auto& epoch_record_cache : *epoch_record_caches_snapshot) {
      if (!epoch_record_cache) {
        continue;
      }
      ssize_t epoch_record_cache_result =
          epoch_record_cache->sizeInLinearBuffer();
      if (epoch_record_cache_result < 0) {
        return -1;
      }
      result += epoch_record_cache_result;
    }
  }
  return result;
}

ssize_t RecordCache::toLinearBuffer(char* buffer, size_t size) const {
  size_t cumulative_size = sizeof(CacheHeader);
  if (size < cumulative_size) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Cannot linearize record to a buffer of size %ju, since "
                    "the header alone is of size %ju",
                    size,
                    cumulative_size);
    return -1;
  }

  CacheHeader header;
  decltype(epoch_caches_)::VersionPtr epoch_record_caches_snapshot;
  {
    std::lock_guard<std::mutex> cache_lock(epoch_cache_lock_);
    epoch_record_caches_snapshot = epoch_caches_.getVersion();
    header = CacheHeader{
        RecordCacheSerializer::MAGIC,
        RecordCacheSerializer::CURRENT_VERSION,
        log_id_,
        0, // will be overwritten if we found any non-null EpochRecordCaches
        head_epoch_cached_.load(),
        next_epoch_to_cache_.load(),
        last_nonauthoritative_epoch_.load()};
  }

  // Store each epoch
  if (epoch_record_caches_snapshot) {
    size_t num_epoch_caches_written = 0;
    ssize_t linear_size = 0;
    for (const auto& epoch_record_cache : *epoch_record_caches_snapshot) {
      if (epoch_record_cache == nullptr) {
        continue;
      }
      linear_size = epoch_record_cache->toLinearBuffer(
          buffer + cumulative_size, size - cumulative_size);
      if (linear_size == -1) {
        return -1;
      }
      cumulative_size += linear_size;
      ld_check(cumulative_size <= size);
      num_epoch_caches_written++;
    }
    header.num_non_empty_epoch_caches = num_epoch_caches_written;
  }

  memcpy(buffer, &header, sizeof(CacheHeader));
  return cumulative_size;
}

std::unique_ptr<RecordCache>
RecordCache::fromLinearBuffer(const char* buffer,
                              size_t size,
                              RecordCacheDependencies* deps,
                              shard_index_t shard) {
  size_t cumulative_size = sizeof(CacheHeader);
  if (size < cumulative_size) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Cannot linearize record from a buffer of size %ju, since "
                    "the header alone is of size %ju",
                    size,
                    cumulative_size);
    return nullptr;
  }

  CacheHeader header;
  memcpy(&header, buffer, sizeof(CacheHeader));
  // Enforce invariants (data may be corrupted):
  if (!CacheHeader::isValid(header)) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    2,
                    "Failed, header is invalid! Header: %s",
                    CacheHeader::toString(header).c_str());
    return nullptr;
  }

  // Use new below, since make_unique would pass field of packed struct by
  // reference (which is dangerous)
  std::unique_ptr<RecordCache> record_cache;
  if (header.head_epoch_cached == EPOCH_INVALID.val()) {
    // Nothing was ever stored in this cache. Initialize as new cache, and
    // store last nonauthoritative epoch
    record_cache.reset(new RecordCache(header.log_id, shard, deps));
    record_cache->last_nonauthoritative_epoch_.store(
        header.last_nonauthoritative_epoch);
    return record_cache;
  } else {
    // Reconstruct state: Head/next/last nonauthoritative epoch, and initialize
    // the RandomAccessQueue of epochs to the correct range, filled w/ nullptr
    record_cache.reset(new RecordCache(header.log_id,
                                       shard,
                                       deps,
                                       header.head_epoch_cached,
                                       header.next_epoch_to_cache,
                                       header.last_nonauthoritative_epoch));
    ld_check(record_cache->epoch_caches_.nextID() ==
             header.next_epoch_to_cache);
  }

  // For any epochs != nullptr, place them in the RandomAccessQueue of epochs
  size_t epoch_record_cache_size = 0;
  epoch_t prev_epoch = EPOCH_INVALID;
  for (int i = 0; i < header.num_non_empty_epoch_caches; i++) {
    auto epoch_record_cache = std::shared_ptr<EpochRecordCache>(
        EpochRecordCache::fromLinearBuffer(header.log_id,
                                           shard,
                                           buffer + cumulative_size,
                                           size - cumulative_size,
                                           deps,
                                           &epoch_record_cache_size));
    if (epoch_record_cache == nullptr) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Failed to repopulate EpochRecordCache from the "
                      "linearized data - aborting repopulation of RecordCache "
                      "for log id %lu. %d EpochRecordCaches previously "
                      "processed. Previous epoch: %s",
                      header.log_id.val(),
                      i + 1,
                      i ? std::to_string(prev_epoch.val()).c_str() : "none");
      return nullptr;
    }
    if (epoch_record_cache->getEpoch() <= prev_epoch) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Halting repopulation of RecordCache for log id %lu from "
                      "the linearized data - found entries in not strictly "
                      "increasing order for epoch %d! EpochRecordCaches "
                      "processed: %d",
                      header.log_id.val(),
                      epoch_record_cache->getEpoch().val(),
                      i + 1);
      return nullptr;
    }
    ld_check(epoch_record_cache_size > 0);
    prev_epoch = epoch_record_cache->getEpoch();

    // Put since we already have the correct range filled with nullptrs
    record_cache->epoch_caches_.put(
        epoch_record_cache->getEpoch().val(), epoch_record_cache);
    cumulative_size += epoch_record_cache_size;
  }
  return record_cache;
}

bool RecordCacheCompare::testRecordCachesIdentical(const RecordCache& a,
                                                   const RecordCache& b) {
  auto snapshot_a = a.epoch_caches_.getVersion();
  auto snapshot_b = b.epoch_caches_.getVersion();
  if (a.log_id_ != b.log_id_ ||
      a.head_epoch_cached_.load() != b.head_epoch_cached_.load() ||
      a.next_epoch_to_cache_.load() != b.next_epoch_to_cache_.load() ||
      a.last_nonauthoritative_epoch_.load() !=
          b.last_nonauthoritative_epoch_.load()) {
    return false;
  }

  // Compare snapshots: if either is null, both must be. If not, compare sizes.
  if (!snapshot_a || !snapshot_b) {
    return snapshot_a == snapshot_b;
  }
  if (snapshot_a->size() != snapshot_b->size()) {
    return false;
  }

  // Check whether the range of epoch_caches_ is the same
  if (a.epoch_caches_.firstID() != b.epoch_caches_.firstID()) {
    return false;
  }
  if (a.epoch_caches_.nextID() != b.epoch_caches_.nextID()) {
    return false;
  }

  // Compare epoch record caches in order.
  for (id_t id = snapshot_a->firstID(); id < snapshot_a->nextID(); id++) {
    auto* epoch_record_cache_a = snapshot_a->get(id).get();
    auto* epoch_record_cache_b = snapshot_b->get(id).get();
    // Check if one is nullptr and the other is not
    if (!(epoch_record_cache_a && epoch_record_cache_b)) {
      if (epoch_record_cache_a != epoch_record_cache_b) {
        return false;
      }
      // Check if the non-null epoch record caches are equivalent
    } else if (!(EpochRecordCacheSerializer::EpochRecordCacheCompare ::
                     testEpochRecordCachesIdentical(
                         *epoch_record_cache_a, *epoch_record_cache_b))) {
      return false;
    }
  }

  return true;
}

bool CacheHeader::isValid(const CacheHeader& header) {
  // Check invariants (data may be corrupted)
  if (header.magic != RecordCacheSerializer::MAGIC) {
    return false;
  }

  if (header.version != RecordCacheSerializer::CURRENT_VERSION) {
    // only allows the snapshot written with the exact same version without
    // any backward or forward compatibility. This is OK since the we just
    // lost the record cache during the upgrade and epoch recovery can fall
    // back to read from the local log store.
    return false;
  }

  // Only both or neither of head or next epoch may be invalid
  if (header.head_epoch_cached == EPOCH_INVALID.val()) {
    if (header.next_epoch_to_cache != EPOCH_INVALID.val()) {
      return false;
    }
    if (header.num_non_empty_epoch_caches > 0) {
      return false;
    }
  }

  // Require next >= head
  if (header.next_epoch_to_cache < header.head_epoch_cached) {
    return false;
  }

  // If cache is non-empty, require next > head and head, next not invalid
  if (header.num_non_empty_epoch_caches > 0 &&
      (header.head_epoch_cached == header.next_epoch_to_cache ||
       header.head_epoch_cached == EPOCH_INVALID.val() ||
       header.next_epoch_to_cache == EPOCH_INVALID.val())) {
    return false;
  }

  return true;
}

std::string CacheHeader::toString(const CacheHeader& h) {
  std::string prefix = isValid(h) ? "" : "(Invalid) ";
  return "[" + prefix + " M:" + std::to_string(h.magic) +
      +" V:" + std::to_string(h.version) +
      +" L:" + std::to_string(h.log_id.val()) +
      +" N:" + std::to_string(h.num_non_empty_epoch_caches) +
      +" H:" + std::to_string(h.head_epoch_cached) +
      +" X:" + std::to_string(h.next_epoch_to_cache) +
      +" L:" + std::to_string(h.last_nonauthoritative_epoch) + "]";
}

}} // namespace facebook::logdevice
