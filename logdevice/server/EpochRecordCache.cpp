/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/EpochRecordCache.h"

#include <algorithm>
#include <chrono>

#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Digest.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/RecordID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/EpochRecordCacheEntry.h"
#include "logdevice/server/RecordCacheDependencies.h"

namespace facebook { namespace logdevice {

using namespace EpochRecordCacheSerializer;

EpochRecordCache::EpochRecordCache(logid_t log_id,
                                   shard_index_t shard,
                                   epoch_t epoch,
                                   EpochRecordCacheDependencies* deps,
                                   size_t capacity,
                                   TailOptimized tail_optimized,
                                   StoredBefore stored)
    : log_id_(log_id),
      shard_(shard),
      epoch_(epoch),
      deps_(deps),
      tail_optimized_(tail_optimized),
      stored_(stored),
      buffer_(capacity) {
  ld_check(capacity > 0);
  ld_check(deps_ != nullptr);
}

std::unique_ptr<EpochRecordCache>
EpochRecordCache::createFromSnapshot(logid_t log_id,
                                     shard_index_t shard,
                                     const EpochRecordCache::Snapshot& snapshot,
                                     EpochRecordCacheDependencies* deps) {
  if (!snapshot.isSerializable()) {
    return nullptr;
  }

  CacheHeader* header = snapshot.header_.get();
  ld_check(header != nullptr);

  if (!CacheHeader::isValid(*header) ||
      header->buffer_entries != snapshot.entry_map_.size()) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Corrupt snapshot for log %lu, not creating "
                    "EpochRecordCache! Header: %s, given %zu buffer entries",
                    log_id.val(),
                    CacheHeader::toString(*header).c_str(),
                    snapshot.entry_map_.size());
    return nullptr;
  }

  // Use new as header struct is packed; make_unique takes references as input
  auto result = std::unique_ptr<EpochRecordCache>(
      new EpochRecordCache(log_id,
                           shard,
                           header->epoch,
                           deps,
                           header->buffer_capacity,
                           header->tail_optimized,
                           header->stored));
  result->setOffsetsWithinEpoch(snapshot.offsets_within_epoch_);
  result->first_seen_lng_.store(header->first_seen_lng);
  result->head_.store(header->head);
  result->max_seen_esn_.store(header->max_seen_esn);
  result->max_seen_timestamp_.store(header->max_seen_timestamp);
  result->buffer_entries_ = header->buffer_entries;
  result->buffer_payload_bytes_ = header->buffer_payload_bytes;

  // copy the tail record if it is valid
  if (header->flags & CacheHeader::VALID_TAIL_RECORD) {
    result->tail_record_ = snapshot.tail_record_;
  } else {
    ld_check(!result->tail_record_.isValid());
  }

  // Repopulate circular buffer
  size_t buffer_payload_bytes_cumulative = 0;
  for (auto& kv : snapshot.entry_map_) {
    auto rid = RecordID(compose_lsn(header->epoch, kv.first), log_id);
    if (rid.esn.val() < header->head || rid.esn > result->maxESNToAccept()) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Found out-of-range esn of an entry! esn: %d, allowed "
                      "range: [%d, %d], snapshot header: %s",
                      rid.esn.val(),
                      header->head,
                      result->maxESNToAccept().val(),
                      CacheHeader::toString(*header).c_str());
      return nullptr;
    }

    size_t index = result->getIndex(rid.esn);
    ld_check(result->buffer_[index] == nullptr);
    buffer_payload_bytes_cumulative += kv.second->payload_raw.size;

    result->buffer_[index] = std::move(kv.second);
    ld_check(result->buffer_[index] != nullptr);
  }

  if (buffer_payload_bytes_cumulative != result->buffer_payload_bytes_) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Payload bytes from header does not match sum from "
                    "entries! Log id %lu, epoch %d, header: %s, payload bytes "
                    "found in entries: %zu",
                    log_id.val(),
                    header->epoch.val(),
                    CacheHeader::toString(*header).c_str(),
                    buffer_payload_bytes_cumulative);
    return nullptr;
  }
  ld_check(result->buffer_entries_ == header->buffer_entries);
  return result;
}

EpochRecordCache::~EpochRecordCache() {
  // the cache must be disabled with all buffered records drained upon
  // destruction
  disableCache();
  ld_check(disabled_.load());
}

esn_t EpochRecordCache::getLNG() const {
  // lng is maintained as head_ - 1
  return getLNG(head_.load());
}

/*static*/
esn_t EpochRecordCache::getLNG(esn_t::raw_type head) {
  // lng is maintained as head_ - 1
  return (head > ESN_INVALID.val_ ? esn_t(head - 1) : ESN_INVALID);
}

bool EpochRecordCache::empty() const {
  // TODO: if locking turns out to be cpu-intensive, consider
  // merging the two atomics into a single 64-bit atomic variable
  FairRWLock::ReadHolder read_guard(rw_lock_);
  // never stored or all records evicted (lng >= max_seen)
  return head_ == ESN_INVALID.val_ || head_ > max_seen_esn_;
}

bool EpochRecordCache::emptyWithoutTailPayload() const {
  FairRWLock::ReadHolder read_guard(rw_lock_);
  // never stored or all records evicted (lng >= max_seen)
  return (head_ == ESN_INVALID.val_ || head_ > max_seen_esn_) &&
      !tail_record_.hasPayload();
}

bool EpochRecordCache::isConsistent() const {
  if (disabled_.load()) {
    // not consistent if disabled
    return false;
  }

  if (stored_ == StoredBefore::NEVER) {
    // if the storage node has never stored records of this epoch before
    // the creation of the cache, then it is always considered as consistent
    return true;
  }

  // It is more complicated if there may be records of this epoch stored
  // before the cache was created. The reason is that we don't know for an
  // esn not in the cache, whether there is a record of that esn stored
  // persistently in the local log store by a previous instance of the LogDevice
  // daemon.
  //
  // The solution here is:
  // 1) record the first seen `last known good` value;
  // 2) keep caching new records but mark the cache as inconsistent until
  //    it sees a record with its lng >= first_seen_lng + capacity()
  //
  // The solution relies on the assumption that the store we first see MUST
  // be originated (on the sequencer side) after the cache was created. This is
  // reasonable since the cache was created on node startup and all previous
  // connections were closed. Therefore, at the cache creation time,
  // the sequencer must have NOT started appending ESN of lng_target =
  // first_lng + sequencer sliding window size. So once the cache sees a record
  // with lng >= lng_target, the cache is considered consistent.
  const esn_t::raw_type first_lng = first_seen_lng_.load();
  const esn_t lng_target =
      (first_lng < ESN_MAX.val_ - capacity() ? esn_t(first_lng + capacity())
                                             : ESN_MAX);

  return getLNG() >= lng_target;
}

size_t EpochRecordCache::bufferedRecords() const {
  return buffer_entries_.load(std::memory_order_relaxed);
}

size_t EpochRecordCache::bufferedPayloadBytes() const {
  return buffer_payload_bytes_.load(std::memory_order_relaxed);
}

int EpochRecordCache::putRecord(
    RecordID rid,
    uint64_t timestamp,
    esn_t lng,
    uint32_t wave_or_recovery_epoch,
    const copyset_t& copyset,
    STORE_flags_t flags,
    std::map<KeyType, std::string>&& keys,
    Slice payload_raw,
    const std::shared_ptr<PayloadHolder>& payload_holder,
    OffsetMap offsets_within_epoch) {
  esn_t head = esn_t(head_.load());
  if (disabled_.load() || (head != ESN_INVALID && rid.esn < head)) {
    // the record should not be put into the cache if:
    // 1) cache is disabled; OR
    // 2) the record is smaller or equal than the current LNG maintained
    //    and it should never be used for recovery

    // TODO: Evict here, if lng >= head?
    return -1;
  }

  if (rid.esn < lng) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Got a record of log %lu lsn %s with its esn less than "
                      "lng of %u. Skip caching the record.",
                      rid.logid.val_,
                      lsn_to_string(rid.lsn()).c_str(),
                      lng.val_);
    // TODO: Evict here, if lng >= head?
    return -1;
  }

  if (rid.esn.val_ - lng.val_ > capacity()) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Got a record of log %lu lsn %s flags %x with the "
                      "difference between its esn %u and lng %u larger than "
                      "the capacity of the EpochRecordCache of %lu. It is "
                      "likely that the sliding window size of Sequencer has "
                      "been changed in configuration. Disabling the cache for "
                      "correctness.",
                      rid.logid.val_,
                      lsn_to_string(rid.lsn()).c_str(),
                      flags,
                      rid.esn.val_,
                      lng.val_,
                      capacity());
    disableCache();
    STAT_INCR(deps_->getStatsHolder(), record_cache_record_out_of_capacity);
    return -1;
  }

  // it is likely that the record will be stored in cache, pre-allocate
  // its entry
  auto entry = std::shared_ptr<EpochRecordCacheEntry>(
      new EpochRecordCacheEntry(rid.lsn(),
                                flags,
                                timestamp,
                                lng,
                                wave_or_recovery_epoch,
                                copyset,
                                offsets_within_epoch,
                                std::move(keys),
                                payload_raw,
                                payload_holder),
      EpochRecordCacheEntry::Disposer(deps_));

  ReleasedVector entries_to_drop;
  int rv = 0;

  auto getRecordMetadata = [](const EpochRecordCacheEntry& e) {
    return Digest::RecordMetadata{
        e.flags & LocalLogStoreRecordFormat::FLAG_MASK,
        e.wave_or_recovery_epoch};
  };

  do {
    FairRWLock::WriteHolder write_guard(rw_lock_);

    // recheck with the lock held
    head = esn_t(head_.load());
    if (disabled_.load() || (head != ESN_INVALID && rid.esn < head)) {
      rv = -1;
      break;
    }

    // update the cache based on the new lng received, evicting entries if
    // necessary
    advanceLNGImpl(head, lng, entries_to_drop);

    // insert the record to the cache
    head = esn_t(head_.load());
    ld_check(head != ESN_INVALID);

    if (rid.esn < head) {
      // This only happens when we got a record with its esn == lng.
      // Do not cache the record.
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        2,
                        "Unable to cache a record for log %lu record %s with "
                        "lng %u.",
                        rid.logid.val_,
                        lsn_to_string(rid.lsn()).c_str(),
                        lng.val_);
      rv = -1;
      break;
    }

    // esn must fit into the cache as lng advances
    ld_assert(rid.esn <= maxESNToAccept());

    // update max seen esn
    atomic_fetch_max(max_seen_esn_, rid.esn.val_);

    // update max seen timestamp
    atomic_fetch_max(max_seen_timestamp_, timestamp);

    // update accumulative epoch size with offsets_within_epoch
    setOffsetsWithinEpoch(offsets_within_epoch);

    // there might be old entry for the same esn, drop it first
    auto& old_entry = buffer_[getIndex(rid.esn)];
    if (old_entry != nullptr) {
      if (getRecordMetadata(*entry) < getRecordMetadata(*old_entry)) {
        // the existing entry has higher precedence, ignore the update
        ld_check(rv == 0);
        break;
      } else if (entry->flags & STORE_Header::AMEND) {
        // do not discard existing entry but amend its metadata
        ld_check(rv == 0);
        amendExistingEntry(old_entry.get(), entry.get());
        break;
      } else {
        // the existing entry is going to be replaced, discard it
        noteEntryRemoved(*old_entry);
        entries_to_drop.push_back(std::move(old_entry));
      }
    }

    // we reach here either because there is no exising entry, or we decided
    // to replace it with the new one
    ld_check(buffer_[getIndex(rid.esn)] == nullptr);
    noteEntryAdded(*entry);
    buffer_[getIndex(rid.esn)] = std::move(entry);

  } while (0);

  // destroying shared_ptrs of Entries may cause the action of disposal, do
  // it from outside of the lock
  return rv;
}

// must be called with write lock held
void EpochRecordCache::updateTailRecord(
    const std::shared_ptr<EpochRecordCacheEntry>& tail) {
  ld_check(tail != nullptr);
  // we must update LSN in increasing order
  ld_check(tail->lsn > tail_record_.header.lsn);

  if (tail->flags & STORE_Header::HOLE) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Released a hole record (log id %lu, lsn %s, flags %s) "
                    "as a tail record in record cache! This shouldn't happen.",
                    getLogId().val_,
                    lsn_to_string(tail->lsn).c_str(),
                    STORE_Message::flagsToString(tail->flags).c_str());
  }

  // not all record has the offsets_within_epoch, if not, use the
  // value in the existing tail_record instead
  OffsetMap offsets =
      (tail->offsets_within_epoch.isValid() ? tail->offsets_within_epoch
                                            : tail_record_.offsets_map_);
  TailRecordHeader::flags_t flags = TailRecordHeader::OFFSET_WITHIN_EPOCH;
  // TODO (T35832374) : remove if condition when all servers support OffsetMap
  if (tail->flags & STORE_Header::OFFSET_MAP) {
    flags |= TailRecordHeader::OFFSET_MAP;
  }
  if (tail_optimized_ == TailOptimized::YES) {
    flags |= TailRecordHeader::HAS_PAYLOAD;
    flags |= (tail->flags &
              (STORE_Header::CHECKSUM | STORE_Header::CHECKSUM_64BIT |
               STORE_Header::CHECKSUM_PARITY));
  } else {
    // do not store payload or checksum
    flags |= TailRecordHeader::CHECKSUM_PARITY;
  }

  // replace the existing tail record
  tail_record_ =
      TailRecord({log_id_,
                  tail->lsn,
                  tail->timestamp,
                  {BYTE_OFFSET_INVALID /* deprecated, use offsets instead */},
                  flags,
                  {}},
                 std::move(offsets),
                 tail_optimized_ == TailOptimized::YES ? tail : nullptr);
}

// must be called with write lock held
void EpochRecordCache::advanceLNGImpl(esn_t current_head,
                                      esn_t lng,
                                      ReleasedVector& disposal) {
  ld_assert(!disabled_.load());

  if (current_head != ESN_INVALID) {
    if (lng >= current_head) {
      // may need to discard some records
      bool was_consistent = isConsistent();
      bool evicted = false;
      const esn_t::raw_type new_head = next_esn(lng).val_;
      const esn_t discard_until = std::min(maxESNToAccept(), lng);
      const size_t until_offset = getIndex(discard_until);

      // discard records in esn range [current_head, discard_until]
      for (size_t i = 0; i <= until_offset; ++i) {
        if (buffer_[i] != nullptr) {
          noteEntryRemoved(*buffer_[i]);
          disposal.push_back(std::move(buffer_[i]));
          evicted = true;
        }
      }

      buffer_.rotate(new_head - current_head.val_);
      head_.store(new_head);

      if (evicted) {
        ld_check(!disposal.empty());
        updateTailRecord(disposal.back());
      }

      if (was_consistent) {
        deps_->onRecordsReleased(*this,
                                 compose_lsn(epoch_, current_head),
                                 compose_lsn(epoch_, esn_t(lng)),
                                 disposal);
      }
    }

  } else {
    // cache is empty, initialize first_seen_lng_ and head_
    ld_assert(first_seen_lng_.load() == ESN_INVALID.val_);
    head_.store(next_esn(lng).val_);
    first_seen_lng_.store(lng.val_);
  }
}

void EpochRecordCache::advanceLNG(esn_t new_lng) {
  esn_t head = esn_t(head_.load());
  if (new_lng == ESN_INVALID || disabled_.load() || new_lng < head) {
    // cache is disabled or new_lng is smaller than the head, nothing to do.
    return;
  }

  ReleasedVector entries_to_drop;
  {
    FairRWLock::WriteHolder write_guard(rw_lock_);
    // recheck with the lock held
    head = esn_t(head_.load());
    if (disabled_.load() || new_lng < head) {
      return;
    }

    advanceLNGImpl(head, new_lng, entries_to_drop);
  }
}

void EpochRecordCache::amendExistingEntry(
    EpochRecordCacheEntry* existing_entry,
    const EpochRecordCacheEntry* amend_entry) {
  ld_check(existing_entry != nullptr);
  ld_check(amend_entry != nullptr);
  // amend the FLAG_WRITTEN_BY_RECOVERY flag: set the flag if the amend
  // record has it
  if (amend_entry->flags & STORE_Header::WRITTEN_BY_RECOVERY) {
    existing_entry->flags |= STORE_Header::WRITTEN_BY_RECOVERY;
  }
  // with bridge record, a hole can be amended for adding or removing
  // the BRIDGE flag
  if (amend_entry->flags & STORE_Header::BRIDGE) {
    existing_entry->flags |= STORE_Header::BRIDGE;
  } else {
    existing_entry->flags &= ~STORE_Header::BRIDGE;
  }

  // amend the copyset and wave_or_recovery_epoch
  existing_entry->wave_or_recovery_epoch = amend_entry->wave_or_recovery_epoch;
  existing_entry->copyset = amend_entry->copyset;
}

TailRecord EpochRecordCache::getTailRecord() const {
  FairRWLock::ReadHolder read_guard(rw_lock_);
  return tail_record_;
}

void EpochRecordCache::disableCache() {
  bool prev_disabled;
  {
    FairRWLock::WriteHolder write_guard(rw_lock_);
    prev_disabled = disabled_.exchange(true);
    // clear the tail record, have to do it under the lock since there
    // might be readers for the tail optimized log.
    if (!prev_disabled) {
      tail_record_.reset();
    }
  }

  if (!prev_disabled) {
    // there should be no other writers or readers accessing buffer_
    // at this time, so it is safe to directly work on buffer_
    for (size_t i = 0; i < capacity(); ++i) {
      if (buffer_[i] != nullptr) {
        noteEntryRemoved(*buffer_[i]);
        buffer_[i].reset();
        ld_check(buffer_[i] == nullptr);
      }
    }
  }
}

std::pair<bool, std::shared_ptr<EpochRecordCacheEntry>>
EpochRecordCache::getEntry(esn_t esn) const {
  if (esn.val_ < head_.load()) {
    return std::make_pair(false, nullptr);
  }

  FairRWLock::ReadHolder read_guard(rw_lock_);
  if (esn.val_ < head_.load() || esn > maxESNToAccept()) {
    return std::make_pair(false, nullptr);
  }

  auto& e = buffer_[getIndex(esn)];
  if (e == nullptr) {
    return std::make_pair(false, nullptr);
  }
  return std::make_pair(true, e);
}

std::unique_ptr<EpochRecordCache::Snapshot>
EpochRecordCache::createSnapshot(esn_t esn_start, esn_t esn_end) const {
  return createSnapshotImpl(
      Snapshot::SnapshotType::PARTIAL, esn_start, esn_end);
}

std::unique_ptr<EpochRecordCache::Snapshot>
EpochRecordCache::createSerializableSnapshot(bool enable_offset_map) const {
  return createSnapshotImpl(
      Snapshot::SnapshotType::FULL, ESN_INVALID, ESN_MAX, enable_offset_map);
}

std::unique_ptr<EpochRecordCache::Snapshot>
EpochRecordCache::createSnapshotImpl(Snapshot::SnapshotType type,
                                     esn_t esn_start,
                                     esn_t esn_end,
                                     bool enable_offset_map) const {
  if (type == Snapshot::SnapshotType::FULL) {
    ld_check(esn_start == ESN_INVALID || esn_end == ESN_MAX);
  }

  std::unique_ptr<Snapshot> snapshot;
  {
    // We may only be contenting with writers running on storage threads that
    // are doing mutation. This only happens when there is another EpochRecovery
    // with a higer seal epoch going on.
    FairRWLock::ReadHolder read_guard(rw_lock_);
    if (type == Snapshot::SnapshotType::FULL) {
      OffsetMap offsets_within_epoch = getOffsetsWithinEpoch();
      // Initialize header fields
      CacheHeader header{};
      header.epoch = epoch_;
      header.tail_optimized = tail_optimized_;
      header.buffer_capacity = capacity();
      header.offset_within_epoch = offsets_within_epoch.getCounter(BYTE_OFFSET);
      if (tail_record_.isValid()) {
        header.flags |=
            EpochRecordCacheSerializer::CacheHeader::VALID_TAIL_RECORD;
      } else {
        ld_check(header.flags == 0);
      }
      // TODO (T35832374) : remove if condition when all servers support
      // OffsetMap
      if (enable_offset_map) {
        header.flags |=
            EpochRecordCacheSerializer::CacheHeader::SUPPORT_OFFSET_MAP;
      }

      // If disabled, store as an empty inconsistent cache, so that we maintain
      // correctness while allowing it to eventually become consistent.
      if (disabled_.load()) {
        header.disabled = true;
        header.flags = 0;
        header.stored = StoredBefore::MAYBE;
        header.head = ESN_INVALID.val();
        header.first_seen_lng = ESN_INVALID.val();
        header.max_seen_esn = ESN_INVALID.val();
        header.max_seen_timestamp = 0;
        header.buffer_entries = 0;
        header.buffer_payload_bytes = 0;
        ld_check(!tail_record_.isValid());
      } else {
        header.disabled = false;
        header.stored = stored_;
        header.head = head_.load();
        header.first_seen_lng = first_seen_lng_.load();
        header.max_seen_esn = max_seen_esn_.load();
        header.max_seen_timestamp = max_seen_timestamp_.load();
        header.buffer_entries = buffer_entries_;
        header.buffer_payload_bytes = buffer_payload_bytes_;
      }
      if (!CacheHeader::isValid(header)) {
        RATELIMIT_CRITICAL(std::chrono::seconds(30),
                           5,
                           "EpochRecordCache in invalid state! Header: %s",
                           CacheHeader::toString(header).c_str());
        ld_check(false);
        return nullptr;
      }
      snapshot = std::unique_ptr<Snapshot>(
          new Snapshot(header, tail_record_, std::move(offsets_within_epoch)));
    } else { // PARTIAL Snapshot
      snapshot = std::unique_ptr<Snapshot>(new Snapshot(esn_start, esn_end));
    }

    esn_t::raw_type head = head_.load();
    esn_t::raw_type max_seen = max_seen_esn_.load();
    if (disabled_.load() || head == ESN_INVALID.val_ || max_seen < head ||
        esn_start.val_ > max_seen || esn_end.val_ < head) {
      return snapshot;
    }
    const size_t start_idx = std::max(head, esn_start.val_) - head;
    const size_t until_idx = std::min(max_seen, esn_end.val_) - head;
    for (size_t i = start_idx; i <= until_idx; ++i) {
      auto& e = buffer_[i];
      if (e != nullptr) {
        snapshot->entry_map_[esn_t(head + i)] = e;
      }
    }
  }

  return snapshot;
}

std::unique_ptr<EpochRecordCache::Snapshot>
EpochRecordCache::Snapshot::createFromLinearBuffer(
    const char* buffer,
    size_t size,
    EpochRecordCacheDependencies* deps,
    size_t* result_size) {
  ld_check(buffer);
  if (result_size != nullptr) {
    *result_size = 0;
  }
  if (size < sizeof(CacheHeader)) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Failed to populate EpochRecordCache from linear buffer "
                    "of size %ju, since the header alone is of size %ju!",
                    size,
                    sizeof(CacheHeader));
    return nullptr;
  }

  std::unique_ptr<Snapshot> snapshot;
  size_t cumulative_size = 0;
  {
    // 1) Read the header
    CacheHeader header;
    memcpy(&header, buffer, sizeof(CacheHeader));
    if (!CacheHeader::isValid(header)) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Failed to populate EpochRecordCache from linear "
                      "buffer: header is corrupt: %s",
                      CacheHeader::toString(header).c_str());
      return nullptr;
    }
    cumulative_size += sizeof(CacheHeader);

    // 2) Read offsets_within_epoch
    OffsetMap offsets_within_epoch;
    if (header.flags & CacheHeader::SUPPORT_OFFSET_MAP) {
      int rv = offsets_within_epoch.deserialize(
          Slice(buffer + cumulative_size, size - cumulative_size));
      if (rv < 0) {
        RATELIMIT_ERROR(std::chrono::seconds(30),
                        5,
                        "Failed to populate EpochRecordCache from linear "
                        "buffer: bad offsets_within_epoch! header: %s",
                        CacheHeader::toString(header).c_str());
        return nullptr;
      }
      cumulative_size += rv;
    } else {
      // TODO (T35659884) : Remove offset_within_epoch
      // Populate offsets_within_epoch from previous header format
      offsets_within_epoch.setCounter(BYTE_OFFSET, header.offset_within_epoch);
    }

    // 3) Read TailRecord
    TailRecord tail_record;
    if (header.flags & CacheHeader::VALID_TAIL_RECORD) {
      // otherwise it won't be a valid header
      ld_check(!header.disabled);
      int rv = tail_record.deserialize(
          Slice(buffer + cumulative_size, size - cumulative_size));
      if (rv < 0 || !tail_record.isValid()) {
        RATELIMIT_ERROR(std::chrono::seconds(30),
                        5,
                        "Failed to populate EpochRecordCache from linear "
                        "buffer: bad tail record! header: %s",
                        CacheHeader::toString(header).c_str());
        return nullptr;
      }

      cumulative_size += rv;
      ld_check(cumulative_size <= size);
    }

    snapshot.reset(
        new Snapshot(header, tail_record, std::move(offsets_within_epoch)));
  }

  ld_check(snapshot);
  CacheHeader* header = snapshot->header_.get();
  if (header->disabled) {
    if (result_size != nullptr) {
      *result_size = cumulative_size;
    }
    return snapshot;
  }

  // 4) Read cache entries
  esn_t::raw_type previous_esn = ESN_INVALID.val();
  esn_t lng = prev_esn(esn_t(header->head));

  // Read the entries one by one
  for (int i = 0; i < header->buffer_entries; i++) {
    esn_t::raw_type esn_raw;
    if (cumulative_size + sizeof(esn_t::raw_type) > size) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Halting repopulation: out of linear buffer space! "
                      "Buffer size: %ju",
                      size);
      return nullptr;
    }
    memcpy(&esn_raw, buffer + cumulative_size, sizeof(esn_t::raw_type));
    if (esn_raw <= previous_esn) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Halting repopulation: entries out of order, data must "
                      "be corrupt!");
      return nullptr;
    }
    if (esn_raw - lng.val() > header->buffer_capacity) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Halting repopulation: entry has esn outside buffer "
                      "range, data must be corrupted! ESN: %d, LNG: %d, "
                      "buffer capacity: %ju",
                      esn_raw,
                      lng.val(),
                      header->buffer_capacity);
      return nullptr;
    }
    previous_esn = esn_raw;
    cumulative_size += sizeof(esn_t::raw_type);
    size_t entry_size = 0;
    auto entry = EpochRecordCacheEntry::createFromLinearBuffer(
        compose_lsn(header->epoch, esn_t{esn_raw}),
        buffer + cumulative_size,
        size - cumulative_size,
        EpochRecordCacheEntry::Disposer(deps),
        &entry_size);
    if (entry == nullptr) {
      return nullptr;
    }
    ld_check(entry_size > 0);

    cumulative_size += entry_size;
    snapshot->entry_map_[esn_t(esn_raw)] = std::move(entry);
  }
  if (result_size != nullptr) {
    *result_size = cumulative_size;
  }
  return snapshot;
}

EpochRecordCache::Snapshot::Record
EpochRecordCache::Snapshot::ConstIterator::getRecord() const {
  ld_assert(!atEnd());
  EpochRecordCacheEntry* e = it_->second.get();
  ld_check(e != nullptr);

  return Record{e->flags,
                e->timestamp,
                e->last_known_good,
                e->wave_or_recovery_epoch,
                e->copyset,
                e->offsets_within_epoch,
                e->payload_raw};
}

void EpochRecordCache::getDebugInfo(InfoRecordCacheTable& table) const {
  FairRWLock::ReadHolder read_guard(rw_lock_);
  table.next()
      .set<0>(log_id_)
      .set<1>(shard_)
      .set<2>(epoch_)
      .set<3>(bufferedPayloadBytes())
      .set<4>(bufferedRecords())
      .set<5>(isConsistent())
      .set<6>(disabled_.load())
      .set<7>(head_.load())
      .set<8>(max_seen_esn_.load())
      .set<9>(first_seen_lng_.load())
      .set<10>(offsets_within_epoch_.load().toString());

  if (tail_record_.isValid()) {
    table.set<11>(tail_record_.header.lsn);
    table.set<12>(tail_record_.header.timestamp);
  }
}

ssize_t EpochRecordCache::sizeInLinearBuffer() const {
  auto snapshot = createSerializableSnapshot();
  if (!snapshot) {
    return -1;
  }
  return snapshot->sizeInLinearBuffer();
}

ssize_t EpochRecordCache::toLinearBuffer(char* buffer, size_t size) const {
  auto snapshot = createSerializableSnapshot();
  if (!snapshot) {
    return -1;
  }
  return snapshot->toLinearBuffer(buffer, size);
}

std::unique_ptr<EpochRecordCache>
EpochRecordCache::fromLinearBuffer(logid_t log_id,
                                   shard_index_t shard,
                                   const char* buffer,
                                   size_t size,
                                   EpochRecordCacheDependencies* deps,
                                   size_t* result_size) {
  auto snapshot =
      Snapshot::createFromLinearBuffer(buffer, size, deps, result_size);
  if (!snapshot) {
    return nullptr;
  }
  return createFromSnapshot(log_id, shard, *snapshot, deps);
}

void EpochRecordCache::noteEntryAdded(const EpochRecordCacheEntry& entry) {
  buffer_entries_.fetch_add(1, std::memory_order_relaxed);
  buffer_payload_bytes_.fetch_add(
      entry.payload_raw.size, std::memory_order_relaxed);
}

void EpochRecordCache::noteEntryRemoved(const EpochRecordCacheEntry& entry) {
  ld_check(buffer_entries_ > 0);
  ld_check(buffer_payload_bytes_ >= entry.payload_raw.size);
  buffer_entries_.fetch_sub(1, std::memory_order_relaxed);
  buffer_payload_bytes_.fetch_sub(
      entry.payload_raw.size, std::memory_order_relaxed);
}

EpochRecordCache::Snapshot::Snapshot(esn_t start, esn_t until)
    : start_(start), until_(until), type_(SnapshotType::PARTIAL) {}

EpochRecordCache::Snapshot::Snapshot(const CacheHeader& header,
                                     const TailRecord& tail_record,
                                     OffsetMap offsets_within_epoch)
    : start_(ESN_INVALID),
      until_(ESN_MAX),
      tail_record_(tail_record),
      offsets_within_epoch_(std::move(offsets_within_epoch)),
      type_(SnapshotType::FULL) {
  header_ = std::make_unique<CacheHeader>(header);
  ld_check(!!(header_->flags & CacheHeader::VALID_TAIL_RECORD) ==
           tail_record.isValid());
}

size_t EpochRecordCache::Snapshot::sizeInLinearBuffer() const {
  // 1) header
  size_t result = sizeof(CacheHeader);

  // 2) offsets_within_epoch
  if (header_->flags & CacheHeader::SUPPORT_OFFSET_MAP) {
    result += offsets_within_epoch_.sizeInLinearBuffer();
  }

  // 3) TailRecord
  if (header_->flags & CacheHeader::VALID_TAIL_RECORD) {
    ld_check(tail_record_.isValid());
    result += tail_record_.sizeInLinearBuffer();
  } else {
    ld_check(!tail_record_.isValid());
  }

  // 4) cache entries
  for (const auto& kv : entry_map_) {
    result += sizeof(esn_t::raw_type) + kv.second->sizeInLinearBuffer();
  }
  return result;
}

ssize_t EpochRecordCache::Snapshot::toLinearBuffer(char* buffer,
                                                   size_t size) const {
  if (type_ != SnapshotType::FULL) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Only snapshots covering all ESNs may be serialized");
    ld_check(!header_);
    return -1;
  }
  ld_check(header_);
  ld_check(start_ == ESN_INVALID);
  ld_check(until_ == ESN_MAX);
  ld_check(buffer);

  // 1) write header
  size_t cumulative_size = sizeof(CacheHeader);
  if (cumulative_size > size) {
    RATELIMIT_ERROR(std::chrono::seconds(30),
                    5,
                    "Not writing to linear buffer: out of space");
    return -1;
  }

  memcpy(buffer, header_.get(), sizeof(CacheHeader));

  // 2) write offsets_within_epoch
  if (header_->flags & CacheHeader::SUPPORT_OFFSET_MAP) {
    int rv = offsets_within_epoch_.serialize(
        buffer + cumulative_size, size - cumulative_size);
    if (rv < 0) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Not writing to linear buffer: failed to write "
                      "OffsetMap counters.");
      return -1;
    }
    cumulative_size += rv;
  }

  // 3) write TailRecord if it is valid
  if (header_->flags & CacheHeader::VALID_TAIL_RECORD) {
    ld_check(tail_record_.isValid());
    int rv = tail_record_.serialize(
        buffer + cumulative_size, size - cumulative_size);
    if (rv < 0) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Not writing to linear buffer: failed to write "
                      "TailRecord.");
      return -1;
    }
    cumulative_size += rv;
  } else {
    ld_check(!tail_record_.isValid());
  }

  // 4) write cache entries
  ld_check(entry_map_.size() == header_->buffer_entries);
  for (const auto& kv : entry_map_) {
    auto& esn = kv.first;
    auto& entry = kv.second;

    if (entry->sizeInLinearBuffer() + sizeof(esn_t::raw_type) >
        size - cumulative_size) {
      RATELIMIT_ERROR(std::chrono::seconds(30),
                      5,
                      "Not writing to linear buffer: out of space");
      return -1;
    }

    memcpy(buffer + cumulative_size, &esn, sizeof(esn_t::raw_type));
    cumulative_size += sizeof(esn_t::raw_type);
    ssize_t entry_linear_size =
        entry->toLinearBuffer(buffer + cumulative_size, size - cumulative_size);
    if (entry_linear_size < 0) {
      return -1;
    }

    cumulative_size += entry_linear_size;
  }
  return cumulative_size;
}

std::pair<bool, EpochRecordCache::Snapshot::Record>
EpochRecordCache::Snapshot::getRecord(esn_t esn) const {
  auto it = entry_map_.find(esn);
  if (it == entry_map_.end()) {
    return std::make_pair(false, Record());
  }

  EpochRecordCacheEntry* e = it->second.get();
  ld_check(e != nullptr);
  return std::make_pair(true,
                        Record{e->flags,
                               e->timestamp,
                               e->last_known_good,
                               e->wave_or_recovery_epoch,
                               e->copyset,
                               e->offsets_within_epoch,
                               e->payload_raw});
}

std::unique_ptr<EpochRecordCache::Snapshot::ConstIterator>
EpochRecordCache::Snapshot::createIterator() const {
  return std::make_unique<Snapshot::ConstIterator>(*this);
}

bool EpochRecordCacheCompare::testEntriesIdentical(
    const EpochRecordCacheEntry& a,
    const EpochRecordCacheEntry& b) {
  return a.flags == b.flags && a.timestamp == b.timestamp &&
      a.last_known_good == b.last_known_good &&
      a.wave_or_recovery_epoch == b.wave_or_recovery_epoch &&
      a.copyset.size() == b.copyset.size() &&
      std::equal(a.copyset.begin(), a.copyset.end(), b.copyset.begin()) &&
      a.offsets_within_epoch == b.offsets_within_epoch && a.keys == b.keys &&
      a.payload_raw.size == b.payload_raw.size &&
      memcmp(a.payload_raw.data, b.payload_raw.data, a.payload_raw.size) == 0;
}

bool EpochRecordCacheCompare::testSnapshotsIdentical(
    const EpochRecordCache::Snapshot& a,
    const EpochRecordCache::Snapshot& b) {
  // Compare member vars
  if (a.start_ != b.start_ || a.until_ != b.until_ ||
      a.entry_map_.size() != b.entry_map_.size()) {
    return false;
  }

  // Compare headers
  if (a.header_ == nullptr || b.header_ == nullptr) {
    if (a.header_ != b.header_) {
      return false;
    }
  } else if (memcmp(a.header_.get(), b.header_.get(), sizeof(CacheHeader))) {
    return false;
  }

  // compare TailRecords
  if (!a.tail_record_.sameContent(b.tail_record_)) {
    return false;
  }

  // Compare entries by iterating over entry maps
  auto it_a = a.entry_map_.begin();
  auto it_b = b.entry_map_.begin();
  for (; it_a != a.entry_map_.end() && it_b != b.entry_map_.end();
       it_a++, it_b++) {
    // Compare ESNs
    if (it_a->first != it_b->first) {
      return false;
    }
    // Compare entry content
    if (!testEntriesIdentical(*(it_a->second), *(it_b->second))) {
      return false;
    }
  }
  ld_check(it_a == a.entry_map_.end());
  ld_check(it_b == b.entry_map_.end());
  return true;
}

bool EpochRecordCacheCompare::testEpochRecordCachesIdentical(
    const EpochRecordCache& a,
    const EpochRecordCache& b) {
  auto snapshot_a = a.createSerializableSnapshot();
  auto snapshot_b = b.createSerializableSnapshot();
  return a.log_id_ == b.log_id_ &&
      testSnapshotsIdentical(*snapshot_a, *snapshot_b);
}

bool CacheHeader::isValid(const CacheHeader& header) {
  // Check invariants (data may be corrupted):

  // Epoch must be valid
  if (header.epoch == EPOCH_INVALID) {
    return false;
  }

  // If disabled, must not include any entries. Leave it inconsistent, i.e.
  // StoredBefore set to MAYBE
  if (header.disabled) {
    return header.buffer_entries == 0 && header.buffer_payload_bytes == 0 &&
        header.first_seen_lng == ESN_INVALID.val() &&
        header.max_seen_esn == ESN_INVALID.val() &&
        header.max_seen_timestamp == 0 && header.head == ESN_INVALID.val() &&
        header.stored == EpochRecordCache::StoredBefore::MAYBE &&
        header.flags == 0;
  }

  // Cannot have more buffer entries than capacity
  if (header.buffer_entries > header.buffer_capacity) {
    return false;
  }

  // Require head > first seen LNG, but it may be head == first seen LNG if
  // 1) we have received no records, or 2) the first seen LNG is ESN_MAX.
  if (header.head < header.first_seen_lng) {
    return false;
  } else if (header.head == header.first_seen_lng &&
             header.first_seen_lng != ESN_INVALID.val() &&
             header.first_seen_lng != ESN_MAX.val()) {
    return false;
  }

  // If we've seen any records, head must not be invalid
  if (header.max_seen_esn != ESN_INVALID.val()) {
    if (header.head == ESN_INVALID.val()) {
      return false;
    }
  }

  // If there are entries in the buffer, we must have seen some esns and head
  // must have been advanced. Also, can't have buffer payload without entries.
  if (header.buffer_entries > 0) {
    if (header.max_seen_esn == ESN_INVALID.val() ||
        header.head == ESN_INVALID.val()) {
      return false;
    }
  } else if (header.buffer_payload_bytes > 0) {
    return false;
  }
  return true;
}

std::string CacheHeader::toString(const CacheHeader& h) {
  std::string prefix = isValid(h) ? "" : "(Invalid) ";

  // clang-format off
  return "[" + prefix +
      "D:" + std::to_string(h.disabled) +
      " E:" + std::to_string(h.epoch.val()) +
      " L:" + (h.tail_optimized == EpochRecordCache::TailOptimized::YES ?
               "y" :
               "n") +
      " S:" + (h.stored == EpochRecordCache::StoredBefore::MAYBE ? "m" : "n") +
      " G:" + std::to_string(h.flags) +
      " H:" + std::to_string(h.head) +
      " F:" + std::to_string(h.first_seen_lng) +
      " M:" + std::to_string(h.max_seen_esn) +
      " T:" + std::to_string(h.max_seen_timestamp) +
      " O:" + std::to_string(h.offset_within_epoch) +
      " B:" + std::to_string(h.buffer_entries) +
      " P:" + std::to_string(h.buffer_payload_bytes) +
      " C:" + std::to_string(h.buffer_capacity) +
      "]";
  // clang-format on
}
}} // namespace facebook::logdevice
