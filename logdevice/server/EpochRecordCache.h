/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <utility>

#include <folly/SharedMutex.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/CircularBuffer.h"
#include "logdevice/common/CopySet.h"
#include "logdevice/common/TailRecord.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/server/RecordCacheDependencies.h"

namespace facebook { namespace logdevice {

/**
 * @file EpochRecordCache is the per-epoch cache component in RecordCache.
 *       It is reponsible for actually storing `unclean' records for one
 *       particular epoch. It is also responsible for keeping track of the
 *       `last known good' lsn for the epoch of the log on this storage node.
 *       EpochRecordCache is being written frequently by many threads, and
 *       read rarely only during log recovery procedures. Currently it is
 *       implemented as a circular buffer protected by a write favorable
 *       high performance lock.
 */

class EpochRecordCacheEntry;
class PayloadHolder;
struct RecordID;

namespace EpochRecordCacheSerializer {
struct EntryHeader;
struct CacheHeader;
class EpochRecordCacheCompare;
} // namespace EpochRecordCacheSerializer

class EpochRecordCache {
 public:
  // Indicates that if there are records with the same epoch that has
  // been stored before on this storage nodes. Used for determining
  // cache consistency.
  enum class StoredBefore { MAYBE, NEVER };

  // if YES, EpochRecordCache will store the value (i.e., payload) of the
  // tail record (i.e., last record evicted from the cache by releases or lng
  // updates). Otherwise, only attribute of the tail record is stored.
  enum class TailOptimized { NO, YES };

  /**
   * Construct a EpochRecordCache
   * @param deps          dependency object for disposing of evicted cache
   *                      entries, MUST outlive the cache and all Entry_s
   *                      (including all snapshots)
   * @param capacity      max number of records that are not confirmed by the
   *                      writer. Usually should be the sliding window size
   *                      of the Sequencer.
   *
   * @param tail_optimized  see doc in TailOptimized
   * @param stored          see doc in StoredBefore
   */
  EpochRecordCache(logid_t log_id,
                   shard_index_t shard,
                   epoch_t epoch,
                   EpochRecordCacheDependencies* deps,
                   size_t capacity,
                   TailOptimized tail_optimized,
                   StoredBefore stored = StoredBefore::MAYBE);

  /**
   * Try to put a record into the cache.
   * see RecordCache::putRecord() for params and return value.
   */
  int putRecord(RecordID rid,
                uint64_t timestamp,
                esn_t lng,
                uint32_t wave_or_recovery_epoch,
                const copyset_t& copyset,
                STORE_flags_t flags,
                std::map<KeyType, std::string>&& keys,
                Slice payload_raw,
                const std::shared_ptr<PayloadHolder>& payload_holder,
                OffsetMap offsets_within_epoch = OffsetMap());

  /**
   * Update the cache when the last known good lsn of the log advances.
   * This is used when storage learned that LNG advances from source other than
   * receving a record to store (e.g, from releases).
   */
  void advanceLNG(esn_t new_lng);

  /**
   * @return  capacity of the cache on construction
   */
  size_t capacity() const {
    return buffer_.size();
  }

  /**
   * @return   true if the cache is for a tail optimized log
   */
  bool isTailOptimized() const {
    return tail_optimized_ == TailOptimized::YES;
  }

  /**
   * @return       true if the cache is in a consistent state and it is safe to
   *               consult the cache as the source of truth.
   *               false if otherwise, which may mean that the cache is disabled
   *               (e.g., because of configuration changes) or not yet
   *               consistent (not enough information).
   * Can be called with locked or unlocked rw_lock_.
   */
  bool isConsistent() const;

  /**
   * @return       Number of records above LNG.
   */
  size_t bufferedRecords() const;

  /**
   * @return       Total size of payloads above LNG.
   */
  size_t bufferedPayloadBytes() const;

  /**
   * @return      return the `last known good' lsn for this epoch on this
   *              storage node.
   *              Note: caller needs to ensure that the cache is consistent
   *              (isConsistent() == true) before calling this function.
   */
  esn_t getLNG() const;
  static esn_t getLNG(esn_t::raw_type head);

  /**
   * @return     return the maximum esn ever cached. ESN_INVALID if the
   *             cache never stored a record. Only valid if the cache is
   *             consistent.
   */
  esn_t getMaxSeenESN() const {
    return esn_t(max_seen_esn_.load());
  }

  /**
   * @return     return the maximum timestamp ever cached. 0 if the
   *             cache never stored a record. Only valid if the cache is
   *             consistent.
   */
  uint64_t getMaxSeenTimestamp() const {
    return max_seen_timestamp_.load();
  }

  /**
   * @return     tail record (i.e., last per-epoch released) record of the
   *             epoch. Invalid record if the cache never stored or never
   *             evicted a record on per-epoch release. The return value is
   *             correct only if the cache is consistent.
   */
  TailRecord getTailRecord() const;

  /**
   * @return  first esn from which the record storage state can be determined by
   *          examining the epoch record cache.
   *
   * Note only valid if the epoch cache is consistent
   */
  esn_t getAuthoritativeRangeBegin() const {
    return getAuthoritativeRangeBegin(tail_record_);
  }

  static esn_t getAuthoritativeRangeBegin(const TailRecord& tail) {
    return tail.isValid() ? lsn_to_esn(tail.header.lsn) : ESN_INVALID;
  }

  /**
   * @return   check if the cache has never stored a record, if the cache is
   *           also consistent at the same time, then the epoch is empty.
   */
  bool neverStored() const {
    return head_.load() == ESN_INVALID.val_;
  }

  /**
   * @return  true if there is currently no record stored in the cache.
   */
  bool empty() const;

  /**
   * @return  true if there is currently no record stored in the cache and
   *          there is no tail record payload stored in the cache. Used during
   *          eviction under memory pressure and shutdown sequence.
   */
  bool emptyWithoutTailPayload() const;

  /**
   * @return    return most recent OffsetMap of amount of data written in given
   *            epoch as seen by given node.
   */
  OffsetMap getOffsetsWithinEpoch() const {
    return offsets_within_epoch_.load();
  }

  /**
   * @param offsets_within_epoch     most recent OffsetMap of amount of data
   *                                 written in given epoch as seen by given
   *                                 node.
   */
  void setOffsetsWithinEpoch(const OffsetMap& offsets_within_epoch) {
    if (offsets_within_epoch.isValid()) {
      offsets_within_epoch_.atomicFetchMax(offsets_within_epoch);
    }
  }

  /**
   * Disable the cache by disallowing further read and writes.
   * All existing cache entries will be disposed of.
   */
  void disableCache();

  //// functions for reading the cache

  /**
   * A snapshot of the cache. Contain copies of all cache entries (or all
   * entries after a given lsn) of the EpochRecordCache at a particular time
   * point. Records gotten from the snapshot are valid as long as the Snapshot
   * object exists. Can outlive the EpochRecordCache that creates the snapshot.
   */
  class Snapshot {
   public:
    class ConstIterator;

    // Represent a cached record.
    // Do not expose EpochRecordCacheEntry directly to users to reduce
    // the complexity of entry life cycle handling. Snapshot holds references
    // to the payload so it is safe to use raw pointers as long as the Snapshot
    // is alive.
    struct Record {
      STORE_flags_t flags;
      uint64_t timestamp;
      esn_t last_known_good;
      uint32_t wave_or_recovery_epoch;
      copyset_t copyset;
      OffsetMap offsets_within_epoch;
      Slice payload_raw;
    };

    // Indicator as to whether or not a snapshot is full (ie covers the whole
    // range of ESNs and may be serialized). Partial snapshots may or may not
    // cover the whole range of ESNs. Therefore they are not allowed to be
    // serialized.
    enum class SnapshotType { FULL, PARTIAL };

    bool isSerializable() const {
      return type_ == SnapshotType::FULL;
    }

    /**
     * Returns the size that the serialized representation of this snapshot
     * would require.
     */
    size_t sizeInLinearBuffer() const;

    /**
     * Write a serialized representation of this snapshot to the given buffer.
     * Returns the size used by the representation in the buffer, or -1 if
     * the buffer size is not large enough to fit it.
     */
    ssize_t toLinearBuffer(char* buffer, size_t size) const;

    /**
     * Creates a Snapshot from linear memory. If result_size is not nullptr, it
     * will be set to the number of bytes that were used to create the Snapshot.
     */
    static std::unique_ptr<Snapshot>
    createFromLinearBuffer(const char* buffer,
                           size_t size,
                           EpochRecordCacheDependencies* deps,
                           size_t* result_size = nullptr);

    // for reading the snapshot randomly
    std::pair<bool, Record> getRecord(esn_t esn) const;

    // for reading the snapshot sequentially
    std::unique_ptr<ConstIterator> createIterator() const;

    // get the effective esn range used to create the snapshot
    std::pair<esn_t, esn_t> getEffectiveRange() const {
      return std::make_pair(start_, until_);
    }

    bool empty() const {
      return entry_map_.empty();
    }

    esn_t getAuthoritativeRangeBegin() const {
      return EpochRecordCache::getAuthoritativeRangeBegin(tail_record_);
    }

    const TailRecord& getTailRecord() const {
      return tail_record_;
    }

    const EpochRecordCacheSerializer::CacheHeader* getHeader() const {
      return header_.get();
    }

   private:
    // private constructor, can be created by EpochRecordCache only
    Snapshot(esn_t start, esn_t until);
    explicit Snapshot(const EpochRecordCacheSerializer::CacheHeader& header,
                      const TailRecord& TailRecord,
                      OffsetMap offsets_within_epoch);

    using InternalMap = std::map<esn_t, std::shared_ptr<EpochRecordCacheEntry>>;

    const esn_t start_;
    const esn_t until_;
    InternalMap entry_map_;
    // Used to create EpochRecordCaches from linear buffers via Snapshots.
    // Is nullptr for PARTIAL snapshots
    std::unique_ptr<EpochRecordCacheSerializer::CacheHeader> header_;
    TailRecord tail_record_;
    OffsetMap offsets_within_epoch_;

    const SnapshotType type_;

    friend class EpochRecordCache;
    friend class ConstIterator;
    friend class EpochRecordCacheSerializer::EpochRecordCacheCompare;

   public:
    // iterator for read-only access
    class ConstIterator {
     public:
      explicit ConstIterator(const Snapshot& snapshot)
          : snapshot_(snapshot), it_(snapshot.entry_map_.begin()) {}

      bool atEnd() const {
        return it_ == snapshot_.entry_map_.end();
      }

      void next() {
        ++it_;
      }

      esn_t getESN() const {
        ld_check(!atEnd());
        return it_->first;
      }

      Record getRecord() const;

      ConstIterator(const ConstIterator&) = delete;
      ConstIterator& operator=(const ConstIterator&) = delete;
      ConstIterator(ConstIterator&&) noexcept = delete;
      ConstIterator& operator=(ConstIterator&&) = delete;

     private:
      const Snapshot& snapshot_;
      Snapshot::InternalMap::const_iterator it_;
    };
  };

  std::unique_ptr<Snapshot>
  createSerializableSnapshot(bool enable_offset_map = false) const;
  std::unique_ptr<Snapshot> createSnapshot(esn_t esn_start = ESN_INVALID,
                                           esn_t esn_end = ESN_MAX) const;

  // get a copy of Entry for a given esn, currently used for testing only
  std::pair<bool, std::shared_ptr<EpochRecordCacheEntry>>
  getEntry(esn_t esn) const;

  /**
   * Adds one row.
   */
  void getDebugInfo(InfoRecordCacheTable& table) const;

  /**
   * Returns the size that representing this EpochRecordCache in serial form
   * would require, or -1 if the cache is disabled.
   */
  ssize_t sizeInLinearBuffer() const;

  ssize_t toLinearBuffer(char* buffer, size_t size) const;

  /**
   * Creates an EpochRecordCache from linear memory. If result_size is not
   * nullptr, it will be set to the number of bytes that were used to create
   * the EpochRecordCache.
   */
  static std::unique_ptr<EpochRecordCache>
  fromLinearBuffer(logid_t log_id,
                   shard_index_t shard,
                   const char* buffer,
                   size_t size,
                   EpochRecordCacheDependencies* deps,
                   size_t* result_size = nullptr);

  static std::unique_ptr<EpochRecordCache>
  createFromSnapshot(logid_t log_id,
                     shard_index_t shard,
                     const EpochRecordCache::Snapshot& snapshot,
                     EpochRecordCacheDependencies* deps);

  epoch_t getEpoch() const {
    return epoch_;
  }

  logid_t getLogId() const {
    return log_id_;
  }

  shard_index_t getShardIndex() const {
    return shard_;
  }

  // destructor calls disableCache() and drains all entries
  ~EpochRecordCache();

 private:
  const logid_t log_id_;
  const shard_index_t shard_;
  const epoch_t epoch_;

  EpochRecordCacheDependencies* const deps_;
  const TailOptimized tail_optimized_;
  const StoredBefore stored_;

  // a write perferrable lock for serialize write operations for the
  // whole data structure
  using FairRWLock = folly::SharedMutexWritePriority;
  mutable FairRWLock rw_lock_;

  // head of the buffer
  std::atomic<esn_t::raw_type> head_{ESN_INVALID.val_};

  // The first `last known good lsn' the cache has ever seen since its creation
  // Used to determine if the cache is in a consistent state if the cache
  // was created with StoredBefore::MAYBE
  std::atomic<esn_t::raw_type> first_seen_lng_{ESN_INVALID.val_};

  // largest esn ever put into the cache, if the cache is not empty and not
  // disabled, the esn must be cached
  std::atomic<esn_t::raw_type> max_seen_esn_{ESN_INVALID.val_};

  // largest timestamp ever put into the cache, if the cache is not empty and
  // not disabled, the timestamp must be cached
  std::atomic<uint64_t> max_seen_timestamp_{0};

  // tail record of the epoch (i.e., last per-epoch released record).
  // for non tail-optimized logs, only record attribute is stored, otherwise
  // payload is included.
  // NOTE: access must be protected by rw_lock_
  TailRecord tail_record_;

  // actual buffer for storing (pointers to) cache entries
  CircularBuffer<std::shared_ptr<EpochRecordCacheEntry>> buffer_;

  // number and total size of payloads in buffer_.
  // Uses the same synchronization (rw_lock_) as buffer_. They are atomic
  // to allow concurrent reading while being modified
  std::atomic<size_t> buffer_entries_{0};
  std::atomic<size_t> buffer_payload_bytes_{0};

  // Most recent value of amount of data written in given epoch as seen by
  // given node. Storage node get it from STORE message which carried this info
  // (offsets sent periodically with some STORE messages).
  AtomicOffsetMap offsets_within_epoch_;

  // true if cache is disabled
  std::atomic<bool> disabled_{false};

  /// helper functions
  // @return   right end of the buffer
  esn_t maxESNToAccept() const {
    const esn_t::raw_type head = head_.load();
    return (head < ESN_MAX.val_ - capacity() + 1 ? esn_t(head + capacity() - 1)
                                                 : ESN_MAX);
  }

  // @return  index in buffer_ for @param esn
  //          esn must fit inside the buffer
  size_t getIndex(esn_t esn) const {
    const esn_t::raw_type head = head_.load();
    ld_check(esn.val_ >= head);
    ld_assert(esn <= maxESNToAccept());
    return esn.val_ - head;
  }

  static esn_t next_esn(esn_t esn) {
    return esn == ESN_MAX ? ESN_MAX : esn_t(esn.val_ + 1);
  }

  static esn_t prev_esn(esn_t esn) {
    return esn == ESN_INVALID ? ESN_INVALID : esn_t(esn.val_ - 1);
  }

  // internal helper function for advancing LNG and evicting entries.
  // Note: must be called with write lock of rw_lock_ held
  void advanceLNGImpl(esn_t current_head, esn_t lng, ReleasedVector& disposal);

  // update tail_record_ with the most recently evicted tail_, called
  // with write lock of rw_lock_ held
  void updateTailRecord(const std::shared_ptr<EpochRecordCacheEntry>& tail);

  // amend record metadata for an exisiting epoch record cache entry with a new
  // amend entry. Existig payload won't be changed.
  // Note: must be called with write lock of rw_lock_ held
  void amendExistingEntry(EpochRecordCacheEntry* existing_entry,
                          const EpochRecordCacheEntry* amend_entry);

  // Update stats (buffer_entries_ and buffer_payload_bytes_) when entry is
  // added or removed from buffer_.
  void noteEntryAdded(const EpochRecordCacheEntry& entry);
  void noteEntryRemoved(const EpochRecordCacheEntry& entry);

  /**
   * create a snapshot of the cache for reading, complexity is
   * O(N) where N is the number of entries in the cache to be included in
   * the snapshot.
   *
   * Only snapshots from ESN_INVALID to ESN_MAX may be written to linear buffer
   * or used to create an EpochRecordCache.
   *
   * @param esn_start     starting esn of the snapshot, entries smaller than
   *                      esn will not be included in the snapshot
   *
   * @param esn_end       maximum esn to be included in the snapshot
   *
   * @param enable_offset_map flag from settings that enables OffsetMap feature
   */
  std::unique_ptr<Snapshot>
  createSnapshotImpl(Snapshot::SnapshotType type,
                     esn_t esn_start = ESN_INVALID,
                     esn_t esn_end = ESN_MAX,
                     bool enable_offset_map = false) const;

  friend class EpochRecordCacheSerializer::EpochRecordCacheCompare;
};

// Used for serializing and deserializing an EpochRecordCache and its
// components, so that it can be read from and written to disk
namespace EpochRecordCacheSerializer {

// Important: Bump RecordCacheSerializer::CURRENT_VERSION when adding or
// removing fields!

struct EntryHeader {
  STORE_flags_t flags;
  uint64_t timestamp;
  esn_t last_known_good;
  uint32_t wave_or_recovery_epoch;
  copyset_size_t copyset_size;
  // TODO(T35659884) : Remove offset_within_epoch. Add a new protocol in
  // toLinearBuffer and fromLinearBuffer in EpochRecordCacheEntry.cpp
  uint64_t offset_within_epoch;
  size_t key_size[to_integral(KeyType::MAX)];
  size_t payload_size;
} __attribute__((__packed__));

struct CacheHeader {
  static bool isValid(const CacheHeader& header);
  static std::string toString(const CacheHeader& header);

  using flags_t = uint16_t;

  bool disabled;
  epoch_t epoch;
  EpochRecordCache::TailOptimized tail_optimized;
  EpochRecordCache::StoredBefore stored;
  flags_t flags;
  esn_t::raw_type head;
  esn_t::raw_type first_seen_lng;
  esn_t::raw_type max_seen_esn;
  uint64_t max_seen_timestamp;
  // TODO(T35659884) : Remove offset_within_epoch. Add a new protocol in
  // toLinearBuffer and fromLinearBuffer in EpochRecordCache.cpp
  uint64_t offset_within_epoch;
  size_t buffer_entries;
  size_t buffer_payload_bytes;
  size_t buffer_capacity;

  // the cache has a valid tail record
  static const flags_t VALID_TAIL_RECORD = 1u << 0; //=1

  // OffsetMap is used to represent offsets_within_epoch
  static const flags_t SUPPORT_OFFSET_MAP = 1u << 1; //=2

} __attribute__((__packed__));

// Used in RecordCacheTest to verify equivalence of repopulated objects
class EpochRecordCacheCompare {
 public:
  static bool testEntriesIdentical(const EpochRecordCacheEntry& a,
                                   const EpochRecordCacheEntry& b);
  static bool testSnapshotsIdentical(const EpochRecordCache::Snapshot& a,
                                     const EpochRecordCache::Snapshot& b);
  static bool testEpochRecordCachesIdentical(const EpochRecordCache& a,
                                             const EpochRecordCache& b);
};
} // namespace EpochRecordCacheSerializer
}} // namespace facebook::logdevice
