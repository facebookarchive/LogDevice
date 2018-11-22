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
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/CopySet.h"
#include "logdevice/common/RandomAccessQueue.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

/**
 * @file RecordCache caches records for a log that are not yet confirmed as
 *       fully stored by the sequencer. Its entries are stored records whose esn
 *       are greater than the maximum seen LNG (last known good esn) of the
 *       epoch so far on the storage node. For such reason, records are
 *       maintained and managed by a per-epoch sub-cache EpochRecordCache, and
 *       RecordCache maintains a window of EpochRecordCache objects for each
 *       _unclean_ epoch for the log on the storage node. Records of cleaned
 *       epochs, on the other hand, are never used for log recovery afterwards
 *       and do not need to be cached. Therefore, as the last clean epoch
 *       advances on the storage node (e.g., after purging), EpochRecordCache
 *       objects for these epochs are also dropped. Providing an in-memory cache
 *       for unclean records improves the efficiency of the log recovery
 *       procedure as it no longer needs to access the local log store and
 *       perform potentially expensive seek operations during the Sealing and
 *       Digesting phases.
 */

class EpochRecordCache;
class PayloadHolder;
class RecordCacheDependencies;
class StatsHolder;
struct RecordID;

namespace RecordCacheSerializer {
class RecordCacheCompare;
}

class RecordCache {
 public:
  explicit RecordCache(logid_t log_id,
                       shard_index_t shard,
                       RecordCacheDependencies* deps);

  /**
   * Insert a record to the record cache.
   * @param rid               RecordID
   * @param timestamp         timestamp of the record as in STORE_Header
   * @param lng               last known good esn in the STORE_Header for
   *                          the record
   *
   * @param wave_or_recovery_epoch   wave of the record as in STORE_Header
   *                                 OR the seal epoch of log recovery if
   *                                 FLAG_WRITTEN_BY_RECOVERY is set in
   *                                 flags
   *
   * @param copyset           copyset of the record as in STORE_Header
   * @param flags             store flags of the record
   * @param payload_raw       Slice for the linearized payload
   * @param paylaod_holder    PayloadHolder object that owns the allocated
   *                          payload
   * @param offsets_within_epoch  offsets stored in the same epoch before
   *                              current record as in STORE_Header. Present
   *                              only for part of periodic records. Default
   *                              value is invalid.
   *
   * @return                  0 if the record is successfully put into the
   *                            cache, and the cache now holds a reference
   *                            to the payload until the record is evicted.
   *                         -1 if the record is not cached. It might be
   *                            that the record will not be used for recovery,
   *                            or an error occurred.
   */
  int putRecord(RecordID rid,
                uint64_t timestamp,
                esn_t lng,
                uint32_t wave_or_recovery_epoch,
                const copyset_t& copyset,
                STORE_flags_t flags,
                std::map<KeyType, std::string>&& optional_keys,
                Slice payload_raw,
                const std::shared_ptr<PayloadHolder>& payload_holder,
                OffsetMap offsets_within_epoch = OffsetMap());

  /**
   * Result of the cache lookup.
   * HIT:           The cache has the requested record or per-epoch cache and
   *                the cached entry is returned
   *
   * NO_RECORD:     There is no record for the requested lsn/epoch
   *
   * MISS:          The cache does not know or may not be correct about the
   *                lsn or epoch requested. It may be disabled, or not yet
   *                consistent. Reader should read from local log store
   *                instead.
   */
  enum class Result { HIT, NO_RECORD, MISS };

  /**
   * For a given epoch, look up the EpochRecordCache object which maintains
   * last known good esn and all unclean records for the epoch.
   *
   * @return       a pair of Result indicating the outcome of the look up,
   *               and a shared_ptr to the EpochRecordCache object (nullptr
   *               on NO_RECORD and MISS).
   */
  std::pair<Result, std::shared_ptr<EpochRecordCache>>
  getEpochRecordCache(epoch_t epoch) const;

  /**
   * Called when the last clean epoch metadata is updated on the storage node
   * so that these per-epoch caches of already cleaned epochs can be dropped.
   *
   * @param lce   the latest last clean epoch for the log on the storage node
   */
  void onLastCleanEpochAdvanced(epoch_t lce);

  /**
   * Called when the storage node receives a release message. Try to update the
   * cache to evict records that are known to be fully replicated.
   */
  void onRelease(lsn_t release_lsn);

  /**
   * Attempt to update last_non_authoritative_epoch_ from the soft seal metadata
   * if it is not intialized.
   */
  void updateLastNonAuthoritativeEpoch(logid_t logid);

  /**
   * Called only if it is certain that there is not a single record of log_id_
   * existed in the local log store. last_non_authoritative_epoch_ will be set
   * to EPOCH_INVALID.
   *
   * Currently used in tests.
   */
  void neverStored();

  /**
   * Reset the EpochRecordCache for an epoch, evicting all existing records
   * cached for this epoch. If the epoch cache does not exist or is empty at
   * the time of the call, the operation is a no-op. Otherwise, evict all
   * entries and reset its internal state as if the epoch cache was newly
   * created with StoreBefore::MAYBE. As a result, the epoch cache becomes
   * inconsistent after eviction. However, unlike disableCache(), the epoch
   * cache can still take further writes and can become consistent again.
   *
   * Note: eviction only affect subsequent calls to get the EpochRecordCache
   * for the epoch. Existing reference to the EpochRecordCache (i.e.,
   * std::shared_ptr<EpochRecordCache> through getEpochRecordCache()) will
   * remain untouched and consistent. For the same reason, entries in the cache
   * may not be immediately freed upon eviction but until the reference count
   * of the existing EpochRecordCache dropped to zero.
   */
  void evictResetEpoch(epoch_t epoch);

  /**
   * like evictResetEpoch() but evict all existing epochs cached.
   */
  void evictResetAllEpochs();

  /**
   * @return   the size estimate of the all record payloads currently stored
   *           in the record cache
   */
  size_t getPayloadSizeEstimate() const;

  /**
   * Shutdown the cache, clearing all entries in all epochs. Caller should
   * ensure that cache is not used after shutdown() is called.
   */
  void shutdown();

  /**
   * Adds a row for each epoch in cache.
   */
  void getDebugInfo(InfoRecordCacheTable& table) const;

  /**
   * Returns the size that the serial representation of this RecordCache would
   * require, or -1 if the cache is disabled.
   */
  ssize_t sizeInLinearBuffer() const;

  /**
   * Write a linear representation of this RecordCache to the given buffer.
   * Returns the resulting size, or -1 if the buffer size was too small.
   */
  ssize_t toLinearBuffer(char* buffer, size_t size) const;

  /**
   * Construct a RecordCache from a linearized representation in the given
   * buffer. Will return nullptr if it failed.
   */
  static std::unique_ptr<RecordCache>
  fromLinearBuffer(const char* buffer,
                   size_t size,
                   RecordCacheDependencies* deps,
                   shard_index_t shard);

  // NOTE Should only be used for deserialization of record caches, left public
  // only for testing. ONLY to be called when head != EPOCH_INVALID. If
  // head == EPOCH_INVALID, the RecordCache should be like an empty one, but to
  // achieve that you should not use this constructor; rather, use one which
  // leaves epoch_caches_ uninitialized.
  explicit RecordCache(logid_t log_id,
                       shard_index_t shard,
                       RecordCacheDependencies* deps,
                       epoch_t::raw_type head_epoch_cached,
                       epoch_t::raw_type next_epoch_to_cache,
                       epoch_t::raw_type last_nonauthoritative_epoch);

 private:
  const logid_t log_id_;

  const shard_index_t shard_;

  RecordCacheDependencies* const deps_;

  // Lock to serialize the modification to the per-epoch cache.
  // Locking is needed only when EpochRecordCache entries are created
  // (i.e., encountered a store with new epoch) and dropped (i.e., on
  // last clean epoch advanced). The vast majority operations on both write
  // and read to existing epochs in cache do not involve locking this mutex.
  mutable std::mutex epoch_cache_lock_;

  // storage for EpochRecordCache objects
  RandomAccessQueue<std::shared_ptr<EpochRecordCache>> epoch_caches_;

  // first epoch that is cached, can only be increased but never decreased.
  // EPOCH_INVALID value means that the cache is not stored a record yet.
  std::atomic<epoch_t::raw_type> head_epoch_cached_{EPOCH_INVALID.val_};

  // next epoch value yet to be cached, can only be increased but never
  // decreased. Unclean epochs kept in the cache are in the range of
  // [head_epoch_cached_, next_epoch_to_cache_). However, if
  // head_epoch_cached_ == next_epoch_to_cache_, the cache is empty and does
  // not have any unclean epochs cached.
  std::atomic<epoch_t::raw_type> next_epoch_to_cache_{EPOCH_INVALID.val_};

  // last epoch for the log in which we do not know for sure whether a record
  // has been stored persistently in local log store before creation of the
  // cache. In other words, for epochs in
  // [last_nonauthoritative_epoch_ + 1, EPOCH_MAX], it is guaranteed that
  // no records were stored before the cache was created.
  // used to determine the initial state of the EpochRecordCache as well as
  // NO_RECORD vs MISS. Initialized to EPOCH_MAX so that all epochs are treated
  // as `maybe stored'. Updated from soft seal values.
  std::atomic<epoch_t::raw_type> last_nonauthoritative_epoch_{EPOCH_MAX.val_};

  // indicate the record cache is shutdown and shouldn't take new writes
  std::atomic<bool> shutdown_{false};

  // actual implementation for evictResetEpoch(), must be called under
  // mutex_
  void evictResetEpochImpl(epoch_t epoch);

  // helper utility to call @param func on all epoch caches in the current
  // snapshot
  template <typename Func>
  void accessAllEpochCaches(const Func& func);

  template <typename Func>
  void accessAllEpochCaches(const Func& func) const;

  // check if the cache is empty, should be called with mutex_ held
  bool cacheEmpty() const {
    return head_epoch_cached_.load() == next_epoch_to_cache_.load();
  }

  static epoch_t next_epoch(epoch_t epoch) {
    return epoch == EPOCH_MAX ? EPOCH_MAX : epoch_t(epoch.val_ + 1);
  }

  friend class RecordCacheSerializer::RecordCacheCompare;
};

template <typename Func>
void RecordCache::accessAllEpochCaches(const Func& func) {
  auto epoch_caches = epoch_caches_.getVersion();
  if (epoch_caches == nullptr) {
    return;
  }
  for (uint64_t epoch = epoch_caches->firstID(); epoch < epoch_caches->nextID();
       ++epoch) {
    auto epoch_cache = epoch_caches->get(epoch);
    if (epoch_cache != nullptr) {
      func(*epoch_cache);
    }
  }
}

template <typename Func>
void RecordCache::accessAllEpochCaches(const Func& func) const {
  const_cast<RecordCache*>(this)->accessAllEpochCaches(func);
}

namespace RecordCacheSerializer {

using Version = uint32_t;

constexpr uint16_t MAGIC{0xCACE};
// version 3: storing optional keys.
static_assert(to_integral(KeyType::MAX) == 2,
              "Update CURRENT_VERSION if you add/remove keys.");
constexpr Version CURRENT_VERSION{3};

// Header for persisting and repopulating RecordCaches
struct CacheHeader {
  static bool isValid(const CacheHeader& header);
  static std::string toString(const CacheHeader& h);

  uint16_t magic;
  Version version;
  logid_t log_id;
  size_t num_non_empty_epoch_caches;
  epoch_t::raw_type head_epoch_cached;
  epoch_t::raw_type next_epoch_to_cache;
  epoch_t::raw_type last_nonauthoritative_epoch;
} __attribute__((__packed__));

// Used in RecordCacheTest to verify equivalence of repopulated RecordCaches
class RecordCacheCompare {
 public:
  // NOTE ONLY for use in tests, not threadsafe!
  static bool testRecordCachesIdentical(const RecordCache& a,
                                        const RecordCache& b);
};
} // namespace RecordCacheSerializer
}} // namespace facebook::logdevice
