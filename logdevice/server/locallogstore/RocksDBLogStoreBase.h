/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <atomic>

#include <boost/noncopyable.hpp>
#include <folly/Synchronized.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/statistics.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBColumnFamily.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreConfig.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

class RocksDBIterator;
class RocksDBMemTableRepFactory;
class RocksDBWriter;
class IOTracing;

/**
 * @file Base class for RocksDB-backed local log stores.
 * Exposes RocksDB database and provides default implementation for
 * acceptingWrites().
 */

class RocksDBLogStoreBase : public LocalLogStore {
 public:
  // We keep a key-value pair in RocksDB with key ".schema_version" and
  // value "2", and refuse to open a DB if the value is different.
  // TODO (#39174994):
  //   We're migrating from the old key "schema_version" to the new key
  //   ".schema_version", because the old key clashes with SealMetadata.
  //   Just the key changes, not the actual schema.
  static const char* const OLD_SCHEMA_VERSION_KEY;
  static const char* const NEW_SCHEMA_VERSION_KEY;

  ~RocksDBLogStoreBase() override;

  /**
   * @return  boolean indicating if DB can be written currently
   */
  Status acceptingWrites() const override {
    if (fail_safe_mode_.load()) {
      return E::DISABLED;
    } else if (!space_for_writes_.load()) {
      return E::NOSPC;
    } else if (low_watermark_crossed_.load()) {
      return E::LOW_ON_SPC;
    } else {
      return E::OK;
    }
  }

  int sync(Durability durability) override;
  FlushToken maxFlushToken() const override;
  FlushToken flushedUpThrough() const override;
  SteadyTimestamp oldestUnflushedDataTimestamp() const override;

  void stallLowPriWrite() override;

  StatsHolder* getStatsHolder() const {
    return stats_;
  }

  /**
   * @return rocksdb::DB instance owned by this class.
   */
  rocksdb::DB& getDB() const {
    return *db_;
  }

  /**
   * @return the column family that contains StoreMetadata, LogMetadata etc.
   */
  virtual rocksdb::ColumnFamilyHandle* getMetadataCFHandle() const {
    return db_->DefaultColumnFamily();
  }

  /**
   * Returns a new rocksdb::Iterator wrapped in RocksDBIterator.
   * Use this instead of calling getDB()->NewIterator() directly.
   */
  RocksDBIterator newIterator(rocksdb::ReadOptions ropt,
                              rocksdb::ColumnFamilyHandle* cf) const;

  void onStorageThreadStarted() override {
    // Make RocksDB's PerfContext track time stats.
    rocksdb::SetPerfLevel(rocksdb::kEnableTime);
  }

  /**
   * @return value of the specified ticker or 0 if statistics are disabled.
   */
  uint64_t getStatsTickerCount(rocksdb::Tickers ticker_type) const {
    if (!statistics_) {
      return 0;
    }

    return statistics_->getTickerCount(ticker_type);
  }

  /**
   * @return  various RocksDB stats in a human-readable form.
   */
  virtual std::string getStats() const {
    std::string res;
    db_->GetProperty("rocksdb.stats", &res);

    if (statistics_) {
      res += "\n";
      res += statistics_->ToString();
    }

    return res;
  }

  struct RocksDBMemTableStats {
    // Size of current active and unflushed memtable.
    uint64_t active_memtable_size{0};
    // Size of immutable memtables scheduled for flush.
    uint64_t immutable_memtable_size{0};
    // Size of immutable memtables pinned in memory.
    uint64_t pinned_memtable_size{0};
  };

  struct WriteBufStats {
    Status err{E::OK};
    // Memory held in buffers that are active and accepting writes.
    uint64_t active_memory_usage{0};
    // Memory held in buffers that are immutable and yet to persist.
    uint64_t memory_being_flushed{0};
    // Buffers that are immutable but pinned and cannot release memory right
    // away.
    uint64_t pinned_buffer_usage{0};
    // How many column families have a nonempty active memtable.
    uint64_t num_active_memtables{0};
  };

  // Fetches memtable memory usage for a column family
  RocksDBMemTableStats getMemTableStats(rocksdb::ColumnFamilyHandle* cf) {
    RocksDBMemTableStats stats;
    db_->GetIntProperty(cf,
                        rocksdb::DB::Properties::kCurSizeActiveMemTable,
                        &stats.active_memtable_size);
    uint64_t aggregated_flushed_stat = 0;
    db_->GetIntProperty(cf,
                        rocksdb::DB::Properties::kCurSizeAllMemTables,
                        &aggregated_flushed_stat);
    stats.immutable_memtable_size =
        aggregated_flushed_stat > stats.active_memtable_size
        ? aggregated_flushed_stat - stats.active_memtable_size
        : 0;
    uint64_t aggregated_memtable_stat = 0;
    db_->GetIntProperty(cf,
                        rocksdb::DB::Properties::kSizeAllMemTables,
                        &aggregated_memtable_stat);
    stats.pinned_memtable_size =
        aggregated_memtable_stat > aggregated_flushed_stat
        ? aggregated_memtable_stat - aggregated_flushed_stat
        : 0;
    return stats;
  }

  int getShardIdx() const override {
    return shard_idx_;
  }

  // fsync the rocksdb WAL so that it's contents are guaranteed to
  // survive a system crash.
  int syncWAL();

  FlushToken maxWALSyncToken() const override;
  FlushToken walSyncedUpThrough() const override;

  // Flush memtables for all column families. If wait is true, block until
  // the flushes have completed. This is currently used by sync() to persist
  // data when some writes may have been issued that bypassed the WAL.
  virtual int flushAllMemtables(bool wait = true);

  virtual void onMemTableWindowUpdated() {}

  /**
   * @return  path of RocksDB directory
   */
  std::string getDBPath() const {
    return db_path_;
  }

  RocksDBLogStoreConfig& getRocksDBLogStoreConfig() {
    return rocksdb_config_;
  }
  const RocksDBLogStoreConfig& getRocksDBLogStoreConfig() const {
    return rocksdb_config_;
  }

  std::shared_ptr<const RocksDBSettings> getSettings() const {
    return rocksdb_config_.getRocksDBSettings();
  }

  std::shared_ptr<const RebuildingSettings> getRebuildingSettings() const {
    return rocksdb_config_.getRebuildingSettings();
  }

  bool hasEnoughSpaceForWrites() const {
    return space_for_writes_.load();
  }

  void setHasEnoughSpaceForWrites(bool space_for_writes) {
    space_for_writes_.store(space_for_writes);
  }

  void setLowWatermarkForWrites(bool low_wm_crossed) {
    low_watermark_crossed_.store(low_wm_crossed);
  }

  bool crossedLowWatermark() const {
    return low_watermark_crossed_.load();
  }

  static rocksdb::Status FaultTypeToStatus(IOFaultInjection::FaultType ft) {
    switch (ft) {
      case IOFaultInjection::FaultType::IO_ERROR:
        return rocksdb::Status::IOError("Injected Error");
      case IOFaultInjection::FaultType::CORRUPTION:
        return rocksdb::Status::Corruption("Injected Error");
      case IOFaultInjection::FaultType::LATENCY:
        // Latency FaultType is not supported at this level.
        ld_check(false);
      default:
        return rocksdb::Status::OK();
    }
  }

  // For reading data records use getReadOptionsSinglePrefix() whenever
  // possible. getDefaultReadOptions() will prevent the use of bloom filters.
  static rocksdb::ReadOptions getDefaultReadOptions() {
    rocksdb::ReadOptions options;
    options.total_order_seek = true;
    options.background_purge_on_iterator_cleanup = true;
    return options;
  }

  // Options with rocksdb::ReadOptions::total_order_seek = false.
  //
  // An iterator created with these options will see keys with the same
  // prefix as the key of the last Seek(), but it may or may not see keys with
  // other prefixes. "Prefix" usually means log (see RocksDBLogStoreConfig for
  // a precise definition). I.e. these options confine the iterator to a single
  // log (except that sometimes they don't; see rule below).
  //
  // If you need the iterator to move across multiple logs/prefixes,
  // use getDefaultReadOptions().
  //
  // This option is not always easy to use correctly. Use this rule: if such
  // iterator ever ends up on a key with prefix (log) different than the prefix
  // you last seeked to, consider the iterator invalid. I.e. don't do
  // Next()/Prev() and don't trust the key()/value() until you do
  // another Seek*(). In particular, to seek to the last key with a given prefix
  // use SeekForPrev(), not Seek()+Prev().
  //
  // More detailed caveats (optional reading):
  //  * If you seek an iterator to key x, and the iterator ends up pointing to
  //    key y, normally that would mean that y is the smallest key >= x.
  //    But with these options this is not always the case:
  //    if prefix(y) != prefix(x), there may be keys between x and y;
  //    however, it's guaranteed that none of these keys have same prefix as x.
  //  * Continuing the previous example, if you do a Prev() after the Seek(x),
  //    you may end up on a key that's greater than x! I.e. some keys could be
  //    invisible to the Seek() but visible to the Prev().
  //  * After such Prev(), iterator may also end up !Valid() or on a key with
  //    prefix smaller than prefix(x), even if there actually exist records
  //    smaller than x with the same prefix as x.
  static rocksdb::ReadOptions getReadOptionsSinglePrefix() {
    rocksdb::ReadOptions options;
    options.background_purge_on_iterator_cleanup = true;
    return options;
  }

  // True if an error has occurred which indicates partial loss of
  // access or corruption of the database. Reads can still be attempted,
  // but writes will be denied.
  bool inFailSafeMode() const {
    return fail_safe_mode_.load();
  }

  // Transition into fail-safe mode.
  bool enterFailSafeMode(const char* context,
                         const char* error_string) override {
    if (!fail_safe_mode_.exchange(true)) {
      ld_error("%s failed, entering fail safe mode; error: %s",
               context,
               error_string);
      PER_SHARD_STAT_INCR(getStatsHolder(), failed_safe_log_stores, shard_idx_);
      return true;
    } else {
      return false;
    }
  }

  // If status is an error, count it in stats.
  // No-op if status is ok or incomplete.
  // Convention: if you directly call a rocksdb method that returns a Status,
  // you need to call noteRocksDBStatus() or enterFailSafeModeIfFailed() with
  // the result, unless it's ok() or IsIncomplete(). If you call a logdevice
  // function that returns rocksdb::Status, don't call noteRocksDBStatus().
  void noteRocksDBStatus(const rocksdb::Status& s, const char* context) const {
    const_cast<RocksDBLogStoreBase*>(this)->gotRocksDBStatusImpl(
        s, false, context);
  }

  // Similar to noteRocksDBStatus() but if status is some permanent-looking
  // error (IO error or corruption) enters fail-safe mode.
  void enterFailSafeIfFailed(const rocksdb::Status& s,
                             const char* context) const {
    gotRocksDBStatusImpl(s, true, context);
  }

  // Returns a ReadOptions struct to be passed to RocksDB.
  // @param single_log  if true, total_order_seek will be set to false,
  //                    i.e. the iterator will be confined to a single log.
  // @param upper_bound if non-nullptr and non-empty, will be used as
  //                    iterate_upper_bound. The ReadOptions will have a pointer
  //                    to the Slice, so the provided Slice must outlive the
  //                    any iterators created with these ReadOptions.
  static rocksdb::ReadOptions
  translateReadOptions(const LocalLogStore::ReadOptions& opts,
                       bool single_log,
                       rocksdb::Slice* upper_bound);

  // Default implementation of metadata operations. Thin wrappers around
  // RocksDBWriter methods.
  // The separation between RocksDBWriter and RocksDBLogStoreBase is unclear;
  // it would probably make sense to merge the two.

  int readLogMetadata(logid_t log_id, LogMetadata* metadata) override;
  int readStoreMetadata(StoreMetadata* metadata) override;
  int readPerEpochLogMetadata(logid_t log_id,
                              epoch_t epoch,
                              PerEpochLogMetadata* metadata,
                              bool find_last_available = false,
                              bool allow_blocking_io = true) const override;

  int writeLogMetadata(
      logid_t log_id,
      const LogMetadata& metadata,
      const WriteOptions& write_options = WriteOptions()) override;
  int writeStoreMetadata(
      const StoreMetadata& metadata,
      const WriteOptions& write_options = WriteOptions()) override;

  int updateLogMetadata(
      logid_t log_id,
      ComparableLogMetadata& metadata,
      const WriteOptions& write_options = WriteOptions()) override;
  int updatePerEpochLogMetadata(
      logid_t log_id,
      epoch_t epoch,
      PerEpochLogMetadata& metadata,
      LocalLogStore::SealPreemption seal_preempt,
      const WriteOptions& write_options = WriteOptions()) override;

  int deleteStoreMetadata(
      const StoreMetadataType type,
      const WriteOptions& write_options = WriteOptions()) override;
  int deleteLogMetadata(
      logid_t first_log_id,
      logid_t last_log_id,
      const LogMetadataType type,
      const WriteOptions& write_options = WriteOptions()) override;
  int deletePerEpochLogMetadata(
      logid_t log_id,
      epoch_t epoch,
      const PerEpochLogMetadataType type,
      const WriteOptions& write_options = WriteOptions()) override;

  virtual void onSettingsUpdated(
      const std::shared_ptr<const RocksDBSettings> /* unused */) {}

  static size_t getIOBytesUnnormalized() {
    return rocksdb::get_perf_context()->block_read_byte;
  }

  // Get column family holder shared ptr. Returned instance could be
  // freed from other thread as part of column family drop or during shutdown
  // hence it is necessary to check validity of ptr before using. This method
  // returns nullptr if the column family was not found.
  RocksDBCFPtr getColumnFamilyPtr(uint32_t column_family_id);

  // Adjusts write throttle state if total size of memtables is too big.
  void throttleIOIfNeeded(WriteBufStats buf_stats, uint64_t memory_limit);

  WriteThrottleState getWriteThrottleState() override {
    return write_throttle_state_.load();
  }

  // A wrapper around rocksdb::DB::Write() which also updates stats and injects
  // IO errors if needed. Subclasses can override it to add some hooks to all
  // rocksdb writes.
  virtual rocksdb::Status writeBatch(const rocksdb::WriteOptions& options,
                                     rocksdb::WriteBatch* batch);

 protected:
  /**
   * Assumes ownership of the raw rocksdb::DB pointer.
   */
  RocksDBLogStoreBase(uint32_t shard_idx,
                      uint32_t num_shards,
                      const std::string& path,
                      RocksDBLogStoreConfig rocksdb_config,
                      StatsHolder* stats_holder,
                      IOTracing* io_tracing);

  /**
   * Verifies that the schema version entry matches the code version.  If this
   * is a brand new database, writes the schema version entry.
   */
  int checkSchemaVersion(rocksdb::DB* db,
                         rocksdb::ColumnFamilyHandle* cf,
                         int expected) const {
    std::string expected_str = std::to_string(expected);
    std::set<std::string> keys_found;
    for (const char* key : {OLD_SCHEMA_VERSION_KEY, NEW_SCHEMA_VERSION_KEY}) {
      rocksdb::Slice key_slice(key, strlen(key));
      std::string value;
      rocksdb::Status status =
          db->Get(getDefaultReadOptions(), cf, key_slice, &value);
      if (status.ok()) {
        if (value != expected_str) {
          ld_error("Schema version mismatch: code version is %d, database "
                   "version is \"%s\" (key: \"%s\").",
                   expected,
                   value.c_str(),
                   key);
          return -1;
        }
        keys_found.insert(key);
      } else if (!status.IsNotFound()) {
        ld_error("Error reading schema version from database: %s",
                 status.ToString().c_str());
        noteRocksDBStatus(status, "Get() (schema version)");
        return -1;
      }
    }

    if (!keys_found.empty()) {
      // Schema found and matches.
      return 0;
    }

    // Schema version key not found.  Check if the database is empty ...
    int rv = isCFEmpty(cf);
    if (rv != 1) {
      return -1;
    }

    // This is a new database with no contents. Write the schema version.
    rocksdb::Slice key_slice(
        OLD_SCHEMA_VERSION_KEY, strlen(OLD_SCHEMA_VERSION_KEY));
    rocksdb::Status status =
        db->Put(rocksdb::WriteOptions(),
                cf,
                key_slice,
                rocksdb::Slice(expected_str.data(), expected_str.size()));
    if (!status.ok()) {
      ld_error("Error writing schema version to database");
      return -1;
    }
    return 0;
  }

  // Checks that there are no keys in CF, except, maybe, "schema_version" key.
  int isCFEmpty(rocksdb::ColumnFamilyHandle* cf) const;

  uint64_t getVersion() const override {
    uint64_t version;
    if (!db_->GetIntProperty(
            "rocksdb.current-super-version-number", &version)) {
      return 0;
    }
    return version;
  }

  int readAllLogSnapshotBlobsImpl(LogSnapshotBlobType snapshots_type,
                                  LogSnapshotBlobCallback callback,
                                  rocksdb::ColumnFamilyHandle* snapshots_cf);

  void gotRocksDBStatusImpl(const rocksdb::Status& s,
                            bool enter_fail_safe_if_bad,
                            const char* context) const {
    if (s.ok() || s.IsIncomplete() || s.IsNotFound() ||
        s.IsShutdownInProgress()) {
      return;
    }
    if (s.IsIOError()) {
      PER_SHARD_STAT_INCR(getStatsHolder(), disk_io_errors, shard_idx_);
    } else if (s.IsCorruption()) {
      PER_SHARD_STAT_INCR(getStatsHolder(), corruption_errors, shard_idx_);
    } else {
      enter_fail_safe_if_bad = false;
    }
    if (enter_fail_safe_if_bad && !fail_safe_mode_.exchange(true)) {
      ld_error("Got rocksdb error in %s: %s. Entering fail-safe mode.",
               context,
               s.ToString().c_str());
      PER_SHARD_STAT_INCR(getStatsHolder(), failed_safe_log_stores, shard_idx_);
    } else if (enter_fail_safe_if_bad || fail_safe_mode_.load()) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      2,
                      "Got rocksdb error in %s: %s. Already in fail-safe mode.",
                      context,
                      s.ToString().c_str());
    } else {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          2,
          "Got rocksdb error in %s: %s. Not entering fail-safe mode.",
          context,
          s.ToString().c_str());
    }
  }

  void disableWriteStalling() override;

  // Called from throttleIOIfNeeded(). If you're overriding
  // subclassSuggestedThrottleState(), you must call throttleIOIfNeeded()
  // periodically.
  virtual WriteThrottleState subclassSuggestedThrottleState() {
    return WriteThrottleState::NONE;
  }

  std::unique_ptr<rocksdb::DB> db_;

  // Index of this shard and how many shards this node has in total.
  // Used for logging, stats, and converting limits from per-node to per-shard.
  uint32_t shard_idx_;
  uint32_t num_shards_;

  // path to the rocksdb directory
  const std::string db_path_;

  // object used to perform writes to the db
  std::unique_ptr<RocksDBWriter> writer_;

  // pointer to the stats holder
  StatsHolder* stats_;

  // flag that indidates if there is enough space to take writes
  // Currently only set by the monitoring thread
  std::atomic<bool> space_for_writes_{true};

  // statistics object used by RocksDB
  std::shared_ptr<rocksdb::Statistics> statistics_;

  // true if store has failed such that we will not attempt writes.
  mutable std::atomic<bool> fail_safe_mode_{false};

  // True if we're low on space.
  std::atomic<bool> low_watermark_crossed_{false};

  RocksDBLogStoreConfig rocksdb_config_;

  // Contains references to all the column family handles within this
  // rocksdb instance.
  using RocksDBColumnFamilyMap = std::unordered_map<uint32_t, RocksDBCFPtr>;
  folly::Synchronized<RocksDBColumnFamilyMap> cf_accessor_;

 private:
  // Write stalling stuff.

  std::shared_ptr<RocksDBMemTableRepFactory> mtr_factory_;

  // Things related to stalling/rejecting all/some writes when we're overloaded,
  // especially running out of memory for memtables.

  // The mutex protects all fields in this paragraph.
  std::mutex throttle_state_mutex_;
  // Notified after write_throttle_state_ or disable_stalling_ changes.
  std::condition_variable throttle_state_cv_;
  // Set to true during shutdown. Disables stalling regardless of throttle
  // state.
  bool disable_stalling_{false};
  // This state throttles writes as memory consumption goes beyond limit.
  std::atomic<WriteThrottleState> write_throttle_state_{
      WriteThrottleState::NONE};
  // When did write_throttle_state_ last change.
  SteadyTimestamp write_throttle_state_since_{SteadyTimestamp::min()};
  // When write_throttle_state_ was recalculated.
  SteadyTimestamp last_throttle_update_time_{SteadyTimestamp::min()};

  // Installs a MemTableRepFactory so that LogDevice's MemTabelRep is
  // used when constructing all MemTables.
  void installMemTableRep();
};

// Holds a rocksdb key that can be used as
// rocksdb::ReadOptions::iterate_upper_bound when iterating over records of a
// single log.
struct IterateUpperBoundHelper {
  // Use folly::none to disable iterate_upper_bound
  // (it'll be set to empty Slice).
  explicit IterateUpperBoundHelper(folly::Optional<logid_t> log_id)
      : data_key(log_id.value_or(LOGID_INVALID),
                 std::numeric_limits<lsn_t>::max()) {
    if (log_id.hasValue()) {
      // Set (log_id, LSN_MAX) as the upper bound.
      // Note that this may prevent iterator from seeing record LSN_MAX because
      // iterate_upper_bound is the exclusive upper bound.
      // This is fine because no valid record can have this LSN because its
      // epoch is too high to be valid (EPOCH_MAX = 2^32 - 2).
      upper_bound = data_key.sliceForBackwardSeek();
    }
  }

  // Key used as an upper bound for the iterator (a key such that all records
  // for the log are strictly smaller than it).
  RocksDBKeyFormat::DataKey data_key;

  // Slice pointing to last_key. Passed to RocksDB.
  rocksdb::Slice upper_bound;
};

/**
 * A wrapper for rocksdb iterator. All logdevice code should use it instead
 * of using rocksdb iterators directly.
 *  - Bumps stats for IO errors and corruption errors
 *    (RocksDBLogStoreBase::noteRocksDBStatus()).
 *  - Caches status. Vanilla rocksdb iterator's status() collects status from
 *    all subiterators on each call, which can be slow.
 *  - Adds some asserts, e.g. that status is always checked before accessing
 *    the key/value.
 *  - Movable (has a unique_ptr to the underlying iterator).
 *    Trying to use a moved-out RocksDBIterator will fail an assert.
 * This class intentionally doesn't inherit from rocksdb::Iterator to make sure
 * it doesn't break when methods are added to rocksdb::Iterator.
 */
class RocksDBIterator {
 public:
  RocksDBIterator(std::unique_ptr<rocksdb::Iterator> iterator,
                  const rocksdb::ReadOptions& read_options,
                  const RocksDBLogStoreBase* store)
      : iterator_(std::move(iterator)),
        store_(store),
        total_order_seek_(read_options.total_order_seek) {
    ld_check(iterator_ != nullptr);
    ld_check(store_ != nullptr);
  }

  // Moved-out RocksDBIterators are not usable.
  // Calling any method will fail an assert.
  RocksDBIterator(RocksDBIterator&& rhs) = default;
  RocksDBIterator& operator=(RocksDBIterator&& rhs) = default;

  bool Valid() const {
    // iterator_ can be nullptr if it was moved out into another RocksDBIterator
    ld_check(iterator_ != nullptr);
    valid_checked_ = true;

    return iterator_->Valid();
  }

  void SeekToFirst() {
    ld_check(iterator_ != nullptr);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->SeekToFirst();

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "SeekToFirst()");
  }

  void SeekToLast() {
    ld_check(iterator_ != nullptr);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->SeekToLast();

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "SeekToLast()");
  }

  void Seek(const rocksdb::Slice& target) {
    ld_check(iterator_ != nullptr);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->Seek(target);

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "Seek()");
  }

  void SeekForPrev(const rocksdb::Slice& target) {
    ld_check(iterator_ != nullptr);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->SeekForPrev(target);

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "SeekForPrev()");
  }

  void Next() {
    ld_check(iterator_ != nullptr);
    ld_check(valid_checked_);
    ld_check(status_checked_);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->Next();

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "Next()");
  }

  void Prev() {
    ld_check(iterator_ != nullptr);
    ld_check(valid_checked_);
    ld_check(status_checked_);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->Prev();

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "Prev()");
  }

  void Refresh() {
    ld_check(iterator_ != nullptr);
    valid_checked_ = false;
    status_checked_ = false;

    iterator_->Refresh();

    status_ = getRocksdbStatus();
    store_->enterFailSafeIfFailed(status_.value(), "Refresh()");
  }

  rocksdb::Slice key() const {
    ld_check(iterator_ != nullptr);
    ld_check(valid_checked_);
    ld_check(status_checked_);

    return iterator_->key();
  }

  rocksdb::Slice value() const {
    ld_check(iterator_ != nullptr);
    ld_check(valid_checked_);
    ld_check(status_checked_);

    return iterator_->value();
  }

  rocksdb::Status status() const {
    ld_check(iterator_ != nullptr);
    // Makes no sense to call status() before doing the first seek.
    ld_check(status_.hasValue());

    status_checked_ = true;

    return status_.value();
  }

  rocksdb::Status GetProperty(std::string prop_name, std::string* prop) {
    ld_check(iterator_ != nullptr);
    return iterator_->GetProperty(std::move(prop_name), prop);
  }

  bool totalOrderSeek() const {
    return total_order_seek_;
  }

 private:
  rocksdb::Status getRocksdbStatus() {
    using IOType = IOFaultInjection::IOType;
    using DataType = IOFaultInjection::DataType;
    using FaultType = IOFaultInjection::FaultType;

    rocksdb::Status status;
    auto* rb_store = static_cast<const RocksDBLogStoreBase*>(store_);
    auto& io_fault_injection = IOFaultInjection::instance();
    auto sim_error = io_fault_injection.getInjectedFault(
        store_->getShardIdx(),
        IOType::READ,
        FaultType::IO_ERROR | FaultType::CORRUPTION,
        DataType::DATA);
    if (sim_error != FaultType::NONE) {
      status = RocksDBLogStoreBase::FaultTypeToStatus(sim_error);
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      2,
                      "Returning injected error '%s' for shard '%s'.",
                      status.ToString().c_str(),
                      rb_store->getDBPath().c_str());
    } else {
      status = iterator_->status();
    }
    return status;
  }

  // The iterator we're wrapping.
  std::unique_ptr<rocksdb::Iterator> iterator_;

  // For stats.
  const RocksDBLogStoreBase* store_;

  // Cached status.
  folly::Optional<rocksdb::Status> status_;

  // For asserts.
  bool total_order_seek_;
  mutable bool valid_checked_ = false;
  mutable bool status_checked_ = false;
};

}} // namespace facebook::logdevice
