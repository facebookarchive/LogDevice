/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Memory.h>
#include <folly/Optional.h>
#include <rocksdb/compaction_filter.h>

#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"
#include "logdevice/server/storage/LocalLogStoreUtils.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * Filter that RocksDB calls while compacting our records.  Looks for records
 * that have been trimmed (their LSN is earlier than the trim point for the
 * log) and removes them.
 *
 * The compaction filter is also used for automatic trimming based on time
 * information (i.e., backlog duration of the log). The implementation follows
 * a "two-phase" mechanism:
 *
 * 1. For each compaction, the decison is solely made on the CURRENT trim points
 *    without considering time information. During compaction, the filter
 *    decides whether the trim point needs to be updated for those logs with
 *    backlog duration. The filter keeps track of these updated trim points
 *    and finally commits the updated values to LogStorageStateMap and
 *    TrimMetadata at the end of the compaction.
 *
 * 2. On the subsequent compaction, expired records will ACTUALLY be erased
 *    from db based on the updated trim point values. It then iterates from here
 *    to compute the next set of trim point values for the next compaction.
 */

class StorageThreadPool;

// This struct is the only means of exchanging information between
// RocksDBCompactionFilter corresponding to a manual compaction and the caller
// of rocksdb::DB::CompactRange()/CompactFiles() that initiated this compaction.
struct CompactionContext {
  // Passed from caller to the compaction filter.
  // If has value, compaction filter will assume that any logs that are _not_
  // on this list don't contain any useful records/index/CSI in this CF.
  // The compaction will (mostly) only read logs that are on the list,
  // will apply the usual filtering to them, and will throw away everything
  // else.
  // Note that empty Optional and empty vector mean different (opposite) things.
  folly::Optional<std::vector<logid_t>> logs_to_keep;
  PartitionedRocksDBStore::PartitionToCompact::Reason reason;
};

// When using a filter factory (as we do), RocksDB will use each filter on a
// single thread so no synchronization is needed in the filter
class RocksDBCompactionFilter : public rocksdb::CompactionFilter {
 public:
  explicit RocksDBCompactionFilter(StorageThreadPool* pool,
                                   UpdateableSettings<RocksDBSettings> settings,
                                   CompactionContext* context)
      : storage_thread_pool_(pool),
        context_(context),
        settings_(settings),
        rate_limiter_(settings->compaction_rate_limit_),
        drrBytesAllowed_(0),
        force_no_skips_(settings->force_no_compaction_optimizations_) {
    ld_check(pool != nullptr);
  }

  ~RocksDBCompactionFilter() override;

  Decision FilterV2(int /*level*/,
                    const rocksdb::Slice& key,
                    ValueType value_type,
                    const rocksdb::Slice& existing_value,
                    std::string* /*new_value*/,
                    std::string* skip_until) const override {
    switch (value_type) {
      case ValueType::kValue: {
        return const_cast<RocksDBCompactionFilter*>(this)->filterImpl(
            key, existing_value, skip_until);
      }
      case ValueType::kMergeOperand: {
        return const_cast<RocksDBCompactionFilter*>(this)->filterMergeImpl(
            key, existing_value, skip_until);
      }
      default:
        ld_check(false);
    }
    ld_check(false);
    return Decision::kKeep;
  }

  const char* Name() const override {
    return "logdevice::RocksDBCompactionFilter";
  }

  using TrimPointUpdateMap = LocalLogStoreUtils::TrimPointUpdateMap;
  using PerEpochLogMetadataTrimPointUpdateMap =
      LocalLogStoreUtils::PerEpochLogMetadataTrimPointUpdateMap;

 protected:
  // Pointer to StorageThreadPool through which the filter can see the world
  // (access LogStorageStateMap to read trim points and Processor to create
  // Requests).
  //
  // The StorageThreadPool does not directly own or know about this instance
  // but owns it indirectly.  (It owns the RocksDBLocalLogStore which owns the
  // rocksdb instance which presumably owns the filter.)  So the filter cannot
  // outlive the StorageThreadPool and it's safe for the filter to hold a raw
  // pointer to the pool.
  StorageThreadPool* storage_thread_pool_;

  CompactionContext* context_; // can be nullptr

  UpdateableSettings<RocksDBSettings> settings_;

  // Cache for trim point/cutoff timestamp queries (given a log, find its trim
  // point and cutoff timestamp).  Because compaction processes keys in
  // lexicographical order and our records are sorted by log ID, this cache is
  // simple and very effective.
  struct TrimInfoCache {
    logid_t log_id = LOGID_INVALID;
    folly::Optional<lsn_t> trim_point;

    // Every time we first encounter a new log, we compute
    // (based on the current time) a cutoff_timestamp for logs that have
    // the backlogDuration property set. We then remove all records of the log
    // with its timestamp smaller than the cutoff_timestamp.
    folly::Optional<std::chrono::milliseconds> cutoff_timestamp;

    folly::Optional<epoch_t> per_epoch_log_metadata_trim_point;
  } cache;

  virtual std::chrono::milliseconds currentTime();

  // Makes sure `cache' is filled with information about the given log.
  void getTrimInfo(logid_t log_id);

  virtual Decision filterImpl(const rocksdb::Slice& key,
                              const rocksdb::Slice& value,
                              std::string* skip_until);
  Decision filterMergeImpl(const rocksdb::Slice& key,
                           const rocksdb::Slice& operand,
                           std::string* skip_until);

  Decision filterOutAndMaybeSkip(logid_t log_id,
                                 const rocksdb::Slice& key,
                                 std::string* skip_until);

  // List of logs that we encountered during compaction and could not get trim
  // points for.  We periodically hand this list over to the Processor to try
  // to find the trim points in time for the next compaction run.
  std::vector<logid_t> logs_skipped_;

  // A hashmap of <logid, lsn> indicating the trim points to be updated based on
  // timestamp and backlog duration of the log. Update will take place after the
  // current compaction finishes and new trim points can be used for the next
  // compaction.
  TrimPointUpdateMap next_trim_points_;

  // a hash map of <logid, epoch_t> indicating the trim points to be updated
  // for per-epoch log metadata based on the current trim points for their
  // corresponding data logs
  PerEpochLogMetadataTrimPointUpdateMap next_metadata_trim_points_;

  // Number of records removed during compaction, published to a stat after
  // the compaction run finishes
  size_t count_removed_ = 0;

  // Number of metadata log records removed during compaction
  size_t metadata_log_records_removed_ = 0;

  // Number of per epoch log metadata records removed during compaction
  size_t per_epoch_log_metadata_removed_ = 0;

  RateLimiter rate_limiter_;

  uint64_t drrBytesAllowed_;

  // true if filterImpl() has never been called yet.
  bool first_record_ = true;

  // Don't use kRemoveAndSkipUntil.
  bool force_no_skips_ = false;

  // Called by the filter when it doesn't find a trim point for a log.
  void noteLogSkipped(logid_t log_id);

  // Flush the list of skipped logs, asking the Processor to look for trim
  // points.
  // Can be mocked for tests to skip dependency on workers and storage threads.
  virtual void flushSkippedLogs();

  // Called after the end of the current compaction operation, commits
  // updated trim points to LogStorageMap and LocalLogStore metadata.
  // Can be mocked for tests to skip dependency on workers and storage threads.
  virtual void flushUpdatedTrimPoints();

  Decision makeDecisionPerEpochLogMetadata(logid_t log_id, epoch_t epoch);

  void advanceTrimPointIfNeeded(logid_t log_id,
                                lsn_t lsn,
                                const rocksdb::Slice& record);

  void updateRecordAge(logid_t log_id, lsn_t lsn, const rocksdb::Slice& value);
};

/**
 * Factory of filters to give us a new filter instance on every compaction run
 */
class RocksDBCompactionFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit RocksDBCompactionFilterFactory(
      UpdateableSettings<RocksDBSettings> settings)
      : settings_(std::move(settings)) {}

  /**
   * Called by ShardedRocksDBLocalLogStore during server startup,
   * soon after creating the factory.
   */
  void setStorageThreadPool(StorageThreadPool* pool) {
    storage_thread_pool_.store(pool, std::memory_order_release);
  }

  /**
   * This context will be passed to all compaction filters created for manual
   * compactions, until the returned object is destroyed. The returned object
   * also holds a lock blocking other calls to this method. It can be used
   * to serialize manual compactions.
   * WARNING: if some code uses this method, all manual compactions of the same
   *          DB must hold this lock too.
   */
  std::shared_ptr<void> __attribute__((__warn_unused_result__))
  startUsingContext(CompactionContext* context) {
    struct Holder {
      Holder(RocksDBCompactionFilterFactory& owner)
          : owner(owner), lock(owner.context_mutex_) {}
      ~Holder() {
        owner.current_context_.store(nullptr);
      }

      RocksDBCompactionFilterFactory& owner;
      std::unique_lock<std::mutex> lock;
    };

    auto res = std::make_shared<Holder>(*this);
    current_context_.store(context);
    return res;
  }

  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context& context) override {
    StorageThreadPool* pool =
        storage_thread_pool_.load(std::memory_order_acquire);
    // Only create a filter if we have a StorageThreadPool pointer to give it.
    // Not having a pointer is unlikely, can only happen during server
    // startup.
    if (pool == nullptr) {
      return nullptr;
    }
    return createCompactionFilterImpl(
        pool, context.is_manual_compaction ? current_context_.load() : nullptr);
  }

  const char* Name() const override {
    return "logdevice::RocksDBCompactionFilterFactory";
  }

 protected:
  UpdateableSettings<RocksDBSettings> settings_;

  // Pointer to StorageThreadPool that owns the RocksDBLocalLogStore instance
  std::atomic<StorageThreadPool*> storage_thread_pool_{nullptr};

  std::mutex context_mutex_;
  std::atomic<CompactionContext*> current_context_{nullptr};

  virtual std::unique_ptr<rocksdb::CompactionFilter>
  createCompactionFilterImpl(StorageThreadPool* pool,
                             CompactionContext* context) {
    return std::make_unique<RocksDBCompactionFilter>(pool, settings_, context);
  }
};

}} // namespace facebook::logdevice
