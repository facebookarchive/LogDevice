/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <utility>
#include <vector>

#include <boost/noncopyable.hpp>
#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/SCDCopysetReordering.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Durability.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/strong_typedef.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/IteratorTracker.h"

namespace facebook { namespace logdevice {

/**
 * @file  Interface for local log store implementations.
 */

class IOTracing;
class LocalLogStore;
class Processor;
class WriteOp;

/**
 * State of a LocalLogStore::ReadIterator.
 */
enum class IteratorState {
  // An error (typically IO) occurred.
  ERROR,
  // Only possible if allow_blocking_io = false.
  // Operation couldn't be fulfilled without blocking IO.
  WOULDBLOCK,
  // Iterator went out of bounds for current log.
  AT_END,
  // Only possible after seek() or next() with non-null filter and stats.
  // Filtered operation hit some limit in ReadStats (e.g. max LSN,
  // read byte limit or execution time limit) before finding a record passing
  // the filter. getLSN() is the LSN at which the search stopped.
  // To continue the search (after renewing the limits), either seek to that LSN
  // or just call next().
  // Note that ReadStats::last_read is _not_ always equal
  // to {getLogID(), getLSN()}.
  LIMIT_REACHED,
  // Iterator is positioned on a record.
  AT_RECORD,

  // Doesn't represent an actual state.
  // Used to count the number of elements.
  MAX
};

class FlushCallback {
 public:
  /**
   * Called by LocalLogStore when asynchronously written data has been
   * preserved on stable storage.
   *
   * @param store  The LocalLogStore that experienced the flush event.
   *
   * @param token  Data previously submitted to the store, whose completion
   *               status included a FlushToken that compares <= to the
   *               FlushToken provided by this callback, has been preserved.
   *
   * @note  The following activities are prohibited from within this
   *         callback method (will cause a deadlock):
   *          - FlushCallback::deactivate()
   *          - LocalLogStore::registerOnFlushCallback()
   *          - LocalLogStore::unregisterOnFlushCallback()
   *          - deleting a FlushCallback
   */
  virtual void operator()(LocalLogStore* store, FlushToken token) const = 0;

  /**
   * If the object is linked into a callback list at the time of destruction,
   * the destructor will unlink it.
   */
  virtual ~FlushCallback() {
    deactivate();
  }

  /**
   * @return true iff this FlushCallback is currently on a callback list
   *              of some LocalLogStore.
   */
  bool active() const {
    return registered_with.load() != nullptr;
  }

  /**
   * Disable all future calls to this callback.
   *
   * Equivalent to invoking LocalLogStore::unregisterOnFlushCallback(*this).
   */
  void deactivate();

  // Links this callback onto a LocalLogStore Instance.
  folly::IntrusiveListHook links;

  // If non-NULL, the store this callback is registered with.
  std::atomic<LocalLogStore*> registered_with{nullptr};
};
using FlushCallbackList =
    folly::IntrusiveList<FlushCallback, &FlushCallback::links>;

class LocalLogStore : boost::noncopyable {
 public:
  struct ReadStats;
  class ReadFilter;

  /**
   * Interface for read iterators.
   * The same iterator is never used from different threads concurrently.
   * Therefore iplementation doesn't need to be thread safe.
   *
   * Note about filtering in seek() and next() (typically implmented using
   * copyset index):
   * If non-null `ReadFilter` and `ReadStats` are provided, the operation calls
   * `ReadFilter::operator()` on every record it encounters to determine which
   * record should the iterator end up at. `ReadFilter` and `ReadStats` must
   * either both be nullptr or both non-nullptr. `ReadStats` is used
   * for accounting for records that have been read during an operation.
   * There are 3 possible outcomes of a filtered operation:
   * 1) iterator positioned on a record that passes the filter:
   *    state() == AT_RECORD, or
   * 2) iterator reached a position where it cannot read the data:
   *    state() == ERROR or WOULDBLOCK or AT_END, or
   * 3) one of the limits in ReadStats was hit. In this case
   *    state() == LIMIT_REACHED and ReadStats::readLimitReached().
   * Note that a scenario is possible where despite exceeding the byte read
   * limit, the iterator points to a record that passes the filter - so, it
   * is important to check status() before checking
   * `ReadStats::readLimitReached()`.
   * The stats will include any records that have been read during the
   * operation (i.e. the record that was seeked to for `seek()`, and
   * the record that was skipped to for `next()`, as well as all the
   * records that were read but skipped due to not passing the filter.
   */
  class ReadIterator : public TrackableIterator {
   public:
    // State of the iterator

    virtual IteratorState state() const = 0;

    /**
     * Returns true if the iterator has landed in or traversed any region
     * of the log space that is potentially under-replicated.
     *
     * NOTE: accessUnderReplicatedRegion() is a sticky property. It is
     *       reset to false just prior to any seek on the iterator. It
     *       becomes true and stays true if the seek or any operation after
     *       the seek is impacted by an under-replicated region.
     */
    virtual bool accessedUnderReplicatedRegion() const = 0;

    // Current record

    // Requires: state() == AT_RECORD or LIMIT_REACHED.
    virtual lsn_t getLSN() const = 0;
    // Requires: state() == AT_RECORD.
    virtual Slice getRecord() const = 0;

    // Seeking

    // Can be called regardless of state().
    virtual void seek(lsn_t lsn,
                      ReadFilter* filter = nullptr,
                      ReadStats* stats = nullptr) = 0;
    virtual void seekForPrev(lsn_t lsn) = 0;

    // Moving

    // Requires: state() == AT_RECORD or LIMIT_REACHED.
    virtual void next(ReadFilter* filter = nullptr,
                      ReadStats* stats = nullptr) = 0;
    virtual void prev() = 0;

    const LocalLogStore* getStore() const override {
      return store_;
    }

    // Not copyable or movable
    explicit ReadIterator(const LocalLogStore* store) : store_(store) {}
    ReadIterator(const ReadIterator&) = delete;
    ReadIterator& operator=(const ReadIterator&) = delete;
    ReadIterator(ReadIterator&&) noexcept = delete;
    ReadIterator& operator=(ReadIterator&&) = delete;

    // Doesn't do blocking IO, ok to call from worker threads.
    ~ReadIterator() override {}

   protected:
    const LocalLogStore* store_;
  };

  // Iterator over all the data in the LocalLogStore
  // (unlike ReadIterator, which iterates over a single log).
  //
  // The order of iteration is largely implementation-defined, but it's
  // guaranteed that logs are visited in the following order:
  //  (1) internal logs except metadata logs,
  //  (2) metadata logs,
  //  (3) non-internal logs.
  // The order of logs and records within each of the 3 groups is undefined.
  // For LogsDB, the iteration happens in order of increasing
  // tuple <partition, log, LSN>, i.e. we read partitions one by one, reading
  // sequentially inside each partition.
  //
  // Records of different logs can interleave, but records of each log are
  // almost always visited in order of increasing LSN. It's safe to ignore the
  // the records that have LSN below the max previously seen LSN for the log.
  // TODO (#T24665001):
  //   There's one corner case when it's not safe: T22197755. Fix it.
  //
  // The iterator may or may not see records that were written after the
  // iterator was created. (It may also see some but not others, or see
  // a record and then not see it after a seek(). The only guarantee is that
  // we'll get all the records that were written before creating AllLogsIterator
  // and weren't subsequently deleted or trimmed.)
  class AllLogsIterator : public TrackableIterator {
   public:
    // A place in the sequence of records over which we're iterating.
    // For LogsDB, contains the tuple <partition, log, LSN>
    struct Location {
      virtual std::string toString() const = 0;
      virtual ~Location() = default;
    };

    // Works the same way as ReadIterator::state(); see comment above.
    // Never WOULDBLOCK because nonblocking reads are not supported by
    // AllLogsIterator.
    virtual IteratorState state() const = 0;

    // Requires: state() == AT_RECORD or LIMIT_REACHED.
    virtual std::unique_ptr<Location> getLocation() const = 0;
    virtual logid_t getLogID() const = 0;
    virtual lsn_t getLSN() const = 0;
    // Requires: state() == AT_RECORD.
    virtual Slice getRecord() const = 0;

    // Approximately what fraction of data we went through, assuming that we're
    // reading from the beginning to the end (from minLocation() and until
    // state() becomes AT_END). Between 0 and 1.
    // -1 means progress estimation is not supported.
    // state() must be one of: LIMIT_REACHED, AT_RECORD, AT_END.
    virtual double getProgress() const {
      return -1;
    }

    // The filtering works the same way as in ReadIterator; see comment above.
    virtual void seek(const Location& location,
                      ReadFilter* filter = nullptr,
                      ReadStats* stats = nullptr) = 0;
    virtual void next(ReadFilter* filter = nullptr,
                      ReadStats* stats = nullptr) = 0;

    // To read everything, seek to minLocation() and iterate
    // until state() == AT_END.
    virtual std::unique_ptr<Location> minLocation() const = 0;
    // To read all metadata logs, seek to metadataLogsBegin() and iterate
    // until you get a record of a non-metadata log or state() == AT_END.
    virtual std::unique_ptr<Location> metadataLogsBegin() const = 0;

    // Resets the iterator to an unseeked state, unpinning all the data.
    // Almost equivalent to destroying the iterator and creating a new one.
    // The only difference is that invalidate() doesn't refresh iterator's view
    // of the data (e.g. iterator's copy of logsdb directory).
    // Doesn't do blocking IO, ok to call from worker threads.
    virtual void invalidate() = 0;

    // Not copyable or movable
    AllLogsIterator() = default;
    AllLogsIterator(const AllLogsIterator&) = delete;
    AllLogsIterator& operator=(const AllLogsIterator&) = delete;

    // Doesn't do blocking IO, ok to call from worker threads.
    virtual ~AllLogsIterator() = default;
  };

  struct ReadOptions {
    // Require params for tracking_ctx to be set for all iterators.
    explicit ReadOptions(const char* reading_ctx, bool rebuilding = false)
        : tracking_ctx(rebuilding, reading_ctx) {}

    // The context for tracking iterators for debugging purposes
    TrackableIterator::TrackingContext tracking_ctx;

    // Allow blocking I/O?  If not, restricted to cache.
    bool allow_blocking_io = true;

    // If true, everything written before seek() is guaranteed to be visible.
    // Otherwise, everything written before creation.
    // Some of the more recent data may be visible (i.e. ReadIterator is not
    // strictly a snapshot iterator even with tailing=false).
    // Tailing iterators only support moving forward (next()),
    // and are better optimized for this.
    // Not supported by AllLogsIterator.
    bool tailing = false;

    // If true, the iterator allows the backing store to fill its caches with
    // data it reads.  This is the normal way, however for bulk scans of old
    // data it may be preferable to set this to false to suppress caching.
    bool fill_cache = true;

    // If true, attempts to use the copyset index for skipping records with
    // copysets that don't match the filter. NB: this can be overridden by
    // RocksDBSettings::use_copyset_index, see `shouldUseCSIIterator()` in
    // RocksDBLocalLogStore.cpp.
    bool allow_copyset_index = false;

    // Skip reading records altogether and provide only data available
    // through copyset index. If copyset index not available or overridden
    // by RocksDBSettings falling back to regular reads and returning all
    // data.
    bool csi_data_only = false;

    // Whether to inject a synthetic latency in read requests; Makes sense only
    // if blocking I/O is allowed.
    bool inject_latency = false;
  };

  // Stats and limits of an iterator read. Passed to filtered iterator
  // operations. The iterator updates the stats and stops reading if a limit
  // if reached.
  struct ReadStats {
    // record filtering stats
    size_t read_records{0};
    size_t read_record_bytes{0};
    size_t filtered_records{0};
    size_t filtered_record_bytes{0};
    size_t sent_records{0};
    size_t sent_record_bytes{0};

    // copyset index entry filtering stats
    size_t read_csi_bytes{0};
    size_t read_csi_entries{0};
    size_t filtered_csi_entries{0};
    size_t sent_csi_entries{0};

    // This counter is bumped each time we seek to a new partition in LogsDB.
    size_t seen_logsdb_partitions{0};

    size_t max_bytes_to_read{std::numeric_limits<size_t>::max()};

    std::chrono::time_point<std::chrono::steady_clock> read_start_time;
    // max execution time for reading
    std::chrono::milliseconds max_execution_time{
        std::chrono::milliseconds::max()};

    // Max lsn to read. Not supported by AllLogsIterator.
    std::pair<logid_t, lsn_t> stop_reading_after{
        logid_t(std::numeric_limits<logid_t::raw_type>::max()),
        LSN_MAX};

    // Max timestamp to read. Not supported by AllLogsIterator.
    std::chrono::milliseconds stop_reading_after_timestamp{
        std::chrono::milliseconds::max()};

    // Checked against stop_reading_after to see if we should stop.
    // Can be underestimated.
    std::pair<logid_t, lsn_t> last_read{LOGID_INVALID, LSN_INVALID};
    // Lower bound of max timestamp read. This is only filled by the
    // partitioned iterator from the partition directory, and doesn't
    // necessarily reflect the max timestamp of all records that were read.
    folly::Optional<std::chrono::milliseconds> max_read_timestamp_lower_bound;

    // True if we went outside the requested range of records, either by
    // LSN or by timestamp. The current record shouldn't be delivered even if
    // it's readily available.
    bool hardLimitReached() {
      return maxLSNReached() || maxTimestampReached();
    }

    // True if we exceeded some resource limit. In this case the
    // iterator should avoid doing any more work, but if there's a record
    // readily available in memory, we should deliver it.
    bool softLimitReached() {
      return read_record_bytes + read_csi_bytes >= max_bytes_to_read ||
          (max_execution_time != std::chrono::milliseconds::max() &&
           msec_since(read_start_time) >= max_execution_time.count());
    }

    bool readLimitReached() {
      return hardLimitReached() || softLimitReached();
    }

    bool maxLSNReached() const {
      return last_read > stop_reading_after;
    }

    // True if we read past `stop_reading_after_timestamp`.
    // This check is approximate: it uses `max_read_timestamp_lower_bound`
    // (which usually comes from logsdb partition boundaries), not the exact
    // record timestamps.
    bool maxTimestampReached() const {
      return max_read_timestamp_lower_bound.hasValue() &&
          *max_read_timestamp_lower_bound > stop_reading_after_timestamp;
    }

    // Sets the last lsn and increments counters depending on
    // whether the record was filtered or not
    void countReadRecord(logid_t log, lsn_t lsn, size_t record_size) {
      last_read = std::make_pair(log, lsn);
      ++read_records;
      read_record_bytes += record_size;
    }
    void countFilteredRecord(size_t record_size, bool sent) {
      if (sent) {
        ++sent_records;
        sent_record_bytes += record_size;
      } else {
        ++filtered_records;
        filtered_record_bytes += record_size;
      }
    }

    void countReadCSIEntry(size_t size, logid_t log, lsn_t lsn) {
      ++read_csi_entries;
      read_csi_bytes += size;
      last_read = std::make_pair(log, lsn);
    }
    void countFilteredCSIEntry(bool sent) {
      if (sent) {
        ++sent_csi_entries;
      } else {
        ++filtered_csi_entries;
      }
    }
  };

  /**
   * Interface for filtering records by next() and seek() methods of the
   * iterator. operator() shoud return true if the record should
   * be returned by the ReadIterator, false otherwise.
   */
  class ReadFilter {
   public:
    using csi_flags_t = LocalLogStoreRecordFormat::csi_flags_t;
    // Called for every record or CSI entry the iterator visits.
    //
    // Note: in future, if we implement block CSI, this call will have to
    // correspond to a range of LSNs rather than a single LSN, and the upper end
    // of the range may or may not be known (depending on which design we pick).
    // In this case feel free to replace the `lsn` argument with e.g. (a) a pair
    // of lsns, min and max, or (b) just the min lsn of the block, with a
    // guarantee that a block never crosses epoch boundary, or (c) just the
    // epoch number.
    virtual bool operator()(logid_t log,
                            lsn_t lsn,
                            const ShardID* copyset,
                            const copyset_size_t copyset_size,
                            const csi_flags_t flags,
                            RecordTimestamp min_ts,
                            RecordTimestamp max_ts) = 0;
    virtual ~ReadFilter() {}

    // These methods allow skipping ranges of records at once. Not all iterators
    // use these methods: currently only logsdb AllLogsIterator does.

    virtual bool shouldProcessTimeRange(RecordTimestamp /* min */,
                                        RecordTimestamp /* max */) {
      return true;
    }

    virtual bool shouldProcessRecordRange(logid_t,
                                          lsn_t /* min_lsn */,
                                          lsn_t /* max_lsn */,
                                          RecordTimestamp /* min_ts */,
                                          RecordTimestamp /* max_ts */) {
      return true;
    }
  };

  struct WriteOptions {
    // This may seem a bit useless :) but there were members here before.
    // Delete if no new options are added for a while.
  };

  // Place the store into a persistent read-only mode due to an error.
  // @return  false if the store was already in fail-safe mode.
  virtual bool enterFailSafeMode(const char* /*context*/,
                                 const char* /*error_string*/) {
    return false;
  }

  IOTracing* getIOTracing() const {
    return io_tracing_;
  }

  /**
   * Called after the processor is created
   * (it's normally created after LocalLogStore).
   * Used for trimming.
   */
  virtual void setProcessor(Processor* /*processor*/) {}

  /**
   * If there's too much data waiting to be flushed to disk, waits for it to be
   * flushed. Call it before doing non-latency-sensitive writeMulti().
   */
  virtual void stallLowPriWrite() {}

  // Write throttling modes, ordered from least to most severe.
  enum class WriteThrottleState { NONE, STALL_LOW_PRI_WRITE, REJECT_WRITE };

  /**
   * Get current throttling state for the store. Depending on current state of
   * unflushed data, this method returns the need to stall low priority stores
   * or reject stores.
   */
  virtual WriteThrottleState getWriteThrottleState() {
    return WriteThrottleState::NONE;
  }

  /**
   * Called once during shutdown.
   * Unblocks all current and future stallLowPriWrite() calls.
   */
  virtual void disableWriteStalling() {}

  /**
   * Performs multiple writes in an atomic batch.  The operations may be
   * writes, deletes or both.
   *
   * @return On success, returns 0.  On failure, returns -1 and sets err to
   *         LOCAL_LOG_STORE_WRITE, or CHECKSUM_MISMATCH if a corrupt write was
   *         encountered.
   */
  virtual int
  writeMulti(const std::vector<const WriteOp*>& writes,
             const WriteOptions& write_options = WriteOptions()) = 0;

  /**
   * Syncs the writes to stable storage.
   *
   * @param Durability  The durability classification of writes to sync.
   *                    Writes at the specified class or greater (more durable)
   *                    will be committed to stable storage.
   *
   *                    To sync all durability classes, use Durability::ALL.
   *
   * @return On success, returns 0.  On failure, returns -1 and sets err to
   *         LOCAL_LOG_STORE_WRITE.
   */
  virtual int sync(const Durability) = 0;

  /**
   * Returns the max token issued by implementation
   *
   * @return FlushToken_INVALID when not supported. Otherwise
   *         a valid FlushToken
   */
  virtual FlushToken maxFlushToken() const = 0;

  /**
   * Returns the largest FlushToken for which writes have
   * been committed to stable storage. All FlushTokens at or
   * below this number have been retired.
   *
   * @return FlushToken_INVALID when not supported. Otherwise
   *         a valid FlushToken
   */
  virtual FlushToken flushedUpThrough() const = 0;

  /**
   * @return the token that will be retired by the next syncWAL operation.
   *
   * To ensure writes utilizing the WAL are committed to stable storage,
   * issue the writes to RocksDB with write options that will cause the
   * writes to be appended to the WAL. Then, *after* these writes complete
   * successfully, record the FlushToken returned by calling maxWALSyncToken().
   * When the writes have been retired, walSyncedUpThrough() will return a
   * FlushToken that is greater than or equal to the token returned by
   * maxWALSyncToken().
   *
   * @note  Tokens from different APIs or issued by differen shards may
   *        overlap.  The token returned by this API can only be compared
   *        with the result of walSyncedUpThrough() on the same shard.
   */
  virtual FlushToken maxWALSyncToken() const = 0;

  /**
   * @return the largest token, issued by maxWALSyncToken(), that has been
   *         retired by completion of a syncWAL() operation.
   */
  virtual FlushToken walSyncedUpThrough() const = 0;

  /**
   * The steady clock time at which the oldest uncommitted write was
   * received by the system.
   *
   * This is used to determine the maximum age of uncommitted data and
   * to update age estimates in logic that implements flush triggers.
   *
   * @note  A value of SteadyTimestamp::max() indicates that currently
   *        all data has been committed to stable storage.
   */
  virtual SteadyTimestamp oldestUnflushedDataTimestamp() const {
    return SteadyTimestamp::max();
  }

  /**
   * Reads a sequence of records.  Provides a streaming interface for reading
   * from a given log. Caller may read as many records as desired, but it must
   * seek to the starting lsn before using the returned iterator.
   *
   * @return A ReadIterator instance.
   */
  virtual std::unique_ptr<ReadIterator>
  read(logid_t log_id, const ReadOptions& options) const = 0;

  /**
   * Creates a new AllLogsIterator.
   * May wait for blocking IO, don't call from worker threads.
   * @param logs
   *    If not folly::none, read only the given LSN ranges of the given logs
   *    and use shouldProcessRecordRange() to filter out groups of records.
   *    Current implementation has some limitations:
   *     1. If `logs` is not folly::none, all calls to seek() and next()
   *        require non-null `filter` and `stats` arguments.
   *     2. Nonempty `logs` may make readAllLogs() call somewhat slow because it
   *        makes a copy of the logsdb directory, locking per-log mutexes along
   *        the way.
   *     3. The set of logs and LSN ranges are just a hint. The iterator may
   *        still return records for logs that are not on the list or for LSNs
   *        that are outside the range. Use ReadFilter to filter out records
   *        of undesired logs.
   */
  virtual std::unique_ptr<AllLogsIterator>
  readAllLogs(const ReadOptions& options,
              const folly::Optional<
                  std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>& logs =
                  folly::none) const = 0;

  /**
   * Reads per-store or per-log metadata.
   *
   * @param log_id      log ID to read metadata of
   * @param metadata    a subclass of
   *                    StoreMetadata/LogMetadata
   *                    that will be updated with metadata read from the local
   *                    log store
   *
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         NOTFOUND             when metadata for log_id is not found
   *                              (`metadata` parameter is not modified)
   *         LOCAL_LOG_STORE_READ on any other error
   */
  virtual int readLogMetadata(logid_t log_id, LogMetadata* metadata) = 0;
  virtual int readStoreMetadata(StoreMetadata* metadata) = 0;

  /**
   * Reads per-epoch metadata for a log.
   *
   * @param log_id                log ID to read metadata of
   * @param metadata              output parameter for a subclass of
   *                              PerEpochLogMetadata that will be updated
   *                              with metadata read from the local log store
   * @param find_last_available   PerEpochLogMetadata for target epoch may not
   *                              exist. In this case, if find_last_available
   *                              set to true, last available metadata for some
   *                              smaller epoch will be returned if exists.
   * @param allow_blocking_io     Option that specifies if blocking I/O is
   *                              allowed. If false, restricted to non-blocking
   *                              (cache).
   *
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         NOTFOUND             when metadata for log_id is not found
   *                              (`metadata` parameter is not modified)
   *         LOCAL_LOG_STORE_READ on any other error
   */
  virtual int readPerEpochLogMetadata(logid_t log_id,
                                      epoch_t epoch,
                                      PerEpochLogMetadata* metadata,
                                      bool find_last_available = false,
                                      bool allow_blocking_io = true) const = 0;

  /**
   * Write global or per-log metadata.
   *
   * @param log_id     ID of the log to write metadata for
   * @param metadata   a subclass of StoreMetadata/LogMetadata representing
   *                   metadata to be stored
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         LOCAL_LOG_STORE_WRITE
   */
  virtual int writeLogMetadata(logid_t log_id,
                               const LogMetadata& metadata,
                               const WriteOptions& opts = WriteOptions()) = 0;
  virtual int writeStoreMetadata(const StoreMetadata& metadata,
                                 const WriteOptions& opts = WriteOptions()) = 0;

  /**
   * Methods for erasing metadata. Only used in tests and emergency tools.
   *
   * @param first_log_id     ID of the first log to erase metadata (inclusive)
   * @param last_log_id      ID of the last log to erase metadata (inclusive)
   * @param epoch            epoch to erase (for per-epoch metadata)
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         LOCAL_LOG_STORE_WRITE
   */
  virtual int
  deleteStoreMetadata(const StoreMetadataType type,
                      const WriteOptions& opts = WriteOptions()) = 0;
  virtual int deleteLogMetadata(logid_t first_log_id,
                                logid_t last_log_id,
                                const LogMetadataType type,
                                const WriteOptions& opts = WriteOptions()) = 0;
  virtual int
  deletePerEpochLogMetadata(logid_t log_id,
                            epoch_t epoch,
                            const PerEpochLogMetadataType type,
                            const WriteOptions& opts = WriteOptions()) = 0;

  enum class LogSnapshotBlobType { RECORD_CACHE, MAX };

  using LogSnapshotBlobCallback = std::function<int(logid_t, Slice)>;

  // Methods to read and write a dump of some per-log state. Unlike
  // LogMetadata, the blobs for all logs have to be read/deleted at once
  // (atomically), i.e. logically the whole list of <logid, blob> pairs is
  // treated as one entity. The split by log ID will allow us to distribute the
  // snapshots across the shards, which should improve performance.
  virtual int readAllLogSnapshotBlobs(LogSnapshotBlobType type,
                                      LogSnapshotBlobCallback callback) = 0;

  // Writing can be done in batches
  virtual int writeLogSnapshotBlobs(
      LogSnapshotBlobType snapshots_type,
      const std::vector<std::pair<logid_t, Slice>>& snapshots) = 0;

  virtual int deleteAllLogSnapshotBlobs() = 0;

  /**
   * Atomically update metadata entry for a log to a higher value.
   *
   * @param log_id     ID of the log to update metadata for
   * @param metadata   metadata to be stored; this argument may be modified by
   *                   this method to reflect the current value in the local
   *                   log store
   *
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         UPTODATE              local log store already contains a value
   *                               which is greater or equal than 'metadata'
   *                               ('metadata' is updated to reflect that value)
   *         LOCAL_LOG_STORE_WRITE on any other error
   */
  virtual int updateLogMetadata(logid_t log_id,
                                ComparableLogMetadata& metadata,
                                const WriteOptions& opts = WriteOptions()) = 0;

  /**
   * Option that decides whether to check seal metadata preemption for
   * PerEpochLogMetadata.
   */
  enum class SealPreemption { ENABLE = 0, DISABLE };

  /**
   * Atomically update metadata entry for an epoch of a log.
   *
   * @param log_id       ID of the log to update metadata for
   * @param epoch        epoch to update for
   * @param metadata     in/out parameter for metadata, may be updated to
   *                     the most recent value
   *
   * @param seal_preempt  if ENABLE, atomically checks SealMetadata stored on
   *                      the same local log store, if metadata is preempted
   *                      by the seal, do not update metadata
   *
   * Note that as an optimization local log store may not store the metadata
   * if it is considered `empty' (metadata.empty() == true). If there are
   * existing values and update is success for the empty metadata, the existing
   * metadata record will be erased. In both cases, 0 is returned.
   *
   * @return On success, returns 0. On failure, returns -1 and sets err to
   *         UPTODATE    (1) the store already per-epoch log metadata that
   *                         is up-to-date
   *                     (2) if SealPreemption::ENABLE and metadata is preempted
   *                         by an existing seal metadata
   *                     In both cases, no change is done for the metadata
   *                     stored (if any)
   *         LOCAL_LOG_STORE_WRITE on any other error
   */
  virtual int
  updatePerEpochLogMetadata(logid_t log_id,
                            epoch_t epoch,
                            PerEpochLogMetadata& metadata,
                            SealPreemption seal_preempt,
                            const WriteOptions& opts = WriteOptions()) = 0;

  /**
   * @return Status indicating if the log store currently is accepting writes:
   *         OK          yes
   *         LOW_ON_SPC  yes, but the store is low on free space
   *         NOSPC       no, low on space
   *         DISABLED    no, this store will never accept writes
   */
  virtual Status acceptingWrites() const = 0;

  /**
   * Checks whether the store is empty, i.e. doesn't contain any records or
   * metadata.
   *
   * @return 1 if the store is empty, 0 if it's not empty, -1 in case of error.
   *         In case of error sets err to LOCAL_LOG_STORE_READ.
   */
  virtual int isEmpty() const = 0;

  /**
   * Provides the highest LSN that was inserted into the local storage for
   * this log. Used to check wether a log is empty.
   * Note that highestLSN may be set to LSN_INVALID if no record was ever
   * inserted to this log.
   *
   * This method can be called from Worker threads. Implementation should not
   * perform any blocking I/O.
   *
   * @return
   *   0 on sucess and copies the highest LSN into highestLSN,
   *   -1 on failure and sets err to appropriate error code:
   *     NOTSUPPORTED     The local log store backend does not implement
   *                      this feature
   *     NOTSUPPORTEDLOG  This local log store backend does not implement this
   *                      feature for this particular log.
   *                      E.g. PartitionedRocksDBStore doesn't support this
   *                      for internal logs (e.g. metadata logs, event log,
   *                      config log).
   *
   */
  virtual int getHighestInsertedLSN(logid_t log_id, lsn_t* highestLSN) = 0;

  /**
   * Gets a timestamp of the of the first record with lsn >= @param lsn in
   * target log_id.
   * The result may be approximate. It allows to use some underlying specifics
   * of LocalLogStore to get timestamp without actually reading record from
   * disk. For example, PartitionDirectoryKey is used in PartitionedRocksDBStore
   * which allows to read only partition metadata and return timestamp of
   * partition as result.
   * @param log_id              Log id on which to search.
   * @param lsn                 LSN on which to search.
   */
  virtual int
  getApproximateTimestamp(logid_t log_id,
                          lsn_t lsn,
                          bool allow_blocking_io,
                          std::chrono::milliseconds* timestamp_out) = 0;

  /**
   * Implement the FindTime API. @see logdevice/common/FindTimeRequest.h.
   *
   * @param log_id              Log id on which to search.
   * @param timestamp           Target timestamp.
   * @param lo                  The lsn of the newest record such that its
   *                            timestamp is < `timestamp` will be written here.
   *                            The initial value of *lo is the lower bound of
   *                            the range of records we should be looking at, ie
   *                            records with lesser lsns will not be considered.
   *                            The input value of lo is usually equal to trim
   *                            point; it's not recommended for the
   *                            implementation this method to decrease `lo` -
   *                            if it does, the client may get findTime result
   *                            that's below trim point.
   * @param hi                  The lsn of the oldest record such that its
   *                            timestamp is >=`timestamp` will be written here.
   *                            The initial value of *hi is the upper bound of
   *                            the range of records we should be looking at, ie
   *                            records with greater lsns will not be
   *                            considered. The input value is only a hint;
   *                            the implementation may choose to ignore it.
   * @param approximate         with this flag turned on findTime() will perform
   *                            faster but will return approximate result lsn
   *                            which may be smaller than biggest lsn which
   *                            timestamp is <= given timestamp.
   * @param allow_blocking_io   Option that specifies if blocking I/O is
   *                            allowed. If false, restricted to non-blocking
   *                            (cache).
   *                            Note: only approximate findTime with
   *                            approximate == true can support non
   *                            blocking I/O
   * @param deadline            Time point when result of execution is no longer
   *                            important. If execution pass this deadline it
   *                            may terminates and return -1 with err set to
   *                            E::TIMEDOUT.
   */
  virtual int findTime(logid_t log_id,
                       std::chrono::milliseconds timestamp,
                       lsn_t* lo,
                       lsn_t* hi,
                       bool approximate = false,
                       bool allow_blocking_io = true,
                       std::chrono::steady_clock::time_point deadline =
                           std::chrono::steady_clock::time_point::max()) const;

  virtual int findKey(logid_t log_id,
                      std::string key,
                      lsn_t* lo,
                      lsn_t* hi,
                      bool approximate = false,
                      bool allow_blocking_io = true) const;

  /**
   * Expand the provided time ranges to cover the timestamps of any records
   * that are bounded by records with timestamps in the ranges, but have
   * timestamps that are not monotonically increasing in lsn order. This
   * can occur when sequencers move between nodes with skewed clocks.
   */
  virtual void normalizeTimeRanges(RecordTimeIntervals&) const;

  /**
   * Some stores (e.g. PartitionedRocksDBStore) can support non blocking
   * FindTime.
   */
  virtual bool supportsNonBlockingFindTime() const {
    return false;
  }

  /**
   * Some stores (e.g. PartitionedRocksDBStore) can support non blocking
   * FindKey.
   */
  virtual bool supportsNonBlockingFindKey() const {
    return false;
  }

  /**
   * Called by each StorageThread before starting to execute
   * StorageTasks. Allows the store to initialize any per-thread data it may
   * use.
   */
  virtual void onStorageThreadStarted() {}

  /**
   * @return current version of the data. This should match up with the version
   * that TrackableIterator sets for an unpartitioned store. For a partitioned
   * store, the meaning of the result is implementation-specific. For instance,
   * for a PartitionedRocksDBStore, this will return the version of the default
   * column family.
   */
  virtual uint64_t getVersion() const {
    return 0;
  }

  /**
   * @return this shard's index. Used for stats.
   */
  virtual int getShardIdx() const = 0;

  /**
   * Register to receive notifications as buffered data is retired
   * to stable storage.
   * Only tests override it.
   */
  virtual int registerOnFlushCallback(FlushCallback& cb);

  /**
   * Cancel notifications of buffered data flush events.
   */
  void unregisterOnFlushCallback(FlushCallback& cb);

  void broadcastFlushEvent(FlushToken flushedUpTo);

  /**
   * Notification that there will be no more writes to this LocalLogStore.
   * Called either zero or one time before destruction.
   * The caller promises that after the call starts there will be no attempts
   * to write to the LocalLogStore (including metadata updates) or to flush.
   * All read operations must still work after the call.
   *
   * The current implementation flushes memtables and stops background threads.
   * Separating this method from the destructor allows calling it earlier in
   * shutdown sequence, allowing more time for memtable flushes.
   */
  virtual void markImmutable() {}

  virtual ~LocalLogStore() {}

 protected:
  IOTracing* io_tracing_ = nullptr;

  std::mutex flushing_mtx_;
  FlushCallbackList on_flush_cbs_;
};

/**
 * A filter on the copysets, used to skip records in the ReadIterator
 */

class LocalLogStoreReadFilter : public LocalLogStore::ReadFilter {
 public:
  explicit LocalLogStoreReadFilter() {}

  bool operator()(logid_t log,
                  lsn_t lsn,
                  const ShardID* copyset,
                  const copyset_size_t copyset_size,
                  const csi_flags_t flags,
                  RecordTimestamp min_ts,
                  RecordTimestamp max_ts) override;

  // If valid(), this is the id of this storage shard and scd filtering should
  // be used.
  // @see doc/single-copy-delivery.md for more information about scd.
  ShardID scd_my_shard_id_;

  // Used only if scd is used (scd_my_shard_id_.isValid()).
  // List of shards that the client considers down.
  copyset_custsz_t<4> scd_known_down_;
  // Replication factor this client expects for records.
  // This value helps us figure out if the copy we have is actually an extra
  // so we can ship it if SCD is active.
  // Set to 0 if replication is unknown.
  uint16_t scd_replication_{0};
  // Used for rebuilding. Only return records that have at least one shard from
  // that list in the copyset.
  copyset_t required_in_copyset_;
  SCDCopysetReordering scd_copyset_reordering_{SCDCopysetReordering::NONE};
  // Hashed session id (128 bits in total) of client initiating the read.
  // Used as the seed for scd copyset shuffling
  // when SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED is the active mode
  uint64_t csid_hash_pt1 = 0;
  uint64_t csid_hash_pt2 = 0;
  // If not null, this is the location of the client and local scd should be
  // used.
  std::unique_ptr<NodeLocation> client_location_;

  // When `scd_copyset_reordering_' != NONE, reorder the copyset using the
  // chosen algorithm.  Public for testing.
  void applyCopysetReordering(ShardID* copyset,
                              copyset_size_t copyset_size) const;

  // Used for local scd filtering.
  void setUpdateableConfig(std::shared_ptr<UpdateableConfig> config);

 protected:
  // Used for local scd filtering.
  virtual std::shared_ptr<const NodesConfiguration>
  getNodesConfiguration() const;

 private:
  std::shared_ptr<UpdateableConfig> updateable_config_;
  void copysetShuffle(ShardID* copyset,
                      copyset_size_t copyset_size,
                      uint64_t hash_pt1,
                      uint64_t hash_pt2) const;
};

/**
 * Interface for a sharded local log store, which can own multiple
 * LocaLogStore instances and any resources shared by them.
 */
class ShardedLocalLogStore {
 public:
  virtual int numShards() const = 0;

  virtual LocalLogStore* getByIndex(int idx) = 0;

  /**
   * Called when the sequencer has initiated space-based trimming for the disk
   * where the given shard is stored. If that disk is still out of space, this
   * will trigger space-based retention on it.
   */
  virtual void setSequencerInitiatedSpaceBasedRetention(int /* shard_idx */) {}

  virtual ~ShardedLocalLogStore() {}
};

std::ostream& operator<<(std::ostream&, IteratorState);

}} // namespace facebook::logdevice
