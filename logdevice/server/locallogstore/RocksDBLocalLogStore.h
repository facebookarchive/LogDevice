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
#include <cstdlib>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/statistics.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/IteratorTracker.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreConfig.h"
#include "logdevice/server/locallogstore/RocksDBWriter.h"

namespace facebook { namespace logdevice {

/**
 * @file RocksDB-backed local log store.  Provides all the facilities storage
 *       nodes need to persist log contents.
 */

class RocksDBLocalLogStore : public RocksDBLogStoreBase {
 public:
  /**
   * This class includes the DataIterator and the CopySetIndexIterator. It uses
   * the CopySetIndexIterator to iterate over copyset index entries  and when
   * it finds an entry that passes the filter, attempts to find a record with
   * a matching LSN in the record store.
   */
  class CSIWrapper : public LocalLogStore::ReadIterator, boost::noncopyable {
   public:
    enum class Direction {
      FORWARD,
      BACKWARD,
    };

    struct Location {
      logid_t log_id;
      lsn_t lsn;
      // Location with at_end = true is a special value for a result of
      // advance() that goes out of bounds. Such Location can be used as a
      // target for moveTo(), resulting in an AT_END iterator.
      // Such Location is never returned from getLocation().
      bool at_end = false;

      Location() {}
      Location(logid_t log_id, lsn_t lsn)
          : log_id(log_id), lsn(lsn), at_end(false) {}
      static Location end() {
        Location loc;
        loc.at_end = true;
        return loc;
      }

      bool before(Location rhs, Direction dir) const {
        return dir == Direction::FORWARD
            ? std::tie(log_id, lsn) < std::tie(rhs.log_id, rhs.lsn)
            : std::tie(log_id, lsn) > std::tie(rhs.log_id, rhs.lsn);
      }
      // Increments/decrements the pair (log_id, lsn). If the pair is already
      // smallest/largest possible, sets at_end = true.
      // If stay_within is not none, log_id is not allowed to get different
      // from stay_within.value().
      Location advance(Direction dir,
                       folly::Optional<logid_t> stay_within) const {
        ld_check(!at_end);
        if (stay_within.hasValue()) {
          ld_assert_eq(log_id, stay_within.value());
        }
        if (dir == Direction::FORWARD) {
          if (lsn == std::numeric_limits<lsn_t>::max()) {
            if (stay_within.hasValue() ||
                log_id.val_ == std::numeric_limits<logid_t::raw_type>::max()) {
              return Location::end();
            }
            return Location(logid_t(log_id.val_ + 1), lsn_t(0));
          } else {
            return Location(log_id, lsn + 1);
          }
        } else {
          if (lsn == 0) {
            if (stay_within.hasValue() || log_id.val_ == 0) {
              return Location::end();
            }
            return Location(
                logid_t(log_id.val_ - 1), std::numeric_limits<lsn_t>::max());
          } else {
            return Location(log_id, lsn - 1);
          }
        }
      }

      bool operator==(const Location& rhs) const {
        if (at_end || rhs.at_end) {
          return at_end && rhs.at_end;
        }
        return std::tie(log_id, lsn) == std::tie(rhs.log_id, rhs.lsn);
      }
      bool operator!=(const Location& rhs) const {
        return !(*this == rhs);
      }
    };

    CSIWrapper(const RocksDBLogStoreBase* store,
               folly::Optional<logid_t> log_id,
               const LocalLogStore::ReadOptions& read_opts,
               rocksdb::ColumnFamilyHandle* column_family);
    ~CSIWrapper() override;

    IteratorState state() const override;
    bool accessedUnderReplicatedRegion() const override;
    void prev() override;
    void next(ReadFilter* filter = nullptr,
              ReadStats* stats = nullptr) override;
    void seek(lsn_t lsn,
              ReadFilter* filter = nullptr,
              ReadStats* stats = nullptr) override;
    void seekForPrev(lsn_t lsn) override;
    lsn_t getLSN() const override;
    Slice getRecord() const override;
    logid_t getLogID() const;

    // Multi-log API.
    Location getLocation() const;
    void seek(logid_t log,
              lsn_t lsn,
              ReadFilter* filter = nullptr,
              ReadStats* stats = nullptr);

    // Releases the underlying iterators.
    void invalidate();

    const rocksdb::ColumnFamilyHandle* getCF() const {
      return cf_;
    }

    // Propagate debug info to subiterators.
    void setContextString(const char* str) override;

    size_t getIOBytesUnnormalized() const override {
      return RocksDBLogStoreBase::getIOBytesUnnormalized();
    }

    // Passed to ReadFilter.
    // Used by PartitionedRocksDBStore::Iterator: it uses a CSIWrapper confined
    // to a single partition, and the time range comes from partition metadata.
    RecordTimestamp min_ts_ = RecordTimestamp::min();
    RecordTimestamp max_ts_ = RecordTimestamp::max();

   private:
    class CopySetIndexIterator;
    class DataIterator;

    // Implementation of seek(), next(), seekForPrev(), prev() operations.
    // @param target  the log/lsn to which we should move the iterator
    // @param dir  whether we're moving iterator forward (seek/next) or
    //             backward (seekForPrev/prev)
    // @param near  if true, we're doing a next/prev,
    //              if false, we're doing seek/seekForPrev
    void moveTo(const Location& target,
                Direction dir,
                bool near,
                ReadFilter* filter,
                ReadStats* stats);

    // Helper function to parse record and run ReadFilter on it.
    // Returns true if the record
    //  (a) is not a dangling amend (i.e. doesn't have FLAG_AMEND), and
    //  (b) passes the filter (or filter is nullptr).
    // If the record is malformed, conservatively returns true.
    // If @param csi_it is not nullptr, also checks the record's copyset,
    // flags and wave against the ones from csi_it. Logs an error
    // if they don't match.
    bool applyFilterToDataRecord(Location loc,
                                 Slice record_blob,
                                 ReadFilter* filter,
                                 CopySetIndexIterator* csi_it);

    const RocksDBLogStoreBase* getRocksDBStore() const {
      return static_cast<const RocksDBLogStoreBase*>(store_);
    }

    StatsHolder* getStatsHolder() const {
      return getRocksDBStore()->getStatsHolder();
    }

    rocksdb::DB& getDB() const {
      return getRocksDBStore()->getDB();
    }

    int getShardIdx() const {
      return store_->getShardIdx();
    }

    folly::Optional<logid_t> log_id_;
    ReadOptions read_opts_;
    rocksdb::ColumnFamilyHandle* cf_;

    // Iterator over records. nullptr if we're a CSI-only iterator.
    // If state_ == AT_RECORD, points to the current record.
    std::unique_ptr<DataIterator> data_iterator_;
    // Iterator over copyset index. nullptr if we're not using CSI.
    // If state_ == AT_RECORD, points to the current record.
    std::unique_ptr<CopySetIndexIterator> csi_iterator_;

    // state() returns this.
    IteratorState state_;

    // If state_ == LIMIT_REACHED, LimitReachedState contains information
    // needed to continue the search from where we left off.
    struct LimitReachedState {
      // getLocation() returns this.
      Location location = Location::end();
      // True if csi_iterator_/data_iterator_ is valid and positioned just below
      // `current`. This means we don't need to re-seek that iterator.
      bool csi_near = false;
      bool data_near = false;

      bool valid() const {
        return !location.at_end;
      }

      void clear() {
        location = Location::end();
        csi_near = false;
        data_near = false;
      }
    };

    LimitReachedState limit_reached_;
  };

  // A simple wrapper around CSIWrapper with log_id = none.
  class AllLogsIteratorImpl : public AllLogsIterator {
   public:
    struct LocationImpl : public Location {
      logid_t log;
      lsn_t lsn;

      LocationImpl() = default;
      LocationImpl(logid_t log, lsn_t lsn) : log(log), lsn(lsn) {}

      std::string toString() const override {
        // "123 e4n56"
        return std::to_string(log.val_) + " " + lsn_to_string(lsn);
      }
    };

    explicit AllLogsIteratorImpl(std::unique_ptr<CSIWrapper> iterator);

    IteratorState state() const override;
    std::unique_ptr<Location> getLocation() const override;
    logid_t getLogID() const override;
    lsn_t getLSN() const override;
    Slice getRecord() const override;
    void seek(const Location& location,
              ReadFilter* filter = nullptr,
              ReadStats* stats = nullptr) override;
    void next(ReadFilter* filter = nullptr,
              ReadStats* stats = nullptr) override;
    std::unique_ptr<Location> minLocation() const override;
    std::unique_ptr<Location> metadataLogsBegin() const override;
    void invalidate() override;
    const LocalLogStore* getStore() const override;
    size_t getIOBytesUnnormalized() const override {
      return RocksDBLogStoreBase::getIOBytesUnnormalized();
    }

   private:
    std::unique_ptr<CSIWrapper> iterator_;
  };

  /**
   * Creates or opens an existing RocksDB-backed local log store in the
   * specified directory on the filesystem.
   *
   * @param path      path to directory on disk
   * @param settings  RocksDB database options
   *
   * @return On success, returns the newly created instance.  On failure,
   *         returns nullptr.
   */
  explicit RocksDBLocalLogStore(uint32_t shard_idx,
                                uint32_t num_shards,
                                const std::string& path,
                                RocksDBLogStoreConfig rocksdb_config,
                                StatsHolder*,
                                IOTracing*);

  int writeMulti(const std::vector<const WriteOp*>& writes,
                 const WriteOptions& options) override {
    rocksdb::WriteBatch wal_batch;
    rocksdb::WriteBatch mem_batch;
    return writer_->writeMulti(writes,
                               options,
                               /* column families */ nullptr,
                               nullptr,
                               wal_batch,
                               mem_batch);
  }

  std::unique_ptr<ReadIterator>
  read(logid_t log_id, const LocalLogStore::ReadOptions&) const override;

  std::unique_ptr<AllLogsIterator>
  readAllLogs(const LocalLogStore::ReadOptions& options_in,
              const folly::Optional<
                  std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>& logs)
      const override;

  int readLogMetadata(logid_t log_id, LogMetadata* metadata) override {
    return writer_->readLogMetadata(log_id,
                                    metadata,
                                    /* column family */ nullptr);
  }

  int readAllLogSnapshotBlobs(LogSnapshotBlobType type,
                              LogSnapshotBlobCallback callback) override;

  int writeLogSnapshotBlobs(
      LogSnapshotBlobType snapshots_type,
      const std::vector<std::pair<logid_t, Slice>>& snapshots) override;

  int deleteAllLogSnapshotBlobs() override;

  /**
   * Perform a manual compation run on the entire rocksDB logstore.
   * Will block the calling thread until the compaction finishes.
   *
   * @return  0 on success, -1 on failure
   */
  int performCompaction();

  int isEmpty() const override {
    return isCFEmpty(db_->DefaultColumnFamily());
  }

  int getHighestInsertedLSN(logid_t log_id, lsn_t* highestLSN) override;

  int getApproximateTimestamp(
      logid_t log_id,
      lsn_t lsn,
      bool allow_blocking_io,
      std::chrono::milliseconds* timestamp_out) override;
  /**
   * Update the timestamp of last attemped/performed manual compaction.
   * used by the log store monitor thread
   */
  void updateManualCompactTime();

  /**
   * @return  time point of the last manual compaction,
   */
  std::chrono::steady_clock::time_point getLastCompactTime() const {
    return std::chrono::steady_clock::time_point(
        last_manual_compact_time_.load());
  }

  int findTime(logid_t log_id,
               std::chrono::milliseconds timestamp,
               lsn_t* lo,
               lsn_t* hi,
               bool approximate = false,
               bool allow_blocking_io = true,
               std::chrono::steady_clock::time_point deadline =
                   std::chrono::steady_clock::time_point::max()) const override;

  int findKey(logid_t log_id,
              std::string key,
              lsn_t* lo,
              lsn_t* hi,
              bool approximate = false,
              bool allow_blocking_io = true) const override;

 private:
  // timestamp of the last attempted/completed manual compaction,
  // initialized to be duration::min() meaning it was never scheduled
  // manual compaction before.
  // Updated both upon attempting to manual compact and finishing manual
  // compaction
  std::atomic<std::chrono::steady_clock::duration> last_manual_compact_time_{
      std::chrono::steady_clock::duration::min()};
};

}} // namespace facebook::logdevice
