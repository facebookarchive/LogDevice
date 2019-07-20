/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"

namespace facebook { namespace logdevice {

class PartitionedRocksDBStore::Iterator : public LocalLogStore::ReadIterator {
 public:
  Iterator(const PartitionedRocksDBStore* pstore,
           logid_t log_id,
           const LocalLogStore::ReadOptions& options);

  IteratorState state() const override;
  bool accessedUnderReplicatedRegion() const override {
    return accessed_underreplicated_region_;
  }

  void prev() override;
  void next(ReadFilter* filter = nullptr, ReadStats* stats = nullptr) override;

  void seek(lsn_t lsn,
            ReadFilter* filter = nullptr,
            ReadStats* stats = nullptr) override;
  void seekForPrev(lsn_t lsn) override;
  lsn_t getLSN() const override;
  Slice getRecord() const override;

  // Seek to the first record in partitions with
  // id > partition_id and min_lsn >= lsn.
  // Requires options_.read_tier == rocksdb::kReadAllTier.
  void seekToPartitionAfter(lsn_t lsn, partition_id_t partition_id) {
    seekToPartitionBeforeOrAfter(lsn, partition_id, true);
  }
  // Seek to the last record in partitions with
  // id < partition_id and min_lsn < lsn.
  // Requires options_.read_tier == rocksdb::kReadAllTier.
  void seekToPartitionBefore(lsn_t lsn, partition_id_t partition_id) {
    seekToPartitionBeforeOrAfter(lsn, partition_id, false);
  }

  void setContextString(const char* str) override {
    TrackableIterator::setContextString(str);
    if (data_iterator_) {
      data_iterator_->setContextString(str);
    }
  }

  size_t getIOBytesUnnormalized() const override {
    return RocksDBLogStoreBase::getIOBytesUnnormalized();
  }

 private:
  struct PartitionInfo {
    PartitionPtr partition_;
    // Smallest LSN for this log contained in this partition,
    // part of directory entry key. Since directory key can be updated to
    // a smaller value, this is only an upper bound.
    lsn_t min_lsn_ = LSN_INVALID;

    // The largest LSN recorded in metadata for this log in this partition.
    // Since directory data can be updated to a higher value, this is only a
    // lower bound. Used to filter out records that exist in the partition but
    // had their metadata record lost due to a crash.
    lsn_t max_lsn_ = LSN_INVALID;

    void clear() {
      *this = PartitionInfo();
    }

    std::string toString() const {
      return partition_
          ? std::to_string(partition_->id_)
          : std::string("none") + "(min: " + lsn_to_string(min_lsn_) +
              ", max: " + lsn_to_string(max_lsn_) + ")";
    }
  };

  // There are three things that can point to some partition or another, and
  // normally point to the current partition:
  //  * meta_iterator_ points to directory entry describing a partition,
  //    or is folly::none or !Valid() or !status().ok(),
  //  * current_ describes a partition, or is nullptr,
  //  * data_iterator_ points into a partition, or is nullptr.
  // A few methods are used for keeping these 3 things in sync with each other.
  // The iterator can be in one of two modes:
  //  - Latest partition fast path: meta_iterator_ is unset,
  //    current_ equal to latest_.
  //  - Normal mode: meta_iterator_, current_ and data_iterator_ all point to
  //    the same partition.

  // Seeks meta_iterator_ to partition containing `lsn`, updates current_
  // to describe that partition.
  // Iff the log is empty, sets current_.partition_ == nullptr.
  void setMetaIteratorAndCurrentFromLSN(lsn_t lsn);

  // Sets data_iterator_ to point to the partition described by current_.
  // If filter is provided, filter->shouldProcessTimeRange() is applied to the
  // partition's time range; if the time range is rejected by the filter,
  // data_iterator_ is set to nullptr, as a signal to moveUntilValid() to
  // try other partitions.
  void setDataIteratorFromCurrent(ReadFilter* filter = nullptr);

  // If new_current is different from current_, set current_ = latest_
  // and destroy data_iterator_.
  void setCurrent(PartitionInfo new_current);

  // Creates and seeks meta_iterator_ to partition described by current_.
  // Returns false if no entry for current_.partition_ was found (e.g. it
  // was removed after current_ was set); in this case meta_iterator_
  // is left with the result of SeekForPrev(current_), i.e. on the previous
  // entry if it exists.
  bool setMetaIteratorFromCurrent();

  // Called when we find that the log is empty. Frees both iterators and both
  // partition pointers (current_ and latest_), and updates
  // accessed_underreplicated_region_.
  void handleEmptyLog();

  // Updates latest_ and oldest_partition_id_.
  // latest_.partition_ == nullptr indicates that the log is empty.
  void updatePartitionRange();
  // If meta_iterator_ == nullptr, create it.
  void createMetaIteratorIfNull();

  // If data_iterator_ is currently nullptr or AT_END, move to the next/previous
  // partition until we reach the end or see a record with lsn >= `current_lsn`
  // (<= if `forward` = false) that passes `filter`. Also assigns state_.
  //
  // Called after doing a seek/next/prev/whatever on data_iterator_.
  // This is needed even when there is no filtering because it's possible
  // for a partition to have directory entry for some log but no records for
  // that log.
  // `current_lsn` is needed to make sure next() always moves to a higher lsn,
  // seek() always seeks to lsn >= `lsn` etc, even if a record with lower lsn
  // was written to a higher partition during the operation.
  // The last 3 args are only used for filtered operations, and calling
  // the function with them will result in the iterator positioned on
  // a record that matches the filter (if there is one and the read byte limit
  // is not exceeded before it is found). Calling the function with filtering
  // args is only compatible with forward==true.
  // If filtering args are set, data_iterator_ may be nullptr. This indicates
  // that the current_ partition was filtered out, and we need to skip to the
  // next non-filtered one.
  // If skip_current is true, the current partition and data_iterator_ won't
  // be considered, and we'll move on to the next partition right away.
  void moveUntilValid(bool forward,
                      lsn_t current_lsn,
                      ReadFilter* filter = nullptr,
                      ReadStats* stats = nullptr,
                      bool skip_current = false);

  void seekToPartitionBeforeOrAfter(lsn_t lsn,
                                    partition_id_t partition_id,
                                    bool after);

  // Prints a warning if the meta_iterator_ is pointing to a logsdb directory
  // with invalid value. Assumes that the key is valid.
  void checkDirectoryValue() const;

  // Sets accessed_underreplicated_region_ to true if any partition
  // in [start, end] is underreplicated.
  // `start.partition_ == nullptr` means -infinity.
  // `  end.partition_ == nullptr` means +infinity.
  void checkAccessedUnderReplicatedRegion(PartitionInfo start,
                                          PartitionInfo end);

  // Gets time range of current_.partition_ and calls
  // filter.shouldProcessTimeRange() with it.
  bool checkFilterTimeRange(ReadFilter& filter,
                            RecordTimestamp* out_min_ts,
                            RecordTimestamp* out_max_ts);
  // Asserts that data_iterator_'s time range is contained in the time range of
  // current_. Called before each filtered operation on data_iterator_.
  void assertDataIteratorHasCorrectTimeRange();

  // Which partition data_iterator_ currently points to.
  // Shouldn't be destroyed before data_iterator_.
  // Whoever changes current_ is responsible for deleting data_iterator_ if
  // current_.partition_ changes.
  //
  // If current_->partition is non-null, data_iterator_ is non-null too. There
  // is one exception: if ReadFilter::shouldProcessTimeRange() returned false,
  // data_iterator_ will be null, but current_->partition_ will point to the
  // partition that got filtered out. This discrepancy is always reconciled by
  // moveUntilValid() before next()/seek() returns.
  PartitionInfo current_;

  // What we currently believe to be latest partition having data for this log.
  PartitionInfo latest_;

  // Last seen id of the oldest existing partition.
  partition_id_t oldest_partition_id_ = PARTITION_INVALID;

  // If meta_iterator_'s status() is not ok, state() returns ERROR or WOULDBLOCK
  // based on meta_iterator_. Otherwise, state() returns state_.
  IteratorState state_ = IteratorState::AT_END;

  const logid_t log_id_;

  // Metadata iterator (used to read from the directory).
  // Can't use tailing iterator because tailing iterators don't support Prev().
  // This iterator is invalidated each time a partition is created or dropped.
  folly::Optional<RocksDBIterator> meta_iterator_;

  // Data iterator (reads from an individual column family).
  std::unique_ptr<RocksDBLocalLogStore::CSIWrapper> data_iterator_;

  const PartitionedRocksDBStore* pstore_;
  const LocalLogStore::ReadOptions options_;

  // Reset to false during seek operations if all partitions in the range
  // [starting partition, destination partition] are fully replicated.
  // Set to true if next() transits an under-replicated partition.
  //
  // This sticky behavior may over report under-replication, but avoids
  // signaling that the current log strand is fully-replicated in the case
  // where a seek would have stayed in the same, under-replicated, partition
  // if records had not been lost.
  bool accessed_underreplicated_region_ = false;
};

class PartitionedRocksDBStore::PartitionedAllLogsIterator
    : public LocalLogStore::AllLogsIterator {
 public:
  struct PartitionedLocation : public Location {
    // PARTITION_INVALID means unpartitioned.
    partition_id_t partition = PARTITION_INVALID;
    logid_t log = LOGID_INVALID;
    lsn_t lsn = LSN_INVALID;

    PartitionedLocation() = default;
    PartitionedLocation(partition_id_t partition, logid_t log, lsn_t lsn)
        : partition(partition), log(log), lsn(lsn) {}

    std::string toString() const override {
      // "p42 1234 e420n1337"
      return "p" + std::to_string(partition) + " " + std::to_string(log.val_) +
          " " + lsn_to_string(lsn);
    }
  };

  PartitionedAllLogsIterator(
      const PartitionedRocksDBStore* pstore,
      const LocalLogStore::ReadOptions& options,
      const folly::Optional<
          std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>& logs);

  IteratorState state() const override;

  logid_t getLogID() const override;
  lsn_t getLSN() const override;
  Slice getRecord() const override;
  std::unique_ptr<Location> getLocation() const override;
  // Only supported if filter_using_directory_ is true (i.e. if `logs` parameter
  // was passed to constructor).
  double getProgress() const override;

  void seek(const Location& location,
            ReadFilter* filter = nullptr,
            ReadStats* stats = nullptr) override;
  void next(ReadFilter* filter = nullptr, ReadStats* stats = nullptr) override;

  std::unique_ptr<Location> minLocation() const override;
  std::unique_ptr<Location> metadataLogsBegin() const override;

  void invalidate() override;

  const LocalLogStore* getStore() const override;

  size_t getIOBytesUnnormalized() const override {
    return RocksDBLogStoreBase::getIOBytesUnnormalized();
  }

 private:
  // This implements the "move until valid"-style logic of stepping through
  // directory and partitions until we find a suitable record or reach a limit.
  //
  // The initial seek target is given by `seek_to` and either
  // current_entry_ (if filter_using_directory_ is true) or
  // current_partition_id (if filter_using_directory_ is false).
  //
  // This method only reads from partitions. The unpartitioned column family is
  // handled by seek() and next() directly.
  void seekToCurrentEntry(partition_id_t current_partition_id,
                          PartitionedLocation seek_to,
                          ReadFilter* filter,
                          ReadStats* stats);

  // Fills out progress_lookup_. Called once in constructor.
  void buildProgressLookupTable();

  using LogDirectoryEntry = std::pair<logid_t, DirectoryEntry>;

  const PartitionedRocksDBStore* pstore_;
  const LocalLogStore::ReadOptions options_;

  // Latest partition at the time of iterator creation. This is just an
  // optimization to prevent rebuilding from unnecessarily reading partitions
  // that were created after the rebuilding started.
  const partition_id_t last_partition_id_;

  // Used by getProgress(). Same length as directory_. If the iterator is
  // on directory entry i, getProgress() reports progress_lookup_[i].
  // Precalculated based on data size estimates in directory.
  // If filter_using_directory_, progress estimation is not supported.
  std::vector<double> progress_lookup_;

  // If true, we'll keep a copy of logsdb directory and call
  // ReadFilter::shouldProcessRecordRange() based on it.
  const bool filter_using_directory_;

  // Copy of the logsdb directory for the requested logs. Only populated if
  // filter_using_directory_ is true.
  std::vector<LogDirectoryEntry> directory_;
  // Index in directory_ at which we're currently positioned.
  std::vector<LogDirectoryEntry>::iterator current_entry_;

  // nullptr if we're either AT_END or in unpartitioned column family.
  PartitionPtr current_partition_;
  // nullptr if we're AT_END.
  // Points into the CF owned by current_partition_. Therefore, whenever
  // current_partition_ pointer is changed, data_iterator_ needs to be reset
  // first.
  std::unique_ptr<RocksDBLocalLogStore::CSIWrapper> data_iterator_;
};

// Behaves as two nested iterators.
// The outer iterator is over logs that have records in any partition.
// The inner iterator is over partitions that have records for current log.
// Skips latest partition.
// Used for updating trim points and checking how many partitions are fully
// behind trim points.
// Typical use:
// while (it.nextLog()) {
//   while (it.nextPartition()) {
//     ...
//   }
// }
class PartitionedRocksDBStore::PartitionDirectoryIterator {
 public:
  // Initially not pointed to any log. Call nextLog() before anything else.
  explicit PartitionDirectoryIterator(PartitionedRocksDBStore& store)
      : store_(store),
        latest_partition_(store_.latest_.get()->id_),
        meta_it_(store_.createMetadataIterator()) {}

  // Returns true if rocksdb reported an error, and this iterator is in a bad
  // state. If this is true, next*() will do nothing and return false.
  bool error();

  // Go to next log that occurs in non-latest partitions. After that
  // the iterator is not pointed to any partition. Call nextPartition() to
  // advance iterator to the first partition for this log.
  // Amortized complexity is one RocksDB seek for each log
  // (whether it occurs in non-latest partitions or not).
  // @return false if at end
  bool nextLog();
  logid_t getLogID();

  // Go to next partition for current log.
  // Complexity is one RocksDB seek.
  // @return false if at end
  bool nextPartition();
  partition_id_t getPartitionID();

  // Unlike above methods, the following are not trivial wrappers of rocksdb
  // iterator and PartitionDirectoryKey. They get data from either the next
  // directory entry or (if this is the last partition for current log) by
  // reading the last record for this log in this partition.

  // Upper bound of maximum lsn of records for current log in
  // current partition. Can return LSN_INVALID if partition is already dropped.
  // Complexity is O(1) in typical case, and
  // O(RocksDB seek and reading a record) if it's the last partition having
  // this log (but not latest partition overall).
  lsn_t getLastLSN();

 private:
  PartitionedRocksDBStore& store_;
  partition_id_t latest_partition_;
  // Positioned on _next_ partition (so that we can get last_* from it).
  RocksDBIterator meta_it_;
  logid_t log_id_{0};
  partition_id_t partition_id_{PARTITION_INVALID};
  lsn_t first_lsn_;

  lsn_t last_lsn_;

  bool itValid();
  logid_t itLogID();
};

}} // namespace facebook::logdevice
