/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

#include <cstdlib>
#include <ctime>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include <folly/Memory.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/IteratorSearch.h"
#include "logdevice/server/locallogstore/RocksDBEnv.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"
#include "logdevice/server/locallogstore/WriteOps.h"

namespace facebook { namespace logdevice {

/**
 * This should be bumped any time an incompatible change is made to the
 * schema.  RocksDBLocalLogStore::create() will fail if the database version
 * does not match this constant.
 */
static const int SCHEMA_VERSION = 2;

using Direction = RocksDBLocalLogStore::CSIWrapper::Direction;
using Location = RocksDBLocalLogStore::CSIWrapper::Location;

using namespace RocksDBKeyFormat;

RocksDBLocalLogStore::RocksDBLocalLogStore(uint32_t shard_idx,
                                           uint32_t num_shards,
                                           const std::string& path,
                                           RocksDBLogStoreConfig rocksdb_config,
                                           StatsHolder* stats,
                                           IOTracing* io_tracing)
    : RocksDBLogStoreBase(shard_idx,
                          num_shards,
                          path,
                          std::move(rocksdb_config),
                          stats,
                          io_tracing) {
  rocksdb::DB* db;
  rocksdb::Status status;

  status = rocksdb::DB::Open(rocksdb_config_.options_, path, &db);

  if (!status.ok()) {
    ld_error("could not open RocksDB store at \"%s\",  Open() failed with "
             "error \"%s\"",
             path.c_str(),
             status.ToString().c_str());
    noteRocksDBStatus(status, "Open()");
    throw ConstructorFailed();
  }

  db_.reset(db);
  if (checkSchemaVersion(db, db->DefaultColumnFamily(), SCHEMA_VERSION) != 0) {
    throw ConstructorFailed();
  }
}

int RocksDBLocalLogStore::getHighestInsertedLSN(logid_t, lsn_t*) {
  err = E::NOTSUPPORTED;
  return -1;
}

int RocksDBLocalLogStore::getApproximateTimestamp(
    logid_t log_id,
    lsn_t lsn,
    bool allow_blocking_io,
    std::chrono::milliseconds* timestamp_out) {
  LocalLogStore::ReadOptions options(
      "RocksDBLocalLogStore::getApproximateTimestamp");
  options.fill_cache = false;
  options.allow_blocking_io = allow_blocking_io;
  std::unique_ptr<LocalLogStore::ReadIterator> it = read(log_id, options);
  it->seek(lsn);
  switch (it->state()) {
    case IteratorState::AT_RECORD:
      break;
    case IteratorState::WOULDBLOCK:
      ld_check(!allow_blocking_io);
      err = E::WOULDBLOCK;
      return -1;
    case IteratorState::AT_END:
      err = E::NOTFOUND;
      return -1;
    case IteratorState::LIMIT_REACHED:
    case IteratorState::MAX:
      ld_check(false);
      FOLLY_FALLTHROUGH;
    case IteratorState::ERROR:
      err = E::FAILED;
      return -1;
  }
  auto record = it->getRecord();
  int rv = LocalLogStoreRecordFormat::parseTimestamp(record, timestamp_out);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Unable to parse record for log %lu, lsn %lu: %s",
                    log_id.val_,
                    lsn,
                    error_name(err));
    err = E::FAILED;
    return -1;
  }
  return 0;
}

// Helper method that returns true if we should read the copyset index for
// a particular ReadIterator
static bool shouldUseCSIIterator(folly::Optional<logid_t> /*log_id*/,
                                 const RocksDBSettings& settings,
                                 const LocalLogStore::ReadOptions& read_opts) {
  // TODO (t9068316): when we stop writing copysets to records, we cannot skip
  // reading the copysets from the copyset index if sticky copysets are enabled
  // for this log.

  // Not using CSI if it is disabled in settings
  if (!settings.use_copyset_index) {
    return false;
  }

  return read_opts.allow_copyset_index;
}

std::unique_ptr<LocalLogStore::ReadIterator>
RocksDBLocalLogStore::read(logid_t log_id,
                           const LocalLogStore::ReadOptions& options_in) const {
  return std::make_unique<CSIWrapper>(this, log_id, options_in, nullptr);
}
std::unique_ptr<LocalLogStore::AllLogsIterator>
RocksDBLocalLogStore::readAllLogs(
    const LocalLogStore::ReadOptions& options_in,
    const folly::Optional<
        std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>&) const {
  return std::make_unique<AllLogsIteratorImpl>(
      std::make_unique<CSIWrapper>(this, folly::none, options_in, nullptr));
}

void RocksDBLocalLogStore::updateManualCompactTime() {
  atomic_fetch_max(last_manual_compact_time_,
                   std::chrono::steady_clock::now().time_since_epoch());
}

int RocksDBLocalLogStore::readAllLogSnapshotBlobs(
    LogSnapshotBlobType type,
    LogSnapshotBlobCallback callback) {
  return readAllLogSnapshotBlobsImpl(
      type, callback, db_->DefaultColumnFamily());
}

int RocksDBLocalLogStore::writeLogSnapshotBlobs(
    LogSnapshotBlobType snapshots_type,
    const std::vector<std::pair<logid_t, Slice>>& snapshots) {
  if (getSettings()->read_only) {
    return -1;
  }

  return writer_->writeLogSnapshotBlobs(
      db_->DefaultColumnFamily(), snapshots_type, snapshots);
}

int RocksDBLocalLogStore::deleteAllLogSnapshotBlobs() {
  // TODO 12730327: Implement iterating delete. Note that it needs to be atomic,
  // i.e. delete everything in one batch.
  // For now return success to let tests pass. However, we should never
  // enable persisting record cache on non-logsdb use cases in production.
  // It is reasonable and we are deprecating non-logsdb local logstores.
  return 0;
}

int RocksDBLocalLogStore::performCompaction() {
  rocksdb::Status status =
      db_->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr);
  if (!status.ok()) {
    enterFailSafeIfFailed(status, "CompactRange()");
    return -1;
  }
  ld_debug(
      "Performed manual compaction on RocksDB shard: %s.", getDBPath().c_str());
  updateManualCompactTime();
  return 0;
}

int RocksDBLocalLogStore::findTime(
    logid_t log_id,
    std::chrono::milliseconds timestamp,
    lsn_t* lo,
    lsn_t* hi,
    bool /*approximate*/,
    bool allow_blocking_io,
    std::chrono::steady_clock::time_point deadline) const {
  IteratorSearch search(this,
                        db_->DefaultColumnFamily(),
                        FIND_TIME_INDEX,
                        timestamp.count(),
                        std::string(""),
                        log_id,
                        *lo,
                        *hi,
                        allow_blocking_io,
                        deadline);
  return search.execute(lo, hi);
}

int RocksDBLocalLogStore::findKey(logid_t log_id,
                                  std::string key,
                                  lsn_t* lo,
                                  lsn_t* hi,
                                  bool /*approximate*/,
                                  bool allow_blocking_io) const {
  IteratorSearch search(this,
                        db_->DefaultColumnFamily(),
                        FIND_KEY_INDEX,
                        0,
                        std::move(key),
                        log_id,
                        *lo,
                        *hi,
                        allow_blocking_io);
  return search.execute(lo, hi);
}

// Accounting operations for RocksDB Seek()/SeekForPrev()/Next()/Prev()
// operations
#define ROCKSDB_COUNT_STAT(name, amount) \
  STAT_ADD(getStatsHolder(), read_streams_rocksdb_locallogstore_##name, amount)

#define ROCKSDB_ACCOUNTED_SEEK(iterator, slice, name) \
  ROCKSDB_ACCOUNTED_OP(iterator, seek, Seek(slice), name)
#define ROCKSDB_ACCOUNTED_SEEK_FOR_PREV(iterator, slice, name) \
  ROCKSDB_ACCOUNTED_OP(iterator, seek, SeekForPrev(slice), name)
#define ROCKSDB_ACCOUNTED_NEXT(iterator, name) \
  ROCKSDB_ACCOUNTED_OP(iterator, next, Next(), name)
#define ROCKSDB_ACCOUNTED_PREV(iterator, name) \
  ROCKSDB_ACCOUNTED_OP(iterator, prev, Prev(), name)

#define ROCKSDB_ACCOUNTED_OP(iterator, opname, op, itname)                  \
  do {                                                                      \
    auto* perf_context = rocksdb::get_perf_context();                       \
    uint64_t prev_reads = perf_context->block_read_byte;                    \
    uint64_t prev_read_count = perf_context->block_read_count;              \
    uint64_t prev_hits = perf_context->block_cache_hit_count;               \
    (iterator)->op;                                                         \
    ROCKSDB_COUNT_STAT(itname##_##opname##_reads, 1);                       \
    if (prev_reads != perf_context->block_read_byte) {                      \
      ROCKSDB_COUNT_STAT(itname##_##opname##_reads_from_disk, 1);           \
      ROCKSDB_COUNT_STAT(itname##_##opname##_block_bytes_read_from_disk,    \
                         perf_context->block_read_byte - prev_reads);       \
      ROCKSDB_COUNT_STAT(itname##_##opname##_blocks_read_from_disk,         \
                         perf_context->block_read_count - prev_read_count); \
    }                                                                       \
    if (prev_hits != perf_context->block_cache_hit_count) {                 \
      ROCKSDB_COUNT_STAT(itname##_##opname##_reads_from_block_cache, 1);    \
      ROCKSDB_COUNT_STAT(itname##_##opname##_blocks_read_from_block_cache,  \
                         perf_context->block_cache_hit_count - prev_hits);  \
    }                                                                       \
  } while (0)

namespace {
uint64_t getIteratorVersion(RocksDBIterator* iterator,
                            const RocksDBSettings& settings) {
  if (!settings.track_iterator_versions) {
    return LLONG_MAX;
  }
  std::string string_val;
  if (!iterator
           ->GetProperty("rocksdb.iterator.super-version-number", &string_val)
           .ok()) {
    return LLONG_MAX;
  }
  return static_cast<uint64_t>(std::atoi(string_val.c_str()));
}
} // namespace

class RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator
    : public TrackableIterator {
 public:
  explicit CopySetIndexIterator(const CSIWrapper* parent);
  ~CopySetIndexIterator() override;

  IteratorState state() const;

  void seek(Location, Direction dir);
  void step(Direction dir);

  // Destroys the rocksdb iterator.
  void invalidate();

  // returns log ID and LSN of the current entry
  Location getLocation() const;

  // Information from the current entry.
  uint32_t getCurrentWave() const;
  const std::vector<ShardID>& getCurrentCopySet() const;
  LocalLogStoreRecordFormat::csi_flags_t getCurrentFlags() const;

  // returns the size of the copyset index entry in bytes
  size_t getCurrentEntrySize();

  // returns record of current entry constructed with CSI data only
  Slice getCurrentRecord() const;

  StatsHolder* getStatsHolder() const {
    return parent_->getStatsHolder();
  }

  const LocalLogStore* getStore() const override {
    return parent_->store_;
  }

 private:
  // creates the iterator, if there isn't any
  void createIteratorIfNeeded();
  // Checks status of iterator_ and sets state_ accordingly.
  // If iterator_ points to a record, parses it and sets
  // current_single_lsn_, current_single_copyset_ etc.
  void checkIteratorAndParseCurrentEntry();

  const CSIWrapper* parent_;

  // Iterator to the copyset index contents
  folly::Optional<RocksDBIterator> iterator_;

  // What state() should return.
  IteratorState state_ = IteratorState::ERROR;

  // Information from the current single entry the iterator is pointing to.
  logid_t current_single_log_id_{LOGID_INVALID};
  lsn_t current_single_lsn_{LSN_INVALID};
  uint32_t current_single_wave_{0};
  std::vector<ShardID> current_single_copyset_;
  LocalLogStoreRecordFormat::csi_flags_t current_single_flags_{0};

  // Size of the copyset index entry (NB: not the record it represents) in
  // bytes. Used to apply disk i/o limits when reading.
  size_t current_entry_size_{0};

  // Holding a representation of complete record but only with data from CSI
  // used for getRecord
  mutable std::string current_record_;

  // the upper-bound value for the copyset index iterator. See
  // RocksDBLogStoreBase::LastKey
  RocksDBKeyFormat::CopySetIndexKey last_key_;

  // slice pointing into last_key_;
  rocksdb::Slice upper_bound_{nullptr, 0};

  rocksdb::ReadOptions rocks_options_;
};

class RocksDBLocalLogStore::CSIWrapper::DataIterator
    : public TrackableIterator {
 public:
  explicit DataIterator(const CSIWrapper* parent);

  IteratorState state() const;

  void seek(Location loc, Direction dir);
  void step(Direction dir);

  Location getLocation() const;
  Slice getRecord() const;

  // Releases the rocksdb Iterator, unpinning the blocks.
  // After this, before using the iterator again, you'll need to seek() this
  // iterator, which would create a new underlying iterator.
  void releaseIterator();

  StatsHolder* getStatsHolder() const {
    return parent_->getStatsHolder();
  }

  const LocalLogStore* getStore() const override {
    return parent_->store_;
  }

  // TODO (#10357210): Temporary, delet.
  // When handleKeyFormatMigration() skips over a dangling amend, it assigns its
  // location to this field, unless it's already assigned.
  folly::Optional<Location> skipped_dangling_amend_;

 private:
  void createIteratorIfNeeded();

  const CSIWrapper* parent_;

  IterateUpperBoundHelper upper_bound_;
  rocksdb::ReadOptions rocks_options_;

  folly::Optional<RocksDBIterator> iterator_;

  // TODO (#10357210):
  //   merged_value_, skipped_dangling_amend_ and handleKeyFormatMigration() are
  //   a temporary compatibility measure. Remove them after migrating to new
  //   rocksdb key format. Remove the #include of RocksDBWriterMergeOperator.h
  //   too.

  // If not empty, getRecord() returns this string instead of iterator_'s value.
  std::string merged_value_;

  // Called after moving iterator forward.
  // If the current LSN has both an old-format and a new-format key, merges the
  // values for these keys together, into merged_value_.
  // There are some correctness caveats:
  //  - For performance, the merging is only done if the new-format record is
  //    an amend. If it's a full record, we just use that record without looking
  //    for an old-format record. In particular, if you downgrade to a version
  //    that writes in the old format, that version sometimes won't be able to
  //    amend records.
  //  - The merging is only done when moving iterator forward. seekForPrev()
  //    and prev() will just return the first value they see. This should be
  //    ok because backwards iteration is only used for finding LNG (which is
  //    immutable) and tail record (which is not very important).
  void handleKeyFormatMigration();
};

RocksDBLocalLogStore::CSIWrapper::CSIWrapper(
    const RocksDBLogStoreBase* store,
    folly::Optional<logid_t> log_id,
    const LocalLogStore::ReadOptions& read_opts,
    rocksdb::ColumnFamilyHandle* cf)
    : ReadIterator(store), log_id_(log_id), read_opts_(read_opts), cf_(cf) {
  if (cf_ == nullptr) {
    cf_ = getDB().DefaultColumnFamily();
  }

  registerTracking(cf_->GetName(),
                   log_id.value_or(LOGID_INVALID),
                   read_opts.tailing,
                   read_opts.allow_blocking_io,
                   IteratorType::CSI_WRAPPER,
                   read_opts.tracking_ctx);
  bool use_csi = shouldUseCSIIterator(log_id, *store->getSettings(), read_opts);
  if (use_csi) {
    csi_iterator_ = std::make_unique<CopySetIndexIterator>(this);
  }
  // If ReadOptions tell to only read from CSI, but CSI is disabled,
  // disregard the ReadOptions and read data.
  if (!use_csi || !read_opts.csi_data_only) {
    data_iterator_ = std::make_unique<DataIterator>(this);
  }
  ld_check(csi_iterator_ != nullptr || data_iterator_ != nullptr);
}

RocksDBLocalLogStore::CSIWrapper::~CSIWrapper() {}

IteratorState RocksDBLocalLogStore::CSIWrapper::state() const {
  return state_;
}

Location RocksDBLocalLogStore::CSIWrapper::getLocation() const {
  if (state_ == IteratorState::LIMIT_REACHED) {
    ld_check(limit_reached_.valid());
    return limit_reached_.location;
  }
  ld_check_eq(state_, IteratorState::AT_RECORD);
  if (data_iterator_ != nullptr) {
    return data_iterator_->getLocation();
  }
  ld_check(csi_iterator_ != nullptr);
  return csi_iterator_->getLocation();
}

lsn_t RocksDBLocalLogStore::CSIWrapper::getLSN() const {
  return getLocation().lsn;
}

logid_t RocksDBLocalLogStore::CSIWrapper::getLogID() const {
  return getLocation().log_id;
}

Slice RocksDBLocalLogStore::CSIWrapper::getRecord() const {
  ld_check_eq(state_, IteratorState::AT_RECORD);
  if (data_iterator_ != nullptr) {
    return data_iterator_->getRecord();
  }
  ld_check(csi_iterator_ != nullptr);
  return csi_iterator_->getCurrentRecord();
}

bool RocksDBLocalLogStore::CSIWrapper::accessedUnderReplicatedRegion() const {
  return false;
}

void RocksDBLocalLogStore::CSIWrapper::invalidate() {
  state_ = IteratorState::ERROR;
  if (csi_iterator_ != nullptr) {
    csi_iterator_->invalidate();
  }
  if (data_iterator_ != nullptr) {
    data_iterator_->releaseIterator();
  }
  trackIteratorRelease();
}

void RocksDBLocalLogStore::CSIWrapper::setContextString(const char* str) {
  TrackableIterator::setContextString(str);
  if (data_iterator_) {
    data_iterator_->setContextString(str);
  }
  if (csi_iterator_) {
    csi_iterator_->setContextString(str);
  }
}

void RocksDBLocalLogStore::CSIWrapper::seek(logid_t log,
                                            lsn_t lsn,
                                            ReadFilter* filter,
                                            ReadStats* stats) {
  SCOPED_IO_TRACING_CONTEXT_FROM_ITERATOR(
      this, filter ? "seek(filtered)" : "seek");
  // Note: if state_ == LIMIT_REACHED, and we're seeking to exactly where the
  // limit was reached, we could set near=true to tell moveTo() to resume
  // where the last operation left off and avoid the initial seeks.
  // But we choose not to do that, and effectively force seek() to always start
  // by seeking csi iterator. This prevents csi iterator from pinning an old
  // memtable indefinitely.
  moveTo(Location(log, lsn),
         Direction::FORWARD,
         /* near */ false,
         filter,
         stats);
  trackSeek(lsn, 0);
}

void RocksDBLocalLogStore::CSIWrapper::seek(lsn_t lsn,
                                            ReadFilter* filter,
                                            ReadStats* stats) {
  ld_check(log_id_.hasValue());
  seek(log_id_.value(), lsn, filter, stats);
}

void RocksDBLocalLogStore::CSIWrapper::seekForPrev(lsn_t lsn) {
  SCOPED_IO_TRACING_CONTEXT_FROM_ITERATOR(this, "seekForPrev");
  ld_check(log_id_.hasValue());
  moveTo(Location(log_id_.value(), lsn),
         Direction::BACKWARD,
         /* near */ false,
         nullptr,
         nullptr);
  trackSeek(lsn, 0);
}

void RocksDBLocalLogStore::CSIWrapper::next(ReadFilter* filter,
                                            ReadStats* stats) {
  ld_check_in(
      state_, ({IteratorState::AT_RECORD, IteratorState::LIMIT_REACHED}));
  SCOPED_IO_TRACING_CONTEXT_FROM_ITERATOR(
      this, filter ? "next(filtered)" : "next");
  Location loc = state_ == IteratorState::LIMIT_REACHED
      ? limit_reached_.location
      : getLocation().advance(Direction::FORWARD, log_id_);
  moveTo(loc,
         Direction::FORWARD,
         /* near */ true,
         filter,
         stats);
}

void RocksDBLocalLogStore::CSIWrapper::prev() {
  SCOPED_IO_TRACING_CONTEXT_FROM_ITERATOR(this, "prev");
  moveTo(getLocation().advance(Direction::BACKWARD, log_id_),
         Direction::BACKWARD,
         /* near */ true,
         nullptr,
         nullptr);
}

// "Seeks" the iterator to the given logid/lsn using either a series of
// next()/prev() calls or a single seek()/seekForPrev() call.
// `It` should be either CopySetIndexIterator or DataIterator.
// @param near  If false, just do a seek. If true, try a few steps, then
//              fall back to a seek.
template <typename It>
static void adaptiveSeek(It& it,
                         Location loc,
                         Direction dir,
                         bool near,
                         StatsHolder* stats) {
  if (near) {
    ld_check_eq(IteratorState::AT_RECORD, it.state());
    int count = 0;
    while (it.state() == IteratorState::AT_RECORD &&
           it.getLocation().before(loc, dir)) {
      if (++count > 10) { // TODO: named constant
        near = false;
        STAT_INCR(stats, csi_unexpectedly_long_skips);
        break;
      }

      it.step(dir);
    }
  }
  if (!near) {
    it.seek(loc, dir);
  }
}

// Helper function to parse record and run ReadFilter on it.
// Returns true if the record
//  (a) is not a dangling amend (i.e. doesn't have FLAG_AMEND), and
//  (b) passes the filter (or filter is nullptr).
// If the record is malformed, conservatively returns true.
// If @param csi_copyset is not nullptr, also checks the record's copyset
// and flags against copyset and flags from copyset index. Logs an error
// if they don't match.
bool RocksDBLocalLogStore::CSIWrapper::applyFilterToDataRecord(
    Location loc,
    Slice record_blob,
    ReadFilter* filter,
    CopySetIndexIterator* csi_it) {
  // Parse the flags, timestamp and copyset of the record.
  // Can we not do this when filter is nullptr? No, because we still need to
  // get record's flags to check whether it's a dangling amend.
  // Getting record's flags, as currently implemented, is only marginally
  // faster than getting all the things.
  std::array<ShardID, COPYSET_SIZE_MAX> copyset;
  copyset_size_t copyset_size;
  LocalLogStoreRecordFormat::flags_t flags;
  LocalLogStoreRecordFormat::csi_flags_t converted_flags;
  std::chrono::milliseconds timestamp;
  uint32_t wave;
  int rv = LocalLogStoreRecordFormat::parse(record_blob,
                                            &timestamp,
                                            nullptr,
                                            &flags,
                                            &wave,
                                            &copyset_size,
                                            &copyset[0],
                                            copyset.size(),
                                            nullptr,
                                            nullptr,
                                            nullptr,
                                            getShardIdx());

  if (rv != 0) {
    // If we can't parse the record, be conservative and assume it would
    // pass the filter.
    return true;
  }
  if (flags & LocalLogStoreRecordFormat::FLAG_AMEND) {
    // Copyset amendment pseudorecord not backed by an actual record.
    // This can happen e.g. if we got an amend after the record was trimmed.
    RATELIMIT_DEBUG(std::chrono::seconds(10),
                    5,
                    "Skipping dangling amend %s. Flags: %s, blob: %s",
                    RecordID(loc.lsn, loc.log_id).toString().c_str(),
                    LocalLogStoreRecordFormat::flagsToString(flags).c_str(),
                    hexdump_buf(record_blob, 200).c_str());
    STAT_INCR(getStatsHolder(), dangling_amends_seen);
    return false;
  }

  converted_flags = LocalLogStoreRecordFormat::formCopySetIndexFlags(flags);

  if (csi_it != nullptr) {
    // Check that information in CSI entry matches information in data record.
    const auto& csi_cs = csi_it->getCurrentCopySet();
    if (converted_flags != csi_it->getCurrentFlags() ||
        copyset_size != csi_cs.size() ||
        memcmp(csi_cs.data(), &copyset[0], copyset_size * sizeof(copyset[0])) ||
        wave != csi_it->getCurrentWave()) {
      // Mismatch. This can happen if the CSI and data iterator are using
      // different snapshots of the DB.
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          2,
          "Mismatch between record and copyset index entry %s. "
          "Record flags: %s, csi flags: %s, "
          "record copyset: [%s], csi copyset: %s, "
          "record wave: %u, csi wave: %u",
          RecordID(loc.lsn, loc.log_id).toString().c_str(),
          LocalLogStoreRecordFormat::flagsToString(flags).c_str(),
          LocalLogStoreRecordFormat::csiFlagsToString(csi_it->getCurrentFlags())
              .c_str(),
          rangeToString(copyset.begin(), copyset.begin() + copyset_size)
              .c_str(),
          toString(csi_cs).c_str(),
          wave,
          csi_it->getCurrentWave());
      STAT_INCR(getStatsHolder(), csi_contents_mismatch);
    }
  }

  if (!filter) {
    return true;
  }

  return (*filter)(loc.log_id,
                   loc.lsn,
                   &copyset[0],
                   copyset_size,
                   converted_flags,
                   RecordTimestamp(timestamp),
                   RecordTimestamp(timestamp));
}

void RocksDBLocalLogStore::CSIWrapper::moveTo(const Location& target,
                                              Direction dir,
                                              bool near,
                                              ReadFilter* filter,
                                              ReadStats* stats) {
  // The high-level logic of this method is simple: in a loop, we seek/move both
  // iterators to the target Location and apply the filter (if any).
  //  * If one of the iterators says that the location doesn't have a record, we
  //    advance the target to what the iterator thinks is the next record and
  //    try again.
  //  * If the record is rejected by the filter, we advance the target to the
  //    next LSN and try again.
  //  * If the two iterators agree that there's a record, and it passes the
  //    filter, we're done.

  if (near) {
    ld_check_in(
        state_, ({IteratorState::AT_RECORD, IteratorState::LIMIT_REACHED}));
  }

  // Candidate LSN to which we want to move both iterators and see if it's
  // good. All LSNs between `target` and `current` have already been ruled
  // out. If this LSN turns out to not contain a record, or contain an invalid
  // record or be filtered out, we'll keep advancing `current` in Direction
  // dir until we get to a good state.
  Location current = target;

  // True if the corresponding iterator points to `current`.
  bool csi_iterator_good = false;
  bool data_iterator_good = false;

  // True if the corresponding iterator points to a record with lsn before,
  // but not far before, current. "Not far" in the sense that it's probably
  // cheaper to do a series of next()/prev() calls rather than
  // a seek()/seekForPrev().
  bool csi_near = near;
  bool data_near = near;

  if (near && state_ == IteratorState::LIMIT_REACHED) {
    // We're continuing from where previous moveTo() reached limit and stopped.
    // Unpack its state.
    ld_check(limit_reached_.valid());
    ld_check(current == limit_reached_.location);
    csi_near = limit_reached_.csi_near;
    data_near = limit_reached_.data_near;
  }

  // True if `current` has been filtered out, based on either CSI or data.
  bool current_is_filtered_out = false;

  if (stats) {
    // We are executing a seek/next that may land us on an LSN that is less than
    // stats->last_read.second. This can happen if the caller has previously
    // seen a record with LSN above read limit but for some reason proceeded to
    // make another seek anyway, usually using a different iterator.
    // (As of the time of writing the only way this can happen is when logsdb
    // iterator discards an "orphaned" record and has to seek an iterator in
    // the next partition even if the orphaned record was over the limit.)
    stats->last_read = std::make_pair(LOGID_INVALID, LSN_INVALID);
  }

  // Assigned right before return.
  state_ = IteratorState::MAX;
  limit_reached_.clear();

  SCOPE_EXIT {
    ld_check(state_ != IteratorState::MAX);
    if (state_ == IteratorState::AT_RECORD) {
      ld_check(!current_is_filtered_out);
      ld_check(!current.before(target, dir));
      if (csi_iterator_ != nullptr) {
        ld_check(csi_iterator_good);
        ld_check(csi_iterator_->getLocation() == current);
        if (filter) {
          ld_assert((*filter)(csi_iterator_->getLocation().log_id,
                              csi_iterator_->getLocation().lsn,
                              csi_iterator_->getCurrentCopySet().data(),
                              csi_iterator_->getCurrentCopySet().size(),
                              csi_iterator_->getCurrentFlags(),
                              min_ts_,
                              max_ts_));
        }
      }
      if (data_iterator_ != nullptr) {
        ld_check(data_iterator_good);
        ld_check(data_iterator_->getLocation() == current);
      }
    } else if (state_ == IteratorState::LIMIT_REACHED) {
      ld_check(stats && stats->readLimitReached());
      ld_check(limit_reached_.valid());
    }

    // To prevent data_iterator_ from pinning a memtable indefinitely,
    // each CSIWrapper seek should either seek or release data_iterator_.
    if (!near && !data_iterator_good && !data_near &&
        data_iterator_ != nullptr) {
      // There's a chance that data_iterator_ hasn't been seeked. Release it.
      data_iterator_->releaseIterator();
    } else {
      // Note that data_iterator_good || data_near means that data_iterator_
      // has been seeked, even if state_ ended up not AT_RECORD.
    }
  };

  auto limit_reached = [&] {
    state_ = IteratorState::LIMIT_REACHED;
    limit_reached_.location = current;
    limit_reached_.csi_near = csi_iterator_good || csi_near;
    limit_reached_.data_near = data_iterator_good || data_near;
  };

  while (true) {
    Location prev_current = current; // used for an assert

    if (current.at_end) {
      // Went over the smallest/largest possible LSN.
      state_ = IteratorState::AT_END;
      return;
    }

    if (stats && stats->readLimitReached()) {
      // Some limit reached. We should probably stop. But:
      //  * If we haven't made any progress, don't stop yet. Otherwise we
      //    could get into an infinite loop if the limit is set too small.
      //  * If we're doing a seek but know in advance that we're above
      //    timestamp
      //    window, stop immediately without even seeking any subiterators.
      //    This is useful for LogRebuilding: normally, timestamp window is
      //    aligned with partition boundaries, and this check prevents us from
      //    creating iterators in the partition we're not interested in.
      if (current != target || (!near && stats->hardLimitReached())) {
        limit_reached();
        return;
      }
    }

    // Move CSI iterator to `current` and see if it passes filter.
    if (csi_iterator_ != nullptr && !csi_iterator_good) {
      adaptiveSeek(*csi_iterator_, current, dir, csi_near, getStatsHolder());
      csi_iterator_good = true;
      csi_near = true;

      if (csi_iterator_->state() != IteratorState::AT_RECORD) {
        // Reached the end/error/wouldblock in CSI.
        state_ = csi_iterator_->state();
        return;
      }

      Location csi_loc = csi_iterator_->getLocation();
      if (csi_loc != current) {
        ld_check(current.before(csi_loc, dir));
        current = csi_loc;
        data_iterator_good = false;
        current_is_filtered_out = false;
      }

      const std::vector<ShardID>& cs = csi_iterator_->getCurrentCopySet();
      LocalLogStoreRecordFormat::csi_flags_t flags =
          csi_iterator_->getCurrentFlags();

      if (stats) {
        stats->countReadCSIEntry(
            csi_iterator_->getCurrentEntrySize(), csi_loc.log_id, csi_loc.lsn);
        // Don't run filter if CSI iterator is above max LSN.
        if (stats->hardLimitReached()) {
          limit_reached();
          return;
        }
      }

      // Running the filter on the copyset. If we're above lsn limit, don't run
      // filter and consider the record filtered out; we'll break out of the
      // loop after the countCSIEntry() below and hardLimitReached() check on
      // next iteration.
      ld_check(!current_is_filtered_out);
      current_is_filtered_out = (filter &&
                                 !(*filter)(current.log_id,
                                            current.lsn,
                                            cs.data(),
                                            cs.size(),
                                            flags,
                                            min_ts_,
                                            max_ts_));

      if (stats) {
        stats->countFilteredCSIEntry(!current_is_filtered_out);
      }

      if (current_is_filtered_out) {
        // If a CSI entry wass filtered out, chances are that many in a row
        // will be filtered out too (usually a sticky copyset block).
        // So we'll want to seek data_iterator_ rather than dragging it
        // through the skipped LSNs.
        data_near = false;
      }
    }

    // Move data iterator to `current` and see if it passes filter.
    if (data_iterator_ != nullptr && !data_iterator_good &&
        !current_is_filtered_out) {
      data_iterator_->skipped_dangling_amend_.clear();
      adaptiveSeek(*data_iterator_, current, dir, data_near, getStatsHolder());
      data_iterator_good = true;
      data_near = true;

      if (data_iterator_->state() != IteratorState::AT_RECORD) {
        // Reached the end/error/wouldblock in data.
        state_ = data_iterator_->state();
        if (csi_iterator_good && state_ == IteratorState::AT_END) {
          STAT_INCR(getStatsHolder(), read_streams_num_csi_skips_no_record);
          if (!data_iterator_->skipped_dangling_amend_.hasValue() ||
              data_iterator_->skipped_dangling_amend_.value() != current) {
            RATELIMIT_INFO(std::chrono::seconds(10),
                           2,
                           "Copyset index entry without a record at end of "
                           "partition, log_id %lu, lsn %s",
                           current.log_id.val_,
                           lsn_to_string(current.lsn).c_str());
          }
        }
        return;
      }

      Location data_loc = data_iterator_->getLocation();
      if (data_loc != current) {
        ld_check(current.before(data_loc, dir));
        if (csi_iterator_good) {
          STAT_INCR(getStatsHolder(), read_streams_num_csi_skips_no_record);
          if (!data_iterator_->skipped_dangling_amend_.hasValue() ||
              data_iterator_->skipped_dangling_amend_.value() != current) {
            RATELIMIT_INFO(std::chrono::seconds(10),
                           2,
                           "Copyset index entry without a record, log_id %lu, "
                           "lsn %s, next "
                           "data record lsn %s",
                           current.log_id.val_,
                           lsn_to_string(current.lsn).c_str(),
                           lsn_to_string(data_loc.lsn).c_str());
          }
        }
        current = data_loc;
        csi_iterator_good = false;
      }

      if (stats) {
        stats->countReadRecord(
            current.log_id, current.lsn, data_iterator_->getRecord().size);
        // Don't run filter if we're above LSN limit.
        // If csi_iterator_good, we already did this check above, no need
        // to check again.
        if (!UNLIKELY(csi_iterator_good) && stats->hardLimitReached()) {
          limit_reached();
          return;
        }
      }

      // Apply the filter and check for dangling amend.
      current_is_filtered_out = !applyFilterToDataRecord(
          data_loc,
          data_iterator_->getRecord(),
          filter,
          csi_iterator_good ? csi_iterator_.get() : nullptr);

      if (stats) {
        stats->countFilteredRecord(
            data_iterator_->getRecord().size, !current_is_filtered_out);
      }
    }

    if (!current_is_filtered_out &&
        (csi_iterator_ == nullptr || csi_iterator_good) &&
        (data_iterator_ == nullptr || data_iterator_good)) {
      // CSI and data iterators point to the same LSN, and the record is not
      // filtered out. We're done!
      state_ = IteratorState::AT_RECORD;
      return;
    }

    if (current_is_filtered_out) {
      current = current.advance(dir, log_id_);
      csi_iterator_good = false;
      data_iterator_good = false;
      current_is_filtered_out = false;
    } else {
      // The only way to get here is encountering a CSI entry without
      // corresponding record. Then data iterator ends up on the next record.
      ld_check(csi_iterator_ != nullptr);
      ld_check(data_iterator_ != nullptr);
      ld_check(!csi_iterator_good);
      ld_check(data_iterator_good);
    }

    // Check that we're making progress.
    ld_check(current.at_end || prev_current.before(current, dir));
  }

  ld_check(false);
}

RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::CopySetIndexIterator(
    const CSIWrapper* parent)
    : parent_(parent),
      last_key_(parent_->log_id_.value_or(LOGID_INVALID),
                std::numeric_limits<lsn_t>::max(),
                CopySetIndexKey::LAST_ENTRY_TYPE) {
  if (parent_->log_id_.hasValue() &&
      !parent_->getRocksDBStore()->getSettings()->disable_iterate_upper_bound) {
    upper_bound_ = rocksdb::Slice(
        reinterpret_cast<const char*>(&last_key_), sizeof(last_key_));
  }
  rocks_options_ = translateReadOptions(
      parent_->read_opts_, parent_->log_id_.hasValue(), &upper_bound_);
  registerTracking(parent_->cf_->GetName(),
                   parent_->log_id_.value_or(LOGID_INVALID),
                   rocks_options_.tailing,
                   rocks_options_.read_tier != rocksdb::kBlockCacheTier,
                   IteratorType::CSI,
                   parent_->read_opts_.tracking_ctx);
}

RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::
    ~CopySetIndexIterator() {
  if (iterator_.hasValue()) {
    STAT_INCR(
        parent_->getStatsHolder(), read_streams_num_csi_iterators_destroyed);
  }
}

void RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::
    createIteratorIfNeeded() {
  if (!iterator_.hasValue()) {
    iterator_ =
        parent_->getRocksDBStore()->newIterator(rocks_options_, parent_->cf_);
    STAT_INCR(
        parent_->getStatsHolder(), read_streams_num_csi_iterators_created);
  }
}

IteratorState
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::state() const {
  ld_check(iterator_.hasValue());
  return state_;
}

void RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::invalidate() {
  iterator_.clear();
  state_ = IteratorState::ERROR;
}

void RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::
    checkIteratorAndParseCurrentEntry() {
  ld_check(iterator_.hasValue());

  state_ = IteratorState::MAX;
  SCOPE_EXIT {
    ld_check(state_ != IteratorState::MAX);
  };

  // Check for rocksdb error.
  rocksdb::Status status = iterator_->status();
  if (status.IsIncomplete()) {
    state_ = IteratorState::WOULDBLOCK;
    return;
  }
  if (!status.ok()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "RocksDB copyset index iterator for log %s failed with status %s",
        parent_->log_id_.hasValue()
            ? folly::to<std::string>(parent_->log_id_.value().val_).c_str()
            : "all",
        status.ToString().c_str());
    PER_SHARD_STAT_INCR(
        parent_->getStatsHolder(), iterator_errors, parent_->getShardIdx());
    state_ = IteratorState::ERROR;
    return;
  }

  // Check if the rocksdb iterator reached the end.
  if (!iterator_->Valid()) {
    state_ = IteratorState::AT_END;
    return;
  }

  // Check that the record we are pointing to still belongs to the
  // copyset index of the right log.
  rocksdb::Slice slice = iterator_->key();

  if (!slice.empty() && slice.data()[0] != CopySetIndexKey::HEADER) {
    // we moved past the copyset index records
    state_ = IteratorState::AT_END;
    return;
  }

  if (!dd_assert(CopySetIndexKey::valid(slice.data(), slice.size()),
                 "Invalid CopySetIndexKey: %s",
                 hexdump_buf(slice.data(), slice.size(), 50).c_str())) {
    state_ = IteratorState::AT_END;
    return;
  }

  current_single_log_id_ = CopySetIndexKey::getLogID(iterator_->key().data());
  current_single_lsn_ = CopySetIndexKey::getLSN(iterator_->key().data());

  if (parent_->log_id_.hasValue() &&
      current_single_log_id_ != parent_->log_id_.value()) {
    state_ = IteratorState::AT_END;
    return;
  }

  // Alright, we have a CSI entry belonging to the right log. Now parse it.

  if (!dd_assert(
          !CopySetIndexKey::isBlockEntry(iterator_->key().data()),
          "Encountered unsupported block entry in copyset index: %s",
          hexdump_buf(iterator_->key().data(), iterator_->key().size(), 50)
              .c_str())) {
    state_ = IteratorState::ERROR;
    return;
  }

  if (!LocalLogStoreRecordFormat::parseCopySetIndexSingleEntry(
          Slice(iterator_->value().data(), iterator_->value().size()),
          &current_single_copyset_,
          &current_single_wave_,
          &current_single_flags_,
          parent_->store_->getShardIdx())) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "Unparseable copyset index value for log %lu at lsn %s, "
        "value: %s",
        current_single_log_id_.val_,
        lsn_to_string(current_single_lsn_).c_str(),
        hexdump_buf(iterator_->value().data(), iterator_->value().size(), 200)
            .c_str());
    state_ = IteratorState::ERROR;
    return;
  }

  current_entry_size_ = iterator_->value().size();

  dd_assert(current_single_copyset_.size() > 0,
            "Empty copyset in copyset index for log_id %lu, lsn %s",
            current_single_log_id_.val(),
            lsn_to_string(current_single_lsn_).c_str());

  state_ = IteratorState::AT_RECORD;
}

Location
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getLocation() const {
  ld_check_eq(state(), IteratorState::AT_RECORD);
  return Location(current_single_log_id_, current_single_lsn_);
}

const std::vector<ShardID>&
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getCurrentCopySet()
    const {
  ld_check(state() == IteratorState::AT_RECORD);
  ld_check(current_single_lsn_ != LSN_INVALID);

  return current_single_copyset_;
}

LocalLogStoreRecordFormat::csi_flags_t
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getCurrentFlags()
    const {
  ld_check(current_single_lsn_ != LSN_INVALID);
  ld_check(state() == IteratorState::AT_RECORD);

  return current_single_flags_;
}

uint32_t
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getCurrentWave() const {
  return current_single_wave_;
}

Slice RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getCurrentRecord()
    const {
  ld_check(state() == IteratorState::AT_RECORD);
  LocalLogStoreRecordFormat::flags_t flags =
      LocalLogStoreRecordFormat::copySetIndexFlagsToRecordFlags(
          current_single_flags_) |
      LocalLogStoreRecordFormat::FLAG_CSI_DATA_ONLY;

  return LocalLogStoreRecordFormat::formRecordHeader(
      0,                            // timestamp
      ESN_INVALID,                  // last known good
      flags,                        // flags
      current_single_wave_,         // wave
      folly::Range<const ShardID*>( // copyset
          current_single_copyset_.data(),
          current_single_copyset_.size()),
      OffsetMap::fromLegacy(0),         // offsets_within_epoch
      std::map<KeyType, std::string>(), // key
      &current_record_);
};
size_t
RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::getCurrentEntrySize() {
  return current_entry_size_;
}

void RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::seek(
    Location loc,
    Direction dir) {
  SCOPED_IO_TRACING_CONTEXT(getStore()->getIOTracing(),
                            "CSI:seek{}",
                            dir == Direction::FORWARD ? "" : "ForPrev");

  createIteratorIfNeeded();
  if (dir == Direction::FORWARD) {
    CopySetIndexKey key(loc.log_id, loc.lsn, CopySetIndexKey::SEEK_ENTRY_TYPE);
    ROCKSDB_ACCOUNTED_SEEK(
        iterator_,
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
        csi);
  } else {
    CopySetIndexKey key(loc.log_id, loc.lsn, CopySetIndexKey::LAST_ENTRY_TYPE);
    ROCKSDB_ACCOUNTED_SEEK_FOR_PREV(
        iterator_,
        rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
        csi);
  }
  checkIteratorAndParseCurrentEntry();
  trackSeek(loc.lsn,
            getIteratorVersion(
                &*iterator_, *parent_->getRocksDBStore()->getSettings()));
}

void RocksDBLocalLogStore::CSIWrapper::CopySetIndexIterator::step(
    Direction dir) {
  ld_check(state() == IteratorState::AT_RECORD);
  SCOPED_IO_TRACING_CONTEXT(getStore()->getIOTracing(),
                            "CSI:{}",
                            dir == Direction::FORWARD ? "next" : "prev");
  if (dir == Direction::FORWARD) {
    ROCKSDB_ACCOUNTED_NEXT(iterator_, csi);
  } else {
    ROCKSDB_ACCOUNTED_PREV(iterator_, csi);
  }
  checkIteratorAndParseCurrentEntry();
}

RocksDBLocalLogStore::CSIWrapper::DataIterator::DataIterator(
    const CSIWrapper* parent)
    : parent_(parent),
      upper_bound_(
          parent_->getRocksDBStore()->getSettings()->disable_iterate_upper_bound
              ? folly::none
              : parent_->log_id_),
      rocks_options_(translateReadOptions(parent_->read_opts_,
                                          parent_->log_id_.hasValue(),
                                          &upper_bound_.upper_bound)) {
  registerTracking(parent_->cf_->GetName(),
                   parent_->log_id_.value_or(LOGID_INVALID),
                   rocks_options_.tailing,
                   rocks_options_.read_tier != rocksdb::kBlockCacheTier,
                   IteratorType::DATA,
                   parent_->read_opts_.tracking_ctx);
}

void RocksDBLocalLogStore::CSIWrapper::DataIterator::createIteratorIfNeeded() {
  if (!iterator_.hasValue()) {
    iterator_ =
        parent_->getRocksDBStore()->newIterator(rocks_options_, parent_->cf_);
  }
}

void RocksDBLocalLogStore::CSIWrapper::DataIterator::releaseIterator() {
  iterator_.clear();
  trackIteratorRelease();
}

IteratorState RocksDBLocalLogStore::CSIWrapper::DataIterator::state() const {
  if (!iterator_.hasValue()) {
    ld_check(false);
    return IteratorState::ERROR;
  }

  rocksdb::Status status = iterator_->status();
  if (status.IsIncomplete()) {
    return IteratorState::WOULDBLOCK;
  }
  if (!status.ok()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        2,
        "RocksDB data iterator for log %s failed with status %s",
        parent_->log_id_.hasValue()
            ? folly::to<std::string>(parent_->log_id_.value().val_).c_str()
            : "all",
        status.ToString().c_str());
    PER_SHARD_STAT_INCR(
        parent_->getStatsHolder(), iterator_errors, parent_->getShardIdx());
    return IteratorState::ERROR;
  }

  // If the underlying rocksdb iterator is not valid, we are at the end of the
  // database.
  if (!iterator_->Valid()) {
    return IteratorState::AT_END;
  }

  // Check that the record we are pointing to still belongs to the right log.
  rocksdb::Slice slice = iterator_->key();

  if (!slice.empty() && slice.data()[0] != DataKey::HEADER) {
    // We moved past the data records.
    return IteratorState::AT_END;
  }

  if (!dd_assert(DataKey::valid(slice.data(), slice.size()),
                 "Invalid DataKey: %s",
                 hexdump_buf(slice.data(), slice.size(), 50).c_str())) {
    return IteratorState::ERROR;
  }
  if (parent_->log_id_.hasValue() &&
      DataKey::getLogID(slice.data()) != parent_->log_id_.value()) {
    return IteratorState::AT_END;
  }
  return IteratorState::AT_RECORD;
}

Location RocksDBLocalLogStore::CSIWrapper::DataIterator::getLocation() const {
  ld_check_eq(state(), IteratorState::AT_RECORD);
  ld_check(DataKey::valid(iterator_->key().data(), iterator_->key().size()));
  Location loc(DataKey::getLogID(iterator_->key().data()),
               DataKey::getLSN(iterator_->key().data()));
  ld_check_eq(loc.log_id, parent_->log_id_.value_or(loc.log_id));
  return loc;
}

Slice RocksDBLocalLogStore::CSIWrapper::DataIterator::getRecord() const {
  ld_check(state() == IteratorState::AT_RECORD);
  if (!merged_value_.empty()) {
    return facebook::logdevice::Slice(
        merged_value_.data(), merged_value_.size());
  }
  rocksdb::Slice slice = iterator_->value();
  return facebook::logdevice::Slice(slice.data(), slice.size());
}

void RocksDBLocalLogStore::CSIWrapper::DataIterator::seek(Location loc,
                                                          Direction dir) {
  SCOPED_IO_TRACING_CONTEXT(getStore()->getIOTracing(),
                            "data:seek{}",
                            dir == Direction::FORWARD ? "" : "ForPrev");
  merged_value_.clear();
  createIteratorIfNeeded();
  DataKey key(loc.log_id, loc.lsn);
  if (dir == Direction::FORWARD) {
    ROCKSDB_ACCOUNTED_SEEK(iterator_, key.sliceForForwardSeek(), record);
    handleKeyFormatMigration();
  } else {
    ROCKSDB_ACCOUNTED_SEEK_FOR_PREV(
        iterator_, key.sliceForBackwardSeek(), record);
  }
  trackSeek(loc.lsn,
            getIteratorVersion(
                &*iterator_, *parent_->getRocksDBStore()->getSettings()));
}

void RocksDBLocalLogStore::CSIWrapper::DataIterator::step(Direction dir) {
  SCOPED_IO_TRACING_CONTEXT(getStore()->getIOTracing(),
                            "data:{}",
                            dir == Direction::FORWARD ? "next" : "prev");
  merged_value_.clear();
  ld_assert(state() == IteratorState::AT_RECORD);
  if (dir == Direction::FORWARD) {
    ROCKSDB_ACCOUNTED_NEXT(iterator_, record);
    handleKeyFormatMigration();
  } else {
    ROCKSDB_ACCOUNTED_PREV(iterator_, record);
  }
}

void RocksDBLocalLogStore::CSIWrapper::DataIterator::
    handleKeyFormatMigration() {
  ld_check(iterator_.hasValue());
  while (true) {
    // If we're standing on a dangling amend in new format, step forward and
    // see if the next key has the same lsn. If it does, merge the two together.
    // Note that the new-format key is a prefix of old-format key and therefore
    // always comes immediately before the old-format key.
    // If we encounter any error, just stop. The outer code will independently
    // notice and handle it.

    if (!iterator_->status().ok() || !iterator_->Valid()) {
      break;
    }

    rocksdb::Slice key = iterator_->key();
    if (!DataKey::valid(key.data(), key.size()) ||
        !DataKey::isInNewFormat(key.data(), key.size())) {
      break;
    }

    logid_t log = DataKey::getLogID(key.data());
    lsn_t lsn = DataKey::getLSN(key.data());

    if (parent_->log_id_.hasValue() && log != parent_->log_id_.value()) {
      break;
    }

    rocksdb::Slice value = iterator_->value();
    facebook::logdevice::Slice slice(value.data(), value.size());
    LocalLogStoreRecordFormat::flags_t flags;
    int rv = LocalLogStoreRecordFormat::parseFlags(slice, &flags);
    if (rv != 0 || !(flags & LocalLogStoreRecordFormat::FLAG_AMEND)) {
      // Common case: we've got a full record (not amend) in new format.
      break;
    }

    // Alright, we've got a dangling amend in new format.
    STAT_INCR(getStatsHolder(), data_key_format_migration_steps);

    if (!skipped_dangling_amend_.hasValue()) {
      skipped_dangling_amend_ = Location(log, lsn);
    }

    // Save the amend.
    std::string amend_str(slice.size + 1, '\0');
    amend_str[0] = RocksDBWriterMergeOperator::DATA_MERGE_HEADER;
    memcpy(&amend_str[1], slice.data, slice.size);

    // Step forward and check if we're still on record and still on same lsn.
    {
      SCOPED_IO_TRACING_CONTEXT(getStore()->getIOTracing(), "migration");
      ROCKSDB_ACCOUNTED_NEXT(iterator_, record);
    }
    if (!iterator_->status().ok() || !iterator_->Valid()) {
      break;
    }
    key = iterator_->key();
    if (!DataKey::valid(key.data(), key.size())) {
      break;
    }
    if (DataKey::getLogID(key.data()) != log ||
        DataKey::getLSN(key.data()) != lsn) {
      // Fell through to the next record. Let's keep going.
      continue;
    }

    // Alright, we've got two values for the same LSN.
    // Merge them, the same way rocksdb would merge values for the same key.
    STAT_INCR(getStatsHolder(), data_key_format_migration_merges);
    RocksDBWriterMergeOperator merge_op(parent_->getShardIdx());
    std::vector<rocksdb::Slice> operands = {
        rocksdb::Slice(amend_str.data(), amend_str.size())};
    value = iterator_->value();
    rocksdb::MergeOperator::MergeOperationInput input(
        key, &value, operands, nullptr);
    rocksdb::Slice existing_operand(nullptr, 0);
    rocksdb::MergeOperator::MergeOperationOutput output(
        merged_value_, existing_operand);
    bool ok = merge_op.FullMergeV2(input, &output);
    if (!ok) {
      // Weird. Let's just stop here. The merge operator has logged the error.
      merged_value_.clear();
    } else {
      // We applied the new-format amend to the old-format record.
      // Convert merge operator's output to string and we're done.
      ld_check_ne(existing_operand.empty(), merged_value_.empty());
      ld_check_eq(existing_operand.empty(), existing_operand.data() == nullptr);
      if (merged_value_.empty()) {
        merged_value_ = existing_operand.ToString();
      }
    }
    break;
  }
}

// ==== AllLogsIteratorImpl ====

RocksDBLocalLogStore::AllLogsIteratorImpl::AllLogsIteratorImpl(
    std::unique_ptr<CSIWrapper> iterator)
    : iterator_(std::move(iterator)) {}

IteratorState RocksDBLocalLogStore::AllLogsIteratorImpl::state() const {
  return iterator_->state();
}
std::unique_ptr<LocalLogStore::AllLogsIterator::Location>
RocksDBLocalLogStore::AllLogsIteratorImpl::getLocation() const {
  auto loc = iterator_->getLocation();
  ld_assert(!loc.at_end);
  return std::make_unique<LocationImpl>(loc.log_id, loc.lsn);
}
logid_t RocksDBLocalLogStore::AllLogsIteratorImpl::getLogID() const {
  return iterator_->getLogID();
}
lsn_t RocksDBLocalLogStore::AllLogsIteratorImpl::getLSN() const {
  return iterator_->getLSN();
}
Slice RocksDBLocalLogStore::AllLogsIteratorImpl::getRecord() const {
  return iterator_->getRecord();
}
void RocksDBLocalLogStore::AllLogsIteratorImpl::seek(const Location& location,
                                                     ReadFilter* filter,
                                                     ReadStats* stats) {
  ld_assert(dynamic_cast<const LocationImpl*>(&location));
  const LocationImpl& loc = static_cast<const LocationImpl&>(location);
  iterator_->seek(loc.log, loc.lsn, filter, stats);
}
void RocksDBLocalLogStore::AllLogsIteratorImpl::next(ReadFilter* filter,
                                                     ReadStats* stats) {
  iterator_->next(filter, stats);
}
std::unique_ptr<LocalLogStore::AllLogsIterator::Location>
RocksDBLocalLogStore::AllLogsIteratorImpl::minLocation() const {
  return std::make_unique<LocationImpl>(logid_t(0), lsn_t(0));
}
std::unique_ptr<LocalLogStore::AllLogsIterator::Location>
RocksDBLocalLogStore::AllLogsIteratorImpl::metadataLogsBegin() const {
  return std::make_unique<LocationImpl>(
      MetaDataLog::metaDataLogID(logid_t(0)), lsn_t(0));
}
void RocksDBLocalLogStore::AllLogsIteratorImpl::invalidate() {
  iterator_->invalidate();
}
const LocalLogStore*
RocksDBLocalLogStore::AllLogsIteratorImpl::getStore() const {
  return iterator_->getStore();
}

}} // namespace facebook::logdevice
