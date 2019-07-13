/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/test/TemporaryLogStore.h"

#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/PartitionedRocksDBStore.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "rocksdb/db.h"

namespace facebook { namespace logdevice {

TemporaryLogStore::TemporaryLogStore(factory_func_t factory,
                                     bool open_right_away)
    : factory_(factory) {
  temp_dir_ = createTemporaryDir("TemporaryLogStore", /*keep_data=*/false);
  ld_check(temp_dir_);

  if (open_right_away) {
    open();
  }
  ld_debug("created temporary log store in %s", getPath());
}

TemporaryLogStore::~TemporaryLogStore() {}

void TemporaryLogStore::open() {
  db_ = factory_(getPath());
  ld_check(db_);
}

void TemporaryLogStore::close() {
  db_.reset();
}

void TemporaryLogStore::stallLowPriWrite() {
  db_->stallLowPriWrite();
}

LocalLogStore::WriteThrottleState TemporaryLogStore::getWriteThrottleState() {
  return db_->getWriteThrottleState();
}

void TemporaryLogStore::disableWriteStalling() {
  db_->disableWriteStalling();
}

int TemporaryLogStore::writeMulti(const std::vector<const WriteOp*>& writes,
                                  const WriteOptions& options) {
  return db_->writeMulti(writes, options);
}

int TemporaryLogStore::sync(Durability durability) {
  return db_->sync(durability);
}

FlushToken TemporaryLogStore::maxFlushToken() const {
  return db_->maxFlushToken();
}

FlushToken TemporaryLogStore::flushedUpThrough() const {
  return db_->flushedUpThrough();
}

FlushToken TemporaryLogStore::maxWALSyncToken() const {
  return db_->maxWALSyncToken();
}

FlushToken TemporaryLogStore::walSyncedUpThrough() const {
  return db_->walSyncedUpThrough();
}

std::unique_ptr<LocalLogStore::ReadIterator>
TemporaryLogStore::read(logid_t log_id,
                        const LocalLogStore::ReadOptions& options) const {
  return db_->read(log_id, options);
}

std::unique_ptr<LocalLogStore::AllLogsIterator> TemporaryLogStore::readAllLogs(
    const LocalLogStore::ReadOptions& options,
    const folly::Optional<std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>&
        logs) const {
  return db_->readAllLogs(options, logs);
}

int TemporaryLogStore::readLogMetadata(logid_t log_id, LogMetadata* metadata) {
  return db_->readLogMetadata(log_id, metadata);
}

int TemporaryLogStore::writeLogMetadata(logid_t log_id,
                                        const LogMetadata& metadata,
                                        const WriteOptions& options) {
  return db_->writeLogMetadata(log_id, metadata, options);
}

int TemporaryLogStore::deleteStoreMetadata(const StoreMetadataType type,
                                           const WriteOptions& opts) {
  return db_->deleteStoreMetadata(type, opts);
}
int TemporaryLogStore::deleteLogMetadata(logid_t first_log_id,
                                         logid_t last_log_id,
                                         const LogMetadataType type,
                                         const WriteOptions& opts) {
  return db_->deleteLogMetadata(first_log_id, last_log_id, type, opts);
}
int TemporaryLogStore::deletePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    const PerEpochLogMetadataType type,
    const WriteOptions& opts) {
  return db_->deletePerEpochLogMetadata(log_id, epoch, type, opts);
}

int TemporaryLogStore::updateLogMetadata(logid_t log_id,
                                         ComparableLogMetadata& metadata,
                                         const WriteOptions& options) {
  return db_->updateLogMetadata(log_id, metadata, options);
}

int TemporaryLogStore::readStoreMetadata(StoreMetadata* metadata) {
  return db_->readStoreMetadata(metadata);
}

int TemporaryLogStore::writeStoreMetadata(const StoreMetadata& metadata,
                                          const WriteOptions& options) {
  return db_->writeStoreMetadata(metadata, options);
}

int TemporaryLogStore::readPerEpochLogMetadata(logid_t log_id,
                                               epoch_t epoch,
                                               PerEpochLogMetadata* metadata,
                                               bool find_last_available,
                                               bool allow_blocking_io) const {
  return db_->readPerEpochLogMetadata(
      log_id, epoch, metadata, find_last_available, allow_blocking_io);
}

int TemporaryLogStore::updatePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    PerEpochLogMetadata& metadata,
    LocalLogStore::SealPreemption seal_preempt,
    const WriteOptions& write_options) {
  return db_->updatePerEpochLogMetadata(
      log_id, epoch, metadata, seal_preempt, write_options);
}

int TemporaryLogStore::isEmpty() const {
  return db_->isEmpty();
}

int TemporaryLogStore::getShardIdx() const {
  return db_->getShardIdx();
}

int TemporaryLogStore::getHighestInsertedLSN(logid_t log_id,
                                             lsn_t* highestLSN) {
  return db_->getHighestInsertedLSN(log_id, highestLSN);
}

int TemporaryLogStore::getApproximateTimestamp(
    logid_t log_id,
    lsn_t lsn,
    bool allow_blocking_io,
    std::chrono::milliseconds* timestamp_out) {
  return db_->getApproximateTimestamp(
      log_id, lsn, allow_blocking_io, timestamp_out);
}

int TemporaryLogStore::findTime(
    logid_t log_id,
    std::chrono::milliseconds timestamp,
    lsn_t* lo,
    lsn_t* hi,
    bool approximate,
    bool allow_blocking_io,
    std::chrono::steady_clock::time_point deadline) const {
  return db_->findTime(
      log_id, timestamp, lo, hi, approximate, allow_blocking_io, deadline);
}

int TemporaryLogStore::findKey(logid_t log_id,
                               std::string key,
                               lsn_t* lo,
                               lsn_t* hi,
                               bool approximate,
                               bool allow_blocking_io) const {
  return db_->findKey(
      log_id, std::move(key), lo, hi, approximate, allow_blocking_io);
}

Status TemporaryLogStore::acceptingWrites() const {
  return db_->acceptingWrites();
}

int TemporaryLogStore::readAllLogSnapshotBlobs(
    LocalLogStore::LogSnapshotBlobType type,
    LogSnapshotBlobCallback callback) {
  return db_->readAllLogSnapshotBlobs(type, callback);
}

int TemporaryLogStore::writeLogSnapshotBlobs(
    LocalLogStore::LogSnapshotBlobType snapshots_type,
    const std::vector<std::pair<logid_t, Slice>>& snapshots) {
  return db_->writeLogSnapshotBlobs(snapshots_type, snapshots);
}

int TemporaryLogStore::deleteAllLogSnapshotBlobs() {
  return db_->deleteAllLogSnapshotBlobs();
}

TemporaryRocksDBStore::TemporaryRocksDBStore(bool read_find_time_index)
    : TemporaryLogStore([read_find_time_index](const std::string& path) {
        // All tests should assume this is shard 0.
        shard_index_t shard_idx = 0;

        RocksDBSettings raw_settings = RocksDBSettings::defaultTestSettings();
        raw_settings.use_copyset_index = true;
        raw_settings.read_find_time_index = read_find_time_index;

        UpdateableSettings<RocksDBSettings> settings(raw_settings);
        UpdateableSettings<RebuildingSettings> rebuilding_settings;

        RocksDBLogStoreConfig rocksdb_config(
            settings, rebuilding_settings, nullptr, nullptr, nullptr);
        rocksdb_config.createMergeOperator(shard_idx);

        return std::make_unique<RocksDBLocalLogStore>(shard_idx,
                                                      1,
                                                      path,
                                                      std::move(rocksdb_config),
                                                      /* stats */ nullptr,
                                                      /* io_tracing */ nullptr);
      }) {}

class TemporaryPartitionedStoreImpl : public PartitionedRocksDBStore {
 public:
  explicit TemporaryPartitionedStoreImpl(const std::string& path,
                                         RocksDBLogStoreConfig rocksdb_config,
                                         const Configuration* config,
                                         StatsHolder* stats,
                                         IOTracing* io_tracing,
                                         SystemTimestamp* time)
      : PartitionedRocksDBStore(0,
                                1,
                                path,
                                std::move(rocksdb_config),
                                config,
                                stats,
                                io_tracing,
                                DeferInit::YES),
        time_(time) {
    PartitionedRocksDBStore::init(config);
  }

  SystemTimestamp currentTime() override {
    ld_check(time_);
    return *time_;
  }

  SystemTimestamp* time_;
};

void TemporaryPartitionedStore::setTime(SystemTimestamp time) {
  time_ = time;
}

TemporaryPartitionedStore::TemporaryPartitionedStore(bool use_csi)
    : TemporaryLogStore(
          [use_csi, this](const std::string& path) {
            RocksDBSettings raw_settings =
                RocksDBSettings::defaultTestSettings();
            raw_settings.use_copyset_index = use_csi;

            UpdateableSettings<RocksDBSettings> settings(raw_settings);
            UpdateableSettings<RebuildingSettings> rebuilding_settings;

            RocksDBLogStoreConfig rocksdb_config(
                settings, rebuilding_settings, nullptr, nullptr, nullptr);
            rocksdb_config.createMergeOperator(0);

            return std::make_unique<TemporaryPartitionedStoreImpl>(
                path,
                std::move(rocksdb_config),
                /* config */ nullptr,
                /* stats */ nullptr,
                /* io_tracing */ nullptr,
                &time_);
          },
          false) {
  open();
}
TemporaryPartitionedStore::~TemporaryPartitionedStore() {
  close();
}

int TemporaryPartitionedStore::putRecord(
    logid_t log,
    lsn_t lsn,
    RecordTimestamp timestamp,
    copyset_t copyset,
    LocalLogStoreRecordFormat::flags_t extra_flags,
    folly::Optional<Slice> payload) {
  LocalLogStoreRecordFormat::flags_t flags =
      extra_flags | LocalLogStoreRecordFormat::FLAG_SHARD_ID;
  if (!(flags &
        (LocalLogStoreRecordFormat::FLAG_CHECKSUM |
         LocalLogStoreRecordFormat::FLAG_CHECKSUM_64BIT))) {
    flags |= LocalLogStoreRecordFormat::FLAG_CHECKSUM_PARITY;
  }
  LocalLogStoreRecordFormat::csi_flags_t csi_flags =
      LocalLogStoreRecordFormat::formCopySetIndexFlags(flags);

  std::string header_buf;
  Slice header = LocalLogStoreRecordFormat::formRecordHeader(
      timestamp.toMilliseconds().count(),
      esn_t(0), // LNG
      flags,
      1, // wave
      folly::Range<const ShardID*>(
          copyset.data(), copyset.data() + copyset.size()),
      OffsetMap::fromLegacy(0), // offsets
      {},                       // keys
      &header_buf);

  std::string csi_entry_buf;
  Slice csi_entry = LocalLogStoreRecordFormat::formCopySetIndexEntry(
      1, // wave
      copyset.data(),
      copyset.size(),
      LSN_INVALID, // block starting LSN
      csi_flags,
      &csi_entry_buf);

  std::string payload_buf;
  if (!payload.hasValue()) {
    payload_buf = toString(log.val()) + lsn_to_string(lsn);
    payload = Slice::fromString(payload_buf);
  }

  PutWriteOp op(log,
                lsn,
                header,
                payload.value(),
                node_index_t(1), // coordinator
                LSN_INVALID,     // block starting LSN
                csi_entry,
                {}, // keys
                Durability::ASYNC_WRITE,
                flags &
                    LocalLogStoreRecordFormat::
                        FLAG_WRITTEN_BY_REBUILDING); // is_rebuilding

  return writeMulti({&op}, LocalLogStore::WriteOptions());
}

void TemporaryPartitionedStore::createPartition() {
  dynamic_cast<PartitionedRocksDBStore*>(db_.get())->createPartition();
}

std::vector<size_t> TemporaryPartitionedStore::getNumLogsPerPartition() {
  std::vector<std::pair<logid_t, PartitionedRocksDBStore::DirectoryEntry>> dir;
  dynamic_cast<PartitionedRocksDBStore*>(db_.get())->getLogsDBDirectories(
      {}, {}, dir);
  if (dir.empty()) {
    return {};
  }
  partition_id_t first_partition = PARTITION_MAX;
  for (const auto& p : dir) {
    first_partition = std::min(first_partition, p.second.id);
  }
  std::vector<size_t> res;
  for (const auto& p : dir) {
    auto idx = p.second.id - first_partition;
    if (idx >= res.size()) {
      res.resize(idx + 1);
    }
    ++res[idx];
  }
  return res;
}

}} // namespace facebook::logdevice
