/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "TemporaryLogStore.h"

#include "rocksdb/db.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"

namespace facebook { namespace logdevice {

TemporaryLogStore::TemporaryLogStore(factory_func_t factory)
    : factory_(factory) {
  temp_dir_ = createTemporaryDir("TemporaryLogStore", /*keep_data=*/false);
  ld_check(temp_dir_);

  open();
  ld_debug("initialized temporary log store in %s", getPath());
}

TemporaryLogStore::~TemporaryLogStore() {}

void TemporaryLogStore::open() {
  db_.reset(factory_(getPath()));
  ld_check(db_);
}

void TemporaryLogStore::close() {
  db_.reset();
}

void TemporaryLogStore::stallLowPriWrite() {
  db_->stallLowPriWrite();
}

void TemporaryLogStore::adviseUnstallingLowPriWrites(bool never_stall) {
  db_->adviseUnstallingLowPriWrites(never_stall);
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
    const LocalLogStore::ReadOptions& options) const {
  return db_->readAllLogs(options);
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

namespace {
class RocksDBStoreFactory : public LocalLogStoreFactory {
 public:
  std::unique_ptr<LocalLogStore> create(uint32_t /* shard_idx */,
                                        std::string /*path*/) const override {
    return std::unique_ptr<LocalLogStore>();
  }
};
} // namespace

TemporaryRocksDBStore::TemporaryRocksDBStore(bool read_find_time_index)
    : TemporaryLogStore([read_find_time_index](std::string path) {
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

        return new RocksDBLocalLogStore(
            shard_idx, path.c_str(), std::move(rocksdb_config));
      }) {}

}} // namespace facebook::logdevice
