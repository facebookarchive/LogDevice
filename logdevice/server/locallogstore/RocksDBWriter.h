/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <deque>
#include <mutex>
#include <string>
#include <vector>

#include <folly/Demangle.h>
#include <rocksdb/cache.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file RocksDBWriter is a utility class used to write records and metadata
 *       to RocksDB, as well as read metadata.
 *       (The name is quite inaccurate. Feel free to rename.)
 *       Writes to DB should go through RocksDBLogStoreBase::writeBatch() to
 *       bump stats and keep track of persistent error.
 */

class ComparableLogMetadata;
class LogMetadata;
class StoreMetadata;
class WriteOp;

namespace detail {
// Should updates to metadata entries of the given type be performed using
// RocksDB's Merge() instead of Put()?
inline bool useMerge(const Metadata&) {
  return false;
}
inline bool useMerge(const MutablePerEpochLogMetadata&) {
  return true;
}
bool useMerge(const LogMetadata& metadata);
} // namespace detail

class RocksDBWriter {
 public:
  explicit RocksDBWriter(RocksDBLogStoreBase* store,
                         const RocksDBSettings& rocksdb_settings)
      : store_(store),
        read_only_(rocksdb_settings.read_only),
        locks_(rocksdb_settings.num_metadata_locks) {}

  // see common/LocalLogStore.h for the description of these methods

  // If data_cf_handles != nullptr, uses corresponding column families for Put
  // and Delete, otherwise uses default column family.
  //
  // Write the operations stored in `writes` and `rocksdb_batch` to DB.
  //
  // - If Settings::no_wal is true, all operations are performed atomically
  //   without wal.
  // - If Settings::no_wal is false and RebuildingSettings::no_wal is true,
  //   operations from `writes` that do require WAL will be done atomically with
  //   operations in `rocksdb_batch` and operations from `writes` that do not
  //   require WAL (because they are for rebuilding) will be done with a
  //   separate call to writeBatch.
  // - If both settings are false, all operations are performed atomically with
  //   wal.
  int writeMulti(const std::vector<const WriteOp*>& writes,
                 const LocalLogStore::WriteOptions& write_options,
                 rocksdb::ColumnFamilyHandle* metadata_cf,
                 std::vector<rocksdb::ColumnFamilyHandle*>* data_cf_handles,
                 rocksdb::WriteBatch& wal_batch,
                 rocksdb::WriteBatch& mem_batch,
                 bool skip_checksum_verification = false);

  int readLogMetadata(logid_t log_id,
                      LogMetadata* metadata,
                      rocksdb::ColumnFamilyHandle* cf);
  int writeLogMetadata(logid_t log_id,
                       const LogMetadata& metadata,
                       const LocalLogStore::WriteOptions& options,
                       rocksdb::ColumnFamilyHandle* cf);
  int updateLogMetadata(logid_t log_id,
                        ComparableLogMetadata& metadata,
                        const LocalLogStore::WriteOptions& options,
                        rocksdb::ColumnFamilyHandle* cf);
  int deleteStoreMetadata(const StoreMetadataType& type,
                          const LocalLogStore::WriteOptions& options,
                          rocksdb::ColumnFamilyHandle* cf);
  int deleteLogMetadata( // deletes logs from first to last log id (inclusive)
      logid_t first_log_id,
      logid_t last_log_id,
      const LogMetadataType& type,
      const LocalLogStore::WriteOptions& options,
      rocksdb::ColumnFamilyHandle* cf);
  int deletePerEpochLogMetadata(logid_t log_id,
                                epoch_t epoch,
                                const PerEpochLogMetadataType& type,
                                const LocalLogStore::WriteOptions& options,
                                rocksdb::ColumnFamilyHandle* cf);
  int readStoreMetadata(StoreMetadata* metadata,
                        rocksdb::ColumnFamilyHandle* cf);
  int writeStoreMetadata(const StoreMetadata& metadata,
                         const LocalLogStore::WriteOptions& options,
                         rocksdb::ColumnFamilyHandle* cf);
  int readPerEpochLogMetadata(logid_t log_id,
                              epoch_t epoch,
                              PerEpochLogMetadata* metadata,
                              rocksdb::ColumnFamilyHandle* cf,
                              bool find_last_available = false,
                              bool allow_blocking_io = true);
  int updatePerEpochLogMetadata(logid_t log_id,
                                epoch_t epoch,
                                PerEpochLogMetadata& metadata,
                                LocalLogStore::SealPreemption seal_preempt,
                                const LocalLogStore::WriteOptions& options,
                                rocksdb::ColumnFamilyHandle* cf);

  template <typename Key, typename Meta>
  int readMetadata(const Key& key,
                   Meta* metadata,
                   rocksdb::ColumnFamilyHandle* cf,
                   bool /*allow_blocking_io*/ = true) {
    return readMetadata(store_, key, metadata, cf);
  }

  template <typename Key, typename Meta>
  static int readMetadata(RocksDBLogStoreBase* store,
                          const Key& key,
                          Meta* metadata,
                          rocksdb::ColumnFamilyHandle* cf,
                          bool allow_blocking_io = true) {
    using IOType = IOFaultInjection::IOType;
    using DataType = IOFaultInjection::DataType;
    using FaultType = IOFaultInjection::FaultType;

    ld_check(metadata != nullptr);

    auto& db = store->getDB();
    if (cf == nullptr) {
      cf = db.DefaultColumnFamily();
    }

    rocksdb::Slice key_slice(reinterpret_cast<const char*>(&key), sizeof key);
    std::string value;

    auto read_options = RocksDBLogStoreBase::getReadOptionsSinglePrefix();
    read_options.read_tier =
        allow_blocking_io ? rocksdb::kReadAllTier : rocksdb::kBlockCacheTier;

    rocksdb::Status status;
    shard_index_t shard_idx = store->getShardIdx();
    auto& io_fault_injection = IOFaultInjection::instance();
    auto fault = io_fault_injection.getInjectedFault(
        shard_idx,
        IOType::READ,
        FaultType::CORRUPTION | FaultType::IO_ERROR,
        DataType::METADATA);
    if (fault != FaultType::NONE) {
      status = RocksDBLogStoreBase::FaultTypeToStatus(fault);
      ld_error("RocksDBWriter::readMetadata(): Returning injected error "
               "%s for shard %s.",
               status.ToString().c_str(),
               store->getDBPath().c_str());
      // Don't bump error stats for injected errors.
      if (!status.ok() && !status.IsNotFound() && !status.IsIncomplete()) {
        store->enterFailSafeMode("Write()", "injected error");
      }
    } else {
      status = db.Get(read_options, cf, key_slice, &value);
      store->enterFailSafeIfFailed(status, "Get()");
    }
    if (!status.ok()) {
      if (status.IsNotFound()) {
        err = E::NOTFOUND;
      } else if (status.IsIncomplete()) {
        err = E::WOULDBLOCK;
      } else {
        err = E::LOCAL_LOG_STORE_READ;
        PER_SHARD_STAT_INCR(
            store->getStatsHolder(), local_logstore_failed_metadata, shard_idx);
      }
      return -1;
    }

    int rv = metadata->deserialize(Slice(value.data(), value.size()));
    if (rv != 0 || !metadata->valid()) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          10,
          "%s metadata %s. Key: %s, value: %s",
          rv == 0 ? "Invalid" : "Unable to deserialize",
          folly::demangle(typeid(Meta).name()).c_str(),
          hexdump_buf(key_slice.data(), key_slice.size(), 100).c_str(),
          hexdump_buf(value.data(), value.size(), 100).c_str());
      err = E::LOCAL_LOG_STORE_READ;
      return -1;
    }

    return 0;
  }

  template <typename Key, typename Meta>
  int writeMetadata(const Key& key,
                    const Meta& metadata,
                    const LocalLogStore::WriteOptions& /*write_options*/,
                    rocksdb::ColumnFamilyHandle* cf) {
    if (read_only_) {
      ld_check(false);
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }
    if (store_->acceptingWrites() == E::DISABLED) {
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }

    if (!metadata.valid()) {
      // Metadata is not valid, writing this record would cause permanently
      // corruption, abort
      RATELIMIT_CRITICAL(std::chrono::seconds(10),
                         10,
                         "INTERNAL ERROR: Not writing invalid metadata %s to "
                         "persistent log store!",
                         metadata.toString().c_str());
      err = E::LOCAL_LOG_STORE_WRITE;
      // higher layer should guarantee the validity of data, consider this
      // as internal error
      dd_assert(false, "invalid metadata");
      return -1;
    }

    rocksdb::WriteBatch batch;
    Slice value(metadata.serialize());

    if (detail::useMerge(metadata)) {
      batch.Merge(
          cf,
          rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
          rocksdb::Slice(
              reinterpret_cast<const char*>(value.data), value.size));
    } else {
      batch.Put(cf,
                rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
                rocksdb::Slice(
                    reinterpret_cast<const char*>(value.data), value.size));
    }

    rocksdb::Status status =
        store_->writeBatch(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }
    return 0;
  }

  template <typename Key>
  int deleteMetadata(const Key& key,
                     const LocalLogStore::WriteOptions& /*write_options*/,
                     rocksdb::ColumnFamilyHandle* cf) {
    if (read_only_) {
      ld_check(false);
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }
    rocksdb::WriteBatch batch;
    batch.Delete(
        cf, rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key));

    rocksdb::Status status =
        store_->writeBatch(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }
    return 0;
  }

  // deleteMetadataRange deletes from (and including) first_key to (and
  // excluding) last_key. This is the same behavior as rocksdb::DB and is kept
  // here for consistency.
  template <typename Key>
  int deleteMetadataRange(const Key& first_key,
                          const Key& last_key,
                          const LocalLogStore::WriteOptions& /*write_options*/,
                          rocksdb::ColumnFamilyHandle* cf) {
    // Just iterate over keys in the range and delete them one by one.
    // We don't care about performance here.
    // We could use rocksdb::WriteBatch::DeleteRange() instead, but we'd rather
    // not rely on that API unless necessary - its implementation is pretty
    // complex.
    rocksdb::Slice last_key_slice(
        reinterpret_cast<const char*>(&last_key), sizeof(last_key));
    rocksdb::ReadOptions read_options;
    auto it = store_->newIterator(read_options, cf);
    std::vector<std::string> keys;
    for (it.Seek(rocksdb::Slice(
             reinterpret_cast<const char*>(&first_key), sizeof(first_key)));
         it.status().ok() && it.Valid() && it.key().compare(last_key_slice) < 0;
         it.Next()) {
      // TODO (#39174994): This is a temporary workaround for a key collision.
      //                   Remove when migration is complete.
      if (it.key().compare(rocksdb::Slice(
              RocksDBLogStoreBase::OLD_SCHEMA_VERSION_KEY)) == 0) {
        continue;
      }
      keys.push_back(it.key().ToString());
    }
    if (!it.status().ok()) {
      // Got an error.
      err = E::LOCAL_LOG_STORE_WRITE;
      return -1;
    }

    ld_info("Deleting %lu metadata entries", keys.size());

    if (!keys.empty()) {
      rocksdb::WriteBatch batch;
      for (const std::string& key : keys) {
        batch.Delete(cf, rocksdb::Slice(key.data(), key.size()));
      }

      rocksdb::Status status =
          store_->writeBatch(rocksdb::WriteOptions(), &batch);
      if (!status.ok()) {
        err = E::LOCAL_LOG_STORE_WRITE;
        return -1;
      }
    }

    return 0;
  }

  int writeLogSnapshotBlobs(
      rocksdb::ColumnFamilyHandle* snapshots_cf,
      LocalLogStore::LogSnapshotBlobType snapshot_type,
      const std::vector<std::pair<logid_t, Slice>>& snapshots);

  // A wrapper around rocksdb::DB::SyncWAL() which also updates stats.
  rocksdb::Status syncWAL();

  FlushToken maxWALSyncToken() const {
    return next_wal_sync_token_.load();
  }

  FlushToken walSyncedUpThrough() const {
    return wal_synced_up_to_token_.load();
  }

  // Low-level functions for reading all/some metadata directly from rocksdb.
  // Note that they don't bump stats and don't fail-safe the store if an error
  // occurs. Used by debugging tools, like 'info metadata' admin command and
  // `recordtool` tool.
  //
  // Read all StoreMetadata/LogMetadata/PerEpochLogMetadata's of the given types
  // for the given logs and call `cb` for each.
  // Returns 0 if everything was successful, -1 if there were errors.
  // Sets err to either LOCAL_LOG_STORE_READ or MALFORMED_RECORD.
  // Stops on iterator errors. Doesn't stop on malformed records.
  static int enumerateStoreMetadata(
      rocksdb::DB* db,
      rocksdb::ColumnFamilyHandle* cf,
      std::vector<StoreMetadataType> types,
      std::function<void(StoreMetadataType, StoreMetadata& meta)> cb);
  static int enumerateLogMetadata(
      rocksdb::DB* db,
      rocksdb::ColumnFamilyHandle* cf,
      std::vector<logid_t> logs, // empty means all
      std::vector<LogMetadataType> types,
      std::function<void(LogMetadataType, logid_t, LogMetadata& meta)> cb);
  static int enumeratePerEpochLogMetadata(
      rocksdb::DB* db,
      rocksdb::ColumnFamilyHandle* cf,
      std::vector<logid_t> logs, // empty means all
      std::vector<PerEpochLogMetadataType> types,
      std::function<void(PerEpochLogMetadataType,
                         logid_t,
                         epoch_t,
                         PerEpochLogMetadata& meta)> cb);

 private:
  int readPreviousPerEpochLogMetadata(logid_t log_id,
                                      epoch_t epoch,
                                      PerEpochLogMetadata* metadata,
                                      rocksdb::ColumnFamilyHandle* cf);

  RocksDBLogStoreBase* store_;
  bool read_only_;
  // locks protecting metadata updates (read-modify-write)
  std::vector<std::mutex> locks_;

  std::atomic<FlushToken> next_wal_sync_token_{1};
  std::atomic<FlushToken> wal_synced_up_to_token_{0};
};

}} // namespace facebook::logdevice
