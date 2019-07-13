/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBWriter.h"

#include <algorithm>

#include <folly/small_vector.h>
#include <rocksdb/env.h>

#include "logdevice/common/LocalLogStoreRecordFormat.h"
#include "logdevice/common/Metadata.h"
#include "logdevice/common/RateLimiter.h"
#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/RocksDBKeyFormat.h"
#include "logdevice/server/locallogstore/RocksDBWriterMergeOperator.h"
#include "logdevice/server/locallogstore/WriteOps.h"

namespace facebook { namespace logdevice {

using RocksDBKeyFormat::CopySetIndexKey;
using RocksDBKeyFormat::DataKey;
using RocksDBKeyFormat::LogMetaKey;
using RocksDBKeyFormat::PerEpochLogMetaKey;
using RocksDBKeyFormat::StoreMetaKey;

static_assert(sizeof(DataKey) == 21, "expected 21");
static_assert(sizeof(LogMetaKey) == 9, "expected 9");
static_assert(sizeof(StoreMetaKey) == 1, "expected 1");

int RocksDBWriter::writeMulti(
    const std::vector<const WriteOp*>& writes,
    const LocalLogStore::WriteOptions& /*write_options*/,
    rocksdb::ColumnFamilyHandle* metadata_cf,
    std::vector<rocksdb::ColumnFamilyHandle*>* data_cf_handles,
    rocksdb::WriteBatch& wal_batch,
    rocksdb::WriteBatch& mem_batch,
    bool skip_checksum_verification) {
  if (read_only_) {
    ld_check(false);
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  if (store_->acceptingWrites() == E::DISABLED) {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  size_t csi_entry_writes = 0;
  size_t index_entry_writes = 0;
  SCOPE_EXIT {
    STAT_ADD(store_->getStatsHolder(), csi_entry_writes, csi_entry_writes);
    STAT_ADD(store_->getStatsHolder(), index_entry_writes, index_entry_writes);
  };

  size_t record_bytes = 0;
  size_t csi_bytes = 0;
  size_t index_bytes = 0;

  for (size_t i = 0; i < writes.size(); ++i) {
    const WriteOp* write = writes[i];
    rocksdb::ColumnFamilyHandle* data_cf =
        data_cf_handles ? (*data_cf_handles)[i] : nullptr;
    int rv;

    rocksdb::WriteBatch& rocksdb_batch =
        write->durability() <= Durability::MEMORY ? mem_batch : wal_batch;
    switch (write->getType()) {
      case WriteType::PUT: {
        const PutWriteOp* op = static_cast<const PutWriteOp*>(write);

        DataKey key(op->log_id, op->lsn);

        if (!skip_checksum_verification &&
            store_->getSettings()->verify_checksum_during_store) {
          // Reject to store malformed records.
          rv = LocalLogStoreRecordFormat::checkWellFormed(
              op->record_header, op->data);
          if (rv != 0) {
            RATELIMIT_ERROR(std::chrono::seconds(10),
                            10,
                            "Refusing to write malformed record %lu%s. "
                            "Header: %s, data: %s",
                            op->log_id.val_,
                            lsn_to_string(op->lsn).c_str(),
                            hexdump_buf(op->record_header, 500).c_str(),
                            hexdump_buf(op->data, 500).c_str());
            // Fail and send an error to sequencer. The record was corrupted
            // or malformed by this or sequencer's node, likely due to bad
            // hardware or bug.
            err = E::CHECKSUM_MISMATCH;
            return -1;
          } else if (store_->getSettings()->test_corrupt_stores) {
            RATELIMIT_INFO(std::chrono::seconds(60),
                           5,
                           "Pretending record %lu%s is corrupt. Header: %s, "
                           "data: %s",
                           op->log_id.val_,
                           lsn_to_string(op->lsn).c_str(),
                           hexdump_buf(op->record_header, 500).c_str(),
                           hexdump_buf(op->data, 500).c_str());
            err = E::CHECKSUM_MISMATCH;
            return -1;
          }
        }

        // NOTE: There is an assumption in prepare_write_op() in
        // RocksDBWriterMergeOperator that the value in RocksDB will be
        // exactly the concatenation of the header and data blobs.  If that
        // ever changes here, take care to update the code there as well.
        rocksdb::Slice key_slice =
            key.sliceForWriting(op->TEST_data_key_format);

        folly::small_vector<rocksdb::Slice, 3> value_slices;

        // NOTE: At least RocksDBWriterMergeOperator and
        // RocksDBCompactionFilter expect this format (header byte then same
        // as normal non-merge stores).
        value_slices.emplace_back(
            &RocksDBWriterMergeOperator::DATA_MERGE_HEADER, 1);
        value_slices.emplace_back(
            reinterpret_cast<const char*>(op->record_header.data),
            op->record_header.size);
        value_slices.emplace_back(
            reinterpret_cast<const char*>(op->data.data), op->data.size);

        rocksdb_batch.Merge(
            data_cf,
            rocksdb::SliceParts(&key_slice, 1),
            rocksdb::SliceParts(value_slices.data(), value_slices.size()));

        record_bytes += key_slice.size();
        for (const auto& s : value_slices) {
          record_bytes += s.size();
        }

        if (op->copyset_index_lsn.hasValue()) {
          // Writing copyset index entry
          ++csi_entry_writes;
          ld_check(op->copyset_index_entry.data);
          ld_check(op->copyset_index_entry.size);
          // TODO (t9002309): block records
          ld_check(op->copyset_index_lsn.value() == LSN_INVALID);
          lsn_t csi_lsn = op->lsn;

          // Writing copyset index entry
          CopySetIndexKey key{op->log_id,
                              csi_lsn,
                              // TODO (t9002309): block records
                              CopySetIndexKey::SINGLE_ENTRY_TYPE};
          Slice value = op->copyset_index_entry;
          rocksdb::Slice csi_key_slice(
              reinterpret_cast<const char*>(&key), sizeof key);
          rocksdb::Slice value_slice(
              reinterpret_cast<const char*>(value.data), value.size);
          rocksdb_batch.Merge(data_cf, csi_key_slice, value_slice);

          csi_bytes += csi_key_slice.size() + value_slice.size();
        }

        // Writing index entries
        for (auto it = op->index_key_list.begin();
             it != op->index_key_list.end();
             ++it) {
          // Writing a findTime or findKey index entry
          ++index_entry_writes;

          folly::small_vector<char, 26> index_key =
              RocksDBKeyFormat::IndexKey::create(op->log_id,
                                                 (*it).first,  // index type
                                                 (*it).second, // key
                                                 op->lsn);
          rocksdb::Slice k_slice(index_key.data(), index_key.size());
          rocksdb::Slice v_slice(nullptr, 0);
          rocksdb_batch.Put(data_cf, k_slice, v_slice);

          index_bytes += k_slice.size();
        }
        break;
      }

      case WriteType::DELETE: {
        const DeleteWriteOp* op = static_cast<const DeleteWriteOp*>(write);
        // DataKey is being transitioned to a new format, see comment in
        // RocksDBKeyFormat.h. Here we don't know which format the target
        // record uses, so we delete both possible keys. The two keys only
        // differ in size (old format has constant WAVE_KEY_WHEN_IN_VALUE
        // at the end, the new format doesn't).
        // TODO (#10357210): get rid of this when all data is converted.

        // Delete corresponding copyset index entry if it exists.
        CopySetIndexKey csi_key{op->log_id,
                                op->lsn,
                                // TODO (t9002309): block records
                                CopySetIndexKey::SINGLE_ENTRY_TYPE};
        rocksdb_batch.Delete(
            data_cf,
            rocksdb::Slice(
                reinterpret_cast<const char*>(&csi_key), sizeof csi_key));

        DataKey key(op->log_id, op->lsn);
        for (rocksdb::Slice key_slice : key.slicesForDeleting()) {
          rocksdb_batch.Delete(data_cf, key_slice);
        }

        // Delete the corresponding findKey index entry, if it exists.
        // Currently we read the data record to do this, which probably makes
        // deleting records very inefficient. It doesn't matter now because
        // records are hardly ever deleted. If we ever want to delete them
        // more often (e.g. extras), we may need to get rid of the Get() here,
        // probably by adding the timestamp and keys to
        // DELETE_Message.
        std::string record;
        auto status = store_->getDB().Get(
            rocksdb::ReadOptions(),
            data_cf != nullptr ? data_cf
                               : store_->getDB().DefaultColumnFamily(),
            // Deleting index is not very important, so we don't
            // bother trying both possible key formats here.
            key.sliceForWriting(),
            &record);

        if (!status.ok() && !status.IsNotFound()) {
          err = E::LOCAL_LOG_STORE_WRITE;
          return -1;
        }

        if (status.ok()) {
          std::map<KeyType, std::string> optional_keys;
          std::chrono::milliseconds timestamp;
          rv = LocalLogStoreRecordFormat::parse(
              Slice(record.data(), record.size()),
              &timestamp,
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              nullptr,
              0,
              nullptr,
              &optional_keys,
              nullptr,
              store_->getShardIdx());
          if (rv == 0) {
            // Delete findtime index.
            {
              uint64_t timestamp_big_endian =
                  htobe64((uint64_t)timestamp.count());
              auto index_key = RocksDBKeyFormat::IndexKey::create(
                  op->log_id,
                  FIND_TIME_INDEX,
                  std::string(
                      reinterpret_cast<const char*>(&timestamp_big_endian),
                      sizeof(uint64_t)),
                  op->lsn);
              rocksdb_batch.Delete(
                  data_cf, rocksdb::Slice(index_key.data(), index_key.size()));
            }

            // Delete findkey index if the record has a key.
            const auto key_pair = optional_keys.find(KeyType::FINDKEY);
            if (key_pair != optional_keys.end()) {
              auto index_key = RocksDBKeyFormat::IndexKey::create(
                  op->log_id,
                  FIND_KEY_INDEX,
                  std::move(key_pair->second),
                  op->lsn);
              rocksdb_batch.Delete(
                  data_cf, rocksdb::Slice(index_key.data(), index_key.size()));
            }
          } else {
            // There's a malformed record in the DB. parse() already logged an
            // error about it.
            // Ignore the record, deleting index is not that important.
          }
        }

        break;
      }

      case WriteType::DUMP_RELEASE_STATE: {
        const DumpReleaseStateWriteOp* op =
            static_cast<const DumpReleaseStateWriteOp*>(write);

        for (auto const& release : op->releases_) {
          LogMetaKey key{LogMetadataType::LAST_RELEASED, release.first};
          LastReleasedMetadata value{release.second};
          Slice value_slice = value.serialize();

          rocksdb_batch.Put(
              metadata_cf,
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
              rocksdb::Slice(reinterpret_cast<const char*>(value_slice.data),
                             value_slice.size));
        }

        break;
      }

      case WriteType::PUT_LOG_METADATA: {
        auto op = static_cast<const PutLogMetadataWriteOp*>(write);

        LogMetaKey key{op->metadata_->getType(), op->log_id_};
        Slice value = op->metadata_->serialize();

        rocksdb::Slice key_slice(
            reinterpret_cast<const char*>(&key), sizeof key);
        rocksdb::Slice value_slice(
            reinterpret_cast<const char*>(value.data), value.size);

        if (detail::useMerge(*op->metadata_)) {
          rocksdb_batch.Merge(metadata_cf, key_slice, value_slice);
        } else {
          rocksdb_batch.Put(metadata_cf, key_slice, value_slice);
        }
        break;
      }

      case WriteType::DELETE_LOG_METADATA: {
        auto op = static_cast<const DeleteLogMetadataWriteOp*>(write);
        deleteLogMetadata(op->first_log_id_,
                          op->last_log_id_,
                          op->type_,
                          LocalLogStore::WriteOptions(),
                          metadata_cf);
        break;
      }

      case WriteType::PUT_SHARD_METADATA: {
        auto op = static_cast<const PutStoreMetadataWriteOp*>(write);

        StoreMetaKey key{op->metadata_->getType()};
        Slice value = op->metadata_->serialize();

        rocksdb_batch.Put(
            metadata_cf,
            rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
            rocksdb::Slice(
                reinterpret_cast<const char*>(value.data), value.size));
        break;
      }

      case WriteType::MERGE_MUTABLE_PER_EPOCH_METADATA: {
        auto op =
            static_cast<const MergeMutablePerEpochLogMetadataWriteOp*>(write);

        PerEpochLogMetaKey key{
            PerEpochLogMetadataType::MUTABLE, op->log_id, op->epoch};
        Slice value = op->metadata->serialize();

        // Use mem_batch because we don't need WAL for this type of metadata.
        // It is written in best-effort manner.
        mem_batch.Merge(
            metadata_cf,
            rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
            rocksdb::Slice(static_cast<const char*>(value.data), value.size));
        break;
      }

      default:
        ld_error("Invalid WriteOp!  what=%d", int(write->getType()));
        ld_check(false);
        err = E::INTERNAL;
        return -1;
    }
  }

  rocksdb::WriteOptions options;
  for (auto rocksdb_batch : {&wal_batch, &mem_batch}) {
    if (rocksdb_batch->Count() > 0) {
      rocksdb::Status status = store_->writeBatch(options, rocksdb_batch);
      if (!status.ok()) {
        err = E::LOCAL_LOG_STORE_WRITE;
        return -1;
      }
    }
    options.disableWAL = true;
  }

  STAT_ADD(store_->getStatsHolder(), record_bytes_written, record_bytes);
  STAT_ADD(store_->getStatsHolder(), csi_bytes_written, csi_bytes);
  STAT_ADD(store_->getStatsHolder(), index_bytes_written, index_bytes);
  return 0;
}

// ====== Metadata operations ======

int RocksDBWriter::readLogMetadata(logid_t log_id,
                                   LogMetadata* metadata,
                                   rocksdb::ColumnFamilyHandle* cf) {
  SCOPED_IO_TRACING_CONTEXT(store_->getIOTracing(),
                            "read-log-meta:{}",
                            logMetadataTypeNames()[metadata->getType()]);
  return readMetadata(LogMetaKey(metadata->getType(), log_id), metadata, cf);
}

int RocksDBWriter::readStoreMetadata(StoreMetadata* metadata,
                                     rocksdb::ColumnFamilyHandle* cf) {
  SCOPED_IO_TRACING_CONTEXT(store_->getIOTracing(),
                            "read-store-meta:{}",
                            storeMetadataTypeNames()[metadata->getType()]);
  return readMetadata(StoreMetaKey(metadata->getType()), metadata, cf);
}

int RocksDBWriter::writeLogMetadata(logid_t log_id,
                                    const LogMetadata& metadata,
                                    const LocalLogStore::WriteOptions& options,
                                    rocksdb::ColumnFamilyHandle* cf) {
  return writeMetadata(
      LogMetaKey(metadata.getType(), log_id), metadata, options, cf);
}

int RocksDBWriter::deleteStoreMetadata(
    const StoreMetadataType& type,
    const LocalLogStore::WriteOptions& options,
    rocksdb::ColumnFamilyHandle* cf) {
  return deleteMetadata(StoreMetaKey(type), options, cf);
}

int RocksDBWriter::deleteLogMetadata(logid_t first_log_id,
                                     logid_t last_log_id,
                                     const LogMetadataType& type,
                                     const LocalLogStore::WriteOptions& options,
                                     rocksdb::ColumnFamilyHandle* cf) {
  if (first_log_id == last_log_id) {
    // Optimization: if we're only deleting one log, avoid range deletions.
    return deleteMetadata(LogMetaKey(type, first_log_id), options, cf);
  }
  logid_t last_log_id_p_1(
      std::min(
          std::numeric_limits<logid_t::raw_type>::max() - 1, last_log_id.val_) +
      1);
  return deleteMetadataRange(
      LogMetaKey(type, first_log_id),
      LogMetaKey(type, last_log_id_p_1), // we wish to include the last log
      options,
      cf);
}

int RocksDBWriter::deletePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    const PerEpochLogMetadataType& type,
    const LocalLogStore::WriteOptions& options,
    rocksdb::ColumnFamilyHandle* cf) {
  return deleteMetadata(PerEpochLogMetaKey(type, log_id, epoch), options, cf);
}

int RocksDBWriter::writeStoreMetadata(
    const StoreMetadata& metadata,
    const LocalLogStore::WriteOptions& options,
    rocksdb::ColumnFamilyHandle* cf) {
  return writeMetadata(StoreMetaKey(metadata.getType()), metadata, options, cf);
}

int RocksDBWriter::updateLogMetadata(
    logid_t log_id,
    ComparableLogMetadata& metadata,
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

  LogMetaKey key(metadata.getType(), log_id);
  Slice value(metadata.serialize());

  batch.Put(
      cf,
      rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
      rocksdb::Slice(reinterpret_cast<const char*>(value.data), value.size));

  auto p = LogMetadataFactory::create(metadata.getType());
  ld_assert(dynamic_cast<ComparableLogMetadata*>(p.get()) != nullptr);

  std::unique_lock<std::mutex> lock(locks_[log_id.val_ % locks_.size()]);
  ComparableLogMetadata* prev = static_cast<ComparableLogMetadata*>(p.get());

  int rv = readLogMetadata(log_id, prev, cf);
  if (rv == 0 && !(*prev < metadata)) {
    // update 'metadata' with the fresher value read from the local log store
    metadata.deserialize(prev->serialize());
    err = E::UPTODATE;
    return -1;
  } else if (rv != 0 && err != E::NOTFOUND) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Reading existing metadata type %d for log %lu failed: %s",
                    static_cast<int>(metadata.getType()),
                    log_id.val_,
                    error_description(err));
    return -1;
  }

  rocksdb::Status status = store_->writeBatch(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  return 0;
}

int RocksDBWriter::readPreviousPerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    PerEpochLogMetadata* metadata,
    rocksdb::ColumnFamilyHandle* cf) {
  ld_check(epoch.val_ > 0);
  PerEpochLogMetaKey key(metadata->getType(), log_id, epoch_t(epoch.val_ - 1));

  SCOPED_IO_TRACING_CONTEXT(
      store_->getIOTracing(),
      "read-prev-epoch-log-meta:{}",
      perEpochLogMetadataTypeNames()[metadata->getType()]);

  auto read_options = RocksDBLogStoreBase::getReadOptionsSinglePrefix();

  if (cf == nullptr) {
    cf = store_->getDB().DefaultColumnFamily();
  }

  auto it = std::unique_ptr<rocksdb::Iterator>(
      store_->getDB().NewIterator(read_options, cf));

  auto it_error = [&] {
    if (!it->status().ok()) {
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "rocksdb Iterator error: %s",
                         it->status().ToString().c_str());
      return true;
    }
    return false;
  };
  auto it_valid = [&] {
    return it->status().ok() && it->Valid() &&
        PerEpochLogMetaKey::valid(
               metadata->getType(), it->key().data(), it->key().size());
  };
  rocksdb::Slice key_slice(reinterpret_cast<const char*>(&key), sizeof(key));
  it->SeekForPrev(key_slice);

  if (it_error()) {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  if (it_valid() && PerEpochLogMetaKey::getLogID(it->key().data()) == log_id) {
    PerEpochLogMetaKey new_key =
        PerEpochLogMetaKey(metadata->getType(),
                           PerEpochLogMetaKey::getLogID(it->key().data()),
                           PerEpochLogMetaKey::getEpoch(it->key().data()));
    return readMetadata(new_key, metadata, cf);
  }

  ld_check(err == E::NOTFOUND);
  return -1;
}

int RocksDBWriter::readPerEpochLogMetadata(logid_t log_id,
                                           epoch_t epoch,
                                           PerEpochLogMetadata* metadata,
                                           rocksdb::ColumnFamilyHandle* cf,
                                           bool find_last_available,
                                           bool allow_blocking_io) {
  SCOPED_IO_TRACING_CONTEXT(
      store_->getIOTracing(),
      "read-epoch-log-meta:{}",
      perEpochLogMetadataTypeNames()[metadata->getType()]);
  int rv = readMetadata(PerEpochLogMetaKey(metadata->getType(), log_id, epoch),
                        metadata,
                        cf,
                        allow_blocking_io);
  if (!find_last_available || rv == 0 || err != E::NOTFOUND ||
      epoch.val_ == 0) {
    return rv;
  }

  return readPreviousPerEpochLogMetadata(log_id, epoch, metadata, cf);
}

int RocksDBWriter::updatePerEpochLogMetadata(
    logid_t log_id,
    epoch_t epoch,
    PerEpochLogMetadata& metadata,
    LocalLogStore::SealPreemption seal_preempt,
    const LocalLogStore::WriteOptions& options,
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

  auto p = PerEpochLogMetadataFactory::create(metadata.getType());
  ld_assert(dynamic_cast<PerEpochLogMetadata*>(p.get()) != nullptr);

  std::unique_lock<std::mutex> lock(locks_[log_id.val_ % locks_.size()]);
  PerEpochLogMetadata* prev = static_cast<PerEpochLogMetadata*>(p.get());
  int rv = readPerEpochLogMetadata(log_id, epoch, prev, cf);
  const bool found_existing = (rv == 0);
  if (rv != 0 && err != E::NOTFOUND) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Reading existing per-epoch log metadata type %d for log "
                    "%lu epoch %u failed: %s",
                    static_cast<int>(metadata.getType()),
                    log_id.val_,
                    epoch.val_,
                    error_description(err));
    return -1;
  }

  Status st = prev->update(metadata);
  if (rv == 0 && st == E::UPTODATE) {
    err = E::UPTODATE;
    return -1;
  }

  PerEpochLogMetaKey key(metadata.getType(), log_id, epoch);

  if (metadata.empty()) {
    // if the metadata to update is considered empty, do not store it
    // as an optimization. If there are existing values, delete it first.
    if (found_existing) {
      // TODO 11866467: not possible if t11866467 is implemented.
      return deleteMetadata(key, options, cf);
    } else {
      return 0;
    }
  }

  // TODO 11866467: This will no longer be needed when log recovery protocol
  // is ultimately fixed. EpochRecoveryMetadata, once written, will not be
  // changed as the consensus is guranteed to be carried over among different
  // EpochRecovery instances recoverying the same epoch.
  if (!found_existing &&
      seal_preempt == LocalLogStore::SealPreemption::ENABLE) {
    // if there are no existing metadata found, check if the metadata is
    // preempted by the seal before store. This is to eliminate a corner case
    // of TOCTTOU race happened when both Seal and PerEpochLogMetadata
    // got updated within the window of seal check and storing per-epoch
    // metadata
    SealMetadata seal_metadata;
    rv = readLogMetadata(log_id, &seal_metadata, cf);
    if (rv == 0 && metadata.isPreempted(seal_metadata.seal_)) {
      metadata.reset();
      err = E::UPTODATE;
      return -1;
    }
  }

  rocksdb::WriteBatch batch;
  Slice value(metadata.serialize());
  batch.Put(
      cf,
      rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof key),
      rocksdb::Slice(reinterpret_cast<const char*>(value.data), value.size));

  rocksdb::Status status = store_->writeBatch(rocksdb::WriteOptions(), &batch);
  if (!status.ok()) {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  return 0;
}

int RocksDBWriter::writeLogSnapshotBlobs(
    rocksdb::ColumnFamilyHandle* snapshots_cf,
    LocalLogStore::LogSnapshotBlobType snapshot_type,
    const std::vector<std::pair<logid_t, Slice>>& snapshots) {
  if (read_only_) {
    ld_check(false);
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  if (!snapshots_cf) {
    return -1;
  }

  rocksdb::WriteBatch batch;
  for (auto& snapshot_kv : snapshots) {
    auto logid = snapshot_kv.first;
    RocksDBKeyFormat::LogSnapshotBlobKey key(snapshot_type, logid);
    auto& snapshot = snapshot_kv.second;
    batch.Put(snapshots_cf,
              rocksdb::Slice(reinterpret_cast<const char*>(&key), sizeof(key)),
              rocksdb::Slice(
                  reinterpret_cast<const char*>(snapshot.data), snapshot.size));
  }

  rocksdb::Status status = store_->writeBatch(rocksdb::WriteOptions(), &batch);
  return status.ok() ? 0 : -1;
}

rocksdb::Status RocksDBWriter::syncWAL() {
  FlushToken synced_up_to = next_wal_sync_token_.fetch_add(1);

  auto time_start = std::chrono::steady_clock::now();
  auto status = store_->getDB().SyncWAL();
  auto time_end = std::chrono::steady_clock::now();

  STAT_INCR(store_->getStatsHolder(), wal_syncs);
  STAT_ADD(store_->getStatsHolder(),
           wal_sync_microsec,
           std::chrono::duration_cast<std::chrono::microseconds>(time_end -
                                                                 time_start)
               .count());

  store_->enterFailSafeIfFailed(status, "SyncWAL()");
  if (!status.ok()) {
    PER_SHARD_STAT_INCR(store_->getStatsHolder(),
                        local_logstore_failed_writes,
                        store_->getShardIdx());
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "rocksdb SyncWAL() failed: %s",
                    status.ToString().c_str());
  } else {
    atomic_fetch_max(wal_synced_up_to_token_, synced_up_to);
  }
  return status;
}

int RocksDBWriter::enumerateStoreMetadata(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* cf,
    std::vector<StoreMetadataType> types,
    std::function<void(StoreMetadataType, StoreMetadata&)> cb) {
  rocksdb::ReadOptions read_options;
  bool had_malformed_values = false;
  for (auto type : types) {
    StoreMetaKey key(type);
    rocksdb::Slice key_slice(reinterpret_cast<const char*>(&key), sizeof(key));
    std::string value;
    rocksdb::Status status = db->Get(read_options, cf, key_slice, &value);
    if (!status.ok()) {
      if (status.IsNotFound()) {
        return 0;
      }
      err = E::LOCAL_LOG_STORE_READ;
      return -1;
    }
    auto meta = StoreMetadataFactory::create(type);
    int rv = meta->deserialize(Slice::fromString(value));
    if (rv == -1) {
      had_malformed_values = true;
      continue;
    }
    cb(type, *meta);
  }
  if (had_malformed_values) {
    err = E::MALFORMED_RECORD;
    return -1;
  }
  return 0;
}

int RocksDBWriter::enumerateLogMetadata(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* cf,
    std::vector<logid_t> logs,
    std::vector<LogMetadataType> types,
    std::function<void(LogMetadataType, logid_t, LogMetadata&)> cb) {
  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, cf));
  bool had_malformed_values = false;

  for (auto type : types) {
    size_t log_idx = 0;
    {
      LogMetaKey first_key(type, logs.empty() ? logid_t(0) : logs[0]);
      it->Seek(rocksdb::Slice(
          reinterpret_cast<const char*>(&first_key), sizeof(first_key)));
    }
    while (it->status().ok()) {
      bool it_good = it->Valid() && it->key().size() > 0 &&
          it->key()[0] == LogMetaKey::getHeader(type);

      // TODO (#39174994): This is a temporary workaround for a key collision.
      //                   Remove when migration is complete.
      if (it_good &&
          it->key().compare(rocksdb::Slice(
              RocksDBLogStoreBase::OLD_SCHEMA_VERSION_KEY)) == 0) {
        it->Next();
        continue;
      }

      if (it_good &&
          !LogMetaKey::valid(type, it->key().data(), it->key().size())) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Malformed metadata key. Key: %s, value: %s",
            hexdump_buf(it->key().data(), it->key().size()).c_str(),
            hexdump_buf(it->value().data(), it->value().size()).c_str());
        had_malformed_values = true;
        it->Next();
        continue;
      }
      logid_t log;
      if (it_good) {
        log = LogMetaKey::getLogID(it->key().data());
      }
      if (!it_good || (!logs.empty() && log != logs.at(log_idx))) {
        if (log_idx + 1 < logs.size()) {
          ++log_idx;
          LogMetaKey next_key(type, logs.at(log_idx));
          it->Seek(rocksdb::Slice(
              reinterpret_cast<const char*>(&next_key), sizeof(next_key)));
          continue;
        }
        break;
      }

      auto meta = LogMetadataFactory::create(type);
      int rv = meta->deserialize(Slice(it->value().data(), it->value().size()));
      if (rv == 0) {
        cb(type, log, *meta);
      } else {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            10,
            "Malformed metadata value. Key: %s, value: %s",
            hexdump_buf(it->key().data(), it->key().size()).c_str(),
            hexdump_buf(it->value().data(), it->value().size()).c_str());
        had_malformed_values = true;
      }
      it->Next();
    }
    if (!it->status().ok()) {
      err = E::LOCAL_LOG_STORE_READ;
      return -1;
    }
  }
  if (had_malformed_values) {
    err = E::MALFORMED_RECORD;
    return -1;
  }
  return 0;
}

int RocksDBWriter::enumeratePerEpochLogMetadata(
    rocksdb::DB* db,
    rocksdb::ColumnFamilyHandle* cf,
    std::vector<logid_t> logs,
    std::vector<PerEpochLogMetadataType> types,
    std::function<
        void(PerEpochLogMetadataType, logid_t, epoch_t, PerEpochLogMetadata&)>
        cb) {
  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(read_options, cf));
  bool had_malformed_values = false;

  for (auto type : types) {
    size_t log_idx = 0;
    {
      PerEpochLogMetaKey first_key(
          type, logs.empty() ? logid_t(0) : logs[0], epoch_t(0));
      it->Seek(rocksdb::Slice(
          reinterpret_cast<const char*>(&first_key), sizeof(first_key)));
    }
    while (it->status().ok()) {
      bool it_good = it->Valid() && it->key().size() > 0 &&
          it->key()[0] == PerEpochLogMetaKey::getHeader(type);
      if (it_good &&
          !PerEpochLogMetaKey::valid(
              type, it->key().data(), it->key().size())) {
        had_malformed_values = true;
        it->Next();
        continue;
      }
      logid_t log;
      if (it_good) {
        log = PerEpochLogMetaKey::getLogID(it->key().data());
      }
      if (!it_good || (!logs.empty() && log != logs.at(log_idx))) {
        if (log_idx + 1 < logs.size()) {
          ++log_idx;
          PerEpochLogMetaKey next_key(type, logs.at(log_idx), epoch_t(0));
          it->Seek(rocksdb::Slice(
              reinterpret_cast<const char*>(&next_key), sizeof(next_key)));
          continue;
        }
        break;
      }

      auto meta = PerEpochLogMetadataFactory::create(type);
      int rv = meta->deserialize(Slice(it->value().data(), it->value().size()));
      if (rv == 0) {
        cb(type, log, PerEpochLogMetaKey::getEpoch(it->key().data()), *meta);
      } else {
        had_malformed_values = true;
      }
      it->Next();
    }
    if (!it->status().ok()) {
      err = E::LOCAL_LOG_STORE_READ;
      return -1;
    }
  }
  if (had_malformed_values) {
    err = E::MALFORMED_RECORD;
    return -1;
  }
  return 0;
}

namespace detail {
bool useMerge(const LogMetadata& /*metadata*/) {
  // Some LogMetadata types use merge operator, so they need to be written
  // using Merge() instead of a regular Put().
  // ... except that there are no such LogMetadata types anymore. Maybe there
  // will be some in future. In particular, all ComparableLogMetadata could
  // be migrated to use merge operator instead of mutexes.
  return false;
}
} // namespace detail

}} // namespace facebook::logdevice
