/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

/**
 * @file A fake LocalLogStore subclass on which every operation fails.  Used
 * in place of a DB that cannot be opened at server startup, for example
 * because the disk is being repaired.  The server can start with most DBs
 * working fine.
 */

class FailingLocalLogStore : public LocalLogStore {
  int writeMulti(const std::vector<const WriteOp*>&,
                 const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  int sync(Durability) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  FlushToken maxFlushToken() const override {
    return FlushToken_INVALID;
  }

  FlushToken flushedUpThrough() const override {
    return FlushToken_INVALID;
  }

  FlushToken maxWALSyncToken() const override {
    return FlushToken_INVALID;
  }

  FlushToken walSyncedUpThrough() const override {
    return FlushToken_INVALID;
  }

  // Implemented in .cpp to return a fake failing iterator
  std::unique_ptr<ReadIterator> read(logid_t log_id,
                                     const ReadOptions& options) const override;
  std::unique_ptr<AllLogsIterator>
  readAllLogs(const ReadOptions& options,
              const folly::Optional<
                  std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>& logs)
      const override;

  int readLogMetadata(logid_t, LogMetadata*) override {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  int writeLogMetadata(logid_t,
                       const LogMetadata&,
                       const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  int deleteStoreMetadata(const StoreMetadataType,
                          const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  int deleteLogMetadata(logid_t,
                        logid_t,
                        const LogMetadataType,
                        const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }
  int deletePerEpochLogMetadata(logid_t,
                                epoch_t,
                                const PerEpochLogMetadataType,
                                const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  int readStoreMetadata(StoreMetadata*) override {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  int writeStoreMetadata(const StoreMetadata&, const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  int readAllLogSnapshotBlobs(LogSnapshotBlobType,
                              LogSnapshotBlobCallback) override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  int writeLogSnapshotBlobs(
      LogSnapshotBlobType,
      const std::vector<std::pair<logid_t, Slice>>&) override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  int deleteAllLogSnapshotBlobs() override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  int updateLogMetadata(logid_t,
                        ComparableLogMetadata&,
                        const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  int readPerEpochLogMetadata(logid_t,
                              epoch_t,
                              PerEpochLogMetadata*,
                              bool,
                              bool) const override {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }

  int updatePerEpochLogMetadata(logid_t,
                                epoch_t,
                                PerEpochLogMetadata&,
                                LocalLogStore::SealPreemption,
                                const WriteOptions&) override {
    err = E::LOCAL_LOG_STORE_WRITE;
    return -1;
  }

  Status acceptingWrites() const override {
    return E::DISABLED;
  }

  int isEmpty() const override {
    // Don't want upper layers making decisions based on this
    ld_check(false);
    return 1;
  }

  int getShardIdx() const override {
    // Don't want upper layers making decisions based on this
    ld_check(false);
    return -1;
  }

  int getHighestInsertedLSN(logid_t, lsn_t*) override {
    err = E::NOTSUPPORTED;
    return -1;
  }

  int getApproximateTimestamp(
      logid_t /*log_id*/,
      lsn_t /*lsn*/,
      bool /*allow_blocking_io*/,
      std::chrono::milliseconds* /*timestamp_out*/) override {
    err = E::LOCAL_LOG_STORE_READ;
    return -1;
  }
};

}} // namespace facebook::logdevice
