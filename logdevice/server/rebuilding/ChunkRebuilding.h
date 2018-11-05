/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/PayloadHolder.h"
#include "logdevice/server/RecordRebuildingStore.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

struct ChunkAddress {
  logid_t log;
  lsn_t min_lsn;
  lsn_t max_lsn;

  bool operator<(const ChunkAddress& rhs) const;
};

// A sequence of records for the same log with the same copyset, as read from
// local log store.
// Note that a sticky copyset block may be split into multiple chunks,
// e.g. if the block crosses partition boundary.
struct ChunkData {
  struct RecordInfo {
    lsn_t lsn;
    // Offset of the end of this record in `buffer`.
    // Note that this can't be a pointer because `buffer`'s data can be
    // reallocated when new records are appended to it.
    size_t offset;

    // This is needed to be able to pass individual records as
    // shared_ptr<PayloadHolder> sharing ownership of the whole chunk.
    PayloadHolder payloadHolder;
  };

  ChunkAddress address;
  // Used for seeding the rng for the copyset selector.
  size_t blockID;
  // Timestamp of the first record in the chunk.
  RecordTimestamp oldestTimestamp;
  // Information about records' epoch.
  std::shared_ptr<ReplicationScheme> replication;

  // All records concatenated together.
  std::string buffer;
  // Offset of end of each record in `buffer`.
  std::vector<RecordInfo> records;

  void addRecord(lsn_t lsn, Slice blob) {
    // Check that LSNs are consecutive.
    ld_check_eq(
        lsn, records.empty() ? address.min_lsn : records.back().lsn + 1);

    buffer.append(blob.ptr(), blob.size);
    records.push_back({lsn, buffer.size(), PayloadHolder()});
  }

  size_t numRecords() const {
    return records.size();
  }

  // Size of all records, including header.
  size_t totalBytes() const {
    return buffer.size();
  }

  lsn_t getLSN(size_t idx) const {
    return records[idx].lsn;
  }

  // Serialized header+value combo, to be parsed
  // by LocalLogStoreRecordFormat::parse().
  Slice getRecordBlob(size_t idx) const {
    size_t off = idx ? records[idx - 1].offset : 0ul;
    return Slice(buffer.data() + off, records[idx].offset - off);
  }

  PayloadHolder* getUninitializedPayloadHolder(size_t idx) {
    return &records[idx].payloadHolder;
  }

  ssize_t findLSN(lsn_t lsn) const {
    // For now LSNs in a chunk are required to be consecutive. If you remove
    // this requirement, just replace this with binary search.
    ld_check(address.max_lsn - address.min_lsn + 1 == records.size());
    if (lsn < address.min_lsn || lsn > address.max_lsn) {
      return -1;
    }
    return lsn - address.min_lsn;
  }
};

class ChunkRebuilding : public RecordRebuildingOwner {
 public:
  // Constructor can be called from any thread but start() needs to be called
  // from the worker thread on which this ChunkRebuilding will live.
  ChunkRebuilding(std::unique_ptr<ChunkData> data,
                  log_rebuilding_id_t chunk_id,
                  std::shared_ptr<const RebuildingSet> rebuilding_set,
                  UpdateableSettings<RebuildingSettings> rebuilding_settings,
                  lsn_t rebuilding_version,
                  lsn_t restart_version,
                  uint32_t shard,
                  ShardRebuildingV2Ref owner);

  // If the chunk rebuilding is still running, aborts it.
  ~ChunkRebuilding();

  void start();

  // Various context information that RecordRebuilding wants from us.
  const RebuildingSet& getRebuildingSet() const override;
  logid_t getLogID() const override;
  lsn_t getRebuildingVersion() const override;
  lsn_t getRestartVersion() const override;
  log_rebuilding_id_t getLogRebuildingId() const override;
  ServerInstanceId getServerInstanceId() const override;
  UpdateableSettings<RebuildingSettings> getRebuildingSettings() const override;

  // These are routed to a corresponding RecordRebuilding.
  // Return false if the corresponding RecordRebuilding has completed; the
  // caller prints a warning in this case.
  bool onStoreSent(Status st,
                   const STORE_Header& header,
                   ShardID to,
                   lsn_t rebuilding_version,
                   uint32_t rebuilding_wave);
  bool onStored(const STORED_Header& header,
                ShardID from,
                lsn_t rebuilding_version,
                uint32_t rebuilding_wave,
                log_rebuilding_id_t rebuilding_id,
                ServerInstanceId server_instance_id,
                FlushToken flush_token);

  // These indicate that one of the RecordRebuildings has completed.
  void
  onAllStoresReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override;
  void onCopysetInvalid(lsn_t lsn) override;
  void
  onAllAmendsReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override;

  // Unregisters itself from ServerWorker's ChunkRebuildingMap.
  void deleteThis();

  void getDebugInfo(InfoRebuildingChunksTable& table) const;

 private:
  ShardRebuildingV2Ref owner_;
  // This needs to be a shared_ptr to allow STORE_Message to keep the payload
  // alive until it's passed to TCP.
  std::shared_ptr<ChunkData> data_;
  log_rebuilding_id_t chunkID_;
  std::shared_ptr<const RebuildingSet> rebuildingSet_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  lsn_t rebuildingVersion_;
  lsn_t restartVersion_;
  uint32_t shard_;
  SteadyTimestamp startTime_;

  std::vector<std::unique_ptr<RecordRebuildingStore>> rrStores_;
  std::vector<std::unique_ptr<RecordRebuildingAmend>> rrAmends_;

  size_t numInFlight_ = 0;

  bool readOnly_ = false;

  void onAmendDone(lsn_t lsn);
};

class StartChunkRebuildingRequest : public Request {
 public:
  explicit StartChunkRebuildingRequest(worker_id_t worker_id,
                                       std::unique_ptr<ChunkRebuilding> r);
  int getThreadAffinity(int) override;
  WorkerType getWorkerTypeAffinity() override;

  Execution execute() override;

  // On what workers ChunkRebuilding state machines live.
  static constexpr WorkerType workerType = WorkerType::GENERAL;

 private:
  worker_id_t workerID_;
  std::unique_ptr<ChunkRebuilding> r_;
};

class AbortChunkRebuildingRequest : public Request {
 public:
  AbortChunkRebuildingRequest(worker_id_t worker_id, log_rebuilding_id_t id);

  int getThreadAffinity(int) override;
  WorkerType getWorkerTypeAffinity() override;

  Execution execute() override;

 private:
  worker_id_t workerID_;
  log_rebuilding_id_t id_;
};

struct ChunkRebuildingMap {
  std::unordered_map<log_rebuilding_id_t, std::unique_ptr<ChunkRebuilding>> map;
};

}} // namespace facebook::logdevice
