/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/rebuilding/ChunkRebuilding.h"

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"

namespace facebook { namespace logdevice {

ChunkRebuilding::ChunkRebuilding(
    std::unique_ptr<ChunkData> data,
    log_rebuilding_id_t chunk_id,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    lsn_t rebuilding_version,
    lsn_t restart_version,
    uint32_t shard,
    ShardRebuildingV2Ref owner)
    : owner_(owner),
      data_(std::move(data)),
      chunkID_(chunk_id),
      rebuildingSet_(rebuilding_set),
      rebuildingSettings_(rebuilding_settings),
      rebuildingVersion_(rebuilding_version),
      restartVersion_(restart_version),
      shard_(shard),
      startTime_(SteadyTimestamp::now()) {}

ChunkRebuilding::~ChunkRebuilding() {}

const RebuildingSet& ChunkRebuilding::getRebuildingSet() const {
  return *rebuildingSet_;
}
logid_t ChunkRebuilding::getLogID() const {
  return data_->address.log;
}
lsn_t ChunkRebuilding::getRebuildingVersion() const {
  return rebuildingVersion_;
}
lsn_t ChunkRebuilding::getRestartVersion() const {
  return restartVersion_;
}
log_rebuilding_id_t ChunkRebuilding::getLogRebuildingId() const {
  return chunkID_;
}
ServerInstanceId ChunkRebuilding::getServerInstanceId() const {
  return Worker::onThisThread()->processor_->getServerInstanceId();
}
UpdateableSettings<RebuildingSettings>
ChunkRebuilding::getRebuildingSettings() const {
  return rebuildingSettings_;
}

void ChunkRebuilding::start() {
  ld_check(data_->numRecords() > 0);
  readOnly_ =
      rebuildingSettings_->read_only == RebuildingReadOnlyOption::ON_DONOR;
  rrStores_.resize(data_->numRecords());
  rrAmends_.resize(data_->numRecords());
  for (size_t i = 0; i < rrStores_.size(); ++i) {
    rrStores_[i] = std::make_unique<RecordRebuildingStore>(
        data_->blockID,
        shard_,
        RawRecord(data_->getLSN(i), data_->getRecordBlob(i), /* owned */ false),
        this,
        data_->replication,
        std::shared_ptr<PayloadHolder>(
            data_, data_->getUninitializedPayloadHolder(i)));
  }
  numInFlight_ = rrStores_.size();
  for (size_t i = 0; i < rrStores_.size(); ++i) {
    rrStores_[i]->start(readOnly_);
  }
}

bool ChunkRebuilding::onStoreSent(Status st,
                                  const STORE_Header& header,
                                  ShardID to,
                                  lsn_t rebuilding_version,
                                  uint32_t rebuilding_wave) {
  // We're the one who sent the message, so it better be valid and in range.
  ld_check_eq(header.rid.logid, data_->address.log);
  ld_check_between(
      header.rid.lsn(), data_->address.min_lsn, data_->address.max_lsn);
  ssize_t idx = data_->findLSN(header.rid.lsn());
  ld_check(idx >= 0);

  auto& store = rrStores_.at(idx);
  if (store != nullptr) {
    store->onStoreSent(st, header, to, rebuilding_version, rebuilding_wave);
    return true;
  }

  auto& amend = rrAmends_.at(idx);
  if (amend != nullptr) {
    amend->onStoreSent(st, header, to, rebuilding_version, rebuilding_wave);
    return true;
  }

  return false;
}

bool ChunkRebuilding::onStored(const STORED_Header& header,
                               ShardID from,
                               lsn_t rebuilding_version,
                               uint32_t rebuilding_wave,
                               log_rebuilding_id_t rebuilding_id,
                               ServerInstanceId server_instance_id,
                               FlushToken flush_token) {
  ssize_t idx = data_->findLSN(header.rid.lsn());
  if (header.rid.logid != data_->address.log || idx < 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Log ID or LSN in STORED message (%s) doesn't match the "
                    "ChunkRebuilding (%lu [%s, %s]). This should be "
                    "impossible. Discarding the message.",
                    header.rid.toString().c_str(),
                    data_->address.log.val(),
                    lsn_to_string(data_->address.min_lsn).c_str(),
                    lsn_to_string(data_->address.max_lsn).c_str());
    // No need for the caller to print a warning because we printed an error.
    return true;
  }

  auto& store = rrStores_.at(idx);
  if (store != nullptr) {
    store->onStored(header,
                    from,
                    rebuilding_version,
                    rebuilding_wave,
                    rebuilding_id,
                    server_instance_id,
                    flush_token);
    return true;
  }

  auto& amend = rrAmends_.at(idx);
  if (amend != nullptr) {
    amend->onStored(header,
                    from,
                    rebuilding_version,
                    rebuilding_wave,
                    rebuilding_id,
                    server_instance_id,
                    flush_token);
    return true;
  }

  return false;
}

void ChunkRebuilding::onAllStoresReceived(
    lsn_t lsn,
    std::unique_ptr<FlushTokenMap> flushTokenMap) {
  if (!flushTokenMap->empty()) {
    // TODO (#24665001): Wait for flushes. For now, setting
    // rebuild-store-durability=memory just loses memtables on crash.
  }
  ld_check(numInFlight_ > 0);
  ssize_t idx = data_->findLSN(lsn);
  ld_check(idx != -1);
  auto& store = rrStores_.at(idx);
  auto& amend = rrAmends_.at(idx);
  ld_check(store != nullptr);
  ld_check(amend == nullptr);

  std::unique_ptr<RecordRebuildingAmendState> amend_state =
      store->getRecordRebuildingAmendState();
  store.reset();

  amend = std::make_unique<RecordRebuildingAmend>(lsn,
                                                  shard_,
                                                  this,
                                                  data_->replication,
                                                  amend_state->storeHeader_,
                                                  amend_state->flags_,
                                                  amend_state->newCopyset_,
                                                  amend_state->amendRecipients_,
                                                  amend_state->rebuildingWave_);
  amend->start(readOnly_);
}
void ChunkRebuilding::onCopysetInvalid(lsn_t lsn) {
  onAmendDone(lsn);
}
void ChunkRebuilding::onAllAmendsReceived(
    lsn_t lsn,
    std::unique_ptr<FlushTokenMap> flushTokenMap) {
  if (!flushTokenMap->empty()) {
    // TODO (#24665001): Wait for flushes.
  }
  onAmendDone(lsn);
}

void ChunkRebuilding::onAmendDone(lsn_t lsn) {
  ld_check(numInFlight_ > 0);
  ssize_t idx = data_->findLSN(lsn);
  ld_check(idx != -1);
  auto& amend = rrAmends_.at(idx);
  ld_check(amend != nullptr);

  amend.reset();
  --numInFlight_;

  if (numInFlight_ == 0) {
    owner_.postCallbackRequest([chunk_id = chunkID_,
                                oldest_timestamp = data_->oldestTimestamp](
                                   ShardRebuildingV2* shard_rebuilding) {
      if (!shard_rebuilding) {
        RATELIMIT_INFO(
            std::chrono::seconds(10),
            1,
            "ShardRebuildingV2 went away while ChunkRebuilding was in flight.");
        return;
      }
      shard_rebuilding->onChunkRebuildingDone(chunk_id, oldest_timestamp);
    });

    deleteThis();
  }
}

void ChunkRebuilding::deleteThis() {
  ServerWorker::onThisThread()->runningChunkRebuildings().map.erase(chunkID_);
}

void ChunkRebuilding::getDebugInfo(InfoRebuildingChunksTable& table) const {
  size_t amend_self = 0;
  size_t amend_others = 0;
  for (auto& a : rrAmends_) {
    if (a != nullptr) {
      if (a->isAmendingSelf()) {
        ++amend_self;
      } else {
        ++amend_others;
      }
    }
  }

  table.next()
      .set<0>(data_->address.log)
      .set<1>(shard_)
      .set<2>(data_->address.min_lsn)
      .set<3>(data_->address.max_lsn)
      .set<4>(chunkID_.val())
      .set<5>(data_->blockID)
      .set<6>(data_->totalBytes())
      .set<7>(data_->oldestTimestamp.toMilliseconds())
      .set<8>(numInFlight_ - amend_self - amend_others)
      .set<9>(amend_others)
      .set<10>(amend_self)
      .set<11>(toSystemTimestamp(startTime_).toMilliseconds());
}

StartChunkRebuildingRequest::StartChunkRebuildingRequest(
    worker_id_t worker_id,
    std::unique_ptr<ChunkRebuilding> r)
    : workerID_(worker_id), r_(std::move(r)) {}

int StartChunkRebuildingRequest::getThreadAffinity(int) {
  return workerID_.val();
}

WorkerType StartChunkRebuildingRequest::getWorkerTypeAffinity() {
  return workerType;
}

Request::Execution StartChunkRebuildingRequest::execute() {
  ChunkRebuilding* r = r_.get();
  auto ins =
      ServerWorker::onThisThread()->runningChunkRebuildings().map.emplace(
          r->getLogRebuildingId(), std::move(r_));
  ld_check(ins.second); // ids must be unique
  r->start();
  return Request::Execution::COMPLETE;
}

AbortChunkRebuildingRequest::AbortChunkRebuildingRequest(worker_id_t worker_id,
                                                         log_rebuilding_id_t id)
    : workerID_(worker_id), id_(id) {}

int AbortChunkRebuildingRequest::getThreadAffinity(int) {
  return workerID_.val();
}

WorkerType AbortChunkRebuildingRequest::getWorkerTypeAffinity() {
  return StartChunkRebuildingRequest::workerType;
}

Request::Execution AbortChunkRebuildingRequest::execute() {
  auto& rs = ServerWorker::onThisThread()->runningChunkRebuildings();
  auto it = rs.map.find(id_);
  if (it != rs.map.end()) {
    rs.map.erase(it);
  } else {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Trying to abort ChunkRebuilding that doesn't exist. This "
                   "is normal but should be rare.");
  }

  return Request::Execution::COMPLETE;
}

}} // namespace facebook::logdevice
