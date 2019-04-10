/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/CleanedResponseRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/CLEANED_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace {

class UpdatePerEpochLogMetadataTask : public StorageTask {
 public:
  UpdatePerEpochLogMetadataTask(logid_t log_id,
                                epoch_t epoch,
                                std::unique_ptr<EpochRecoveryMetadata> metadata,
                                WeakRef<CleanedResponseRequest> parent)
      : StorageTask(StorageTask::Type::UPDATE_PER_EPOCH_METADATA),
        log_id_(log_id),
        epoch_(epoch),
        parent_(std::move(parent)),
        metadata_(std::move(metadata)) {
    ld_check(parent_);
    // must have a valid metadata
    ld_check(metadata_ != nullptr);
    ld_check(metadata_->valid());
  }

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

  void execute() override;
  Durability durability() const override {
    return Durability::SYNC_WRITE;
  }
  void onDone() override;
  void onDropped() override;

 private:
  const logid_t log_id_;
  const epoch_t epoch_;
  WeakRef<CleanedResponseRequest> parent_;
  std::unique_ptr<EpochRecoveryMetadata> metadata_;
  Status status_{E::OK};
};

void UpdatePerEpochLogMetadataTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();
  int rv = store.updatePerEpochLogMetadata(
      log_id_, epoch_, *metadata_, LocalLogStore::SealPreemption::ENABLE);

  if (rv < 0 && err != E::UPTODATE) {
    status_ = E::FAILED;
  }

  if (rv == 0 && !metadata_->empty()) {
    // metadata stored only if it is not empty
    STAT_INCR(storageThreadPool_->stats(), epoch_recovery_metadata_stored);
  }
}

void UpdatePerEpochLogMetadataTask::onDone() {
  auto parent = parent_.get();
  if (parent != nullptr) {
    parent->onWriteTaskDone(status_);
  }
}

void UpdatePerEpochLogMetadataTask::onDropped() {
  auto parent = parent_.get();
  if (parent != nullptr) {
    parent->onWriteTaskDone(E::FAILED);
  }
}

} // anonymous namespace

CleanedResponseRequest::CleanedResponseRequest(
    Status status,
    std::unique_ptr<CLEAN_Message> clean_msg,
    worker_id_t worker_id,
    Address reply_to,
    Seal preempted_seal,
    shard_index_t shard)
    : Request(RequestType::CLEANED_RESPONSE),
      status_(status),
      clean_msg_(std::move(clean_msg)),
      worker_id_(worker_id),
      reply_to_(reply_to),
      preempted_seal_(preempted_seal),
      shard_(shard),
      ref_holder_(this) {
  ld_check(status_ != E::PREEMPTED || preempted_seal_.valid());
  ld_check(clean_msg_ != nullptr);
}

void CleanedResponseRequest::prepareMetadata() {
  // Only write per-epoch log metadata if the following conditions are met:
  // 1. Reply status is E::OK (i.e., successfully finished purging upto
  //    header.epoch)
  // 2. CLEAN message has valid sequencer epoch.
  // 3. The epoch is not empty (i.e., last_digest_esn > ESN_INVALID). Currently
  //    we do not store per-epoch log metadata for empty epochs.
  const auto& header = clean_msg_->header_;
  if (status_ != E::OK || header.sequencer_epoch == EPOCH_INVALID) {
    return;
  }

  EpochRecoveryMetadata::FlagsType flags = 0;
  // TODO (T35832374) : remove if condition when all servers support OffsetMap
  if (Worker::settings().enable_offset_map) {
    flags |= EpochRecoveryMetadata::Header::SUPPORT_OFFSET_MAP_AND_TAIL_RECORD;
  }
  ld_check(!clean_msg_->tail_record_.containOffsetWithinEpoch());

  // TODO 9929743: byte offset
  metadata_ = std::make_unique<EpochRecoveryMetadata>(
      header.sequencer_epoch,
      header.last_known_good,
      header.last_digest_esn,
      flags,
      clean_msg_->tail_record_,
      clean_msg_->epoch_size_map_,
      clean_msg_->tail_record_.offsets_map_);

  // CLEAN message was checked when it was received
  ld_check(metadata_->valid());

  if (metadata_->empty()) {
    ld_check(header.last_digest_esn == ESN_INVALID);
    ld_check(header.last_known_good == ESN_INVALID);

    ld_debug("Not writing EpochRecoveryMetadata for log %lu epoch %u because "
             "the epoch is empty according to epoch recovery.",
             header.log_id.val_,
             header.epoch.val_);
    WORKER_STAT_INCR(epoch_recovery_metadata_not_stored_epoch_empty);
    // metadata will not be stored in local log store
    // (see LocalLogStore::updatePerEpochLogMetadata)
  }
}

void CleanedResponseRequest::sendResponse() {
  const auto& header = clean_msg_->header_;
  CLEANED_Header cleaned_header{header.log_id,
                                header.epoch,
                                header.recovery_id,
                                status_,
                                preempted_seal_,
                                shard_};

  auto msg = std::make_unique<CLEANED_Message>(cleaned_header);
  // NOTE: We don't care if message sending or request posting fails.  The
  // sequencer is prepared to handle lack of response anyway.
  Worker::onThisThread()->sender().sendMessage(std::move(msg), reply_to_);
}

void CleanedResponseRequest::writeMetadata() {
  ld_check(status_ == E::OK);
  ld_check(metadata_ != nullptr);
  const auto& header = clean_msg_->header_;
  auto task = std::make_unique<UpdatePerEpochLogMetadataTask>(
      header.log_id, header.epoch, std::move(metadata_), ref_holder_.ref());

  ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard_)->putTask(
      std::move(task));
}

void CleanedResponseRequest::onWriteTaskDone(Status status) {
  if (status != E::OK) {
    status_ = E::FAILED;
  }
  sendResponse();
  delete this;
}

Request::Execution CleanedResponseRequest::execute() {
  Worker* w = Worker::onThisThread();
  ld_check(worker_id_ == w->idx_);

  prepareMetadata();
  if (metadata_ == nullptr) {
    sendResponse();
    return Execution::COMPLETE;
  }

  writeMetadata();
  return Execution::CONTINUE;
}

}} // namespace facebook::logdevice
