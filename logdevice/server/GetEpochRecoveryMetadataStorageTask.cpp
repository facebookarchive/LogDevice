/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/GetEpochRecoveryMetadataStorageTask.h"

#include "logdevice/common/Metadata.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_Message.h"
#include "logdevice/common/protocol/GET_EPOCH_RECOVERY_METADATA_REPLY_Message.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

GetEpochRecoveryMetadataStorageTask::GetEpochRecoveryMetadataStorageTask(
    const GET_EPOCH_RECOVERY_METADATA_Header& header,
    const Address& reply_to,
    const shard_index_t shard)
    : StorageTask(StorageTask::Type::GET_EPOCH_RECOVERY_METADATA),
      log_id_(header.log_id),
      shard_(shard),
      purging_shard_(header.purging_shard),
      purge_to_(header.purge_to),
      start_(header.start),
      end_(header.end),
      reply_to_(reply_to),
      request_id_(header.id),
      status_(E::UNKNOWN) {}

void GetEpochRecoveryMetadataStorageTask::execute() {
  ld_check(storageThreadPool_->getShardIdx() == shard_);
  status_ =
      executeImpl(storageThreadPool_->getLocalLogStore(),
                  storageThreadPool_->getProcessor().getLogStorageStateMap(),
                  storageThreadPool_->stats());
}

Status
GetEpochRecoveryMetadataStorageTask::executeImpl(LocalLogStore& store,
                                                 LogStorageStateMap& state_map,
                                                 StatsHolder* /*stats*/) {
  LogStorageState* log_state = state_map.insertOrGet(log_id_, shard_);
  if (log_state == nullptr) {
    RATELIMIT_ERROR(std::chrono::seconds(5),
                    1,
                    "Unable get LogStorageState for log %lu: %s",
                    log_id_.val_,
                    error_description(err));
    return E::FAILED;
  }

  int rv = 0;
  epoch_t last_clean = EPOCH_INVALID;
  folly::Optional<epoch_t> lce = log_state->getLastCleanEpoch();
  if (!lce.hasValue()) {
    LastCleanMetadata meta{epoch_t(0)};
    rv = store.readLogMetadata(log_id_, &meta);
    if (rv != 0 && err != E::NOTFOUND) {
      log_state->notePermanentError("Reading LCE");
      return E::FAILED;
    }
    ld_check(rv == 0 || meta.epoch_.val_ == 0);

    // lce read or not found
    last_clean = meta.epoch_;
    log_state->updateLastCleanEpoch(last_clean);
  } else {
    last_clean = lce.value();
  }

  if (last_clean < start_) {
    // the epoch is not clean on the node, should reply E::NOTREADY
    return E::NOTREADY;
  }

  lsn_t trim_point;
  folly::Optional<lsn_t> tp = log_state->getTrimPoint();
  if (!tp.hasValue()) {
    TrimMetadata meta{LSN_INVALID};
    rv = store.readLogMetadata(log_id_, &meta);
    if (rv != 0 && err != E::NOTFOUND) {
      log_state->notePermanentError(
          "Reading trim point (in GetEpochRecoveryMetadataStorageTask)");
      return E::FAILED;
    }
    // trim point read or not found
    trim_point = meta.trim_point_;
    log_state->updateTrimPoint(trim_point);
  } else {
    trim_point = tp.value();
  }

  if (lsn_to_epoch(trim_point) > end_) {
    // epoch is completely trimmed, consider it empty
    return E::EMPTY;
  }

  for (auto epoch = start_.val_; epoch <= end_.val_; epoch++) {
    if (last_clean < epoch_t(epoch)) {
      epochRecoveryStateMap_.emplace(
          epoch, std::make_pair(E::NOTREADY, EpochRecoveryMetadata()));
      continue;
    }

    if (lsn_to_epoch(trim_point) > epoch_t(epoch)) {
      epochRecoveryStateMap_.emplace(
          epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
      continue;
    }

    EpochRecoveryMetadata metadata;
    rv = store.readPerEpochLogMetadata(log_id_, epoch_t(epoch), &metadata);
    if (rv != 0) {
      if (err == E::NOTFOUND) {
        // epoch is clean but no epoch recovery metadata found, this indicates
        // that the epoch is empty
        epochRecoveryStateMap_.emplace(
            epoch, std::make_pair(E::EMPTY, EpochRecoveryMetadata()));
        continue;
      } else {
        log_state->notePermanentError("Reading per-epoch metadata");
        return E::FAILED;
      }
    }

    if (!metadata.valid()) {
      RATELIMIT_ERROR(std::chrono::seconds(2),
                      1,
                      "Read EpochRecoveryMetadata for log %lu epoch %u but the "
                      "its content is invalid! metadata: %s. Will reply "
                      "E::FAILED.",
                      log_id_.val_,
                      epoch,
                      metadata.toString().c_str());
      return E::FAILED;
    }
    epochRecoveryStateMap_.emplace(epoch, std::make_pair(E::OK, metadata));
  }

  return E::OK;
}

void GetEpochRecoveryMetadataStorageTask::onDone() {
  sendReply();
}

void GetEpochRecoveryMetadataStorageTask::onDropped() {
  // this node is overloaded, but still allow the sender to try again later
  status_ = E::AGAIN;
  sendReply();
}

void GetEpochRecoveryMetadataStorageTask::sendReply() {
  ld_check(status_ != E::OK || !epochRecoveryStateMap_.empty());
  GET_EPOCH_RECOVERY_METADATA_REPLY::createAndSend(
      reply_to_,
      log_id_,
      shard_,
      purging_shard_,
      purge_to_,
      start_,
      end_,
      /*flags=*/0,
      status_,
      request_id_,
      status_ == E::OK
          ? std::make_unique<EpochRecoveryStateMap>(epochRecoveryStateMap_)
          : nullptr);
}

}} // namespace facebook::logdevice
