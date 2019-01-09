/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogRebuildingCheckpoint.h"

#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/LogRebuilding.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

void ReadLogRebuildingCheckpointTask::execute() {
  RebuildingCheckpointMetadata checkpoint;
  TrimMetadata trim;

  int rv = storageThreadPool_->getLocalLogStore().readLogMetadata(
      log_id_, &checkpoint);
  if (rv == 0) {
    rebuilding_version_ = checkpoint.data_.rebuilding_version;
    rebuilt_upto_ = checkpoint.data_.rebuilt_upto;
  } else if (err != E::NOTFOUND) {
    status_ = err;
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Could not read RebuildingCheckpointMetadata for "
                    "log %lu: %s",
                    log_id_.val_,
                    error_description(err));
    return;
  }

  if (!read_trim_point_) {
    return;
  }

  rv = storageThreadPool_->getLocalLogStore().readLogMetadata(log_id_, &trim);
  if (rv == 0) {
    trim_point_ = trim.trim_point_;
  } else if (err != E::NOTFOUND) {
    status_ = err;
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Could not read TrimMetadata for "
                    "log %lu: %s",
                    log_id_.val_,
                    error_description(err));
    return;
  }
}

void ReadLogRebuildingCheckpointTask::onDone() {
  auto log = Worker::onThisThread()->runningLogRebuildings().find(
      log_id_, storageThreadPool_->getShardIdx());
  if (!log) {
    // The LogRebuilding state machine was aborted.
    return;
  }

  checked_downcast<LogRebuilding*>(log)->onCheckpointRead(status_,
                                                          restart_version_,
                                                          rebuilding_version_,
                                                          rebuilt_upto_,
                                                          trim_point_);
}

void ReadLogRebuildingCheckpointTask::onDropped() {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  10,
                  "Dropped ReadLogRebuildingCheckpointTask for log %lu. "
                  "Rebuilding may stall.",
                  log_id_.val_);

  auto log = Worker::onThisThread()->runningLogRebuildings().find(
      log_id_, storageThreadPool_->getShardIdx());
  if (!log) {
    // The LogRebuilding state machine was aborted.
    return;
  }

  checked_downcast<LogRebuilding*>(log)->onCheckpointRead(E::DROPPED,
                                                          restart_version_,
                                                          rebuilding_version_,
                                                          rebuilt_upto_,
                                                          trim_point_);
}

void WriteLogRebuildingCheckpointTask::execute() {
  LocalLogStore& store = storageThreadPool_->getLocalLogStore();

  RebuildingCheckpointMetadata metadata(rebuilding_version_, rebuilt_upto_);
  LocalLogStore::WriteOptions options;
  int rv = store.writeLogMetadata(log_id_, metadata, options);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "Could not write RebuildingCheckpointMetadata for "
                    "log %lu: %s",
                    log_id_.val_,
                    error_description(err));
  }
}

void WriteLogRebuildingCheckpointTask::onDone() {
  // This is best effort, we don't care about notifying LogRebuilding.
}

void WriteLogRebuildingCheckpointTask::onDropped() {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  10,
                  "Dropped WriteLogRebuildingCheckpointTask for log %lu.",
                  log_id_.val_);
  // This is best effort, we don't care about notifying LogRebuilding.
}

}} // namespace facebook::logdevice
