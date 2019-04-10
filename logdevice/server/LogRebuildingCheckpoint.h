/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Metadata.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

/**
 * @file Tasks used to write and read the RebuildingCheckpoint metadata
 * from the local log store.
 */

namespace facebook { namespace logdevice {

/**
 * Storage task used by LogRebuilding to read a checkpoint before starting
 * reading from local log store.
 *
 * The task is droppable, LogRebuilding implements a retry mechanim to re-issue
 * this task using an exponential backoff timer.
 */
class ReadLogRebuildingCheckpointTask : public StorageTask {
 public:
  explicit ReadLogRebuildingCheckpointTask(logid_t log_id,
                                           lsn_t restart_version,
                                           bool read_trim_point)
      : StorageTask(StorageTask::Type::READ_LOG_REBUILDING_CHECKPOINT),
        log_id_(log_id),
        restart_version_(restart_version),
        read_trim_point_(read_trim_point) {}

  StorageTaskPriority getPriority() const override {
    // Rebuilding reads should be lo-pri compared to regular reads
    return StorageTaskPriority::LOW;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override;
  void onDone() override;
  void onDropped() override;
  bool isDroppable() const override {
    return true;
  }

 private:
  logid_t log_id_;
  lsn_t restart_version_;
  bool read_trim_point_;

  // These are populated when the task comes back successfully.

  // @see LogRebuilding::version_.
  lsn_t rebuilding_version_{LSN_INVALID};
  // LSN up to which this donor node made progress for rebuilding with version
  // `rebuilding_version_`.
  lsn_t rebuilt_upto_{LSN_INVALID};
  // Trim point of the log. Populated only if read_trim_point_ == true.
  lsn_t trim_point_{LSN_INVALID};
  // - E::OK: we were able to read a checkpoint;
  // - E::NOTFOUND: there is no checkpoint in the local log store;
  // - E::LOCAL_LOG_STORE_READ: error reading from the local log store.
  Status status_{E::OK};
};

/**
 * Storage task used by LogRebuilding to write a checkpoint.
 *
 * LogRebuilding writes a checkpoint on completion and at regular intervals.
 *
 * The task is droppable and there is no retry mechanism and no acknowledgment
 * in LogRebuilding, this is best effort.
 */
class WriteLogRebuildingCheckpointTask : public StorageTask {
 public:
  explicit WriteLogRebuildingCheckpointTask(logid_t log_id,
                                            lsn_t rebuilding_version,
                                            lsn_t rebuilt_upto)
      : StorageTask(StorageTask::Type::WRITE_LOG_REBUILDING_CHECKPOINT),
        log_id_(log_id),
        rebuilding_version_(rebuilding_version),
        rebuilt_upto_(rebuilt_upto) {}

  StorageTaskPriority getPriority() const override {
    // Rebuilding storage tasks should be lo-pri compared to client reads
    return StorageTaskPriority::LOW;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override;
  void onDone() override;
  void onDropped() override;
  bool isDroppable() const override {
    return true;
  }

 private:
  logid_t log_id_;

  // @see LogRebuilding::version_.
  lsn_t rebuilding_version_;
  // LSN up to which this donor node made progress for rebuilding with version
  // `rebuilding_version_`.
  lsn_t rebuilt_upto_;
};

}} // namespace facebook::logdevice
