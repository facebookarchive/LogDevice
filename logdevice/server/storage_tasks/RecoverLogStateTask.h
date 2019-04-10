/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/Metadata.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * @file Task used by LogStorageStateMap to recover the state, except seals
 *       (seals are recovered by RecoverSealTask). Attempts to read both
 *       the last released LSN and the trim point for the given log.
 *       After it completes, the task gets sent back to the worker
 *       thread where the LogStorageStateMap gets updated.
 */
class RecoverLogStateTask : public StorageTask {
 public:
  explicit RecoverLogStateTask(logid_t log_id)
      : StorageTask(StorageTask::Type::RECOVER_LOG_STATE), log_id_(log_id) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }
  void execute() override;
  void onDone() override;
  void onDropped() override;

  StorageTaskPriority getPriority() const override {
    // There can be at most one of these tasks per log per server process's
    // lifetime.
    return StorageTaskPriority::HIGH;
  }

  // We can't afford to lose these tasks (and there's nothing better than to
  // retry internally), mark them non-droppable.  We don't expect to have many
  // of these in the steady state; O(1) per log per process.
  bool isDroppable() const override {
    return false;
  }

  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;

 private:
  void readTrimPoint(LocalLogStore&);
  void readLastReleased(LocalLogStore&);
  void readLastClean(LocalLogStore&);

  logid_t log_id_;
  folly::Optional<TrimMetadata> trim_metadata_;
  folly::Optional<LastReleasedMetadata> last_released_metadata_;
  folly::Optional<LastCleanMetadata> last_clean_metadata_;

  // True if some LocalLogStore operation failed.
  bool failed_ = false;
};
}} // namespace facebook::logdevice
