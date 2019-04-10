/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Seal.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Task used to read the seal record from the local log store. After
 *       completion, registered callback will be called on a worker
 *       thread which enqueued this task.
 */

class RecoverSealTask : public StorageTask {
 public:
  explicit RecoverSealTask(logid_t log_id,
                           LogStorageState::seal_callback_t callback)
      : StorageTask(StorageTask::Type::RECOVER_SEAL),
        log_id_(log_id),
        callback_(callback),
        status_(E::OK) {}

  void execute() override;
  void onDone() override;
  void onDropped() override;

  StorageTaskPriority getPriority() const override {
    return StorageTaskPriority::HIGH;
  }

 private:
  logid_t log_id_;
  LogStorageState::seal_callback_t callback_;
  Status status_ = E::INTERNAL;
  LogStorageState::Seals seals_;

  void invokeCallback();
};
}} // namespace facebook::logdevice
