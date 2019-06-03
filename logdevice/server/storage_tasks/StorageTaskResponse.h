/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Storage threads create this Request to let the worker thread know
 *       when the StorageTask has finished processing.
 */

class StorageTaskResponse : public Request {
 public:
  explicit StorageTaskResponse(std::unique_ptr<StorageTask> task)
      : Request(RequestType::STORAGE_TASK_RESPONSE), task_(std::move(task)) {}

  /**
   * Called by storage threads after task->execute(), to transfer the task
   * back to the worker thread that created it.
   */
  static void sendBackToWorker(std::unique_ptr<StorageTask> task);

  /**
   * Called by storage threads when a task is dropped from the queue, to
   * transfer the task back to the worker thread that created it.
   */
  static void sendDroppedToWorker(std::unique_ptr<StorageTask> task);

  Execution execute() override;

  RunContext getRunContext() const override {
    return RunContext(task_->getType());
  }

  int8_t getExecutorPriority() const override;

  std::string describe() const override;

 private:
  std::unique_ptr<StorageTask> task_;
};
}} // namespace facebook::logdevice
