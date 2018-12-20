/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Main part of processing a DELETE message.  A worker thread got the
 * message and handed off processing to a storage thread via this task.
 *
 * As DELETEs are best effort and the sequencer does not expect a reply, this
 * class is very simple.
 */

class DeleteStorageTask : public WriteStorageTask {
 public:
  /**
   * @param op Write operation to perform
   */
  explicit DeleteStorageTask(const DeleteWriteOp& op)
      : WriteStorageTask(StorageTask::Type::DELETE), write_op_(op) {}

  ThreadType getThreadType() const override {
    return ThreadType::FAST_TIME_SENSITIVE;
  }
  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  // All WriteStorageTask subclasses are processed the same way, we just
  // specify what to do when it's done.  In this case, nothing.
  void onDone() override {}
  void onDropped() override {}

  size_t getNumWriteOps() const override {
    return 1;
  }

  size_t getWriteOps(const WriteOp** write_ops,
                     size_t write_ops_len) const override {
    if (write_ops_len > 0) {
      write_ops[0] = &write_op_;
      return 1;
    } else {
      return 0;
    }
  }

 private:
  DeleteWriteOp write_op_;
};

}} // namespace facebook::logdevice
