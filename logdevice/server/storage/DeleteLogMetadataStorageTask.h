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
 * @file Main part of processing a DELETE_LOG_METADATA message.  A worker thread
 * got the message and handed off processing to a storage thread via this task.
 */

class DeleteLogMetadataStorageTask : public WriteStorageTask {
 public:
  using Callback = std::function<void(Status)>;
  /**
   * @param op Write operation to perform
   */
  explicit DeleteLogMetadataStorageTask(const DeleteLogMetadataWriteOp& op,
                                        Callback& cb)
      : WriteStorageTask(StorageTask::Type::DELETE_LOG_METADATA),
        write_op_(op),
        callback_(cb) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void onDone() override {
    callback_(E::OK);
  }

  void onDropped() override {
    callback_(E::DROPPED);
  }

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

  bool allowIfStoreIsNotAcceptingWrites(Status status) const override {
    // Allow when running out of space.
    return status == E::NOSPC;
  }

 private:
  DeleteLogMetadataWriteOp write_op_;
  Callback callback_;
};

}} // namespace facebook::logdevice
