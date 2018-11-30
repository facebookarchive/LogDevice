/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ResourceBudget.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Contains a single write operation that a worker thread needs a
 *       storage thread to perform.
 *
 *       NOTE: see WriteBatchStorageTask for a description of how writes are
 *       batched and actually executed.
 */

class WriteStorageTask : public StorageTask {
 public:
  using StorageTask::StorageTask;
  void execute() override;
  // These still need to be implemented by a subclass:
  // virtual void onDone() override;
  // virtual void onDropped() override;

  bool isWriteTask() const override {
    return true;
  }

  // Undroppable WriteStorageTasks are not supported.
  // The reason is that WriteBatchStorageTask doesn't know in advance whether
  // it will pick up any undroppable WriteStorageTasks.
  // If needed, it can be implemented by having a separate queue in
  // StorageThreadPool for undroppable WriteStorageTasks.
  bool isDroppable() const final {
    return true;
  }

  /**
   * Called by WriteBatchStorageTask to check if this task should fail (with
   * E::PREEMPTED) because this write corresponds to a sealed part of the log.
   *
   * @param preempted_by  if the log was sealed, this will be set to the
   *                      seal record
   *
   */
  virtual bool isPreempted(Seal* /*preempted_by*/) {
    return false;
  }

  /**
   * Called by WriteBatchStorageTask to check if this task is for a record
   * with LSN before the local trim point.
   *
   */
  virtual bool isLsnBeforeTrimPoint() {
    return false;
  }

  virtual bool isTimedout() const {
    return false;
  }

  Durability durability() const override;
  FlushToken syncToken() const override;

  /**
   * Assign memtable id that this write is associated with. The write will be
   * persisted once all memtable with ids less than equal to this will be
   * persisted. This is sent to entity that issued store in the store reply.
   */
  virtual void setSyncToken(FlushToken flush_token) {
    // Make sure flush token is updated with a valid value just once because
    // there is space for a single token in the reply. The protocol message
    // needs to change if there are multiple tokens to be sent in the reply.
    ld_check(flushToken_ == FlushToken_INVALID ||
             flush_token == FlushToken_INVALID);
    if (flushToken_ == FlushToken_INVALID) {
      flushToken_ = flush_token;
    }
  }

  /**
   * Returns the number of write ops wrapped by this WriteStorageTask.
   */
  virtual size_t getNumWriteOps() const = 0;

  /**
   * Returns the number of bytes in this task's payload (default: no payload).
   */
  virtual size_t getPayloadSize() const {
    return 0;
  }

  /**
   * Returns an array of write ops to be passed to LocalLogStore::writeMulti().
   * WriteStorageTask retains ownership of all WriteOps.
   *
   * @param write_ops  caller-allocated array of pointers to write ops, filled
   *                   by getWriteOps() with up to write_ops_len pointers
   * @param write_ops_len  length of write_ops array
   * @return  number of write ops pointers written to write_ops array
   */
  virtual size_t getWriteOps(const WriteOp** write_ops,
                             size_t write_ops_len) const = 0;

  //
  // If LocalLogStore::acceptingWrites() returns non-ok status,
  // this method can override the decision.
  // Used for rebuilding copyset amendments.
  //
  virtual bool allowIfStoreIsNotAcceptingWrites(Status /*status*/) const {
    return false;
  }

  // Insert the stored record (if any) in the record cache if possible.
  // @return   0 if the store is cached, -1 otherwise
  virtual int putCache() {
    return -1;
  }

  //
  // This holds the result after executing
  //
  Status status_{E::UNKNOWN};

  //
  // For Durability::MEMORY writes, the memtable id to which this write was
  // attributed to.  The write will be considered committed to storage
  // after memtables with this id and lower are flushed.
  //
  // (see LocalLogStore::flushedUpThrough()).
  //
  FlushToken flushToken_{FlushToken_INVALID};

  //
  // If status_ == E::PREEMPTED, seal record of the sequencer
  // which sealed the log.
  //
  Seal seal_;

  //
  // After executing, this is true if this write was part of a batch that was
  // synced.
  //
  bool synced_{false};

  // Token from StorageThreadPool's memory_budget for this thread type.
  // Used for tracking total memory usage by in-flight storage tasks of certain
  // types.
  ResourceBudget::Token memToken_;
};
}} // namespace facebook::logdevice
