/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/storage_tasks/StorageTask.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Pseudo-task that tells a storage thread that there *may be* writes waiting
 * on the separate write queue in StorageThreadPool.  When a storage thread
 * picks up one of these, it goes to the other queue and tries to pull off
 * multiple writes to perform in a batch.
 *
 * The full design of writes (with batching) is:
 *
 * Code running on worker threads that is requesting writes (e.g. processing
 * STORE messages) is oblivious to write batching.  This code puts a
 * WriteStorageTask instance onto the PerWorkerStorageTaskQueue and expects to
 * see a WriteStorageTaskResponse eventually.
 *
 * However, to reduce I/O under load, we aggregate writes into batches.
 *
 * PerWorkerStorageTaskQueue handles writes differently from reads, calling a
 * special method on StorageThreadPool to take the write.  That method puts
 * the original task onto a special queue for individual writes, and also
 * posts a new WriteBatchStorageTask onto the main queue.
 *
 * When a storage thread picks up a WriteBatchStorageTask from the main queue,
 * its execute() method looks at the separate queue for writes and pulls a
 * batch of writes to perform.  (This batch can include writes that originated
 * on different storage threads.  The batch can also be empty if another
 * storage thread already emptied the queue.)  After performing the batch of
 * writes, the storage thread sends responses to workers for each individual
 * write.  Additionally, it sends a response to the worker for the
 * WriteBatchStorageTask.  (This is necessary because the
 * WriteBatchStorageTask took up a slot in the main storage thread pool queue,
 * which is a resource workers carefully manage.)
 *
 * Because workers get two responses per write (and so require two reserved
 * spots in the response pipe), writes count 2x against the limit of inflight
 * requests.  The WriteBatchStorageTask response relaxes the limit by 1 and
 * the individual WriteStorageTask response by another 1.  Note that the two
 * responses may arrive in either order, depending on how long the a storage
 * thread takes to perform the batch.  Writes counting double makes sure that
 * the resources (main queue slots and response pipe capacity) are never
 * overused, at the cost of being conservative at times.
 */

class StatsHolder;
class WriteOp;

class WriteBatchStorageTask : public StorageTask {
 public:
  explicit WriteBatchStorageTask(ThreadType thread_type)
      : StorageTask(StorageTask::Type::WRITE_BATCH),
        thread_type_(thread_type) {}
  void execute() override;
  void onDone() override;
  void onDropped() override;
  void onStorageThreadDrop() override;

  ThreadType getThreadType() const override {
    return thread_type_;
  }

 protected:
  ThreadType thread_type_;

  // these get mocked in unit tests

  virtual size_t getWriteBatchSize() const;
  virtual size_t getWriteBatchBytes() const;
  virtual void sendBackToWorker(std::unique_ptr<WriteStorageTask> task);
  virtual void sendDroppedToWorker(std::unique_ptr<WriteStorageTask> task);
  virtual StatsHolder* stats();
  virtual folly::small_vector<std::unique_ptr<WriteStorageTask>, 4>
  tryGetWriteBatch(size_t max_count, size_t max_bytes);
  virtual std::unique_ptr<WriteStorageTask> tryGetWrite();
  virtual int writeMulti(const std::vector<const WriteOp*>& write_ops);
  // Returns true if entire tasks will be rejected.
  virtual bool throttleIfNeeded();
};
}} // namespace facebook::logdevice
