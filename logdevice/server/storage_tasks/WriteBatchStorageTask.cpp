/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/storage_tasks/WriteBatchStorageTask.h"

#include <algorithm>
#include <iterator>
#include <memory>
#include <unistd.h>
#include <vector>

#include <folly/small_vector.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/StorageTaskResponse.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"
#include "logdevice/server/storage_tasks/WriteStorageTask.h"

namespace facebook { namespace logdevice {

void WriteBatchStorageTask::execute() {
  using namespace std::chrono_literals;

  size_t ntasks, limit = getWriteBatchSize();
  auto writes = tryGetWriteBatch(limit, getWriteBatchBytes());
  ntasks = writes.size();

  ld_spew("WriteBatchStorageTask picked up batch of %lu writes", writes.size());

  if (ntasks == 0) {
    // Common case, avoid cost of interaction with local log store
    return;
  }

  // Yield to higher-pri tasks if needed. Since this can take a few seconds
  // or even minutes, this is done before checking timeouts and preemption.
  auto reject_writes = throttleIfNeeded();
  auto& io_fault_injection = IOFaultInjection::instance();
  auto fault =
      io_fault_injection.getInjectedFault(reply_shard_idx_,
                                          IOFaultInjection::IOType::WRITE,
                                          IOFaultInjection::FaultType::LATENCY,
                                          IOFaultInjection::DataType::DATA);

  if (fault == IOFaultInjection::FaultType::LATENCY) {
    folly::Baton<> baton;
    baton.try_wait_for(io_fault_injection.getLatencyToInject(reply_shard_idx_));
    RATELIMIT_INFO(1s, 1, "Injected data write latency.");
  }

  std::vector<const WriteOp*> write_ops;
  write_ops.reserve(ntasks * 2); // expect at most 2 write ops per task
  for (auto& write : writes) {
    if (reject_writes) {
      // Drop write if the store cannot keep up with incoming rate.
      write->status_ = E::DROPPED;
      sendBackToWorker(std::move(write));

      ld_check(!write);
      continue;
    }

    if (write->isTimedout()) {
      // drop timedout write
      write->status_ = E::TIMEDOUT;
      sendBackToWorker(std::move(write));

      ld_check(!write); // NOTE: ref to unique_ptr in `writes'
      continue;
    }

    if (write->isPreempted(&write->seal_)) {
      // fail early
      write->status_ = E::PREEMPTED;
      sendBackToWorker(std::move(write));

      ld_check(!write); // NOTE: ref to unique_ptr in `writes'
      continue;
    }

    // if the lsn of the record is before the local trim point, return success.
    // this is an optimization to avoid writing to RocksDB since such records
    // cannot be read anyways. This can happen during rebuilding since a lot of
    // records with older lsns are written
    if (write->isLsnBeforeTrimPoint()) {
      write->status_ = E::OK;
      sendBackToWorker(std::move(write));

      ld_check(!write); // NOTE: ref to unique_ptr in `writes'
      STAT_INCR(stats(), skipped_record_lsn_before_trim_point);
      continue;
    }

    // append all write ops of the task to write_ops
    size_t num_write_ops = write->getNumWriteOps();
    folly::small_vector<const WriteOp*, 16> task_write_ops(num_write_ops);
    size_t write_ops_written =
        write->getWriteOps(task_write_ops.data(), task_write_ops.size());
    ld_check(num_write_ops == write_ops_written);
    (void)write_ops_written;
    std::copy(task_write_ops.begin(),
              task_write_ops.end(),
              std::back_inserter(write_ops));

    if (reply_shard_idx_ >= 0) {
      // Update the histogram of queueing latency for that individual
      // WriteStorageTask.
      PER_SHARD_HISTOGRAM_ADD(
          stats(),
          storage_threads_queue_time[static_cast<int>(thread_type_)],
          reply_shard_idx_,
          usec_since(write->enqueue_time_));
    }
  }

  if (thread_type_ == StorageTask::ThreadType::FAST_STALLABLE) {
    STAT_ADD(stats(), write_ops_stallable, write_ops.size());
    STAT_INCR(stats(), write_batches_stallable);
  } else {
    STAT_ADD(stats(), write_ops, write_ops.size());
    STAT_INCR(stats(), write_batches);
  }

  int rv = writeMulti(write_ops);
  Status status = rv == 0 ? E::OK : err;

  auto write_ops_iter = write_ops.begin();
  for (auto& write : writes) {
    if (!write) {
      continue;
    }
    write->status_ = status;
    if (status == E::OK) {
      // store success, try to insert the stored record into the record
      // cache. Perform insertion on the storage thread rather than the
      // worker thread to minimize lock contention.
      int rc = write->putCache();
      if (rc == 0) {
        STAT_INCR(stats(), record_cache_store_cached);
      } else {
        STAT_INCR(stats(), record_cache_store_not_cached);
      }
      if (write->durability() <= Durability::MEMORY) {
        for (auto i = 0; i < write->getNumWriteOps(); ++i) {
          auto iter = *(write_ops_iter + i);
          write->setSyncToken(iter->flushToken());
        }
      }
    }

    write_ops_iter += write->getNumWriteOps();

    if (write->durability() == Durability::SYNC_WRITE) {
      // Delay sending back to worker until the write is synced
      write->synced_ = true;
      storageThreadPool_->enqueueForSync(std::move(write));
    } else {
      sendBackToWorker(std::move(write));
    }
  }
  // StorageThread will send back the response for *this
}

bool WriteBatchStorageTask::throttleIfNeeded() {
  auto& store = storageThreadPool_->getLocalLogStore();
  auto writes_throttle_state = store.getWriteThrottleState();
  if (writes_throttle_state == LocalLogStore::WriteThrottleState::NONE) {
    return false;
  }

  if (thread_type_ == StorageTask::ThreadType::FAST_STALLABLE &&
      storageThreadPool_->writeStallingEnabled()) {
    store.stallLowPriWrite();
    return false;
  }

  // For other thread types, reject the write if throttle state asks us to do
  // so.
  return writes_throttle_state ==
      LocalLogStore::WriteThrottleState::REJECT_WRITE;
}

void WriteBatchStorageTask::onDone() {}

void WriteBatchStorageTask::onDropped() {
  // Nothing to do on the worker thread when the task is dropped.
}

void WriteBatchStorageTask::onStorageThreadDrop() {
  // When a WriteBatch task is dropped from the main queue, we need to try to
  // also drop a write from the write queue, in order to maintain the
  // invariant that there are at least as many WriteBatch tasks in the main
  // queue as there are individual writes in the separate write queue.
  std::unique_ptr<WriteStorageTask> write = tryGetWrite();
  if (write) {
    sendDroppedToWorker(std::move(write));
  }
}

size_t WriteBatchStorageTask::getWriteBatchSize() const {
  return storageThreadPool_->getSettings()->write_batch_size;
}

size_t WriteBatchStorageTask::getWriteBatchBytes() const {
  return storageThreadPool_->getSettings()->write_batch_bytes;
}

std::unique_ptr<WriteStorageTask> WriteBatchStorageTask::tryGetWrite() {
  auto res = tryGetWriteBatch(1, 1);
  if (res.size() > 0) {
    ld_check(res.size() == 1);
    return std::move(res[0]);
  }
  return nullptr;
}

folly::small_vector<std::unique_ptr<WriteStorageTask>, 4>
WriteBatchStorageTask::tryGetWriteBatch(size_t max_count, size_t max_bytes) {
  return storageThreadPool_->tryGetWriteBatch(
      thread_type_, max_count, max_bytes);
}

void WriteBatchStorageTask::sendBackToWorker(
    std::unique_ptr<WriteStorageTask> write) {
  StorageTaskResponse::sendBackToWorker(std::move(write));
}

void WriteBatchStorageTask::sendDroppedToWorker(
    std::unique_ptr<WriteStorageTask> write) {
  StorageTaskResponse::sendDroppedToWorker(std::move(write));
}

int WriteBatchStorageTask::writeMulti(
    const std::vector<const WriteOp*>& write_ops) {
  auto& store = storageThreadPool_->getLocalLogStore();
  return store.writeMulti(write_ops);
}

StatsHolder* WriteBatchStorageTask::stats() {
  return storageThreadPool_->stats();
}
}} // namespace facebook::logdevice
