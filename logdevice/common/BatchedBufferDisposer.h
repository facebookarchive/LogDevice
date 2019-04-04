/**[]
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once
#include <folly/AtomicIntrusiveLinkedList.h>
#include <folly/Executor.h>

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

/**
 * This is a Deleter that groups buffers together and deletes them on a specific
 * Executor/thread as a single batch. On adding the first buffer to the list, a
 * function is enqueued into the executor. While the enqueued function waits to
 * get executed, the disposer collects buffers in the list. Once the enqueued
 * function gets scheduled, it sweeps over the entire list and deletes all the
 * records. It then resets the scheduled_cleanup_ flag so that other
 * buffers can be scheduled for cleanup.
 */
template <class BufferT>
class BatchedBufferDisposer {
 public:
  BatchedBufferDisposer(folly::Executor* executor) : executor_(executor) {}

  ~BatchedBufferDisposer() {
    per_exec_list_.sweepOnce([](BufferT* record) { delete record; });
  }

  void dispose(BufferT* record) {
    if (!executor_) {
      delete record;
    } else {
      // Increment counter first, as inserted head can be deleted immediately if
      // a buffer is already enqueued for deletion.
      total_enqueued_++;
      per_exec_list_.insertHead(record);
      if (scheduled_cleanup_.exchange(true)) {
        executor_->add([&] {
          size_t num_deleted = 0;
          per_exec_list_.sweepOnce([&](BufferT* r) {
            delete r;
            ++num_deleted;
          });
          ld_check(num_deleted <= total_enqueued_.load());
          scheduled_cleanup_.store(false);
          total_enqueued_.fetch_sub(
              std::min(num_deleted, total_enqueued_.load()));
        });
      }
    }
  }

  bool scheduledCleanup() const {
    return scheduled_cleanup_.load();
  }

  size_t numBuffers() const {
    return total_enqueued_;
  }

 private:
  folly::Executor* executor_;
  std::atomic<size_t> total_enqueued_{0};
  std::atomic<bool> scheduled_cleanup_{false};

  // Per event loop
  using DisposalList =
      folly::AtomicIntrusiveLinkedList<BufferT, &BufferT::hook>;
  DisposalList per_exec_list_;
};

}} // namespace facebook::logdevice
