/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <vector>

#include <folly/AtomicIntrusiveLinkedList.h>

#include "logdevice/common/ZeroCopiedRecord.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file ZeroCopiedRecordDisposal provides methods for safely freeing a
 *       ZeroCopiedRecord and its associated payload. It provides
 *       thread-safe methods for disposing of records as well as draining
 *       disposed records on a worker.
 *
 *       ZeroCopiedRecordDisposal is owned by Processor shared among all
 *       workers.
 */

class Processor;

class ZeroCopiedRecordDisposal {
 public:
  explicit ZeroCopiedRecordDisposal(Processor* processor);

  void disposeOfRecord(std::unique_ptr<ZeroCopiedRecord> entry);

  /**
   * Drain the disposal for worker indexed by @param worker_id.
   * Should be called on the exact worker thread.
   *
   * @return number of records drained
   */
  size_t drainRecords(WorkerType type, worker_id_t worker_id);

  virtual ~ZeroCopiedRecordDisposal();

 private:
  using DisposalList =
      folly::AtomicIntrusiveLinkedList<ZeroCopiedRecord,
                                       &ZeroCopiedRecord::hook>;
  Processor* const processor_;

  // a per-worker atomic intrusive list for storing disposed records
  std::array<std::vector<DisposalList>, static_cast<size_t>(WorkerType::MAX)>
      worker_lists_;

  // worker id for round robin disposal of records that does not require a
  // particular worker to destroy, we use the general worker pool only for this.
  std::atomic<size_t> next_worker_id_{0};

  size_t drainListRecords(DisposalList* list);

  worker_id_t getCurrentWorkerId() const;
  WorkerType getCurrentWorkerType() const;
};

}} // namespace facebook::logdevice
