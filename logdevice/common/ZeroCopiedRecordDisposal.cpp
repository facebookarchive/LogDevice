/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ZeroCopiedRecordDisposal.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

ZeroCopiedRecordDisposal::ZeroCopiedRecordDisposal(Processor* processor)
    : processor_(processor) {
  ld_check(processor_ != nullptr);
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    worker_lists_[i] = std::vector<DisposalList>(
        processor_->getWorkerCount(workerTypeByIndex(i)));
  }
}

ZeroCopiedRecordDisposal::~ZeroCopiedRecordDisposal() {
  for (int i = 0; i < numOfWorkerTypes(); i++) {
    for (auto& list : worker_lists_[i]) {
      // use of this class must gurantee that the disposal is not being written
      // at the time of destruction.
      // In graceful shutdown ZeroCopiedRecordDisposal is destroyed along with
      // the Processor, and it ensures that each Worker already drained all
      // entries in its list before shutting down.
      ld_check(list.empty());
    }
  }
}

void ZeroCopiedRecordDisposal::disposeOfRecord(
    std::unique_ptr<ZeroCopiedRecord> record) {
  ld_check(record != nullptr);
  worker_id_t worker_id = record->getDisposalThread();
  WorkerType worker_type = record->getDisposalWorkerType();
  if (worker_id == worker_id_t(-1)) {
    // select the worker thread to dispose in round robin
    worker_id =
        worker_id_t(next_worker_id_.fetch_add(1) %
                    worker_lists_[workerIndexByType(worker_type)].size());
    worker_type = WorkerType::GENERAL;
  }

  ld_check(worker_id.val_ >= 0 &&
           worker_id.val_ <
               worker_lists_[workerIndexByType(worker_type)].size());
  // transfer the ownership to the disposal list
  ZeroCopiedRecord* e = record.release();
  worker_lists_[workerIndexByType(worker_type)][worker_id.val_].insertHead(e);
}

size_t ZeroCopiedRecordDisposal::drainListRecords(DisposalList* list) {
  ld_check(list != nullptr);
  size_t num_drained = 0;
  list->sweep([&num_drained](ZeroCopiedRecord* record) {
    // record must be unlinked already
    ld_check(!record->isLinked());

    record->onDisposedOf();
    ++num_drained;

    // this decreases the ref count of payload_holder_ and is likely
    // to free the payload
    delete record;
  });

  WORKER_STAT_ADD(zero_copied_records_drained, num_drained);
  return num_drained;
}

size_t ZeroCopiedRecordDisposal::drainRecords(WorkerType type,
                                              worker_id_t worker_id) {
  ld_check(type != WorkerType::MAX);
  ld_check(worker_id.val_ >= 0 &&
           worker_id.val_ < worker_lists_[workerIndexByType(type)].size());
  // If called on a worker thread, then the thread must match worker_id
  ld_check(
      getCurrentWorkerId() == worker_id_t(-1) ||
      (getCurrentWorkerId() == worker_id && getCurrentWorkerType() == type));

  return drainListRecords(
      &worker_lists_[workerIndexByType(type)][worker_id.val_]);
}

worker_id_t ZeroCopiedRecordDisposal::getCurrentWorkerId() const {
  Worker* w = Worker::onThisThread(false);
  return w == nullptr ? worker_id_t(-1) : w->idx_;
}

WorkerType ZeroCopiedRecordDisposal::getCurrentWorkerType() const {
  Worker* w = Worker::onThisThread(false);
  return w == nullptr ? WorkerType::GENERAL : w->worker_type_;
}

}} // namespace facebook::logdevice
