/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRId64 etc
#include "logdevice/server/RebuildingReadStorageTask.h"

#include <chrono>
#include <cstdlib>
#include <memory>

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

/**
 * @file RebuildingReadStorageTask is a storage task for reading a batch of
 * record for a LogRebuilding state machine.
 */

namespace facebook { namespace logdevice {

namespace {

/**
 * Callback that collects all records into a vector.
 */
class StorageThreadCallback : public LocalLogStoreReader::Callback {
 public:
  int processRecord(const RawRecord& raw_record) override;
  RebuildingReadStorageTask::RecordContainer&& releaseRecords() {
    return std::move(records_);
  }

  size_t totalBytes() const {
    return totalBytes_;
  }

 private:
  size_t totalBytes_{0}; // Used for stats.
  RebuildingReadStorageTask::RecordContainer records_;
};

} // namespace

RebuildingReadStorageTask::RebuildingReadStorageTask(
    lsn_t restart_version,
    folly::Optional<RecordTimestamp> seek_timestamp,
    LocalLogStoreReader::ReadContext read_ctx,
    LocalLogStore::ReadOptions options,
    std::weak_ptr<LocalLogStore::ReadIterator> iterator)
    :

      StorageTask(StorageTask::Type::REBUILDING_READ),
      restartVersion(restart_version),
      seekTimestamp(seek_timestamp),
      readCtx(read_ctx),
      options(options),
      iteratorFromCache(std::move(iterator)) {
  ld_spew("constructor log_id=%" PRIu64
          " seek_timestamp=%s from.lsn=%s until_lsn=%s"
          " window_high=%" PRIu64 " max_bytes_to_deliver_=%zu"
          " first_record_any_size=%d",
          read_ctx.logid_.val_,
          seekTimestamp ? format_time(seekTimestamp.value()).c_str() : "none",
          lsn_to_string(readCtx.read_ptr_.lsn).c_str(),
          lsn_to_string(readCtx.until_lsn_).c_str(),
          readCtx.window_high_,
          readCtx.max_bytes_to_deliver_,
          int(readCtx.first_record_any_size_));
}

void RebuildingReadStorageTask::execute() {
  ld_check(options.allow_blocking_io);
  ld_check(totalBytes == 0);

  STAT_INCR(
      storageThreadPool_->stats(), num_in_flight_rebuilding_read_storage_tasks);

  auto& local_log_store = storageThreadPool_->getLocalLogStore();
  if (seekTimestamp) {
    ld_debug("LogID %" PRIu64 ": Requested seek to %s.",
             readCtx.logid_.val_,
             toString(*seekTimestamp).c_str());
    lsn_t lsn_lo = readCtx.read_ptr_.lsn;
    lsn_t lsn_high = readCtx.window_high_;
    int rc = local_log_store.findTime(
        readCtx.logid_, seekTimestamp->toMilliseconds(), &lsn_lo, &lsn_high);
    lsn_t seek_lsn = std::min(lsn_lo, LSN_MAX - 1) + 1;
    if (rc == -1) {
      ld_info("LogID %" PRIu64 ": Unable to seek to timestamp %s.",
              readCtx.logid_.val_,
              toString(*seekTimestamp).c_str());
      // The seek is an optimization. Just read from the current read
      // location.
    } else if (seek_lsn >= readCtx.read_ptr_.lsn) {
      ld_debug("LogID %" PRIu64 ": Seeked forward from %s -> %s.",
               readCtx.logid_.val_,
               lsn_to_string(readCtx.read_ptr_.lsn).c_str(),
               lsn_to_string(seek_lsn).c_str());
      readCtx.read_ptr_.lsn = seek_lsn;
    } else {
      // Tolerate out of order timestamps in records.
      // We'll read where we are now and then reset our timestamp window
      // if necessary to match the timestamps in the records written.
      ld_info("LogID %" PRIu64 ": Seeking went backward %s - %s. Ignoring.",
              readCtx.logid_.val_,
              lsn_to_string(readCtx.read_ptr_.lsn).c_str(),
              lsn_to_string(seek_lsn).c_str());
    }
  }
  ownedIterator = iteratorFromCache.lock();
  if (!ownedIterator) {
    // Either the iterator wasn't passed in by LogRebuilding because it
    // or the iterator has since then been invalidated because its ttl expired.
    // Create a new iterator that will later be handed over to LogRebuilding.
    ownedIterator =
        storageThreadPool_->getLocalLogStore().read(readCtx.logid_, options);
  }

  ld_check(ownedIterator);

  auto read_ctx_before = readCtx;
  auto start = std::chrono::steady_clock::now();

  StorageThreadCallback callback;
  status = LocalLogStoreReader::read(*ownedIterator,
                                     callback,
                                     &readCtx,
                                     storageThreadPool_->stats(),
                                     *storageThreadPool_->getSettings().get());

  // The `read()` call populated `callback` with some records.  Now move them
  // into the RebuildingReadStorageTask instance, which will get passed back to
  // the worker.
  records = std::move(callback.releaseRecords());
  totalBytes = callback.totalBytes();

  // Log some debugging information each time a storage task takes more than 10s
  // to execute.
  const auto latency_sec = sec_since(start);
  if (latency_sec > 10) {
    ld_warning("Reading a batch took %lds. "
               "log id=%lu, "
               "status=%s, "
               "num records read=%lu, num bytes read=%lu, "
               "read context before: {%s}, "
               "read context after: {%s}",
               latency_sec,
               readCtx.logid_.val_,
               error_name(status),
               records.size(),
               totalBytes,
               read_ctx_before.toString().c_str(),
               readCtx.toString().c_str());
  }
}

void RebuildingReadStorageTask::onDone() {
  WORKER_STAT_DECR(num_in_flight_rebuilding_read_storage_tasks);

  auto log = Worker::onThisThread()->runningLogRebuildings().find(
      readCtx.logid_, storageThreadPool_->getShardIdx());
  if (!log) {
    // The LogRebuilding state machine was aborted.
    return;
  }

  log->onReadTaskDone(*this);
}

void RebuildingReadStorageTask::onDropped() {
  ld_check(totalBytes == 0);

  auto log = Worker::onThisThread()->runningLogRebuildings().find(
      readCtx.logid_, storageThreadPool_->getShardIdx());
  if (!log) {
    // The LogRebuilding state machine was aborted.
    return;
  }

  log->onReadTaskDropped(*this);
}

void RebuildingReadStorageTask::getDebugInfoDetailed(
    StorageTaskDebugInfo& info) const {
  info.log_id = readCtx.logid_;
  info.lsn = readCtx.read_ptr_.lsn;
  info.extra_info = readCtx.toString();
}

int StorageThreadCallback::processRecord(const RawRecord& record) {
  // When doing local log store reads on a storage thread, we need to copy the
  // data out of the local log store into a malloc'd buffer, since records
  // will only get passed to the messaging layer at some later time (when the
  // worker thread gets around to processing the RebuildingReadStorageTask
  // result).
  void* blob_copy = malloc(record.blob.size);
  if (blob_copy == nullptr) {
    throw std::bad_alloc();
  }
  memcpy(blob_copy, record.blob.data, record.blob.size);
  totalBytes_ += record.blob.size;

  records_.emplace_back( // creating a RawRecord
      record.lsn,
      Slice(blob_copy, record.blob.size),
      true // owned, we malloc-d the memory
  );
  return 0;
}

}} // namespace facebook::logdevice
