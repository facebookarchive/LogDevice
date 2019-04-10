/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRId64 etc
#include "logdevice/server/storage_tasks/ReadStorageTask.h"

#include <chrono>
#include <memory>

#include <folly/Memory.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * Callback that collects all records into a vector.
 */
class StorageThreadCallback : public LocalLogStoreReader::Callback {
 public:
  int processRecord(const RawRecord& raw_record) override;
  ReadStorageTask::RecordContainer&& releaseRecords() {
    return std::move(records_);
  }

  size_t totalBytes() const {
    return total_bytes_;
  }

 private:
  size_t total_bytes_{0}; // Used for stats.
  ReadStorageTask::RecordContainer records_;
};

ReadStorageTask::ReadStorageTask(
    WeakRef<ServerReadStream> stream,
    WeakRef<CatchupQueue> catchup_queue,
    server_read_stream_version_t server_read_stream_version,
    filter_version_t filter_version,
    LocalLogStoreReader::ReadContext read_ctx,
    LocalLogStore::ReadOptions options,
    std::weak_ptr<LocalLogStore::ReadIterator> iterator,
    size_t cost_estimate,
    Priority rp,
    StorageTaskType type,
    ThreadType thread_type,
    StorageTaskPriority priority,
    Principal principal,
    Sockaddr client_address)
    : StorageTask(type),
      stream_(std::move(stream)),
      catchup_queue_(std::move(catchup_queue)),
      server_read_stream_version_(server_read_stream_version),
      filter_version_(filter_version),
      read_ctx_(read_ctx),
      options_(options),
      iterator_from_cache_(iterator),
      thread_type_(thread_type),
      priority_(priority),
      principal_(principal),
      throttling_estimate_(cost_estimate),
      rpriority_(rp) {
  ld_check(stream_);
  auto stream_ptr = stream_.get();
  ld_check(stream_ptr);

  // saving debug info
  stream_id_ = stream_ptr->id_;
  client_id_ = stream_ptr->client_id_;
  client_address_ = client_address;
  stream_start_lsn_ = stream_ptr->start_lsn_;
  stream_creation_time_ = stream_ptr->created_;
  stream_scd_enabled_ = stream_ptr->scdEnabled();
  stream_known_down_ = stream_ptr->getKnownDown();

  // catchup_queue_ may be nullptr in tests.

  ld_spew("constructor client_id=%s id=%lu log_id=%" PRIu64
          " from.lsn=%s until_lsn=%s window_high=%" PRIu64
          " server_read_stream_version=%" PRIu64
          " max_bytes_to_deliver_=%zu first_record_any_size=%d",
          catchup_queue_ ? catchup_queue_->client_id_.toString().c_str() : "",
          stream_->id_.val_,
          read_ctx.logid_.val_,
          lsn_to_string(read_ctx_.read_ptr_.lsn).c_str(),
          lsn_to_string(read_ctx_.until_lsn_).c_str(),
          read_ctx_.window_high_,
          server_read_stream_version_.val_,
          read_ctx_.max_bytes_to_deliver_,
          int(read_ctx_.first_record_any_size_));
}

void ReadStorageTask::setMemoryToken(ResourceBudget::Token memory_token) {
  ld_check(memory_token.valid());
  memory_token_ = std::move(memory_token);
}

void ReadStorageTask::execute() {
  ld_check(options_.allow_blocking_io);
  ld_check(total_bytes_ == 0);

  STAT_INCR(storageThreadPool_->stats(), num_in_flight_read_storage_tasks);

  if (stream_) { // Only read if the ServerReadStream still exists.
    if (options_.inject_latency) {
      folly::Baton<> baton;
      auto& io_fault_injection = IOFaultInjection::instance();
      baton.try_wait_for(
          io_fault_injection.getLatencyToInject(stream_->shard_));
    }
    owned_iterator_ = iterator_from_cache_.lock();
    if (!owned_iterator_) {
      // Either the iterator wasn't passed in by CatchupOneStream because it
      // wasn't found in the cache, or the iterator has since then been
      // invalidated because its ttl expired.
      // Create a new iterator that will later be handed over to
      // CatchupOneStream.
      owned_iterator_ = storageThreadPool_->getLocalLogStore().read(
          read_ctx_.logid_, options_);
    }

    ld_check(owned_iterator_);

    StorageThreadCallback callback;
    Status status =
        LocalLogStoreReader::read(*owned_iterator_,
                                  callback,
                                  &read_ctx_,
                                  storageThreadPool_->stats(),
                                  *storageThreadPool_->getSettings().get());

    // The `read()` call populated `callback` with some records.  Now move them
    // into the ReadStorageTask instance, which will get passed back to the
    // worker.
    status_ = status;
    records_ = std::move(callback.releaseRecords());

    total_bytes_ = callback.totalBytes();

    /*
     * TODO (T37204962).
     * uint64_t origSize = reqSize();
     * if (origSize > total_bytes_ && origSize >= 64 * 1024 &&
     *  origSize - total_bytes_ >= 64 * 1024) {
     *    uint64_t unusedBytes = origSize - total_bytes_;
     *    storageThreadPool_->creditScheduler(unusedBytes, getPrincipal());
     *    }
     */
  }

  // Release memory for what we did not read.
  // It's actually possible to have read more than max_bytes_to_deliver_ if
  // first_record_any_size was true. However, AllServerReadStreams only budgeted
  // for max_bytes_to_deliver_ so we don't release anything in that case.
  if (total_bytes_ < read_ctx_.max_bytes_to_deliver_) {
    ld_check(memory_token_.valid());
    memory_token_.release(read_ctx_.max_bytes_to_deliver_ - total_bytes_);
  }

  STAT_ADD(storageThreadPool_->stats(),
           read_storage_tasks_allocated_records_bytes,
           total_bytes_);
}

void ReadStorageTask::onDone() {
  ServerWorker::onThisThread()->serverReadStreams().onReadTaskDone(*this);
  WORKER_STAT_DECR(num_in_flight_read_storage_tasks);
}

void ReadStorageTask::onDropped() {
  ld_check(total_bytes_ == 0);
  ServerWorker::onThisThread()->serverReadStreams().onReadTaskDropped(*this);
}

void ReadStorageTask::releaseRecords() {
  records_.clear();

  Worker* w = Worker::onThisThread(false);
  if (w) {
    // w may be nullptr in tests.
    STAT_SUB(
        w->stats(), read_storage_tasks_allocated_records_bytes, total_bytes_);
  }

  ld_check(memory_token_.valid());
  memory_token_.release();
}

void ReadStorageTask::getDebugInfoDetailed(StorageTaskDebugInfo& info) const {
  info.log_id = read_ctx_.logid_;
  info.lsn = read_ctx_.read_ptr_.lsn;
  info.client_id = client_id_;
  info.client_address = client_address_;
  info.extra_info = read_ctx_.toString() +
      folly::sformat(", Read stream ID: {}, stream start lsn: {}, stream "
                     "version at task creation: {}, stream filter version at "
                     "task creation: {}, stream creation time: {}, SCD: {}, "
                     "SCD known down list: {}",
                     stream_id_.val(),
                     lsn_to_string(stream_start_lsn_),
                     server_read_stream_version_.val(),
                     filter_version_.val(),
                     toString(stream_creation_time_),
                     stream_scd_enabled_,
                     toString(stream_known_down_));
}

int StorageThreadCallback::processRecord(const RawRecord& record) {
  // When doing local log store reads on a storage thread, we need to copy the
  // data out of the local log store into a malloc'd buffer, since records
  // will only get passed to the messaging layer at some later time (when the
  // worker thread gets around to processing the ReadStorageTask result).
  void* blob_copy = malloc(record.blob.size);
  if (blob_copy == nullptr) {
    throw std::bad_alloc();
  }
  memcpy(blob_copy, record.blob.data, record.blob.size);
  total_bytes_ += record.blob.size;

  records_.emplace_back( // creating a RawRecord
      record.lsn,
      Slice(blob_copy, record.blob.size),
      /*owned*/ true, // We malloc-d the memory
      record.from_under_replicated_region);
  return 0;
}
}} // namespace facebook::logdevice
