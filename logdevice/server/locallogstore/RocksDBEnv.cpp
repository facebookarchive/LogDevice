/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/RocksDBEnv.h"

#include <folly/Optional.h>
#include <folly/ThreadLocal.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

folly::ThreadLocal<folly::Optional<std::pair<int, int>>> last_set_thread_prio;

static void maybe_set_prio(std::pair<int, int> prio) {
  auto cur_prio = last_set_thread_prio.get();
  if (!cur_prio->hasValue() || cur_prio->value() != prio) {
    *cur_prio = prio;
    set_io_priority_of_this_thread(prio);
  }
}

void RocksDBEnv::Callback::call() const {
  maybe_set_prio(prio);
  function(arg);
}

void RocksDBEnv::Callback::unschedule() const {
  if (unschedule_function) {
    maybe_set_prio(prio);
    unschedule_function(arg);
  }
}

void RocksDBEnv::callback(void* arg) {
  reinterpret_cast<const Callback*>(arg)->call();
}

void RocksDBEnv::callback_unschedule(void* arg) {
  reinterpret_cast<const Callback*>(arg)->unschedule();
}
void RocksDBEnv::BaseSchedule(void (*function)(void* arg),
                              void* arg,
                              Priority pri,
                              void* tag,
                              void (*unschedFunction)(void* arg)) {
  rocksdb::EnvWrapper::Schedule(function, arg, pri, tag, unschedFunction);
}

void RocksDBEnv::Schedule(void (*function)(void* arg),
                          void* arg,
                          Priority pri,
                          void* tag,
                          void (*unschedFunction)(void* arg)) {
  if (pri != Priority::LOW || !settings_->low_ioprio.hasValue()) {
    BaseSchedule(function, arg, pri, tag, unschedFunction);
    return;
  }

  Callback cb_value = {
      function, arg, settings_->low_ioprio.value(), unschedFunction};
  const Callback* cb_ptr = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    if (callbacks_.size() < 1000000) {
      cb_ptr = &*callbacks_.insert(cb_value).first;
    } else {
      // Currently rocksdb can only pass O(1) different arguments to this
      // method per DB instance. It's seems unlikely that it will change
      // as long as UnSchedule() doesn't offer any way to do cleanup.
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Over %lu different arguments passed to Schedule(). "
                      "Not setting IO priorities anymore.",
                      callbacks_.size());
    }
  }

  if (cb_ptr) {
    BaseSchedule(&RocksDBEnv::callback,
                 const_cast<void*>(reinterpret_cast<const void*>(cb_ptr)),
                 pri,
                 tag,
                 &RocksDBEnv::callback_unschedule);
  } else {
    BaseSchedule(function, arg, pri, tag, unschedFunction);
  }
}

rocksdb::Status
RocksDBEnv::NewRandomAccessFile(const std::string& f,
                                std::unique_ptr<rocksdb::RandomAccessFile>* r,
                                const rocksdb::EnvOptions& options) {
  std::unique_ptr<rocksdb::RandomAccessFile> file;
  auto status = rocksdb::EnvWrapper::NewRandomAccessFile(f, &file, options);
  if (!status.ok()) {
    return status;
  }
  ld_debug("Wrapping random access file %s", f.c_str());
  r->reset(new RocksDBRandomAccessFile(f, std::move(file)));
  return rocksdb::Status::OK();
}

rocksdb::Status
RocksDBEnv::NewWritableFile(const std::string& f,
                            std::unique_ptr<rocksdb::WritableFile>* r,
                            const rocksdb::EnvOptions& options) {
  std::unique_ptr<rocksdb::WritableFile> file;
  auto status = rocksdb::EnvWrapper::NewWritableFile(f, &file, options);
  if (!status.ok()) {
    return status;
  }

  if (!settings_->background_wal_sync ||
      f.substr(f.size() - std::min(f.size(), 4ul)) != ".log") {
    ld_debug("Opened writable file %s", f.c_str());
    r->reset(new RocksDBWritableFile(std::move(file), stats_));
  } else {
    ld_debug("Opened writable file %s with background RangeSync() wrapper",
             f.c_str());
    r->reset(new RocksDBBackgroundSyncFile(std::move(file), stats_));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RocksDBEnv::DeleteFile(const std::string& fname) {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    return rocksdb::EnvWrapper::DeleteFile(fname);
  }

  auto start_time = std::chrono::steady_clock::now();
  auto status = rocksdb::EnvWrapper::DeleteFile(fname);
  auto end_time = std::chrono::steady_clock::now();

  dbg::Level level =
      end_time - start_time >= settings_->worker_blocking_io_threshold_
      ? dbg::Level::INFO
      : dbg::Level::DEBUG;
  ld_log(level,
         "Deleted a file from Worker thread %s (%s) in %.6f seconds, path: %s",
         w->getName().c_str(),
         w->currentlyRunning_.describe().c_str(),
         std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                   start_time)
             .count(),
         fname.c_str());

  return status;
}

rocksdb::Status RocksDBWritableFile::Sync() {
  auto time_start = std::chrono::steady_clock::now();
  auto s = rocksdb::WritableFileWrapper::Sync();
  auto time_end = std::chrono::steady_clock::now();
  STAT_INCR(stats_, fdatasyncs);
  STAT_ADD(stats_,
           fdatasync_microsec,
           std::chrono::duration_cast<std::chrono::microseconds>(time_end -
                                                                 time_start)
               .count());
  return s;
}
rocksdb::Status RocksDBWritableFile::Fsync() {
  auto time_start = std::chrono::steady_clock::now();
  auto s = rocksdb::WritableFileWrapper::Fsync();
  auto time_end = std::chrono::steady_clock::now();
  STAT_INCR(stats_, fsyncs);
  STAT_ADD(stats_,
           fsync_microsec,
           std::chrono::duration_cast<std::chrono::microseconds>(time_end -
                                                                 time_start)
               .count());
  return s;
}

RocksDBRandomAccessFile::RocksDBReadTracer::RocksDBReadTracer(
    const RocksDBRandomAccessFile* f,
    uint64_t offset,
    size_t n)
    : file_(f),
      offset_(offset),
      length_(n),
      start_time_(std::chrono::steady_clock::now()) {}

RocksDBRandomAccessFile::RocksDBReadTracer::~RocksDBReadTracer() {
  const TrackableIterator* iter = IteratorTracker::active_iterator;
  if (!iter) {
    return;
  }

  auto now = std::chrono::steady_clock::now();
  const auto* store = iter->getStore();
  bool local_seek = offset_ != file_->file_offset;
  bool global_seek =
      store->updateSeekPosition(file_->file_name_hash, offset_, length_);

  const auto& it_info = iter->getImmutableTrackingInfo();
  auto it_state = iter->getMutableTrackingInfo();

  ld_info("|shard=%d|column_family=%s|log_id=%lu|ctx=%s::%s%s|filename=%s"
          "|offset=%zd|length=%zd|time=%jdus|local_seek=%s|global_seek=%s|",
          store->getShardIdx(),
          it_info.column_family_name.c_str(),
          it_info.log_id.val_,
          it_state.more_context,
          IteratorTracker::active_iterator_op,
          IteratorTracker::active_iterator_op_context.c_str(),
          file_->file_name.c_str(),
          offset_,
          length_,
          (intmax_t)std::chrono::duration_cast<std::chrono::microseconds>(
              now - start_time_)
              .count(),
          local_seek ? "Y" : "N",
          global_seek ? "Y" : "N");

  file_->file_offset = offset_ + length_;
}

RocksDBRandomAccessFile::RocksDBRandomAccessFile(
    const std::string& f,
    std::unique_ptr<rocksdb::RandomAccessFile> file)
    : RocksDBRandomAccessFileWrapper(file.get()),
      file(std::move(file)),
      file_name(f),
      file_offset(~0),
      file_name_hash(f) {}

rocksdb::Status RocksDBRandomAccessFile::Read(uint64_t offset,
                                              size_t n,
                                              rocksdb::Slice* result,
                                              char* scratch) const {
  auto tracer = RocksDBReadTracer(this, offset, n);
  return RocksDBRandomAccessFileWrapper::Read(offset, n, result, scratch);
}

rocksdb::Status RocksDBBackgroundSyncFile::Close() {
  JoinThread();
  return rocksdb::WritableFileWrapper::Close();
}
RocksDBBackgroundSyncFile::~RocksDBBackgroundSyncFile() {
  JoinThread();
}

rocksdb::Status RocksDBBackgroundSyncFile::RangeSync(uint64_t offset,
                                                     uint64_t nbytes) {
  if (static_cast<off_t>(offset) != to_sync_end_.load()) {
    // We expect RangeSync() to be called for sequential ranges.
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "Something changed in rocksdb implementation around RangeSync() usage. "
        "Please check if RocksDBBackgroundSyncFile behavior still makes "
        "sense.");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  to_sync_begin_ = std::min(to_sync_begin_, static_cast<off_t>(offset));
  to_sync_end_.store(
      std::max(to_sync_end_.load(), static_cast<off_t>(offset + nbytes)));
  cv_.notify_one();

  return rocksdb::Status::OK();
}

void RocksDBBackgroundSyncFile::ThreadRun() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:wal-sync");

  std::unique_lock<std::mutex> lock(mutex_);
  while (!shutting_down_) {
    off_t end;
    while ((end = to_sync_end_.load()) > to_sync_begin_ && !shutting_down_) {
      off_t offset = to_sync_begin_;
      off_t nbytes = end - to_sync_begin_;
      to_sync_begin_ = end;

      lock.unlock();
      auto status = rocksdb::WritableFileWrapper::RangeSync(offset, nbytes);
      if (!status.ok()) {
        RATELIMIT_ERROR(std::chrono::seconds(2),
                        2,
                        "RangeSync() failed with status %s",
                        status.ToString().c_str());
      }
      lock.lock();
    }

    if (!shutting_down_) {
      cv_.wait(lock);
    }
  }
}

void RocksDBBackgroundSyncFile::JoinThread() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (shutting_down_) {
      return;
    }
    shutting_down_ = true;
  }
  cv_.notify_one();
  ld_check(thread_.joinable());
  thread_.join();
}

}} // namespace facebook::logdevice
