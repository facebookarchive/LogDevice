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
#include "logdevice/server/locallogstore/ShardToPathMapping.h"

namespace facebook { namespace logdevice {

void RocksDBEnv::Schedule(void (*function)(void* arg),
                          void* arg,
                          Priority pri,
                          void* tag,
                          void (*unschedFunction)(void* arg)) {
  ScheduledJob* job = new ScheduledJob();
  job->function = function;
  job->unschedFunction = unschedFunction;
  job->arg = arg;
  job->pri = pri;
  job->ioprio_to_set =
      pri == Priority::LOW ? settings_->low_ioprio : folly::none;
  job->env = this;

  rocksdb::EnvWrapper::Schedule(
      &RocksDBEnv::bgJobRun, job, pri, tag, &RocksDBEnv::bgJobUnschedule);
}

void RocksDBEnv::bgJobRun(void* arg) {
  std::unique_ptr<ScheduledJob> job(static_cast<ScheduledJob*>(arg));
  BGThreadState& thread_state = *job->env->bg_threads_;

  if (thread_state.initialized) {
    ld_check(ThreadID::getType() == ThreadID::Type::ROCKSDB);
    ld_check(!thread_state.running_a_job);
  } else {
    // Set thread name to something like "rocks-high-101".
    ld_check_between(
        static_cast<int>(job->pri), 0, static_cast<int>(Priority::TOTAL) - 1);
    std::string pri_name = rocksdb::Env::PriorityToString(job->pri);
    pri_name[0] = std::tolower(pri_name[0]);
    pri_name = pri_name.substr(0, 4); // shorten "bottom" to "bott"
    int idx = job->env->next_bg_thread_id_[static_cast<int>(job->pri)]++;
    // Longest possible name is "rocks-high-999", which fits in 15 characters.
    ThreadID::set(
        ThreadID::Type::ROCKSDB, folly::sformat("rocks-{}-{}", pri_name, idx));

    // Set IO priority if needed.
    if (job->ioprio_to_set.hasValue()) {
      set_io_priority_of_this_thread(job->ioprio_to_set.value());
    }

    thread_state.initialized = true;
  }

  thread_state.running_a_job = true;

  // Actually run the job.
  (*job->function)(job->arg);

  thread_state.running_a_job = false;

  // Clean up after RocksDBListener.
  thread_state.io_tracing_context.clear();
}

void RocksDBEnv::bgJobUnschedule(void* arg) {
  std::unique_ptr<ScheduledJob> job(static_cast<ScheduledJob*>(arg));
  (*job->unschedFunction)(job->arg);
}

IOTracing::AddContext* RocksDBEnv::backgroundJobContextOfThisThread() {
  // There are two cases to distinguish: we're called from rocksdb bg thread
  // inside a job, or we're called from logdevice thread.

  // In both cases ThreadID should be set.
  ThreadID::Type type = ThreadID::getType();
  if (type == ThreadID::Type::UNKNOWN) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "We seem to be on some weird unknown thread. Check "
                    "RocksDBEnv job wrapping logic.");
    return nullptr;
  }

  BGThreadState& thread_state = *bg_threads_;
  if (type != ThreadID::Type::ROCKSDB) {
    // Logdevice thread.
    ld_check(!thread_state.initialized);
    return nullptr;
  }

  ld_check(thread_state.initialized);
  if (!thread_state.running_a_job) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "RocksDBListener called on rocksdb thread but outside "
                    "background job. Check RocksDBEnv job wrapping logic.");
    return nullptr;
  }

  return &thread_state.io_tracing_context;
}

rocksdb::Status
RocksDBEnv::WritableFileOpImpl(WritableFileOp op,
                               const char* op_name,
                               const std::string& f,
                               const std::string& old_fname,
                               std::unique_ptr<rocksdb::WritableFile>* r,
                               const rocksdb::EnvOptions& options) {
  auto tracing = tracingInfoForPath(f);
  std::unique_ptr<rocksdb::WritableFile> file;
  {
    SCOPED_IO_TRACED_OP(tracing.io_tracing,
                        "wf:{}{}{}|{}",
                        old_fname,
                        old_fname.empty() ? "" : "->",
                        tracing.filename,
                        op_name);
    rocksdb::Status status;
    switch (op) {
      case WritableFileOp::NEW:
        status = rocksdb::EnvWrapper::NewWritableFile(f, &file, options);
        break;
      case WritableFileOp::REOPEN:
        status = rocksdb::EnvWrapper::ReopenWritableFile(f, &file, options);
        break;
      case WritableFileOp::REUSE:
        status = rocksdb::EnvWrapper::ReuseWritableFile(
            f, old_fname, &file, options);
        break;
    }
    if (!status.ok()) {
      SCOPED_IO_TRACING_CONTEXT(tracing.io_tracing, "failed");
      return status;
    }
  }

  if (!settings_->background_wal_sync ||
      f.substr(f.size() - std::min(f.size(), 4ul)) != ".log") {
    ld_debug("Opened writable file %s", f.c_str());
    *r =
        std::make_unique<RocksDBWritableFile>(std::move(file), stats_, tracing);
  } else {
    ld_debug("Opened writable file %s with background RangeSync() wrapper",
             f.c_str());
    *r = std::make_unique<RocksDBBackgroundSyncFile>(
        std::move(file), stats_, tracing);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status
RocksDBEnv::NewWritableFile(const std::string& f,
                            std::unique_ptr<rocksdb::WritableFile>* r,
                            const rocksdb::EnvOptions& options) {
  return WritableFileOpImpl(
      WritableFileOp::NEW, "NewWritableFile", f, "", r, options);
}

#ifdef LOGDEVICED_ROCKSDB_HAS_WRAPPERS

rocksdb::Status
RocksDBEnv::NewSequentialFile(const std::string& f,
                              std::unique_ptr<rocksdb::SequentialFile>* r,
                              const rocksdb::EnvOptions& options) {
  auto tracing = tracingInfoForPath(f);
  std::unique_ptr<rocksdb::SequentialFile> file;
  {
    SCOPED_IO_TRACED_OP(
        tracing.io_tracing, "sf:{}|NewSequentialFile", tracing.filename);
    auto status = rocksdb::EnvWrapper::NewSequentialFile(f, &file, options);
    if (!status.ok()) {
      SCOPED_IO_TRACING_CONTEXT(tracing.io_tracing, "failed");
      return status;
    }
  }
  *r = std::make_unique<RocksDBSequentialFile>(std::move(file), tracing);
  return rocksdb::Status::OK();
}
rocksdb::Status
RocksDBEnv::NewRandomAccessFile(const std::string& f,
                                std::unique_ptr<rocksdb::RandomAccessFile>* r,
                                const rocksdb::EnvOptions& options) {
  auto tracing = tracingInfoForPath(f);
  std::unique_ptr<rocksdb::RandomAccessFile> file;
  {
    SCOPED_IO_TRACED_OP(
        tracing.io_tracing, "rf:{}|NewRandomAccessFile", tracing.filename);
    auto status = rocksdb::EnvWrapper::NewRandomAccessFile(f, &file, options);
    if (!status.ok()) {
      SCOPED_IO_TRACING_CONTEXT(tracing.io_tracing, "failed");
      return status;
    }
  }
  *r = std::make_unique<RocksDBRandomAccessFile>(std::move(file), tracing);
  return rocksdb::Status::OK();
}
rocksdb::Status
RocksDBEnv::NewDirectory(const std::string& f,
                         std::unique_ptr<rocksdb::Directory>* r) {
  auto tracing = tracingInfoForPath(f);
  std::unique_ptr<rocksdb::Directory> dir;
  {
    SCOPED_IO_TRACED_OP(
        tracing.io_tracing, "d:{}|NewDirectory", tracing.filename);
    auto status = rocksdb::EnvWrapper::NewDirectory(f, &dir);
    if (!status.ok()) {
      SCOPED_IO_TRACING_CONTEXT(tracing.io_tracing, "failed");
      return status;
    }
  }
  *r = std::make_unique<RocksDBDirectory>(std::move(dir), tracing);
  return rocksdb::Status::OK();
}
rocksdb::Status
RocksDBEnv::ReopenWritableFile(const std::string& f,
                               std::unique_ptr<rocksdb::WritableFile>* r,
                               const rocksdb::EnvOptions& options) {
  return WritableFileOpImpl(
      WritableFileOp::REOPEN, "ReopenWritableFile", f, "", r, options);
}
rocksdb::Status
RocksDBEnv::ReuseWritableFile(const std::string& f,
                              const std::string& old_fname,
                              std::unique_ptr<rocksdb::WritableFile>* r,
                              const rocksdb::EnvOptions& options) {
  return WritableFileOpImpl(
      WritableFileOp::REUSE, "ReuseWritableFile", f, old_fname, r, options);
}

rocksdb::Status RocksDBEnv::FileExists(const std::string& f) {
  auto tracing = tracingInfoForPath(f);
  SCOPED_IO_TRACED_OP(tracing.io_tracing, "f:{}|FileExists", tracing.filename);
  auto s = rocksdb::EnvWrapper::FileExists(f);
  SCOPED_IO_TRACING_CONTEXT(tracing.io_tracing, "{}", s.ToString());
  return s;
}
rocksdb::Status RocksDBEnv::GetChildren(const std::string& dir,
                                        std::vector<std::string>* result) {
  auto tracing = tracingInfoForPath(dir);
  SCOPED_IO_TRACED_OP(tracing.io_tracing, "d:{}|GetChildren", tracing.filename);
  return rocksdb::EnvWrapper::GetChildren(dir, result);
}
rocksdb::Status
RocksDBEnv::GetChildrenFileAttributes(const std::string& dir,
                                      std::vector<FileAttributes>* result) {
  auto tracing = tracingInfoForPath(dir);
  SCOPED_IO_TRACED_OP(
      tracing.io_tracing, "f:{}|GetChildrenFileAttributes", tracing.filename);
  return rocksdb::EnvWrapper::GetChildrenFileAttributes(dir, result);
}
rocksdb::Status RocksDBEnv::DeleteFile(const std::string& f) {
  auto start_time = std::chrono::steady_clock::now();
  rocksdb::Status status;

  {
    auto tracing = tracingInfoForPath(f);
    SCOPED_IO_TRACED_OP(
        tracing.io_tracing, "f:{}|DeleteFile", tracing.filename);
    status = rocksdb::EnvWrapper::DeleteFile(f);
  }

  Worker* w = Worker::onThisThread(false);
  if (w) {
    auto end_time = std::chrono::steady_clock::now();
    dbg::Level level =
        end_time - start_time >= settings_->worker_blocking_io_threshold_
        ? dbg::Level::INFO
        : dbg::Level::DEBUG;
    ld_log(
        level,
        "Deleted a file from Worker thread %s (%s) in %.6f seconds, path: %s",
        w->getName().c_str(),
        w->currentlyRunning_.describe().c_str(),
        std::chrono::duration_cast<std::chrono::duration<double>>(end_time -
                                                                  start_time)
            .count(),
        f.c_str());
  }

  return status;
}
rocksdb::Status RocksDBEnv::Truncate(const std::string& f, size_t size) {
  auto tracing = tracingInfoForPath(f);
  SCOPED_IO_TRACED_OP(
      tracing.io_tracing, "f:{}|Truncate|sz:{}", tracing.filename, size);
  return rocksdb::EnvWrapper::Truncate(f, size);
}
rocksdb::Status RocksDBEnv::GetFileSize(const std::string& f,
                                        uint64_t* file_size) {
  auto tracing = tracingInfoForPath(f);
  SCOPED_IO_TRACED_OP(tracing.io_tracing, "f:{}|GetFileSize", tracing.filename);
  return rocksdb::EnvWrapper::GetFileSize(f, file_size);
}
rocksdb::Status RocksDBEnv::GetFileModificationTime(const std::string& f,
                                                    uint64_t* file_mtime) {
  auto tracing = tracingInfoForPath(f);
  SCOPED_IO_TRACED_OP(
      tracing.io_tracing, "f:{}|GetFileModificationTime", tracing.filename);
  return rocksdb::EnvWrapper::GetFileModificationTime(f, file_mtime);
}
rocksdb::Status RocksDBEnv::RenameFile(const std::string& src,
                                       const std::string& target) {
  auto tracing_src = tracingInfoForPath(src);
  auto tracing = tracingInfoForPath(target);
  SCOPED_IO_TRACED_OP(tracing.io_tracing,
                      "f:{}->{}|RenameFile",
                      tracing_src.filename,
                      tracing.filename);
  return rocksdb::EnvWrapper::RenameFile(src, target);
}

RocksDBSequentialFile::RocksDBSequentialFile(
    std::unique_ptr<rocksdb::SequentialFile> file,
    FileTracingInfo tracing)
    : rocksdb::SequentialFileWrapper(file.get()),
      file_(std::move(file)),
      tracing_(tracing) {}
RocksDBSequentialFile::~RocksDBSequentialFile() {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "sf:{}|close", tracing_.filename);
  file_.reset();
}
rocksdb::Status RocksDBSequentialFile::Read(size_t n,
                                            rocksdb::Slice* result,
                                            char* scratch) {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "sf:{}|Read|sz:{}", tracing_.filename, n);
  return rocksdb::SequentialFileWrapper::Read(n, result, scratch);
}
rocksdb::Status RocksDBSequentialFile::Skip(uint64_t n) {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "sf:{}|Skip|sz:{}", tracing_.filename, n);
  return rocksdb::SequentialFileWrapper::Skip(n);
}
rocksdb::Status RocksDBSequentialFile::PositionedRead(uint64_t offset,
                                                      size_t n,
                                                      rocksdb::Slice* result,
                                                      char* scratch) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "sf:{}|PositionedRead|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      n);
  return rocksdb::SequentialFileWrapper::PositionedRead(
      offset, n, result, scratch);
}

RocksDBRandomAccessFile::RocksDBRandomAccessFile(
    std::unique_ptr<rocksdb::RandomAccessFile> file,
    FileTracingInfo tracing)
    : rocksdb::RandomAccessFileWrapper(file.get()),
      file_(std::move(file)),
      tracing_(tracing) {}
RocksDBRandomAccessFile::~RocksDBRandomAccessFile() {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "rf:{}|close", tracing_.filename);
  file_.reset();
}
rocksdb::Status RocksDBRandomAccessFile::Read(uint64_t offset,
                                              size_t n,
                                              rocksdb::Slice* result,
                                              char* scratch) const {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "rf:{}|Read|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      n);
  return rocksdb::RandomAccessFileWrapper::Read(offset, n, result, scratch);
}
rocksdb::Status RocksDBRandomAccessFile::Prefetch(uint64_t offset, size_t n) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "rf:{}|Prefetch|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      n);
  return rocksdb::RandomAccessFileWrapper::Prefetch(offset, n);
}
size_t RocksDBRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "rf:{}|GetUniqueId", tracing_.filename);
  return rocksdb::RandomAccessFileWrapper::GetUniqueId(id, max_size);
}
void RocksDBRandomAccessFile::Hint(AccessPattern pattern) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "rf:{}|Hint({})",
                      tracing_.filename,
                      static_cast<int>(pattern));
  return rocksdb::RandomAccessFileWrapper::Hint(pattern);
}
rocksdb::Status RocksDBRandomAccessFile::InvalidateCache(size_t offset,
                                                         size_t length) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "rf:{}|InvalidateCache|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      length);
  return rocksdb::RandomAccessFileWrapper::InvalidateCache(offset, length);
}

RocksDBDirectory::RocksDBDirectory(std::unique_ptr<rocksdb::Directory> dir,
                                   FileTracingInfo tracing)
    : rocksdb::DirectoryWrapper(dir.get()),
      dir_(std::move(dir)),
      tracing_(tracing) {}
RocksDBDirectory::~RocksDBDirectory() {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "d:{}|close", tracing_.filename);
  dir_.reset();
}
rocksdb::Status RocksDBDirectory::Fsync() {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "d:{}|Fsync", tracing_.filename);
  return rocksdb::DirectoryWrapper::Fsync();
}
size_t RocksDBDirectory::GetUniqueId(char* id, size_t max_size) const {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "d:{}|GetUniqueId", tracing_.filename);
  return rocksdb::DirectoryWrapper::GetUniqueId(id, max_size);
}

#endif // LOGDEVICED_ROCKSDB_HAS_WRAPPERS

FileTracingInfo RocksDBEnv::tracingInfoForPath(const std::string& path) {
  shard_index_t shard_idx;
  FileTracingInfo info;
  if (ShardToPathMapping::parseFilePath(path, &shard_idx, &info.filename) &&
      shard_idx < io_tracing_by_shard_.size()) {
    info.io_tracing = io_tracing_by_shard_[shard_idx];
  } else {
    // If path is in unexpected format, leave io_tracing null.
    info.filename = path;
  }
  return info;
}

RocksDBWritableFile::RocksDBWritableFile(
    std::unique_ptr<rocksdb::WritableFile> file,
    StatsHolder* stats,
    FileTracingInfo tracing)
    : rocksdb::WritableFileWrapper(file.get()),
      file_(std::move(std::move(file))),
      stats_(stats),
      tracing_(tracing) {}
RocksDBWritableFile::~RocksDBWritableFile() {
  // Make sure file closing is traced.
  Close();
}

rocksdb::Status RocksDBWritableFile::Append(const rocksdb::Slice& data) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "wf:{}|Append|sz:{}",
                      tracing_.filename,
                      data.size());
  return rocksdb::WritableFileWrapper::Append(data);
}
rocksdb::Status
RocksDBWritableFile::PositionedAppend(const rocksdb::Slice& data,
                                      uint64_t offset) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "wf:{}|PositionedAppend|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      data.size());
  return rocksdb::WritableFileWrapper::PositionedAppend(data, offset);
}
rocksdb::Status RocksDBWritableFile::Truncate(uint64_t size) {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "wf:{}|Truncate|sz:{}", tracing_.filename, size);
  return rocksdb::WritableFileWrapper::Truncate(size);
}
rocksdb::Status RocksDBWritableFile::Close() {
  if (closed_) {
    return rocksdb::Status::OK();
  }
  closed_ = true;
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "wf:{}|Close", tracing_.filename);
  return rocksdb::WritableFileWrapper::Close();
}
rocksdb::Status RocksDBWritableFile::Flush() {
  // Flush() is currently a no-op in all rocksdb WritableFile implementations.
  // Don't trace it.
  // (This method is a leftover from the days when rocksdb::WritableFile was
  //  responsible for buffering, before rocksdb::WritableFileWriter existed.)
  return rocksdb::WritableFileWrapper::Flush();
}
rocksdb::Status RocksDBWritableFile::Sync() {
  auto time_start = std::chrono::steady_clock::now();
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "wf:{}|Sync", tracing_.filename);
  auto s = rocksdb::WritableFileWrapper::Sync();
  auto time_end = std::chrono::steady_clock::now();
  STAT_INCR(stats_, fdatasyncs);
  STAT_ADD(stats_, fdatasync_microsec, to_usec(time_end - time_start).count());
  return s;
}
rocksdb::Status RocksDBWritableFile::Fsync() {
  auto time_start = std::chrono::steady_clock::now();
  SCOPED_IO_TRACED_OP(tracing_.io_tracing, "wf:{}|Fsync", tracing_.filename);
  auto s = rocksdb::WritableFileWrapper::Fsync();
  auto time_end = std::chrono::steady_clock::now();
  STAT_INCR(stats_, fsyncs);
  STAT_ADD(stats_, fsync_microsec, to_usec(time_end - time_start).count());
  return s;
}
size_t RocksDBWritableFile::GetUniqueId(char* id, size_t max_size) const {
  SCOPED_IO_TRACED_OP(
      tracing_.io_tracing, "wf:{}|GetUniqueId", tracing_.filename);
  return rocksdb::WritableFileWrapper::GetUniqueId(id, max_size);
}
rocksdb::Status RocksDBWritableFile::InvalidateCache(size_t offset,
                                                     size_t length) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "wf:{}|InvalidateCache|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      length);
  return rocksdb::WritableFileWrapper::InvalidateCache(offset, length);
}
rocksdb::Status RocksDBWritableFile::RangeSync(uint64_t offset,
                                               uint64_t nbytes) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "wf:{}|RangeSync|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      nbytes);
  return rocksdb::WritableFileWrapper::RangeSync(offset, nbytes);
}
rocksdb::Status RocksDBWritableFile::Allocate(uint64_t offset, uint64_t len) {
  SCOPED_IO_TRACED_OP(tracing_.io_tracing,
                      "wf:{}|Allocate|off:{}|sz:{}",
                      tracing_.filename,
                      offset,
                      len);
  return rocksdb::WritableFileWrapper::Allocate(offset, len);
}

rocksdb::Status RocksDBBackgroundSyncFile::Close() {
  // Some enqueued RangeSync-s may be dropped on the floor, but that's ok,
  // they're best-effort anyway.
  JoinThread();
  return RocksDBWritableFile::Close();
}
RocksDBBackgroundSyncFile::~RocksDBBackgroundSyncFile() {
  Close();
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
      auto status = RocksDBWritableFile::RangeSync(offset, nbytes);
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
