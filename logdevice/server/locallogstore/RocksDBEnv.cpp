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
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"

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
    if (job->ioprio_to_set.has_value()) {
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

  if (f.substr(f.size() - std::min(f.size(), 4ul)) != ".log") {
    ld_debug("Opened writable file %s", f.c_str());
    *r =
        std::make_unique<RocksDBWritableFile>(std::move(file), stats_, tracing);
    return rocksdb::Status::OK();
  }

  // It's a write-ahead log file. Wrap it in RocksDBBufferedWALFile if needed.
  // Adjust buffering mode depending on what the underlying file supports.

  WALBufferingMode mode = settings_->wal_buffering;

  if (file->use_direct_io()) {
    // Files with use_direct_io() have some additional alignment requirements
    // that RocksDBBufferedWALFile's background thread doesn't meet.
    // That's ok because rocksdb disables direct IO for WAL files, so this
    // branch is not even supposed to be reachable.

    if (!file->IsSyncThreadSafe()) {
      // Oops, this combination is not supported.
      // This currently can't happen for two reasons: rocksdb doesn't enable
      // direct IO for WAL files, and rocksdb's WritableFile has
      // IsSyncThreadSafe() = true.
      // If somehow both of these change in future, you'll have to make rocksdb
      // not use direct IO for wal files (maybe through
      // Env::OptimizeForLogWrite()), or make underlying WritableFile support
      // IsSyncThreadSafe(), or make RocksDBBufferedWALFile support direct IO.
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          1,
          "rocksdb::WritableFile implementation uses direct IO "
          "and doesn't support thread-safe sync. Such combination "
          "is not supported by logdevice, and rocksdb is not "
          "supposed to do that. Please investigate. For now we'll "
          "be dropping WAL syncs on the floor, which will cause "
          "all sorts of metadata corruption on crashes.");
      std::abort();
    } else if (mode != WALBufferingMode::NONE) {
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          1,
          "rocksdb::WritableFile implementation uses direct IO for WAL file. "
          "This is unexpected. WAL buffering (rocksdb-wal-buffering setting) "
          "is not supported in this case.");
      ld_check(false);
      mode = WALBufferingMode::NONE;
    }
  } else if (!file->IsSyncThreadSafe() &&
             mode != WALBufferingMode::BACKGROUND_APPEND &&
             mode != WALBufferingMode::DELAYED_APPEND) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "rocksdb::WritableFile implementation doesn't support SyncWAL(), and "
        "WAL buffering is disabled by rocksdb-wal-buffering. Overriding "
        "buffering mode to 'background-append' to make SyncWAL() work. "
        "Consider changing the rocksdb-wal-buffering setting to make this "
        "explicit.");
    mode = WALBufferingMode::BACKGROUND_APPEND;
  }

  if (mode == WALBufferingMode::NONE) {
    ld_debug("Opened plain WAL file %s", f.c_str());
    *r =
        std::make_unique<RocksDBWritableFile>(std::move(file), stats_, tracing);
    return rocksdb::Status::OK();
  }

  *r = std::make_unique<RocksDBBufferedWALFile>(
      std::move(file),
      stats_,
      tracing,
      /* defer_writes */ mode == WALBufferingMode::BACKGROUND_APPEND ||
          mode == WALBufferingMode::DELAYED_APPEND,
      /* flush_eagerly */ mode != WALBufferingMode::DELAYED_APPEND,
      settings_->wal_buffer_size);
  ld_debug("Created RocksDBBufferedWALFile for %s", f.c_str());

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
  if (ShardedRocksDBLocalLogStore::parseFilePath(
          path, &shard_idx, &info.filename) &&
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

RocksDBBufferedWALFile::RocksDBBufferedWALFile(
    std::unique_ptr<rocksdb::WritableFile> file,
    StatsHolder* stats,
    FileTracingInfo tracing,
    bool defer_writes,
    bool flush_eagerly,
    size_t buffer_size)
    : RocksDBWritableFile(std::move(file), stats, tracing),
      // If underlying file's sync is not thread safe, we must buffer on our
      // side to make our sync thread safe.
      deferWrites_(defer_writes),
      flushEagerly_(flush_eagerly),
      bufferSize_(buffer_size) {
  if (deferWrites_) {
    // RocksDBEnv ensures this.
    ld_check(!RocksDBWritableFile::use_direct_io());

    writeBuffer_ =
        Buffer(bufferSize_, RocksDBWritableFile::GetRequiredBufferAlignment());
  } else {
    // RocksDBEnv ensures this.
    ld_check(RocksDBWritableFile::IsSyncThreadSafe());
  }

  thread_ = std::thread(std::bind(&RocksDBBufferedWALFile::threadRun, this));
}

RocksDBBufferedWALFile::~RocksDBBufferedWALFile() {
  std::unique_lock lock(mutex_);

  if (!writeBuffer_.empty()) {
    // RocksDB is supposed to Flush() and Close() before calling destructor.
    ld_error(
        "Destroying a WritableFile with nonempty write buffer. Suspicious!");
    requestWriteBufferFlush(lock);
  }

  // Sync()/Fsync() are supposed to wait for completion.
  ld_check(pending_.syncType == SyncType::NONE);

  shutdown_ = true;
  workRequestedCv_.notify_all();
  lock.unlock();
  thread_.join();
}

rocksdb::Status RocksDBBufferedWALFile::Append(const rocksdb::Slice& data) {
  std::unique_lock lock(mutex_);
  size_ += data.size();

  if (!deferWrites_) {
    if (status_.ok()) {
      status_ = RocksDBWritableFile::Append(data);
    }
    return status_;
  }

  const char* ptr = data.data();
  size_t remaining = data.size();
  while (remaining != 0) {
    // If buffer is full, wait for background thread to pick it up for flush
    // and put an empty buffer in its place.
    if (writeBuffer_.full()) {
      requestWriteBufferFlush(lock);
      workCompletedCv_.wait(lock, [&] { return !writeBuffer_.full(); });
    }

    size_t n = writeBuffer_.append(ptr, remaining);
    ld_check(n != 0);
    ptr += n;
    remaining -= n;
  }

  return status_;
}

rocksdb::Status RocksDBBufferedWALFile::Flush() {
  if (!deferWrites_) {
    return RocksDBWritableFile::Flush();
  }

  if (!flushEagerly_) {
    return rocksdb::Status::OK();
  }

  std::unique_lock lock(mutex_);
  requestWriteBufferFlush(lock);

  return status_;
}

rocksdb::Status RocksDBBufferedWALFile::Truncate(uint64_t size) {
  std::unique_lock lock(mutex_);
  flushAndWait(lock);
  if (status_.ok()) {
    status_ = RocksDBWritableFile::Truncate(size);
  }
  return status_;
}

rocksdb::Status RocksDBBufferedWALFile::Close() {
  std::unique_lock lock(mutex_);
  flushAndWait(lock);
  // Call Close() even if previous operations had errors.
  auto s = RocksDBWritableFile::Close();
  // If background thread encountered any errors, this is our last chance to
  // report them.
  return status_.ok() ? s : status_;
}

rocksdb::Status RocksDBBufferedWALFile::Sync() {
  return syncImpl(SyncType::SYNC);
}

rocksdb::Status RocksDBBufferedWALFile::Fsync() {
  return syncImpl(SyncType::FSYNC);
}

rocksdb::Status RocksDBBufferedWALFile::syncImpl(SyncType type) {
  // Deferring Sync() to the background thread has a downside: if underlying
  // file's Sync() is slow enough and our buffer is small enough, Append()s may
  // run out of buffer and block while background thread is busy doing a long
  // Sync(). So we only defer Sync() to background thread if we have to, i.e.
  // if underlying file's Sync() is not thread safe.
  std::unique_lock lock(mutex_);
  if (RocksDBWritableFile::IsSyncThreadSafe()) {
    // Flush buffered appends (if any), then do the sync right here.
    flushAndWait(lock); // flush buffered appends
    if (!status_.ok()) {
      return status_;
    }
    lock.unlock();
    status_ = type == SyncType::SYNC ? RocksDBWritableFile::Sync()
                                     : RocksDBWritableFile::Fsync();
    return status_;
  } else {
    // Request a flush and a sync, then wait for them.

    requestWriteBufferFlush(lock);

    bool had_work = hasWorkPending();

    pending_.syncType = std::max(pending_.syncType, type);

    ld_check(hasWorkPending());
    if (!had_work) {
      workRequestedCv_.notify_all();
    }

    waitForWorkToComplete(lock);

    return status_;
  }
}

bool RocksDBBufferedWALFile::IsSyncThreadSafe() const {
  return true;
}

bool RocksDBBufferedWALFile::use_direct_io() const {
  return deferWrites_ ? false : RocksDBWritableFile::use_direct_io();
}

size_t RocksDBBufferedWALFile::GetRequiredBufferAlignment() const {
  return deferWrites_ ? 1 : RocksDBWritableFile::GetRequiredBufferAlignment();
}

uint64_t RocksDBBufferedWALFile::GetFileSize() {
  return deferWrites_ ? size_.load(std::memory_order_relaxed)
                      : RocksDBWritableFile::GetFileSize();
}

rocksdb::Status RocksDBBufferedWALFile::RangeSync(uint64_t from,
                                                  uint64_t nbytes) {
  std::unique_lock lock(mutex_);

  if (nbytes == 0) {
    return status_;
  }

  uint64_t to = from + nbytes;
  ld_check(to >= from);
  ld_check(to <= size_.load()); // shouldn't sync past end of file

  if (from != pending_.rangeSyncTo) {
    // We expect the ranges passed to RangeSync() to be back to back: each
    // range starting exactly where the previous one ended, and first range
    // starting at zero. This allows us to easily merge multiple calls into one
    // if background thread is falling behind.
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "rocksdb::WritableFile::RangeSync() called for nonconsecutive ranges "
        "([%lu, %lu] -> [%lu, %lu]). Something changed in rocksdb "
        "implementation around RangeSync() usage. "
        "Please check if RocksDBBufferedWALFile::RangeSync() behavior still "
        "makes "
        "sense.",
        pending_.rangeSyncFrom,
        pending_.rangeSyncTo,
        from,
        to);

    if (to <= pending_.rangeSyncTo) {
      return status_;
    }
  }

  if (flushEagerly_) {
    // Make sure the bytes to be range synced are flushed.
    requestWriteBufferFlush(lock);
  } else {
    // We might request a range sync for a range that hasn't yet been flushed
    // or even requested for flushing. The background thread will truncate the
    // range. The precise ranges of range syncs are not important in general,
    // and even less important in situations when we would want to use
    // flushEagerly_ = false.
  }

  bool had_work = hasWorkPending();

  pending_.rangeSyncTo = to;

  if (hasWorkPending() && !had_work) {
    workRequestedCv_.notify_all();
  }

  return status_;
}

void RocksDBBufferedWALFile::requestWriteBufferFlush(
    std::unique_lock<std::mutex>& /* caller_has_locked_the_mutex */) {
  if (writeBuffer_.empty()) {
    return;
  }

  bool had_work = hasWorkPending();

  pending_.flushWriteBuffer = true;

  ld_check(hasWorkPending());
  if (!had_work) {
    workRequestedCv_.notify_all();
  }
}

void RocksDBBufferedWALFile::waitForWorkToComplete(
    std::unique_lock<std::mutex>& lock) {
  workCompletedCv_.wait(
      lock, [&] { return !hasWorkPending() && !workInProgress_; });
}

void RocksDBBufferedWALFile::threadRun() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:wal");

  std::unique_lock lock(mutex_);

  // Buffer holding data we're currently appending to the underlying file.
  // Swapped back and forth with writeBuffer_.
  Buffer second_buffer;
  if (deferWrites_) {
    second_buffer =
        Buffer(bufferSize_, RocksDBWritableFile::GetRequiredBufferAlignment());
  }

  // How much we Append()ed to the underlying file.
  uint64_t bytes_flushed = 0;

  // How far we actually range-synced, as opposed to pending_.rangeSyncFrom,
  // which is how far we promised to sync.
  uint64_t range_actually_synced_to = 0;

  while (true) {
    workRequestedCv_.wait(lock, [&] { return hasWorkPending() || shutdown_; });

    // Finish all work even if shutdown_ is true.
    if (!hasWorkPending()) {
      ld_check(shutdown_);
      break;
    }

    // Move all information about pending operations into local variables,
    // then unlock mutex and do the operations.
    second_buffer.clear();
    if (pending_.flushWriteBuffer) {
      std::swap(writeBuffer_, second_buffer);
      pending_.flushWriteBuffer = false;
    }
    SyncType sync = pending_.syncType;
    pending_.syncType = SyncType::NONE;
    size_t range_sync_to = pending_.rangeSyncTo;
    pending_.rangeSyncFrom = pending_.rangeSyncTo;

    ld_check(!hasWorkPending());

    rocksdb::Status s = status_;
    workInProgress_ = true;

    lock.unlock();

    // Append.
    if (!second_buffer.empty()) {
      if (s.ok()) {
        s = RocksDBWritableFile::Append(
            rocksdb::Slice(second_buffer.data(), second_buffer.size()));
        if (!s.ok()) {
          ld_error("Append() failed: %s", s.ToString().c_str());
        }
      }

      if (s.ok()) {
        s = RocksDBWritableFile::Flush();
        if (!s.ok()) {
          ld_error("Flush() failed: %s", s.ToString().c_str());
        }
      }

      if (s.ok()) {
        bytes_flushed += second_buffer.size();
      }
    }

    // Sync/fsync.
    if (s.ok() && sync != SyncType::NONE) {
      s = sync == SyncType::SYNC ? RocksDBWritableFile::Sync()
                                 : RocksDBWritableFile::Fsync();
      if (!s.ok()) {
        ld_error("%s failed: %s",
                 sync == SyncType::SYNC ? "Sync" : "Fsync",
                 s.ToString().c_str());
      }
    }

    // Range sync.
    if (deferWrites_) {
      // If we're asked to sync something we haven't flushed yet, sync only what
      // we flushed for now, but sync the missing part later. This messes up
      // alignment: sync_file_range() rounds the range to page boundaries, so
      // we may end up requesting sync for the same page twice, but that should
      // be negligible because usually the ranges are big compared to pages.
      range_sync_to = std::min(range_sync_to, bytes_flushed);
    }
    if (s.ok() && range_sync_to > range_actually_synced_to) {
      s = RocksDBWritableFile::RangeSync(
          range_actually_synced_to, range_sync_to - range_actually_synced_to);
      if (!s.ok()) {
        ld_error("RangeSync() failed: %s", s.ToString().c_str());
      } else {
        range_actually_synced_to = range_sync_to;
      }
    }

    lock.lock();

    if (status_.ok()) {
      status_ = s;
    }
    workInProgress_ = false;
    workCompletedCv_.notify_all();
  }
}

}} // namespace facebook::logdevice
