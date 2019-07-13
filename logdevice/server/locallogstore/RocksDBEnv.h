/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <set>
#include <thread>

#include <rocksdb/db.h>
#include <rocksdb/env.h>

#include "logdevice/server/locallogstore/IOTracing.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

#if ROCKSDB_MAJOR > 6 || (ROCKSDB_MAJOR == 6 && ROCKSDB_MINOR >= 1)
#define LOGDEVICED_ROCKSDB_HAS_WRAPPERS
#endif

namespace facebook { namespace logdevice {

class StatsHolder;

struct FileTracingInfo {
  IOTracing* io_tracing = nullptr;
  std::string filename;
};

/**
 * A wrapper around default rocksdb::Env. Does a few things:
 *  - Lowers IO priority of low-pri background threads (if enabled in settings).
 *    The current rocksdb's implementation of
 *    rocksdb::Env::LowerThreadPoolIOPriority() does pretty much the same but
 *    only allows lowering it to IOPRIO_CLASS_IDLE which can be too low.
 *  - For WAL files, defers sync_file_range calls to a background thread.
 *  - IO tracing, see IOTracing.h
 *  - Some stats and logging.
 */
class RocksDBEnv : public rocksdb::EnvWrapper {
 public:
  explicit RocksDBEnv(UpdateableSettings<RocksDBSettings> settings,
                      StatsHolder* stats,
                      std::vector<IOTracing*> io_tracing_by_shard)
      : rocksdb::EnvWrapper(rocksdb::Env::Default()),
        settings_(std::move(settings)),
        stats_(stats),
        io_tracing_by_shard_(io_tracing_by_shard) {}

  // Returns pointer to a thread-local AddContext if we're inside a job running
  // on rocksdb background thread, nullptr otherwise.
  //
  // This is part of a somewhat arcane mechanism for propagating information
  // about a running flush/compaction job to the IO operations done by that job.
  // We want to create an IOTracing::AddContext at the beginning of a
  // flush/compaction and destroyed at the end. The obvious candidates are:
  //  (1) Use RocksDBListener: create AddContext in OnFlushBegin(),
  //      destroy in OnFlushCompleted(). But unfortunately rocksdb sometimes
  //      doesn't call OnFlushCompleted() after calling OnFlushBegin().
  //      E.g. if the column family was dropped during the flush.
  //      So the AddContext would linger and potentially
  //       (a) misattribute other jobs' IO to the completed flush,
  //       (b) access freed IOContext if the lingering AddContext happens to
  //           outlive the DB itself.
  //  (2) Use background job wrapper (see Schedule() and bgJobRun()):
  //      create AddContext when job starts, destroy when it ends.
  //      But that's not very useful because the job wrapper doesn't know
  //      anything about the job itself, it only has a function pointer.
  // So we combine the two approaches: RocksDBListener's
  // OnFlushBegin()/OnCompactionBegin() assigns a thread-local AddContext,
  // then RocksDBEnv's job wrapper clears it.
  IOTracing::AddContext* backgroundJobContextOfThisThread();

  // We wrap all background jobs to:
  //  (a) set information in ThreadID needed to identify rocksdb bg threads,
  //  (b) change io priority if rocksdb-low-ioprio setting is set,
  //  (c) help RocksDBListener reliably clear IOTracing context at the end of
  //      job execution.
  void Schedule(void (*function)(void* arg),
                void* arg,
                Priority pri = LOW,
                void* tag = nullptr,
                void (*unschedFunction)(void* arg) = 0) override;

  // Wrap most of the IO-related stuff to do IO tracing.
  //
  // Many of these methods are either unused in our workload, or used rarely,
  // or used only during startup. We don't really care about such operations,
  // but we wrap most of them anyway, in case we're wrong and they turn out to
  // actually do lots of IO somehow.
  //
  // Here's the list methods that are intentionally _not_ wrapped, on the
  // assumption that they're not used often (as of April 2019):
  // +---------------------------+--------------------------------------+
  // |       method              |      who uses it                     |
  // +---------------------------+--------------------------------------+
  // | NewMemoryMappedFileBuffer | db_stress                            |
  // | NewLogger                 | writes to LOG file                   |
  // | NewRandomRWFile           | external file ingestion              |
  // | CreateDir                 | in logdevice, nobody                 |
  // | CreateDirIfMissing        | in logdevice, nobody                 |
  // | DeleteDir                 | in logdevice, nobody                 |
  // | LinkFile                  | external file ingestion, checkpoints |
  // | NumFileLinks              | deletion ratelimiter                 |
  // | AreFilesSame              | DB repair                            |
  // | LockFile                  | DB open                              |
  // | UnlockFile                | DB close                             |
  // +---------------------------+--------------------------------------+

  rocksdb::Status NewWritableFile(const std::string& f,
                                  std::unique_ptr<rocksdb::WritableFile>* r,
                                  const rocksdb::EnvOptions& options) override;
#ifdef LOGDEVICED_ROCKSDB_HAS_WRAPPERS
  rocksdb::Status
  NewSequentialFile(const std::string& fname,
                    std::unique_ptr<rocksdb::SequentialFile>* result,
                    const rocksdb::EnvOptions& options) override;
  rocksdb::Status
  NewRandomAccessFile(const std::string& f,
                      std::unique_ptr<rocksdb::RandomAccessFile>* r,
                      const rocksdb::EnvOptions& options) override;
  rocksdb::Status
  NewDirectory(const std::string& name,
               std::unique_ptr<rocksdb::Directory>* result) override;
  rocksdb::Status
  ReopenWritableFile(const std::string& fname,
                     std::unique_ptr<rocksdb::WritableFile>* result,
                     const rocksdb::EnvOptions& options) override;
  rocksdb::Status
  ReuseWritableFile(const std::string& fname,
                    const std::string& old_fname,
                    std::unique_ptr<rocksdb::WritableFile>* result,
                    const rocksdb::EnvOptions& options) override;
  rocksdb::Status FileExists(const std::string& fname) override;
  rocksdb::Status GetChildren(const std::string& dir,
                              std::vector<std::string>* result) override;
  rocksdb::Status
  GetChildrenFileAttributes(const std::string& dir,
                            std::vector<FileAttributes>* result) override;
  rocksdb::Status DeleteFile(const std::string& fname) override;
  rocksdb::Status Truncate(const std::string& f, size_t size) override;
  rocksdb::Status GetFileSize(const std::string& fname,
                              uint64_t* file_size) override;
  rocksdb::Status GetFileModificationTime(const std::string& fname,
                                          uint64_t* file_mtime) override;
  rocksdb::Status RenameFile(const std::string& src,
                             const std::string& target) override;
#endif

 private:
  struct ScheduledJob {
    void (*function)(void*);
    void (*unschedFunction)(void* arg);
    void* arg;
    Priority pri;
    folly::Optional<std::pair<int, int>> ioprio_to_set;
    RocksDBEnv* env;
  };

  struct BGThreadState {
    bool initialized = false;
    bool running_a_job = false;
    IOTracing::AddContext io_tracing_context;
  };

  UpdateableSettings<RocksDBSettings> settings_;
  StatsHolder* stats_;
  std::vector<IOTracing*> io_tracing_by_shard_;

  // Description of currently running job in each rocksdb background thread.
  // Used for IO tracing.
  folly::ThreadLocal<BGThreadState> bg_threads_;

  // We asign distinct names to rocksdb background threads using this counter,
  // separate for each type of thread.
  std::array<std::atomic<int>, Priority::TOTAL> next_bg_thread_id_{};

  static void bgJobRun(void* arg);
  static void bgJobUnschedule(void* arg);

  void BaseSchedule(void (*function)(void* arg),
                    void* arg,
                    Priority pri,
                    void* tag,
                    void (*unschedFunction)(void* arg));

  FileTracingInfo tracingInfoForPath(const std::string& path);

  enum class WritableFileOp {
    NEW,
    REOPEN,
    REUSE,
  };
  // Implementation of NewWritableFile(), ReopenWritableFile() and
  // ReuseWritableFile().
  rocksdb::Status WritableFileOpImpl(WritableFileOp op,
                                     const char* op_name,
                                     const std::string& f,
                                     const std::string& old_fname,
                                     std::unique_ptr<rocksdb::WritableFile>* r,
                                     const rocksdb::EnvOptions& options);
};

class RocksDBWritableFile : public rocksdb::WritableFileWrapper {
 public:
  RocksDBWritableFile(std::unique_ptr<rocksdb::WritableFile> file,
                      StatsHolder* stats,
                      FileTracingInfo tracing);
  ~RocksDBWritableFile() override;

  rocksdb::Status Append(const rocksdb::Slice& data) override;
  rocksdb::Status PositionedAppend(const rocksdb::Slice& data,
                                   uint64_t offset) override;
  rocksdb::Status Truncate(uint64_t size) override;
  rocksdb::Status Close() override;
  rocksdb::Status Flush() override;
  rocksdb::Status Sync() override;
  rocksdb::Status Fsync() override;
  size_t GetUniqueId(char* id, size_t max_size) const override;
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override;
  rocksdb::Status RangeSync(uint64_t offset, uint64_t nbytes) override;
  rocksdb::Status Allocate(uint64_t offset, uint64_t len) override;

  // Not tracing PrepareWrite() because it's called a lot, and vast majority
  // of the calls don't do any IO. So we effectively don't trace fallocate()
  // calls, which is too bad.
  // The right way to solve it would be to remove PrepareWrite() from
  // rocksdb::WritableFile and move it to rocksdb::WritableFileWriter.
  // Then instead of calling PrepareWrite() all the time, WritableFileWriter
  // will call Allocate() occasionally, which will get traced.

 protected:
  std::unique_ptr<rocksdb::WritableFile> file_;
  StatsHolder* stats_;
  FileTracingInfo tracing_;
  bool closed_ = false;
};

#ifdef LOGDEVICED_ROCKSDB_HAS_WRAPPERS
// Wrappers for tracing.

class RocksDBSequentialFile : public rocksdb::SequentialFileWrapper {
 public:
  RocksDBSequentialFile(std::unique_ptr<rocksdb::SequentialFile> file,
                        FileTracingInfo tracing);
  ~RocksDBSequentialFile() override;
  rocksdb::Status Read(size_t n,
                       rocksdb::Slice* result,
                       char* scratch) override;
  rocksdb::Status Skip(uint64_t n) override;
  rocksdb::Status PositionedRead(uint64_t offset,
                                 size_t n,
                                 rocksdb::Slice* result,
                                 char* scratch) override;

 private:
  std::unique_ptr<rocksdb::SequentialFile> file_;
  FileTracingInfo tracing_;
};

class RocksDBRandomAccessFile : public rocksdb::RandomAccessFileWrapper {
 public:
  RocksDBRandomAccessFile(std::unique_ptr<rocksdb::RandomAccessFile> file,
                          FileTracingInfo tracing);
  ~RocksDBRandomAccessFile() override;

  rocksdb::Status Read(uint64_t offset,
                       size_t n,
                       rocksdb::Slice* result,
                       char* scratch) const override;
  rocksdb::Status Prefetch(uint64_t offset, size_t n) override;
  size_t GetUniqueId(char* id, size_t max_size) const override;
  void Hint(AccessPattern pattern) override;
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override;

 private:
  std::unique_ptr<rocksdb::RandomAccessFile> file_;
  FileTracingInfo tracing_;
};

class RocksDBDirectory : public rocksdb::DirectoryWrapper {
 public:
  RocksDBDirectory(std::unique_ptr<rocksdb::Directory> dir,
                   FileTracingInfo tracing);
  ~RocksDBDirectory() override;

  rocksdb::Status Fsync() override;
  size_t GetUniqueId(char* id, size_t max_size) const override;

 private:
  std::unique_ptr<rocksdb::Directory> dir_;
  FileTracingInfo tracing_;
};

#endif // LOGDEVICED_ROCKSDB_HAS_WRAPPERS

// A wrapper around rocksdb::WritableFile intended for WAL files.
// Overrides RangeSync() to not do the sync but instead schedule it on a
// background thread.
class RocksDBBackgroundSyncFile : public RocksDBWritableFile {
 public:
  explicit RocksDBBackgroundSyncFile(
      std::unique_ptr<rocksdb::WritableFile> file,
      StatsHolder* stats,
      FileTracingInfo tracing)
      : RocksDBWritableFile(std::move(file), stats, tracing) {
    thread_ =
        std::thread(std::bind(&RocksDBBackgroundSyncFile::ThreadRun, this));
  }

  rocksdb::Status RangeSync(uint64_t offset, uint64_t nbytes) override;

  rocksdb::Status Close() override;
  ~RocksDBBackgroundSyncFile() override;

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool shutting_down_ = false;
  std::thread thread_;

  // Byte range that needs background sync.
  off_t to_sync_begin_{0};
  std::atomic<off_t> to_sync_end_{0};

  void ThreadRun();
  void JoinThread();
};

}} // namespace facebook::logdevice
