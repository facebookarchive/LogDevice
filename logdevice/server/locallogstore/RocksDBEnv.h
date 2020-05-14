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

#include <folly/Memory.h>
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
  explicit RocksDBEnv(rocksdb::Env* underlying_env,
                      UpdateableSettings<RocksDBSettings> settings,
                      StatsHolder* stats,
                      std::vector<IOTracing*> io_tracing_by_shard)
      : rocksdb::EnvWrapper(underlying_env),
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
  // Subclasses should avoid calling file_'s methods directly because that
  // bypasses RocksDBWritableFile's instrumentation.
  // Call RocksDBWritableFile's methods instead.
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
                          FileTracingInfo tracing,
                          UpdateableSettings<RocksDBSettings> settings);
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
  UpdateableSettings<RocksDBSettings> settings_;
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
// Offloads some operations to a background thread to:
//  - avoid IO on write path,
//  - batch writes better,
//  - support thread-safe Sync()/Fsync() even if underlying WritableFile
//    implementation doesn't; this is required for rocksdb::DB::SyncWAL() API to
//    work, which logdevice really needs for correctness.
// This sacrifices some durability: since Flush() doesn't wait for the bytes
// to be passed to the operating system, a process crash may result in losing
// acknowledged writes. Sync()/Fsync() still wait for the flush+sync, so synced
// writes are still durable, and that's all that really matters for logdevice.
class RocksDBBufferedWALFile : public RocksDBWritableFile {
 public:
  RocksDBBufferedWALFile(std::unique_ptr<rocksdb::WritableFile> file,
                         StatsHolder* stats,
                         FileTracingInfo tracing,
                         bool defer_writes,
                         bool flush_eagerly,
                         size_t buffer_size);
  ~RocksDBBufferedWALFile() override;

  rocksdb::Status Append(const rocksdb::Slice& data) override;
  rocksdb::Status Truncate(uint64_t /*size*/) override;
  rocksdb::Status Close() override;
  rocksdb::Status Flush() override;
  rocksdb::Status Sync() override;
  rocksdb::Status Fsync() override;
  bool IsSyncThreadSafe() const override;
  bool use_direct_io() const override;
  size_t GetRequiredBufferAlignment() const override;
  uint64_t GetFileSize() override;
  rocksdb::Status RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/) override;

 private:
  // How this class is used by rocksdb:
  //  - On each write there's a few Append() calls followed by a Flush() call,
  //    and sometimes also a RangeSync() call.
  //    These calls happen single-threadedly (rocksdb holds a mutex).
  //  - When a WAL sync is requested, rocksdb will call either Sync() or Fsync()
  //    on this file. This call may overlap with Append()/Flush()/RangeSync()
  //    calls, but not with any other calls on this file and not with other
  //    Sync()/Fsync() calls.
  //  - When rocksdb is done with this WAL file, it'll wait for all outstanding
  //    Append()/Flush()/RangeSync()/Sync()/Fsync() calls to return, then will
  //    maybe do Truncate(), then Close().
  //  - The RangeSync() calls' byte ranges are back to back: the next call's
  //    start offset is previous call's end offset, and first call's start
  //    offset is zero.
  //
  // Note that concurrency is very limited, which makes RocksDBBufferedWALFile's
  // task much easier.
  //
  // If IsSyncThreadSafe() is false, Sync()/Fsync() calls don't overlap with
  // Append()/Flush()/RangeSync() calls, so *all* usage of WritableFile is
  // single-threaded. We'd like RocksDBBufferedWALFile to have
  // IsSyncThreadSafe() = true even if the underlying WritableFile has
  // IsSyncThreadSafe() = false.
  //
  // In a normal WritableFile implementation, Append() appends to a buffer,
  // Flush() writes that buffer to the file, RangeSync() calls Linux's
  // sync_file_range() (on other platforms RangeSync() may be a no-op),
  // and Sync()/Fsync() call POSIX's sync()/fsync().
  //
  // In RocksDBBufferedWALFile:
  //  - Append() appends to a buffer; if buffer got big enough, asks background
  //    thread to flush it; if there's already another flush in progress, waits
  //    for that flush - it's better to block Append() than to let buffer
  //    grow huge,
  //  - Flush() does nothing,
  //  - RangeSync() asks background thread to do the range sync,
  //  - Sync()/Fsync() asks background thread to do the sync/fsync and waits
  //    for background thread to do it,
  //  - Background thread does the requested flushes/syncs in a loop. If
  //    multiple writes or range syncs were requested while the previous flush
  //    was happening, all these new operations will be merged together into one
  //    write and range sync. Additionally, it may intentionally wait for some
  //    time before doing the operation, to let more writes accumulate and be
  //    batched together; this is particularly useful on remote storage.
  //
  // Note that buffer may remain unflushed indefinitely if writes and syncs
  // stop. This is currently ok for logdevice because we do SyncWAL() after all
  // writes that use WAL and need any durability guarantees.

  class Buffer {
   public:
    Buffer() = default; // invalid Buffer, can only be assigned to
    Buffer(size_t capacity, size_t alignment) {
      ld_check_gt(alignment, 0);
      ld_check((alignment & (alignment - 1)) == 0);
      alignment = std::max(alignment, sizeof(void*));

      void* ptr = folly::aligned_malloc(capacity, alignment);
      if (ptr == nullptr) {
        ld_critical("folly::aligned_malloc (aka posix_memalign()) failed: %s",
                    strerror(errno));
        std::abort();
      }

      data_ = reinterpret_cast<char*>(ptr);
      capacity_ = capacity;
    }
    ~Buffer() {
      folly::aligned_free(data_);
    }

    // Movable, so that we can move two buffers back and forth between
    // background and foreground threads.
    Buffer(Buffer&& rhs) noexcept {
      *this = std::move(rhs);
    }
    Buffer& operator=(Buffer&& rhs) {
      // This move operator should only be used in std::swap(), and std::swap()
      // should only move into a moved-out Buffer. Otherwise we'll have
      // unnecessary memory reallocations, so let's assert that doesn't happen.
      ld_check(data_ == nullptr && capacity_ == 0 && size_ == 0);

      std::swap(data_, rhs.data_);
      std::swap(capacity_, rhs.capacity_);
      std::swap(size_, rhs.size_);

      return *this;
    }

    size_t capacity() const {
      return capacity_;
    }
    size_t size() const {
      return size_;
    }
    bool empty() const {
      return size_ == 0;
    }
    bool full() const {
      ld_check(data_);
      return size_ == capacity_;
    }
    const char* data() const {
      ld_check(data_);
      return data_;
    }

    size_t append(const char* p, size_t s) {
      ld_check(data_);
      ld_check(p != nullptr);
      size_t n = std::min(s, capacity_ - size_);
      memcpy(data_ + size_, p, n);
      size_ += n;
      return n;
    }

    void clear() {
      size_ = 0;
    }

   private:
    char* data_ = nullptr;
    size_t capacity_ = 0;

    size_t size_ = 0;
  };

  // If true, Append()/Flush() on the underlying file will happen from
  // background thread, and our Append()/Flush() won't wait for the writes
  // to complete. This removes rocksdb's guarantee that WAL-enabled write
  // returns only after it's appended to WAL file - now the file append can be
  // still in RocksDBBufferedWALFile's buffer when rocksdb acks the write.
  const bool deferWrites_;

  // If true, our Flush() will initiate a background flush of the buffer (but
  // won't wait for it). If false, our Flush() does nothing.
  const bool flushEagerly_;

  // Size of each buffer. We currently use two such buffers, and they are
  // currently all allocated to full size during construction.
  const size_t bufferSize_;

  // Protects all fields. Unlocked during background IO operations.
  std::mutex mutex_;

  // Foreground thread notifies this cv when a flush or a sync is needed.
  // Specifically, after hasWorkPending() or shutdown_ changes from
  // false to true.
  std::condition_variable workRequestedCv_;

  // Background thread notifies this cv when a flush or a sync is completed.
  // Specifically, after workInProgress_ changes from true to false.
  std::condition_variable workCompletedCv_;

  // Background thread.
  std::thread thread_;

  enum class SyncType {
    NONE = 0,
    SYNC = 1,
    FSYNC = 2,
  };

  // Bytes appended but not yet picked up for flushing by background thread.
  // Used only if deferWrites_ is true; otherwise, appends happen directly
  // from our Append().
  Buffer writeBuffer_;

  // Things that background thread should do on next iteration.
  // Doesn't contain things that background thread is doing right now.
  struct {
    // If true, call Append()+Flush() on the underlying file for the
    // contents of writeBuffer_.
    // Note that it may be false even if writeBuffer_ is nonempty, if we chose
    // to not delay the flush until more bytes accumulate in the buffer.
    // Note also that writeBuffer_ may receive more appends between the moment
    // this flag is set to true and the moment background thread actually picks
    // up the buffer for the flush; this is important: it allows foreground
    // thread to do multiple appends to the buffer without stalling if a
    // background append/sync is taking a long time.
    bool flushWriteBuffer = false;

    // If not NONE, call Sync()/Fsync() on the underlying file. Used only if
    // underlying file's IsSyncThreadSafe() is false; otherwise syncs happen
    // directly from our Sync()/Fsync(), see comment there for explanation.
    SyncType syncType = SyncType::NONE;

    // If these two are not equal, call RangeSync() on the underlying file.
    // If flushEagerly_ is false, we don't take range syncs very seriously,
    // and this range may contain bytes not yet scheduled for flush; in this
    // case, background thread will truncate the range.
    size_t rangeSyncFrom = 0;
    size_t rangeSyncTo = 0;
  } pending_;

  // True if the background thread is doing a flush or a sync now.
  bool workInProgress_ = false;

  // If true, background thread should finish up pending work and quit.
  bool shutdown_ = false;

  // Total bytes we got in Append() calls, including bytes we haven't yet
  // flushed.
  std::atomic<size_t> size_ = 0;

  // If a background operation returns an error, we'll store it here and
  // return it from some foreground operation later. We'll also avoid doing
  // any more operations (especially appends) on the underlying file if any
  // previous operations returned an error.
  rocksdb::Status status_ = rocksdb::Status::OK();

  // mutex_ must be held.
  bool hasWorkPending() const {
    return pending_.flushWriteBuffer || pending_.syncType != SyncType::NONE ||
        pending_.rangeSyncTo > pending_.rangeSyncFrom;
  }

  // Schedules writeBuffer_'s contents to be flushed.
  // If writeBuffer_ is empty, does nothing.
  // The `lock` parameter is just to make it harder to accidentally call this
  // method without locking mutex_; it's not actually used.
  void requestWriteBufferFlush(std::unique_lock<std::mutex>& lock);

  // Waits until workInProgress_ = hasWorkPending() = false.
  void waitForWorkToComplete(std::unique_lock<std::mutex>& lock);

  void flushAndWait(std::unique_lock<std::mutex>& lock) {
    requestWriteBufferFlush(lock);
    waitForWorkToComplete(lock);
  }

  // Implementation of Sync() and Fsync().
  rocksdb::Status syncImpl(SyncType type);

  void threadRun();
};

}} // namespace facebook::logdevice
