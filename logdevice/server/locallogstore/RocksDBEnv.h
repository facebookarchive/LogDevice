/**
 * Copyright (c) 2017-present, Facebook, Inc.
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

#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBSettings.h"

#if ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 4)
#define LOGDEVICED_ROCKSDB_UNSCHED_FUNCTION
#endif

#if ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 3)
#define LOGDEVICED_ROCKSDB_RANGE_SYNC_NEW_TYPES
#endif

namespace facebook { namespace logdevice {

/**
 * A thin wrapper around default rocksdb::Env. Lowers IO priority of low-pri
 * background threads. The current rocksdb's implementation of
 * rocksdb::Env::LowerThreadPoolIOPriority() does pretty much the same but
 * only allows lowering it to IOPRIO_CLASS_IDLE which can be too low.
 */
class RocksDBEnv : public rocksdb::EnvWrapper {
 public:
  explicit RocksDBEnv(UpdateableSettings<RocksDBSettings> settings)
      : rocksdb::EnvWrapper(rocksdb::Env::Default()),
        settings_(std::move(settings)) {}

#ifdef LOGDEVICED_ROCKSDB_UNSCHED_FUNCTION
  void Schedule(void (*function)(void* arg),
                void* arg,
                Priority pri = LOW,
                void* tag = nullptr,
                void (*unschedFunction)(void* arg) = 0) override;
#else
  void Schedule(void (*function)(void* arg),
                void* arg,
                Priority pri = LOW,
                void* tag = nullptr) override;
#endif

  // Instantiate RocksDBRandomAccessFile objects (a RandomAccessFile
  // wrapper) so we can track RocksDB read operations.
  rocksdb::Status
  NewRandomAccessFile(const std::string& f,
                      std::unique_ptr<rocksdb::RandomAccessFile>* r,
                      const rocksdb::EnvOptions& options) override;

  // Instantiate RocksDBWritableFile or RocksDBBackgroundSyncFile objects
  // (WritableFile wrappers) so we can track writes and prevent sync
  // operations on WAL files from blocking the current thread.
  rocksdb::Status NewWritableFile(const std::string& f,
                                  std::unique_ptr<rocksdb::WritableFile>* r,
                                  const rocksdb::EnvOptions& options) override;

  rocksdb::Status DeleteFile(const std::string& fname) override;

 private:
  struct Callback {
    typedef void (*function_t)(void*);
    function_t function;
    void* arg;
    std::pair<int, int> prio;
    function_t unschedule_function;

    bool operator<(const Callback& rhs) const {
      if (function != rhs.function) {
        return std::less<function_t>()(function, rhs.function);
      }
      if (unschedule_function != rhs.unschedule_function) {
        return std::less<function_t>()(
            unschedule_function, rhs.unschedule_function);
      }
      if (arg != rhs.arg) {
        return std::less<void*>()(arg, rhs.arg);
      }
      return prio < rhs.prio;
    }

    void call() const;
    void unschedule() const;
  };

  // Exploit the fact that there are few different (function, arg) pairs that
  // Schedule() is called for. Just keep a set of all encountered callbacks.
  // It is safe to destroy this set before destroying the base Env because Env
  // outlives all rocksdb::DB instances, so no tasks are scheduled when
  // this object is destroyed.
  std::set<Callback> callbacks_;
  std::mutex mutex_;

  UpdateableSettings<RocksDBSettings> settings_;

  static void callback(void* arg);
  static void callback_unschedule(void* arg);

  void BaseSchedule(void (*function)(void* arg),
                    void* arg,
                    Priority pri,
                    void* tag,
                    void (*unschedFunction)(void* arg));
};

// Complement to WritableFileWrapper.
// Written in the style of RocksDB for eventual upstreaming.
//
class RocksDBRandomAccessFileWrapper : public rocksdb::RandomAccessFile {
 public:
  explicit RocksDBRandomAccessFileWrapper(RandomAccessFile* t) : target_(t) {}

  rocksdb::Status Read(uint64_t offset,
                       size_t n,
                       rocksdb::Slice* result,
                       char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }

// ShouldForwardRawRequest() and EnableReadAhead() were removed in 5.5
#if ROCKSDB_MAJOR < 5 || (ROCKSDB_MAJOR == 5 && ROCKSDB_MINOR <= 4)
  bool ShouldForwardRawRequest() const override {
    return target_->ShouldForwardRawRequest();
  }
// EnableReadAhead() was added in 4.4.2
#if ROCKSDB_MAJOR > 4 || (ROCKSDB_MAJOR == 4 && ROCKSDB_MINOR >= 2)
  void EnableReadAhead() override {
    target_->EnableReadAhead();
  }
#endif
#endif

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override {
    return target_->Hint(pattern);
  }
  rocksdb::Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

 private:
  RandomAccessFile* target_;
};

class RocksDBWritableFile : public rocksdb::WritableFileWrapper {
 public:
  explicit RocksDBWritableFile(std::unique_ptr<rocksdb::WritableFile> file)
      : rocksdb::WritableFileWrapper(file.get()),
        file_(std::move(std::move(file))) {}

 protected:
  std::unique_ptr<rocksdb::WritableFile> file_;
};

// Wrapper for tracing.
class RocksDBRandomAccessFile : public RocksDBRandomAccessFileWrapper {
 public:
  // A scoped class used to time read operations and then pass information
  // about the read to LocalLogStore::ReadIterator's that have tracing
  // enabled.
  class RocksDBReadTracer {
   public:
    RocksDBReadTracer(const RocksDBRandomAccessFile* f,
                      uint64_t offset,
                      size_t n);
    ~RocksDBReadTracer();

   private:
    const RocksDBRandomAccessFile* file_;
    const uint64_t offset_;
    const size_t length_;
    const std::chrono::steady_clock::time_point start_time_;
  };

  RocksDBRandomAccessFile(const std::string& f,
                          std::unique_ptr<rocksdb::RandomAccessFile> file);

  rocksdb::Status Read(uint64_t offset,
                       size_t n,
                       rocksdb::Slice* result,
                       char* scratch) const override;

  std::unique_ptr<rocksdb::RandomAccessFile> file;
  const std::string file_name;
  mutable uint64_t file_offset;
  FileNameHash file_name_hash;
};

// A wrapper around rocksdb::WritableFile intended for WAL files.
// Overrides RangeSync() to not do the sync but instead schedule it on a
// background thread.
class RocksDBBackgroundSyncFile : public RocksDBWritableFile {
 public:
  explicit RocksDBBackgroundSyncFile(
      std::unique_ptr<rocksdb::WritableFile> file)
      : RocksDBWritableFile(std::move(file)) {
    thread_ =
        std::thread(std::bind(&RocksDBBackgroundSyncFile::ThreadRun, this));
  }

#ifdef LOGDEVICED_ROCKSDB_RANGE_SYNC_NEW_TYPES
  rocksdb::Status RangeSync(uint64_t offset, uint64_t nbytes) override;
#else
  rocksdb::Status RangeSync(off_t offset, off_t nbytes) override;
#endif

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
