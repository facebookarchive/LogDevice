/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Format.h>
#include <folly/Preprocessor.h>
#include <folly/SpinLock.h>
#include <folly/Synchronized.h>
#include <folly/ThreadLocal.h>
#include <folly/Utility.h>
#include <folly/lang/Aligned.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"

/**
 * @file
 * A facility for logging IO operations, along with some context.
 * The mechanism is not really specific to IO, we're just using it this way.
 *
 * Example output:
 *   [io:S17] flush|cf:1002067|wf:367120.sst|PositionedAppend|
 *   off:9437184|sz:1048576|  70.732ms
 * (it's one line, line break added to fit in 80-character limit)
 *
 * In this example:
 *  - "S17" is shard idx, from IOTracing object
 *  - "flush|cf:1002067|wf:367120.sst|PositionedAppend|off:9437184|sz:1048576|"
 *    is the context string `IOTracing::state_.context`, populated from
 *    multiple places in the call stack:
 *    - "flush|cf:1002067" means we're doing a memtable flush in
 *       column family "1002067"
 *    - "wf:367120.sst|PositionedAppend|off:9437184|sz:1048576"
 *      means we're in WritableFile::PositionedAppend() for file "367120.sst",
 *      writing 1048576 bytes at offset 9437184.
 *  - 70.732ms is how long the IO operation took.
 *
 * To add context, use macro SCOPED_IO_TRACING_CONTEXT() around calls that may
 * do rocksdb IO. This can be done anywhere in the code.
 *
 * To time and report an IO operation, use macro SCOPED_IO_TRACED_OP();
 * this is done in RocksDBEnv.cpp - very close to the actual syscalls for IO.
 *
 * The thread-local context is currently just a free-form std::string to which
 * everyone appends. It could instead be more structured, e.g. describe the
 * reason for iops with an enum (e.g. write path, read path, rebuilding etc)
 * and aggregate some stats per enum value. We don't do it for now, to keep
 * adding context easy, but it may make sense in future, when we have a better
 * idea of what sort of aggregated stats would be useful to expose.
 *
 * Performance-wise, appending to thread-local std::string should be pretty
 * cheap because it doesn't do memory allocations, apart from the initial
 * growth that happens O(1) times per thread's lifetime; and because
 * folly::format() appends directly to the string, without allocating temporary
 * buffers.
 */

namespace facebook { namespace logdevice {

class IOTracing {
 private:
  // Thread-local information about a thread that may do IO.
  struct ThreadState {
    // Description of the current IO operation and its context, formed by
    // concatenating strings provided to all active
    // SCOPED_IO_TRACING_CONTEXT and SCOPED_IO_TRACED_OP invocations.
    std::string context;

    // If this thread is currently performing an IO operation, this is the
    // timestamp when the operation began. Otherwise time_point::max().
    std::chrono::steady_clock::time_point currentOpStartTime =
        std::chrono::steady_clock::time_point::max();
  };

  using ThreadStateWithMutex =
      folly::Synchronized<ThreadState, folly::SpinLock>;
  using LockedThreadState = ThreadStateWithMutex::LockedPtr;

 public:
  // Appends the given string, formatted with folly::format(), to IOTracing's
  // thread-local context string.
  // Destructor truncates the context string back to its previous length.
  // Usually used through SCOPED_IO_TRACING_CONTEXT() rather than directly.
  // Need to be destroyed in LIFO order.
  class AddContext {
   public:
    // Doesn't append anything. Used when tracing is disabled.
    AddContext() = default;

    // Appends to context. Note that it doesn't check if tracing isEnabled();
    // it needs to be checked outside, before calling this.
    template <class... Args>
    AddContext(IOTracing* tracing, folly::StringPiece format, Args&&... args) {
      assign(tracing, format, std::forward<Args>(args)...);
    }

    AddContext(AddContext&& rhs) noexcept
        : threadState_(rhs.threadState_),
          prevSize_(rhs.prevSize_),
          addedSize_(rhs.addedSize_) {
      rhs.threadState_ = nullptr;
      rhs.prevSize_ = std::numeric_limits<size_t>::max();
      rhs.addedSize_ = 0;
    }

    // Assignment operator would be error-prone. Consider:
    //   AddContext a(t, "a");
    //   a = AddContext(t, "bb");
    // This would end up with context "ab": we append "a", then append "bb",
    // then try to remove "a" (but actually remove the last "b").
    // So let's not support that. Instead, do:
    //   AddContext a(t, "a");
    //   a.assign(t, "bb");
    AddContext& operator=(AddContext&& rhs) = delete;

    ~AddContext() {
      clear();
    }

    template <class... Args>
    void assign(IOTracing* tracing, folly::StringPiece format, Args&&... args) {
      clear();

      threadState_ = tracing->threadStates_.get();
      auto locked_state = threadState_->lock();
      std::string& str = locked_state->context;

      prevSize_ = str.size();

      try {
        // This overload of folly::format() appends to string.
        folly::format(&str, format, std::forward<Args>(args)...);
      } catch (std::invalid_argument& ex) {
        // Format string doesn't match argument list.
        // Crash right here to dump a useful stack trace.
        // This is only reachable if tracing is enabled, so if you're getting
        // this crash in production, set rocksdb-io-tracing-shards to "none".
        std::abort();
      }
      str.append("|");

      ld_check_ge(str.size(), prevSize_);
      addedSize_ = str.size() - prevSize_;
    }

    void clear() {
      if (prevSize_ == std::numeric_limits<size_t>::max()) {
        ld_check_eq(0, addedSize_);
        return;
      }

      ld_check(threadState_);
      auto locked_state = threadState_->lock();
      std::string& str = locked_state->context;
      ld_check_eq(str.size(), prevSize_ + addedSize_);
      str.resize(prevSize_);
      prevSize_ = std::numeric_limits<size_t>::max();
      addedSize_ = 0;
    }

   private:
    ThreadStateWithMutex* threadState_ = nullptr;
    size_t prevSize_ = std::numeric_limits<size_t>::max();
    size_t addedSize_ = 0;
  };

  // Times and reports an IO operation. Also contains an AddContext for
  // convenience, since virtually all traced IO operations want to add some
  // context (namely, operation type and parameters) right next to operation.
  // Constructor grabs current time and appends to context, destructor does
  // the actual reporting.
  // Usually used through SCOPED_IO_TRACED_OP() rather than directly.
  class OpTimer {
   public:
    // Doesn't time or report anything. Used when tracing is disabled.
    OpTimer() = default;

    // Appends to context and starts the timer. Note that it doesn't check if
    // tracing isEnabled(); it needs to be checked outside, before calling this.
    template <class... Args>
    OpTimer(IOTracing* tracing, folly::StringPiece format, Args&&... args)
        : tracing_(tracing),
          addContext_(tracing, format, std::forward<Args>(args)...),
          startTime_(std::chrono::steady_clock::now()) {
      auto locked_state = tracing_->threadStates_->lock();
      ld_check(locked_state->currentOpStartTime ==
               std::chrono::steady_clock::time_point::max());
      locked_state->currentOpStartTime = startTime_;
    }

    OpTimer(OpTimer&& rhs) noexcept
        : tracing_(rhs.tracing_),
          addContext_(std::move(rhs.addContext_)),
          startTime_(rhs.startTime_) {
      rhs.tracing_ = nullptr;
      rhs.startTime_ = std::chrono::steady_clock::time_point::min();
    }

    OpTimer& operator=(OpTimer&&) = delete;

    ~OpTimer() {
      if (tracing_ == nullptr) {
        return;
      }
      auto duration = std::chrono::steady_clock::now() - startTime_;

      auto locked_state = tracing_->threadStates_->lock();
      ld_check_eq(locked_state->currentOpStartTime.time_since_epoch().count(),
                  startTime_.time_since_epoch().count());
      locked_state->currentOpStartTime =
          std::chrono::steady_clock::time_point::max();

      tracing_->reportCompletedOp(duration, locked_state);
    }

   private:
    IOTracing* tracing_ = nullptr;
    AddContext addContext_;
    std::chrono::steady_clock::time_point startTime_ =
        std::chrono::steady_clock::time_point::min();
  };

  explicit IOTracing(shard_index_t shard_idx, StatsHolder* stats);
  ~IOTracing();

  bool isEnabled() const {
    return options_->enabled.load(std::memory_order_relaxed);
  }

  // Not thread safe.
  void updateOptions(bool tracing_enabled,
                     std::chrono::milliseconds threshold,
                     std::chrono::milliseconds stall_threshold);

 private:
  struct Options {
    std::atomic<bool> enabled{false};
    std::atomic<std::chrono::milliseconds> threshold{
        std::chrono::milliseconds(0)};
    std::atomic<std::chrono::milliseconds> stall_threshold{
        std::chrono::milliseconds(0)};
  };

  shard_index_t shardIdx_;
  StatsHolder* stats_;
  folly::cacheline_aligned<Options> options_;

  // Information about all threads that touched this IOTracing.
  // The folly::Synchronized is needed to synchronize between the TheadState
  // owner and stallDetectionThread_; there should be hardly any contention,
  // and almost all accesses are from a single thread, so we're using spinlock.
  struct Tag {};
  folly::ThreadLocal<folly::Synchronized<ThreadState, folly::SpinLock>, Tag>
      threadStates_;

  struct {
    std::mutex mutex;           // protects `cv`
    std::condition_variable cv; // notified to wake up the thread
    bool enabled = false;       // if false, thread will exit when woken up
    std::thread thread;
  } stallDetectionThread_;

  void stallDetectionThreadMain();

  // Logs the current context along with operation duration.
  // Does _not_ check isEnabled() or threshold; they need to be checked before
  // calling this.
  // Usually used through OpTimer/SCOPED_IO_TRACED_OP() rather than directly.
  void reportCompletedOp(std::chrono::steady_clock::duration duration,
                         LockedThreadState& locked_state);
};

// Declares a local variable of type AddContext, passing it the given arguments.
// If tracing is disabled, arguments are not evaluated, and nothing is appended
// to the context string; this causes a harmless quirk: if tracing is turned on
// at runtime, after some context was (not) apended, the subsequent traced
// operation will be missing that part of context.
#define SCOPED_IO_TRACING_CONTEXT(tracing, format, args...) \
  auto FB_ANONYMOUS_VARIABLE(io_tracing_ctx) =              \
      ((tracing) && (tracing)->isEnabled())                 \
      ? IOTracing::AddContext((tracing), (format), ##args)  \
      : IOTracing::AddContext()

// Declares a local variable of type OpTimer, passing it the given arguments.
// This macro intentionally doesn't nest, you can have at most one per scope;
// that's because IO tracing is intended for low-level indivisible operations,
// like file reads/writes.
// If tracing is disabled, arguments are not evaluated, and OpTimer is no-op.
// Note that if you add context after this macro but before the OpTimer goes
// out of scope, this context _will_ show up in the trace for this operation;
// this is useful e.g. for reporting the result of the operation:
//
// {
//   SCOPED_IO_TRACED_OP(tracing, "foo_op {} {}", offset, size);
//   int rv = foo_op(fd, offset, size);
//   SCOPED_IO_TRACING_CONTEXT(tracing, "rv:{}", rv);
// }
#define SCOPED_IO_TRACED_OP(tracing, format, args...)         \
  auto _io_tracing_op = ((tracing) && (tracing)->isEnabled()) \
      ? IOTracing::OpTimer((tracing), (format), ##args)       \
      : IOTracing::OpTimer()

}} // namespace facebook::logdevice
