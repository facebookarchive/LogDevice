/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/CachelinePadded.h>
#include <folly/Format.h>
#include <folly/Preprocessor.h>
#include <folly/ThreadLocal.h>

#include "logdevice/common/checks.h"
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
        : str_(rhs.str_), prevSize_(rhs.prevSize_), addedSize_(rhs.addedSize_) {
      rhs.str_ = nullptr;
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
      str_ = &tracing->state_->context;
      prevSize_ = str_->size();

      try {
        // This overload of folly::format() appends to string.
        folly::format(str_, format, std::forward<Args>(args)...);
      } catch (std::invalid_argument& ex) {
        // Format string doesn't match argument list.
        // Crash right here to dump a useful stack trace.
        // This is only reachable if tracing is enabled, so it won't
        // mass-crash production.
        std::abort();
      }
      str_->append("|");

      ld_check_ge(str_->size(), prevSize_);
      addedSize_ = str_->size() - prevSize_;
    }

    void clear() {
      if (prevSize_ == std::numeric_limits<size_t>::max()) {
        ld_check_eq(0, addedSize_);
        return;
      }
      ld_check(str_);
      ld_check_eq(str_->size(), prevSize_ + addedSize_);
      str_->resize(prevSize_);
      prevSize_ = std::numeric_limits<size_t>::max();
      addedSize_ = 0;
    }

   private:
    std::string* str_ = nullptr;
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
          startTime_(std::chrono::steady_clock::now()) {}

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
      tracing_->reportCompletedOp(duration);
    }

   private:
    IOTracing* tracing_ = nullptr;
    AddContext addContext_;
    std::chrono::steady_clock::time_point startTime_ =
        std::chrono::steady_clock::time_point::min();
  };

  explicit IOTracing(shard_index_t shard_idx);

  bool isEnabled() const {
    return enabled_->load(std::memory_order_relaxed);
  }
  void setEnabled(bool enabled) {
    enabled_->store(enabled);
  }

  // Logs the current context along with operation duration.
  // Does _not_ check isEnabled(); it needs to be checked before calling this.
  // Usually used through OpTimer/SCOPED_IO_TRACED_OP() rather than directly.
  void reportCompletedOp(std::chrono::steady_clock::duration duration);

 private:
  struct State {
    std::string context;
  };

  shard_index_t shardIdx_;
  folly::CachelinePadded<std::atomic<bool>> enabled_{false};
  folly::ThreadLocal<State> state_;
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
