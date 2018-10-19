/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <mutex>
#include <vector>

#include <folly/Memory.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/include/types.h"

/**
 * @file A container for keeping track of most rocksdb data iterators, and
 *       a base class for iterators for
 *        (a) registering them in the container,
 *        (b) tracing file IO done by the iterators.
 *
 * Note the distinction between "tracking" and "tracing": tracking refers to
 * maintaining the list of living iterators instances, tracing refers to
 * attributing IO operations to iterators that did them.
 * IteratorTracker/TrackableIterator is used for both.
 */

namespace facebook { namespace logdevice {

class LocalLogStore;
class IteratorTracker;

/**
 * Interface that all iterators that need to be tracked by IteratorTracker
 * should inherit from.
 */
class TrackableIterator {
  friend class IteratorTracker;

 public:
  // DeclareActive is a scoped class used to mark an iterator as the
  // source of lower-level operations performed by the iterator. This
  // sets the iterator and its operation string into TLS so that the
  // iterator can be found without having to pass it through APIs that
  // may be outside of LogDevice's control.
  //
  // Note: To reduce the cost of tracing support when tracing is not
  //       enabled, DeclareActive is a no-op unless an iterator is
  //       configured for tracing. This makes it cheap to short circuit
  //       potentially expensive operations in AddContext and other
  //       tracing infrastructure just by checking to see if any iterator
  //       is active.
  //
  // Note: DeclareActive does not nest. If another iterator is already declared
  //       active, DeclareActive does nothing. This comes in handy e.g.
  //       when a filtered seek() calls next() to skip filtered out records.
  class DeclareActive {
   public:
    inline DeclareActive(const TrackableIterator* iter, const char* op);
    inline ~DeclareActive();

   private:
    bool set_active_ = false;
  };
  // AddContext is a scoped class used to append additional information
  // about an operation (or portions of an operation) performed on an
  // iterator.
  class AddContext {
   public:
    inline explicit AddContext(const char* s);
    inline ~AddContext();

   private:
    size_t ctx_string_size_;
  };

  enum IteratorType {
    DATA = 0,
    CSI,
    CSI_WRAPPER,
    PARTITIONED,
    PARTITIONED_ALL_LOGS
  };

  // Information that helps identify what the iterator is used for.
  struct TrackingContext {
    TrackingContext() = default;
    TrackingContext(bool rebuilding, const char* more_ctx);

    // True if this iterator is used for rebuilding
    bool created_by_rebuilding{false};

    // An identifier for the high-level iterator that should be passed down to
    // all lower level iterators that it creates. Can be used to tie the
    // iterators together in the output.
    uint64_t high_level_id{0};

    // An arbitrary string that provides more context on what this iterator is.
    const char* more_context{nullptr};
  };

  // Immutable part of the information about the iterator.
  struct ImmutableTrackingInfo {
    ImmutableTrackingInfo() = default;

    // Empty for iterators that are not confined to a single column family.
    // E.g. it's empty for LogsDB higher-level iterator (that jumps from
    // partition to partition), but nonempty for LogsDB lower-level iterator
    // that lives in one partition.
    std::string column_family_name;

    logid_t log_id;
    RecordTimestamp created{RecordTimestamp::now()};

    // An identifier for the high-level iterator that should be passed down to
    // all lower level iterators that it creates. Can be used to tie the
    // iterators together in the output.
    uint64_t high_level_id;

    IteratorType type;
    bool tailing;
    bool blocking;

    // True if this iterator is used for rebuilding
    bool created_by_rebuilding;
  };

  // Mutable fields that the IteratorTracker tracks.
  struct MutableTrackingInfo {
    lsn_t last_seek_lsn{LSN_INVALID};
    std::chrono::milliseconds last_seek_time{0};
    uint64_t last_seek_version{0};
    // An arbitrary string that provides more context on what this iterator is.
    const char* more_context;
  };

  struct TrackingInfo {
    ImmutableTrackingInfo imm;
    MutableTrackingInfo mut;
  };

  virtual ~TrackableIterator();

  // Returns true if we should keep track of IO operations made while this
  // iterator is active.
  virtual bool tracingEnabled() const = 0;
  virtual const LocalLogStore* getStore() const = 0;

  // Sets the context string in case it changes over the lifetime of the
  // iterator. Can be overridden in composite iterators to propagate
  // to lower levels.
  virtual void setContextString(const char* str);

  // Returns the immutable part of the information about the iterator.
  const ImmutableTrackingInfo& getImmutableTrackingInfo() const {
    return imm_info_;
  }

  // Returns a copy of the mutable part of the information about the iterator.
  MutableTrackingInfo getMutableTrackingInfo() const;

  // Returns a copy of both immutable and mutable info.
  TrackingInfo getDebugInfo() const;

  // Returns information that can be used to deduce how many bytes this
  // iterator has read from disk (including OS page cache, but excluding any
  // caches and buffers maintained by LocalLogStore).
  // It should be used like this:
  //
  //   size_t before = iterator->getIOBytesUnnormalized();
  //   [do stuff with iterator]
  //   size_t disk_io_done = iterator->getIOBytesUnnormalized() - before;
  //
  // With an additional requirement that the "[do stuff with iterator]" doesn't
  // use any other iterators.
  // This interface allows this method to be implemented in either of two ways:
  //  1. Return a global thread-local counter of total bytes read from disk by
  //     current thread (across all iterators and other readers). This is what
  //     rocksdb-based LocalLogStores do.
  //  2. Return a per-iterator counter of bytes read from disk.
  //
  // This is used only for stats, useful for estimating effectiveness of caches
  // and buffers in LocalLogStore (in particular, rocksdb block cache and
  // memtables). Implementing is optional.
  virtual size_t getIOBytesUnnormalized() const {
    return 0;
  }

 protected:
  // Call this to register the iterator for tracking.
  // Usually called from constructor of the TrackableIterator subclass.
  void registerTracking(std::string column_family,
                        logid_t log_id, // LOGID_INVALID means all logs
                        bool tailing,
                        bool blocking,
                        IteratorType type,
                        TrackingContext context);

  // Call this every time the iterator is re-seeked with the lsn it is seeked
  // (sought? :)) to
  void trackSeek(lsn_t lsn, uint64_t version_number);

  // Call this when the underlying RocksDB iterator is released
  void trackIteratorRelease();

  ImmutableTrackingInfo imm_info_;

  // This mutex protects the tracking state
  mutable std::mutex tracking_mutex_;
  MutableTrackingInfo mut_info_;

 private:
  // Iterator pointing to the entry in the IteratorTracker's list that
  // references this iterator
  std::list<TrackableIterator*>::iterator list_iterator_;

  // pointer to owning IteratorTracker
  IteratorTracker* parent_{nullptr};
};

class IteratorTracker {
 public:
  // returns the IteratorTracker singleton
  static IteratorTracker* get();

  // Registers the iterator on the tracking list
  void registerIterator(TrackableIterator* it);

  // Unregisters an iterator
  void unregisterIterator(TrackableIterator* it);

  // Returns info on all iterators tracked
  std::vector<TrackableIterator::TrackingInfo> getDebugInfo();

  // These maintain thread-local information about the currently active
  // iterator. File IO done on this thread is attributed to this iterator
  // (see RocksDBRandomAccessFile).

  static thread_local const TrackableIterator* active_iterator;
  // A string representing the high level operation being performed on
  // an iterator.
  static thread_local const char* active_iterator_op;
  // Additional context for the operation or portions of the operation.
  // Context can be added at any layer of the implementation of the
  // operation, with each appended piece of context separated from any
  // preceding context by "::".
  static thread_local std::string active_iterator_op_context;

 private:
  std::mutex mutex_;
  // The list doesn't own TrackableIterators. They unregister themselves from
  // the list upon destruction.
  std::list<TrackableIterator*> list_;
};

TrackableIterator::DeclareActive::DeclareActive(const TrackableIterator* iter,
                                                const char* op) {
  if (iter->tracingEnabled()) {
    if (!IteratorTracker::active_iterator) {
      IteratorTracker::active_iterator = iter;
      IteratorTracker::active_iterator_op = op;
      set_active_ = true;
    }
  }
}
TrackableIterator::DeclareActive::~DeclareActive() {
  if (set_active_) {
    IteratorTracker::active_iterator = nullptr;
    IteratorTracker::active_iterator_op = "";
  }
}

TrackableIterator::AddContext::AddContext(const char* s) {
  if (IteratorTracker::active_iterator) {
    auto ctx_string = std::string("::") + s;
    IteratorTracker::active_iterator_op_context += ctx_string;
    ctx_string_size_ = ctx_string.size();
  }
}
TrackableIterator::AddContext::~AddContext() {
  if (IteratorTracker::active_iterator) {
    ld_check(IteratorTracker::active_iterator_op_context.size() >=
             ctx_string_size_);
    auto pos =
        IteratorTracker::active_iterator_op_context.size() - ctx_string_size_;
    ld_assert(
        IteratorTracker::active_iterator_op_context.compare(pos, 2, "::") == 0);
    IteratorTracker::active_iterator_op_context.erase(pos);
  }
}

}} // namespace facebook::logdevice
