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
 *       a base class for iterators for registering them in the container.
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

 private:
  std::mutex mutex_;
  // The list doesn't own TrackableIterators. They unregister themselves from
  // the list upon destruction.
  std::list<TrackableIterator*> list_;
};

// A convenience macro for propagating information from TrackableIterator to
// IOTracing context.
#define SCOPED_IO_TRACING_CONTEXT_FROM_ITERATOR(iterator, op)    \
  SCOPED_IO_TRACING_CONTEXT(                                     \
      (iterator)->getStore()->getIOTracing(),                    \
      "cf:{}|log:{}|it:{}:{}|{}",                                \
      (iterator)->getImmutableTrackingInfo().column_family_name, \
      (iterator)->getImmutableTrackingInfo().log_id.val_,        \
      (iterator)->getImmutableTrackingInfo().high_level_id,      \
      (iterator)->getMutableTrackingInfo().more_context,         \
      (op))

}} // namespace facebook::logdevice
