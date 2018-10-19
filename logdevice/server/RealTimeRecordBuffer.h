/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>

#include "folly/AtomicIntrusiveLinkedList.h"
#include "logdevice/common/UnorderedMapWithLRU.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class ZeroCopiedRecord;
class StatsHolder;

/**
 * These are created in RecordCacheDisposal::evicted() when they're evicted from
 * the RecordCache.  They go into the AllServerReadStreams for each Worker that
 * has clients reading this log.
 *
 * AllServerReadStreams::onRelease() then distributes shared_ptrs to them to all
 * relevant ServerReadStreams.  Finally, at the start of
 * CatchupOneStream::startRead(), we grab all of them.  If we don't use them on
 * this iteration, we delete the shared_ptr.  Once every stream has had a chance
 * to use them (i.e., once CatchupOneStream::startRecord() has been called for
 * every ServerReadStream), the ReleasedRecords struct will be freed.  Once
 * that's happened on all workers, the underlying ReleasedRecords will be freed.
 **/

class RealTimeRecordBuffer;

class ReleasedRecords {
 public:
  ReleasedRecords(logid_t logid,
                  lsn_t begin,
                  lsn_t end,
                  const std::shared_ptr<ZeroCopiedRecord>& entries,
                  size_t bytes_estimate)
      : logid_(logid),
        begin_lsn_(begin),
        end_lsn_(end),
        entries_(entries),
        bytes_estimate_(bytes_estimate) {}

  ReleasedRecords(const ReleasedRecords&) = delete;
  ReleasedRecords& operator=(const ReleasedRecords&) = delete;

  ReleasedRecords(ReleasedRecords&&) = delete;
  ReleasedRecords& operator=(ReleasedRecords&&) = delete;

  ~ReleasedRecords();

  /**
   * True if given LSN is greater than the upper end of LSNs represented by this
   * object. False if in the range or before its start.
   */
  bool operator<(lsn_t lsn) const {
    ld_check(begin_lsn_ <= end_lsn_);
    return end_lsn_ < lsn;
  }

  /**
   * True if given LSN is less than the lower end of LSNs represented by this
   * object. False if in the range or greater than its end.
   */
  bool operator>(lsn_t lsn) const {
    ld_check(begin_lsn_ <= end_lsn_);
    return begin_lsn_ > lsn;
  }

  // An estimate of how many bytes of RAM this takes up, i.e. it includes the
  // overhead of shared_ptrs, etc.  Used to assess when we're over our memory
  // budget and need to evict.
  size_t getBytesEstimate() const {
    return bytes_estimate_;
  }

  std::string toString() const;

  static size_t computeBytesEstimate(const ZeroCopiedRecord* entries);

  const logid_t logid_;
  // This object contains all the records for the log that this storage node has
  // between begin_lsn_ and end_lsn_ inclusive.  Note that, if we're not part of
  // the copy set for part of that range, we won't have records for those LSNs.
  const lsn_t begin_lsn_;
  const lsn_t end_lsn_;
  // entries_ is the head of a list of shared_ptr<ZeroCopiedRecords>
  const std::shared_ptr<ZeroCopiedRecord> entries_;
  const size_t bytes_estimate_;
  folly::AtomicIntrusiveLinkedListHook<ReleasedRecords> hook_;
  RealTimeRecordBuffer* buffer_{nullptr};
};

// There is one of these per Worker, it lives in AllServerReadStreams.
class RealTimeRecordBuffer {
 public:
  RealTimeRecordBuffer(size_t eviction_threshold_bytes,
                       size_t max_bytes,
                       StatsHolder* stats)
      : eviction_threshold_bytes_(eviction_threshold_bytes),
        max_bytes_(max_bytes),
        stats_(stats),
        logids_(LOGID_INVALID, LOGID_INVALID2) {}

  RealTimeRecordBuffer(const RealTimeRecordBuffer&) = delete;
  RealTimeRecordBuffer operator=(const RealTimeRecordBuffer&) = delete;

  ~RealTimeRecordBuffer();

  // Called from another worker, with the EpochRecordCache lock held.  If it
  // returns 'true', caller needs to call scheduleEviction(), presumably after
  // the lock is released.
  bool appendReleasedRecords(std::unique_ptr<ReleasedRecords> records);

  template <typename F>
  void sweep(F&& func) {
    // This sweep() can take arbitrarily long, since it loops over all elements
    // on the list and calls func on each one.
    released_records_.sweep([&func](ReleasedRecords* recs) {
      func(std::unique_ptr<ReleasedRecords>(recs));
    });
  }

  bool overEvictionThreshold() const {
    return released_records_bytes_.load() > eviction_threshold_bytes_;
  }

  bool overMemoryBudget() const {
    return released_records_bytes_.load() > max_bytes_;
  }

  logid_t toEvict() {
    return logids_.getLRU();
  }

  void used(logid_t logid) {
    logids_.get(logid);
  }

  void deletedReleasedRecords(const ReleasedRecords* records) {
    released_records_bytes_.fetch_sub(records->getBytesEstimate());

    auto count_and_iter = logids_.getWithoutPromotion(records->logid_);
    ld_check(count_and_iter.first != nullptr);

    ld_check(*count_and_iter.first > 0);
    (*count_and_iter.first)--;
    if (*count_and_iter.first == 0) {
      logids_.erase(count_and_iter.second);
    }
  }

  // This moves ZeroCopiedRecords to various workers, so must be run before
  // any Worker::~Worker() of this processor is called.
  // Returns the number of groups of records that were removed.
  size_t shutdown();

  void addToLRU(logid_t logid) {
    logids_.getOrAddWithoutPromotion(logid)++;
  }

 private:
  folly::AtomicIntrusiveLinkedList<ReleasedRecords, &ReleasedRecords::hook_>
      released_records_;

  // The size of all records both in relased_records_ here, and in all
  // ServerReadStream's released_records_.  Note that we need to append records
  // to released_records_ before incrementing this, because when this is big
  // enough to trigger eviction, we must be able to find the records to evict.
  std::atomic<size_t> released_records_bytes_{0};

  const size_t eviction_threshold_bytes_;

  const size_t max_bytes_;

  StatsHolder* const stats_;

  struct GetVal {
    size_t operator()(const logid_t logid) const {
      return logid.val();
    }
  };

  // This maps logids to a count of how many ReleasedRecords instances we have
  // for that logid.  Its only for the ones in ServerReadStream's
  // released_records_, NOT the ones in our released_records_.  This also keeps
  // them in order of when we last sent them to a client, or if they've never
  // been sent, when we added them.  It's used for evicting from the cache when
  // it is full.
  //
  // The count is decremeneted whenever a ReleasedRecords() is destroyed and
  // incremented in addToLRU(), so addToLRU() must be called once for
  // every ReleasedRecords we create.

  UnorderedMapWithLRU<logid_t, size_t, GetVal> logids_;

  // After shutdown() is called, no more records should be appended.  Assert
  // that in debug builds.
  std::atomic<bool> shutdown_{false};
};

}} // namespace facebook::logdevice
