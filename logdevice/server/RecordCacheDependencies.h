/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstddef>
#include <memory>

#include <folly/Optional.h>

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class EpochRecordCache;
class EpochRecordCacheEntry;
class StatsHolder;
struct Seal;

using ReleasedVector =
    folly::small_vector<std::shared_ptr<EpochRecordCacheEntry>, 256>;

/**
 * Isolates external dependencies of EpochRecordCache for testing.
 */
class EpochRecordCacheDependencies {
 public:
  /**
   * Method for disposing of an EpochRecordCacheEntry. Needed since the
   * payload of the cached records may need to be freed on the exact same
   * thread that allocated the payload (e.g., evbuffer payload).
   * The function should be a sink and must take ownership of the entry object.
   * Note: must be thread-safe since the disposal action happens outside of
   * the lock.
   *
   * Also, this method must be called while the event loops of all Workers are
   * still running, i.e. before processor->isShuttingDown().
   */
  virtual void disposeOfCacheEntry(std::unique_ptr<EpochRecordCacheEntry>) = 0;

  virtual StatsHolder* getStatsHolder() const {
    return nullptr;
  }

  /**
   * Called, with lock held, whenever entries are removed from the cache because
   * they've been released.  Not called when they're evicted due to memory
   * pressure.  Called for both global releases and per-epoch releases.
   *
   * @param begin The first LSN released
   * @param end The last LSN released (inclusive).
   */
  virtual void onRecordsReleased(const EpochRecordCache&,
                                 lsn_t begin,
                                 lsn_t end,
                                 const ReleasedVector& entries) = 0;

  virtual ~EpochRecordCacheDependencies() {}
};

/**
 * Isolates external dependencies of RecordCache for ease of testing.
 */
class RecordCacheDependencies : public EpochRecordCacheDependencies {
 public:
  /**
   * Obtain normal seal or soft seal metadata value of the log.
   */
  virtual folly::Optional<Seal> getSeal(logid_t logid,
                                        shard_index_t shard,
                                        bool soft = false) const = 0;

  /**
   * Provides the highest LSN that was inserted into the local storage for
   * this log. See LocalLogStore::getHighestInsertedLSN for details.
   *
   * @return  0 on success and -1 on failure.
   */
  virtual int getHighestInsertedLSN(logid_t log_id,
                                    shard_index_t shard,
                                    lsn_t* highestLSN) const = 0;

  /**
   * Get the capacity for creating EpochRecordCache objects of the log.
   * The capacity must be equal or larger than the maximum possible number
   * of outstanding unclean records a storage node can get.
   */
  virtual size_t getEpochRecordCacheSize(logid_t logid) const = 0;

  /**
   * Return true if the log is tail optimized accordding to its log attribute.
   */
  virtual bool tailOptimized(logid_t logid) const = 0;

  ~RecordCacheDependencies() override {}
};

}} // namespace facebook::logdevice
