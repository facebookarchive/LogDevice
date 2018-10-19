/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <vector>

#include <folly/concurrency/ConcurrentHashMap.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/RecordCacheDisposal.h"
#include "logdevice/server/RecordCacheMonitorThread.h"
#include "logdevice/server/read_path/LogStorageState.h"

namespace facebook { namespace logdevice {

/**
 * @file
 * On storage nodes, maps log IDs to LogStorageState instances, state that we
 * need to keep in memory for fast access.
 */

class LogStorageStateMap {
 public:
  /**
   * @param num_shards         Number of shards on this node
   *                           map, required by AtomicHashMap
   * @param recovery_interval  interval between consecutive attempts to recover
   *                           log state
   * @param processor          may be null in tests
   */
  explicit LogStorageStateMap(shard_size_t num_shards,
                              std::chrono::microseconds recovery_interval =
                                  std::chrono::microseconds(500000),
                              ServerProcessor* processor = nullptr);

  LogStorageStateMap(const LogStorageStateMap&) = delete;
  LogStorageStateMap& operator=(const LogStorageStateMap&) = delete;

  LogStorageStateMap(LogStorageStateMap&&) = delete;
  LogStorageStateMap&& operator=(LogStorageStateMap&&) = delete;

  /**
   * Returns a pointer to the state object for the given log, creating it if
   * it does not already exist.
   */
  LogStorageState* insertOrGet(logid_t log_id, shard_index_t shard_idx);

  /**
   * Finds the state object for the given log.  Returns nullptr if it does not
   * exist.
   */
  LogStorageState* find(logid_t log_id, shard_index_t shard_idx);

  /**
   * Finds the state object for the given log, expecting it to already exist.
   * In debug builds, asserts that it exists.
   */
  LogStorageState& get(logid_t log_id, shard_index_t shard_idx);

  /**
   * Used in tests.
   */
  void clear();

  using ReleaseStates = std::vector<std::pair<logid_t, lsn_t>>;

  /**
   * For the LogStorageState for the log_id, repopulate the RecordCache from
   * the given buffer.
   *
   * NOTE this is not threadsafe; should only be called when no other thread
   * is accessing the record caches, e.g. during startup.
   *
   * @return -1 if there's no LogStorageState for that log id, or if
   *         deserialization of the record cache failed.
   */
  int repopulateRecordCacheFromLinearBuffer(logid_t log_id,
                                            shard_index_t shard,
                                            const char* buffer,
                                            size_t size);

  /**
   * Returns the contents of the map as a vector of (log id, lsn) pairs.
   */
  ReleaseStates getAllLastReleasedLSNs(shard_index_t shard) const;

  /**
   * If last released LSN or trim point for log_id is not known, this function
   * tries to recover it by reading it from the local log store and (for last
   * released LSN) asking the sequencer to resend it.
   *
   * This method should be called on a worker thread.
   *
   * @return -1 if the log has a permanent error, and you shouldn't expect
   *         this LogStorageState to ever be recovered.
   *         Sets `err` to E::FAILED in this case.
   *         Otherwise returns 0 and maybe schedules an async task to recover
   *         the state.
   */
  int recoverLogState(logid_t log_id,
                      shard_index_t shard_idx,
                      LogStorageState::RecoverContext ctx,
                      bool force_ask_sequencer = false);

  /**
   * If record cache is enabled, shutdown record caches of all logs by clearing
   * their entries. Called during server shutdown.
   */
  void shutdownRecordCaches();

  /**
   * Shutdown record caches monitor thread. Block until the thread is destroyed.
   * Called during server shutdown.
   *
   * Note: the method is not thread-safe and should be called only on the
   *       thread performing shutdown.
   */
  void shutdownRecordCacheMonitor();

  /**
   * Iterate on each log currently stored in the map, calling @param func
   * for each. Abort the iteration and return -1 if any of the callback
   * returns non-zero status. Otherwise return 0 for successfully executinng
   * callback for each log.
   *
   * @param func A callable with signature int(logid_t, const LogStorageState&).
   */
  template <typename Func>
  int forEachLog(const Func& func) const;

  /**
   * Iterate on each log currently stored in the map for the provided shard,
   * calling @param func for each. Abort the iteration and return -1 if any of
   * the callback returns non-zero status. Otherwise return 0 for successfully
   * executinng callback for each log.
   *
   * @param shard Only call the callback for the LogStorageStates on this shard.
   * @param func A callable with signature int(logid_t, const LogStorageState&).
   */
  template <typename Func>
  int forEachLogOnShard(shard_index_t shard, const Func& func) const;

  // May be nullptr in tests.
  ServerProcessor* getProcessor();
  StatsHolder* getStats();

  /**
   * Disposal for evicted cache entries, nullptr if record caching is
   * disabled.
   */
  std::unique_ptr<RecordCacheDisposal> cache_disposal_;

 private:
  shard_size_t num_shards_;

  // Parent Processor instance. May be null in tests.
  ServerProcessor* const processor_;

  // use Hash64 to mitigate the effect of logid (data and metadata) collision
  using Map = folly::ConcurrentHashMap<logid_t::raw_type,
                                       std::unique_ptr<LogStorageState>,
                                       Hash64<logid_t::raw_type>>;

  const std::vector<std::unique_ptr<Map>> shard_map_;

  static std::vector<std::unique_ptr<Map>> makeMap(shard_size_t num_shards);

  // Attempt to recover log state only once this many usecs.
  std::chrono::microseconds state_recovery_interval_;

  /**
   * A background thread for monitoring record cache size and perform eviction
   * if needed.
   */
  std::unique_ptr<RecordCacheMonitorThread> record_cache_monitor_;
};

template <typename Func>
int LogStorageStateMap::forEachLogOnShard(shard_index_t shard,
                                          const Func& func) const {
  ld_check(shard < shard_map_.size());
  for (auto kv = shard_map_[shard]->cbegin(); kv != shard_map_[shard]->cend();
       ++kv) {
    if (kv->second != nullptr) {
      if (func(logid_t(kv->first), *kv->second) != 0) {
        return -1;
      }
    }
  }
  return 0;
}

template <typename Func>
int LogStorageStateMap::forEachLog(const Func& func) const {
  for (shard_index_t i = 0; i < num_shards_; ++i) {
    if (forEachLogOnShard(i, func) != 0) {
      return -1;
    }
  }
  return 0;
}

}} // namespace facebook::logdevice
