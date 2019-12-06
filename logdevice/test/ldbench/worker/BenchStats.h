/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>

#include <folly/ThreadLocal.h>
#include <folly/dynamic.h>
#include <folly/experimental/FunctionScheduler.h>

#include "logdevice/common/checks.h"
#include "logdevice/test/ldbench/worker/StatsStore.h"

namespace facebook { namespace logdevice { namespace ldbench {

enum class StatsType { SUCCESS, SUCCESS_BYTE, FAILURE, INFLIGHT, SKIPPED };

/**
 * BenchStats defines the stats content and provides access/update operations
 * It will be a thread-local object and managed by BenchStatsHolder
 * Periodically, all BenchStats objects will be aggregated by BenchStatsHolder
 */
class BenchStats {
 public:
  explicit BenchStats(const std::string& type);
  ~BenchStats();
  /**
   * Return a the value of corresponding StatsAttr
   */
  int64_t getAttr(StatsType attr) const;

  /**
   * Increase an StatsAttr by num
   * Set num < 0 to decrease an attribute
   */
  void incStat(StatsType attr, int64_t num);

  /**
   * Aggregate its own attributes with another BenchStats object
   */
  void aggregate(const BenchStats& stats);

  /**
   * Display the current stats content on screen
   * This is a debug funtion
   */
  void display() const;

  /**
   * Convert current stats content to key-value pairs using dynamic object
   */
  folly::dynamic collectStatsAsPairs();

 private:
  // Define stats attributes
  std::atomic<int64_t> success_;      // total number of successful records
  std::atomic<int64_t> success_byte_; // total size of successful appends
  std::atomic<int64_t> in_flight_;    // total number of in-flights records
  std::atomic<int64_t> failure_;      // total number of failed records
  std::atomic<int64_t> skipped_;      // total number of skipped records
  std::string type_;                  // request types for different workloads
};

/**
 *
 * BenchStatsHolder will contain and aggregate the stats for multiple threads
 * It is owned by the worker and updated by the client holder
 * Therefore multi-thread will update the same instance
 */
class BenchStatsHolder {
 public:
  /**
   * @param
   *  name -- request type: append_sync, append_async ...
   */
  explicit BenchStatsHolder(const std::string& name)
      : aggregated_stats_(name), name_(name) {}

  ~BenchStatsHolder() {}

  /**
   * Return the BenchStats object owned by the current thread
   * Create one if none exists for the current thread.
   */
  BenchStats* getOrCreateTLStats();

  /**
   * Aggregate stats from all threads including who have joined
   */
  folly::dynamic aggregateAllStats();

  /**
   * Display every local BenchStats
   * This is a debug function
   */
  void displayAllForDebug();

 private:
  /**
   * Store the stats of reclaimed threads to a global stats variable.
   * LogDevice maintains a threadpool to response appends. After finish their
   * jobs, the reponse threads may be reclaimed to the threadpool.
   * The corresponding local BenchStats will be also reclaimed.
   * Therefore, we store the reclaimed BenchStats in a global stats variable.
   */
  void aggregateDestructedStats(const BenchStats& stats) {
    aggregated_stats_.aggregate(stats);
  }

  struct Tag;
  struct BenchStatsWrapper;
  // store BenchStats to be reclaimed
  // It is not a thread local object, so we use a mutex to protect it
  // bench_thread_stats_ may use the lock to aggregate a reclaimed stats to
  // the global stats. Therefore bench_thread_stats_ is destroyed
  // before the mutex
  BenchStats aggregated_stats_;
  std::mutex aggregated_stats_mutex_;

  folly::ThreadLocalPtr<BenchStatsWrapper, Tag> bench_thread_stats_;

  std::string name_; // request type: append_sync, append_async ...
};

/**
 * A BenchStatsWrapper is is a single-thread thin RAII wrapper that,
 * on destruction (e.g., when the thread has joined), aggregates the stats
 * to BenchStatHolder
 */
struct BenchStatsHolder::BenchStatsWrapper {
  BenchStats stats_;
  BenchStatsHolder* owner_;

  explicit BenchStatsWrapper(BenchStatsHolder* owner)
      : stats_(owner->name_), owner_(owner) {
    ld_check(owner_ != nullptr);
  }

  ~BenchStatsWrapper() {
    {
      std::lock_guard<std::mutex> lock(owner_->aggregated_stats_mutex_);
      owner_->aggregateDestructedStats(stats_);
    }
  }
};

class BenchStatsCollectionThread {
 public:
  /**
   * @param:
   *  stat_source
   *  stats_store
   *  interval -- stats collection interval in seconds
   */
  BenchStatsCollectionThread(std::shared_ptr<BenchStatsHolder> stats_source,
                             std::shared_ptr<StatsStore> stats_store,
                             uint64_t interval_s);
  ~BenchStatsCollectionThread();
  /**
   * Collect stats from stats_source and publish it to stats_des
   * with time interval (second)
   */
  void statsCollectionFunction();

 private:
  std::shared_ptr<BenchStatsHolder> stats_source_;
  std::shared_ptr<StatsStore> stats_store_;
  std::chrono::seconds interval_;
  folly::FunctionScheduler function_scheduler_;
};

}}} // namespace facebook::logdevice::ldbench
