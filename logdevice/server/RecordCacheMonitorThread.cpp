/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RecordCacheMonitorThread.h"

#include <cstdint>
#include <functional>
#include <queue>
#include <vector>

#include "logdevice/common/ThreadID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

RecordCacheMonitorThread::RecordCacheMonitorThread(ServerProcessor* processor)
    : processor_(processor) {
  ld_check(processor != nullptr);
  thread_ = std::thread([this] { threadMain(); });
  ld_info("Record cache monitor thread started.");
}

RecordCacheMonitorThread::~RecordCacheMonitorThread() {
  // currently shutdown_ is only signaled in the destructor, and destructor
  // should be only called in one thread. Currently the object is destroyed
  // along with Processor on the main thread that does the shutdown.
  ld_check(!shutdown_.signaled());
  shutdown_.signal();
  thread_.join();
  ld_info("Record cache monitor thread stopped.");
}

void RecordCacheMonitorThread::threadMain() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:cache-evict");
  ld_info("Record cache eviction thread started. Size limit is set "
          "to %lu bytes.",
          processor_->settings()->record_cache_max_size);

  while (!shutdown_.signaled()) {
    auto result = recordCacheNeedsEviction();
    if (result.first) {
      RATELIMIT_INFO(
          std::chrono::seconds(10),
          1,
          "Total record cache size has exceed the limit of %lu bytes, "
          "attempting to evict %lu bytes.",
          processor_->settings()->record_cache_max_size,
          result.second);
      evictCaches(result.second);
    }

    shutdown_.waitFor(processor_->settings()->record_cache_monitor_interval);
  }
}

std::pair<bool, size_t> RecordCacheMonitorThread::recordCacheNeedsEviction() {
  const size_t size_limit = processor_->settings()->record_cache_max_size;
  if (size_limit == 0) {
    // ulimited, no eviction required
    return std::make_pair(false, 0);
  }

  // Note: calculating the current cache size using the
  // record_cache_bytes_cached_estimate stats will usually get a larger
  // estimate on record cache size compared with aggregating each record cache
  // size by calling getPayloadSizeEstimate(). The reason is that:
  // 1) record_cache_bytes_cached_estimate only gets decreased when the Entry is
  //    destroyed at RecordCacheDisposal, which could be a bit later after the
  //    Entry is evicted.
  // 2) record_cache_bytes_cached_estimate takes into account of the size
  //    overhead of Entry itself, while RecordCache::getPayloadSizeEstimate()
  //    only accounts for the payload size.
  // Although record_cache_bytes_cached_estimate is an overestimate on the
  // current record cache size, it is actually more accurate on estimate the
  // memeory usage of all record cache related data. Therefore, we use this
  // number to calculate the eviction size target which is used to evict current
  // record cache entries.

  int64_t total_cache_size = 0;
  StatsHolder* holder = processor_->stats_;
  if (holder == nullptr) {
    // currently we rely on processor having a valid stats to estimate the
    // record cache size, do nothing if the StatsHolder is not available
    // (e.g., in some unit tests)
    return std::make_pair(false, 0);
  }

  holder->runForEach([&total_cache_size](Stats& stats) {
    total_cache_size += stats.record_cache_bytes_cached_estimate;
  });

  if (total_cache_size > 0 && total_cache_size > (int64_t)size_limit) {
    // beside the bytes exceed the limit, evict another 20% of the max
    // cache size to prevent frequent eviction
    return std::make_pair(
        true, (size_t)total_cache_size - size_limit + size_limit / 5);
  }

  ld_debug("Total record cache size is %ld not greater than the limit %lu "
           "bytes, no eviction needed.",
           total_cache_size,
           size_limit);
  return std::make_pair(false, 0);
}

namespace {

struct LogEntry {
  logid_t log_id;
  shard_index_t shard;
  size_t cache_size;

  bool operator>(const LogEntry& rhs) const {
    return cache_size > rhs.cache_size;
  }
};

} // namespace

void RecordCacheMonitorThread::evictCaches(size_t target_bytes) {
  ld_check(target_bytes > 0);
  using MinQueue = std::
      priority_queue<LogEntry, std::vector<LogEntry>, std::greater<LogEntry>>;

  MinQueue min_queue;
  // number of bytes for logs in the min_queue
  size_t bytes_in_queue = 0;

  auto access_log = [target_bytes, &bytes_in_queue, &min_queue](
                        logid_t logid, const LogStorageState& state) {
    if (state.record_cache_ == nullptr) {
      return 0;
    }
    const size_t log_size = state.record_cache_->getPayloadSizeEstimate();
    if (log_size == 0) {
      return 0;
    }

    min_queue.push(LogEntry{logid, state.getShardIdx(), log_size});
    bytes_in_queue += log_size;

    // pop the queue until 1) it is empty or
    // 2) bytes_in_queue - queue.top() < target_bytes
    while (!min_queue.empty() && bytes_in_queue > target_bytes) {
      const auto& entry = min_queue.top();
      ld_check(bytes_in_queue >= entry.cache_size);
      if (bytes_in_queue - entry.cache_size < target_bytes) {
        break;
      }
      bytes_in_queue -= entry.cache_size;
      min_queue.pop();
    }

    return 0;
  };

  auto& log_map = processor_->getLogStorageStateMap();
  log_map.forEachLog(access_log);

  if (min_queue.empty()) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "Can't find a log to evict, nothing to do.");
    return;
  }

  size_t num_logs_evicted = 0;

  // evict all logs in queue
  while (!min_queue.empty()) {
    const LogEntry& e = min_queue.top();
    auto& record_cache_ptr = log_map.get(e.log_id, e.shard).record_cache_;
    // RecordCache pointer shouldn't get destroyed before this thread exists
    ld_check(record_cache_ptr != nullptr);
    record_cache_ptr->evictResetAllEpochs();
    min_queue.pop();
    ++num_logs_evicted;
  }

  STAT_INCR(processor_->stats_, record_cache_eviction_performed_by_monitor);
  STAT_ADD(processor_->stats_,
           record_cache_bytes_evicted_by_monitor,
           bytes_in_queue);

  RATELIMIT_INFO(
      std::chrono::seconds(10),
      1,
      "Evicted %lu logs from the record cache, estimate actual total "
      "bytes evicted: %lu, bytes evicted target: %lu",
      num_logs_evicted,
      bytes_in_queue,
      target_bytes);
}

}} // namespace facebook::logdevice
