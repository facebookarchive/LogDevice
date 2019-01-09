/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/LogStoreMonitor.h"

#include <boost/filesystem.hpp>
#include <folly/Memory.h>

#include "logdevice/common/ThreadID.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/locallogstore/CompactionRequest.h"
#include "logdevice/server/locallogstore/RocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

/**
 * A request queue for the logstore monitor to send Requests to Workers.
 * It enforces a maximum number of inflight tasks, which in case exceeded,
 * new requests will be queued and retried the next time the monitor wakes up.
 *
 * Noted that Requests posted must call notifyRequestFinished() when they are
 * considered complete (either success, failed or dropped).
 */
class MonitorRequestQueue {
 public:
  MonitorRequestQueue(ServerProcessor* processor,
                      size_t max_in_flight_requests,
                      size_t max_queued_requests);

  /* Called by Workers to notify that a request has finished */
  void notifyRequestFinished();

  /**
   * Take the ownership and try to post a request to Worker threads
   *
   * If max_in_flight_requests_ is already reached, try buffering the request in
   * the queue. Requests will be dropped if the queue size reaches
   * max_queued_requests_.
   * Not thread-safe and can only be called by the LogStoreMonitor
   */
  void postRequest(std::unique_ptr<Request> req);

  /**
   * Called periodically by the logstore monitor, check if
   * there are buffered requests in the queue. If so, try posting
   * these requests to the Worker. Not thread-safe.
   */
  void checkQueue();

  /**
   * Callback wrapper class passed to Workers to call upon request
   * completion.
   */
  class MonitorRequestCallback {
   public:
    explicit MonitorRequestCallback(
        const std::shared_ptr<MonitorRequestQueue>& monitor_queue);
    void operator()(Status st);

   private:
    std::weak_ptr<MonitorRequestQueue> monitor_queue_;
  };

 private:
  // helper rountine that buffers a request
  void enqueue(std::unique_ptr<Request> req);

  // pointer to the Processer, for posting requests
  ServerProcessor* const processor_;

  // internal queue to buffer tasks, the only entity that accesses the
  // queue is the logstore monitor, so there is no need to protect it
  // with a lock
  std::queue<std::unique_ptr<Request>> queue_;

  // number of in-flight monitor requests, updated both by worker threads and
  // logstore monitor
  std::atomic<size_t> in_flight_requests_{0};

  const size_t max_in_flight_requests_;
  const size_t max_queued_requests_;
};

MonitorRequestQueue::MonitorRequestQueue(ServerProcessor* processor,
                                         size_t max_in_flight_requests,
                                         size_t max_queued_requests)
    : processor_(processor),
      max_in_flight_requests_(max_in_flight_requests),
      max_queued_requests_(max_queued_requests) {
  ld_check(processor != nullptr);
  ld_check(max_in_flight_requests > 0 && max_queued_requests > 0);
}

void MonitorRequestQueue::notifyRequestFinished() {
  in_flight_requests_--;
}

void MonitorRequestQueue::enqueue(std::unique_ptr<Request> req) {
  if (queue_.size() < max_queued_requests_) {
    queue_.push(std::move(req));
  }
  // max num of buffered reqs reached, drop the request
}

void MonitorRequestQueue::postRequest(std::unique_ptr<Request> req) {
  if (in_flight_requests_.load() < max_in_flight_requests_) {
    if (processor_->postRequest(req) == 0) {
      // processor takes the ownership, increase the in-flight count
      // noted that the logstore monitor only creates requests that only
      // issue one storage task, so here in_flight_requests_ is incremented
      // by one.
      in_flight_requests_++;
    } else if (err == E::NOBUFS) {
      // Processor is overloaded, enqueue the request to retry later
      enqueue(std::move(req));
    }
    // Internal error, drop the request
  } else {
    // too many requests in-flight, try buffering the request
    enqueue(std::move(req));
  }
}

void MonitorRequestQueue::checkQueue() {
  while (!queue_.empty() &&
         in_flight_requests_.load() < max_in_flight_requests_) {
    std::unique_ptr<Request> req = std::move(queue_.front());
    queue_.pop();
    // drop the request if post failed this time
    if (processor_->postRequest(req) == 0) {
      in_flight_requests_++;
    }
  }
}

MonitorRequestQueue::MonitorRequestCallback::MonitorRequestCallback(
    const std::shared_ptr<MonitorRequestQueue>& monitor_queue)
    : monitor_queue_(monitor_queue) {}

void MonitorRequestQueue::MonitorRequestCallback::operator()(Status /*st*/) {
  std::shared_ptr<MonitorRequestQueue> queue_ptr = monitor_queue_.lock();
  if (queue_ptr) {
    // notify the monitor_queue_ if it still exists
    // currently Status is ignored
    queue_ptr->notifyRequestFinished();
  }
}

LogStoreMonitor::LogStoreMonitor(
    ServerProcessor* processor,
    UpdateableSettings<LocalLogStoreSettings> settings)
    : processor_(processor),
      settings_(std::move(settings)),
      request_queue_(std::make_shared<MonitorRequestQueue>(
          processor,
          settings_->max_in_flight_monitor_requests,
          settings_->max_queued_monitor_requests)) {
  ld_check(processor != nullptr);
}

LogStoreMonitor::~LogStoreMonitor() {
  if (thread_.joinable()) {
    if (!should_stop_.load()) {
      stop();
    }
    thread_.join();
    ld_info("Logstore monitoring thread stopped.");
  }
}

void LogStoreMonitor::start() {
  if (thread_.joinable()) {
    if (!should_stop_.load()) {
      return;
    }
    thread_.join();
    should_stop_.store(false);
  }

  thread_ = std::thread([this] { threadMain(); });
  ld_info("Logstore monitoring thread started.");
}

void LogStoreMonitor::stop() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    should_stop_.store(true);
  }
  cv_.notify_all();
}

void LogStoreMonitor::checkFreeSpace() {
  boost::system::error_code code;

  ld_check(processor_->runningOnStorageNode());

  ShardedRocksDBLocalLogStore* const sharded_rocks_store =
      dynamic_cast<ShardedRocksDBLocalLogStore* const>(
          processor_->sharded_storage_thread_pool_->getShardedLocalLogStore());

  if (sharded_rocks_store == nullptr) {
    return;
  }

  const std::unordered_map<dev_t,
                           ShardedRocksDBLocalLogStore::DiskShardMappingEntry>&
      shards_to_disks = sharded_rocks_store->getShardToDiskMapping();

  for (auto& shards_to_disk : shards_to_disks) {
    boost::filesystem::path path = shards_to_disk.second.example_path;
    boost::filesystem::space_info info = boost::filesystem::space(path, code);
    if (code.value() != boost::system::errc::success) {
      ld_error("Error checking filesystem space for path %s: %s",
               path.c_str(),
               code.message().c_str());
      continue;
    }

    ld_check(info.capacity > 0);
    ld_check(settings_->free_disk_space_threshold > 0 &&
             settings_->free_disk_space_threshold < 1);

    bool no_space =
        info.free <= info.capacity * settings_->free_disk_space_threshold;

    std::vector<RocksDBLogStoreBase*> shards;
    for (auto shard_idx : shards_to_disk.second.shards) {
      LocalLogStore* store = sharded_rocks_store->getByIndex(shard_idx);
      RocksDBLogStoreBase* rocks_store_base =
          dynamic_cast<RocksDBLogStoreBase*>(store);

      // LocalLogStore is not a RocksDB local store or has persistent error
      if (rocks_store_base == nullptr ||
          rocks_store_base->acceptingWrites() == E::DISABLED) {
        continue;
      }
      shards.push_back(rocks_store_base);
    }

    // Skip disk if all shards on the disk aren't rocksDB local store nor have
    // persistent error
    if (shards.empty()) {
      continue;
    }

    // Calculate target trim points for space-based trimming, if it is enabled,
    // and mark node as full if we exceeded the space-based retention threshold
    bool space_based_trimming_threshold_exceeded = false;
    int ret = sharded_rocks_store->trimLogsBasedOnSpaceIfNeeded(
        shards_to_disk.second, info, &space_based_trimming_threshold_exceeded);

    for (RocksDBLogStoreBase* rocks_store_base : shards) {
      RocksDBLocalLogStore* rocks_store =
          dynamic_cast<RocksDBLocalLogStore*>(rocks_store_base);
      ld_debug(
          "RocksDB Shard[%d][disk:%s], rocks_store=(%p), rocks_store_base:%p, "
          "no_space:%s, ret=%d, space_based_trimming_threshold_exceeded:%s, "
          "crossed low-watermark:%s, hasEnoughSpaceForWrites():%s."
          " Capacity: %lu, Free %lu (%f), high watermark %f, ",
          rocks_store_base->getShardIdx(),
          path.c_str(),
          rocks_store,
          rocks_store_base,
          no_space ? "true" : "false",
          ret,
          space_based_trimming_threshold_exceeded ? "yes" : "no",
          rocks_store_base->crossedLowWatermark() ? "yes" : "no",
          rocks_store_base->hasEnoughSpaceForWrites() ? "yes" : "no",
          info.capacity,
          info.free,
          static_cast<double>(info.free) / info.capacity,
          settings_->free_disk_space_threshold);

      if (space_based_trimming_threshold_exceeded &&
          !rocks_store_base->crossedLowWatermark()) {
        rocks_store_base->setLowWatermarkForWrites(true);
      } else if (!space_based_trimming_threshold_exceeded &&
                 rocks_store_base->crossedLowWatermark()) {
        rocks_store_base->setLowWatermarkForWrites(false);
      }

      if (no_space && rocks_store_base->hasEnoughSpaceForWrites()) {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       1,
                       "Not enough disk space on RocksDB Shard [%d]: %s, "
                       "Capacity: %lu, Free %lu (%f), min threshold %f, "
                       "stop accepting writes for the shard",
                       rocks_store_base->getShardIdx(),
                       rocks_store_base->getDBPath().c_str(),
                       info.capacity,
                       info.free,
                       static_cast<double>(info.free) / info.capacity,
                       settings_->free_disk_space_threshold);
        rocks_store_base->setHasEnoughSpaceForWrites(false);
      } else if (!no_space && !rocks_store_base->hasEnoughSpaceForWrites()) {
        // have enough free space again, resume to accept writes
        RATELIMIT_INFO(std::chrono::seconds(1),
                       1,
                       "Got enough disk space on RocksDB Shard [%d]: %s, "
                       "Capacity: %lu, Free %lu (%f), min threshold %f, "
                       "resume accepting writes for the shard",
                       rocks_store_base->getShardIdx(),
                       rocks_store_base->getDBPath().c_str(),
                       info.capacity,
                       info.free,
                       static_cast<double>(info.free) / info.capacity,
                       settings_->free_disk_space_threshold);
        rocks_store_base->setHasEnoughSpaceForWrites(true);
      }

      if (rocks_store == nullptr) {
        continue;
      }

      // If the local rocksdb store meets the following conditions:
      //  (1) supports compactions
      //  (2) out of free disk space
      //  (3) num of level-0 files is below the threshold for compaction
      //  (4) not performed/scheduled a manual compaction for a certain
      //      period of time
      // schedule a manual compaction in hope of reclaiming some disk space
      if (no_space) {
        std::string val;
        static const std::string property{"rocksdb.num-files-at-level0"};

        if (!rocks_store->getDB().GetProperty(property, &val)) {
          ld_error("Cannot get property %s from rocksdb shard [%d]: %s.",
                   property.c_str(),
                   rocks_store_base->getShardIdx(),
                   rocks_store->getDBPath().c_str());
          // skip this rocksdb store
          continue;
        }

        int num = std::stoi(val);
        int threshold =
            rocks_store->getSettings()->level0_file_num_compaction_trigger;
        ld_check(num >= 0);

        if (num < threshold &&
            rocks_store->getLastCompactTime() +
                    settings_->manual_compact_interval <=
                std::chrono::steady_clock::now()) {
          RATELIMIT_INFO(std::chrono::seconds(60),
                         16,
                         "Performing manual compaction on RocksDB shard[%d]: "
                         "%s that is not accepting writes, num-l0-files: %d, "
                         "compaction threshold: %d",
                         rocks_store_base->getShardIdx(),
                         rocks_store->getDBPath().c_str(),
                         num,
                         threshold);

          // create and post a manual compaction request
          request_queue_->postRequest(std::make_unique<CompactionRequest>(
              rocks_store_base->getShardIdx(),
              MonitorRequestQueue::MonitorRequestCallback(request_queue_)));

          rocks_store->updateManualCompactTime();
        }
      }
    }
  }
}

void LogStoreMonitor::checkNeedRebuilding() {
  auto supervisor = processor_->rebuilding_supervisor_;
  if (!supervisor) {
    // Rebuilding is disabled.
    return;
  }

  ShardedLocalLogStore* sharded_store =
      processor_->sharded_storage_thread_pool_->getShardedLocalLogStore();
  ld_check(sharded_store);

  for (shard_index_t shard_idx = 0; shard_idx < sharded_store->numShards();
       ++shard_idx) {
    if (shards_need_rebuilding_.count(shard_idx)) {
      // Already requested.
      continue;
    }
    const LocalLogStore* store = sharded_store->getByIndex(shard_idx);
    ld_check(store);
    if (store->acceptingWrites() == E::DISABLED) {
      shards_need_rebuilding_.insert(shard_idx);
      supervisor->myShardNeedsRebuilding(shard_idx);
    }
  }
}

void LogStoreMonitor::threadMain() {
  ThreadID::set(ThreadID::Type::UTILITY, "ld:LogStoreMon");
  while (!should_stop_.load()) {
    // trying to post requsts buffered earlier
    request_queue_->checkQueue();
    // perform disk space related checks
    checkFreeSpace();
    // request rebuilding if needed
    checkNeedRebuilding();

    // sleep for a certain interval before the next iteration
    std::unique_lock<std::mutex> lock(mutex_);
    auto interval = settings_->logstore_monitoring_interval;
    cv_.wait_for(lock, interval, [&]() { return should_stop_.load(); });
  }
}

}} // namespace facebook::logdevice
