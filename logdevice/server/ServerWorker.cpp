/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/ServerWorker.h"

#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/PrincipalParser.h"
#include "logdevice/common/debug.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/ServerMessageDispatch.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/rebuilding/ChunkRebuilding.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsController.h"
#include "logdevice/server/sequencer_boycotting/NodeStatsControllerLocator.h"
#include "logdevice/server/storage/AllCachedDigests.h"
#include "logdevice/server/storage/PurgeScheduler.h"
#include "logdevice/server/storage/PurgeUncleanEpochs.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

namespace {
// the size of the bucket array of activePurges_ map
static constexpr size_t N_PURGES_MAP_BUCKETS = 128 * 1024;
// the delay between each check if this node is a controller node or not
static constexpr std::chrono::seconds CONTROLLER_LOCATOR_DELAY{60};
} // namespace

class ServerWorkerImpl {
 public:
  explicit ServerWorkerImpl(ServerWorker* w)
      : cachedDigests_(
            w->immutable_settings_->max_active_cached_digests,
            w->immutable_settings_->max_cached_digest_record_queued_kb),
        activePurges_(w->immutable_settings_->server ? N_PURGES_MAP_BUCKETS
                                                     : 1) {}

  AllCachedDigests cachedDigests_;
  PurgeUncleanEpochsMap activePurges_;
  SettingOverrideTTLRequestMap activeSettingOverrides_;
  ChunkRebuildingMap runningChunkRebuildings_;

  /**
   * Should only be instantiated on a single worker, decided by
   * NodeStatsController::getThreadAffinity
   */
  std::unique_ptr<NodeStatsController> node_stats_controller_;
  /**
   * Once executed, decides if this node is a controller or not
   * have to be destructed before the node_stats_controller_
   */
  Timer node_stats_controller_locator_timer_;
};

ServerWorker::ServerWorker(WorkContext::KeepAlive event_loop,
                           ServerProcessor* processor,
                           worker_id_t idx,
                           const std::shared_ptr<UpdateableConfig>& config,
                           StatsHolder* stats = nullptr,
                           WorkerType type = WorkerType::GENERAL)
    : Worker(std::move(event_loop),
             processor,
             idx,
             config,
             stats,
             type,
             ThreadID::SERVER_WORKER),
      processor_(processor),
      impl_(new ServerWorkerImpl(this)) {
  server_read_streams_ = std::make_unique<AllServerReadStreams>(
      processor->updateableSettings(),
      immutable_settings_->read_storage_tasks_max_mem_bytes /
          immutable_settings_->num_workers,
      idx_,
      processor->runningOnStorageNode() ? &processor->getLogStorageStateMap()
                                        : nullptr,
      processor,
      stats);

  if (processor_->runningOnStorageNode()) {
    purge_scheduler_.reset(new PurgeScheduler(processor_));

    // Create a PerWorkerStorageTaskQueue for every database shard
    const shard_size_t nshards =
        processor_->sharded_storage_thread_pool_->numShards();

    for (int i = 0; i < nshards; ++i) {
      std::unique_ptr<PerWorkerStorageTaskQueue> task_queue(
          // may throw
          new PerWorkerStorageTaskQueue(
              i,
              immutable_settings_->max_inflight_storage_tasks,
              immutable_settings_->per_worker_storage_task_queue_size));
      storage_task_queues_.push_back(std::move(task_queue));
    }
  }
}

ServerWorker::~ServerWorker() {
  impl_->node_stats_controller_locator_timer_.cancel();
  impl_->node_stats_controller_.reset();
  ld_check(activePurges().map.empty());
  activeSettingOverrides().map.clear();
  storage_task_queues_.clear();
}

std::unique_ptr<MessageDispatch> ServerWorker::createMessageDispatch() {
  return std::make_unique<ServerMessageDispatch>(processor_);
}

void ServerWorker::subclassFinishWork() {
  // Stop any PurgeUncleanEpochs state machines running on this worker.  These
  // are potentially long-running state machines so we want to abort them
  // during shutdown, and they create storage tasks and send messages so we
  // want to abort them early.
  if (!activePurges().map.empty()) {
    ld_info("Aborting %lu purges", activePurges().map.size());
    activePurges().map.clearAndDispose();
  }

  if (!runningChunkRebuildings().map.empty()) {
    ld_info(
        "Aborting %lu chunk rebuildings", runningChunkRebuildings().map.size());
    runningChunkRebuildings().map.clear();
  }
}

void ServerWorker::subclassWorkFinished() {
  // Destructors for ClientReadStream, ServerReadStream and
  // PerWorkerStorageTaskQueue may involve communication, so take care of
  // that before we tear down the messaging fabric.
  server_read_streams_->clear();
  for (auto& q : storage_task_queues_) {
    ld_check(q);
    q->drop(StorageTask::ThreadType::SLOW);
    q->drop(StorageTask::ThreadType::FAST_STALLABLE);
    q->drop(StorageTask::ThreadType::FAST_TIME_SENSITIVE);
    q->drop(StorageTask::ThreadType::DEFAULT);
  }
}

void ServerWorker::noteShuttingDownNoPendingRequests() {
  impl_->cachedDigests_.clear();
}

//
// Pimpl getters
//

AllCachedDigests& ServerWorker::cachedDigests() const {
  return impl_->cachedDigests_;
}

PurgeUncleanEpochsMap& ServerWorker::activePurges() const {
  return impl_->activePurges_;
}

SettingOverrideTTLRequestMap& ServerWorker::activeSettingOverrides() const {
  return impl_->activeSettingOverrides_;
}

ChunkRebuildingMap& ServerWorker::runningChunkRebuildings() const {
  return impl_->runningChunkRebuildings_;
}

AllServerReadStreams& ServerWorker::serverReadStreams() const {
  ld_assert(server_read_streams_.get());
  return *server_read_streams_;
}

void ServerWorker::onSettingsUpdated() {
  Worker::onSettingsUpdated();
  server_read_streams_->setMemoryBudget(
      immutable_settings_->read_storage_tasks_max_mem_bytes /
      immutable_settings_->num_workers);
  if (server_read_streams_) {
    server_read_streams_->onSettingsUpdate();
  }
}

void ServerWorker::onServerConfigUpdated() {
  ld_check(ServerWorker::onThisThread() == this);
  auto p = processor_;
  auto failure_detector = p->failure_detector_.get();
  if (failure_detector) {
    if (worker_type_ == WorkerType::FAILURE_DETECTOR) {
      // this thread is where failure detector got attached
      failure_detector->noteConfigurationChanged();
    }
  }
  Worker::onServerConfigUpdated();
}

void ServerWorker::setupWorker() {
  Worker::setupWorker();
  server_read_streams_->registerForShardAuthoritativeStatusUpdates();
  if ((idx_.val() ==
       NodeStatsController::getThreadAffinity(
           processor_->getWorkerCount(worker_type_))) &&
      (worker_type_ == WorkerType::GENERAL)) {
    initializeNodeStatsController();
  }
}

PerWorkerStorageTaskQueue*
ServerWorker::getStorageTaskQueueForShard(shard_index_t shard) const {
  ld_check(shard >= 0 && shard < storage_task_queues_.size());
  return storage_task_queues_[shard].get();
}

StorageThreadPool&
ServerWorker::getStorageThreadPoolForShard(shard_index_t shard) const {
  return processor_->sharded_storage_thread_pool_->getByIndex(shard);
}

NodeStatsControllerCallback* ServerWorker::nodeStatsControllerCallback() const {
  return impl_->node_stats_controller_.get();
}

void ServerWorker::initializeNodeStatsController() {
  impl_->node_stats_controller_ = std::make_unique<NodeStatsController>();
  impl_->node_stats_controller_locator_timer_.assign(
      [this, timer = &impl_->node_stats_controller_locator_timer_] {
        auto max_boycott_count =
            this->processor_->settings()
                ->sequencer_boycotting.node_stats_max_boycott_count;
        ld_check(this->getServerConfig() != nullptr);
        auto my_node_id = this->processor_->getMyNodeID();

        auto controller = this->impl_->node_stats_controller_.get();
        if (max_boycott_count == 0 ||
            !NodeStatsControllerLocator{}.isController(
                my_node_id, max_boycott_count + 1)) {
          if (controller->isStarted()) {
            ld_info("This node is not a stats controller anymore");
            WORKER_STAT_DECR(is_controller);
            this->impl_->node_stats_controller_->stop();
          }
        } else {
          if (!controller->isStarted()) {
            ld_info("This node is a stats controller now");

            WORKER_STAT_INCR(is_controller);
            controller->start();
          }
        }

        timer->activate(
            this->settings()
                .sequencer_boycotting.node_stats_controller_check_period);
      });

  impl_->node_stats_controller_locator_timer_.activate(
      settings().sequencer_boycotting.node_stats_controller_check_period);
}
}} // namespace facebook::logdevice
