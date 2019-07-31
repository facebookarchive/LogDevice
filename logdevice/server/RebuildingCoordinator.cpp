/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingCoordinator.h"

#include <folly/hash/Hash.h>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/RebuildingSupervisor.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/AllServerReadStreams.h"
#include "logdevice/server/rebuilding/ShardRebuildingV1.h"
#include "logdevice/server/rebuilding/ShardRebuildingV2.h"
#include "logdevice/server/storage_tasks/PerWorkerStorageTaskQueue.h"
#include "logdevice/server/storage_tasks/ReadStorageTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/storage_tasks/StorageThreadPool.h"

namespace facebook { namespace logdevice {

class ShardedLocalLogStore;

namespace {

/**
 * A storage task used to write a marker in a local log store to indicate that
 * it was rebuilt.
 */
class WriteShardRebuildingCompleteMetadataTask : public StorageTask {
 public:
  explicit WriteShardRebuildingCompleteMetadataTask(
      RebuildingCoordinator* owner,
      lsn_t version)
      : StorageTask(StorageTask::Type::REBUILDING_WRITE_COMPLETE_METADATA),
        owner_(owner),
        version_(version) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override {
    auto& store = storageThreadPool_->getLocalLogStore();
    if (store.acceptingWrites() == E::DISABLED) {
      RATELIMIT_INFO(std::chrono::seconds(10),
                     10,
                     "Not writing RebuildingCompleteMetadata for disabled "
                     "shard %u",
                     storageThreadPool_->getShardIdx());
      status_ = E::DISABLED;
      return;
    }

    // With rebuilding complete, clear any dirty time ranges
    // for the shard.
    LocalLogStore::WriteOptions options;
    RebuildingRangesMetadata range_metadata;
    int rv = storageThreadPool_->getLocalLogStore().writeStoreMetadata(
        range_metadata, options);
    if (rv != 0) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Could not write RebuildingRangesMetadata for "
                      "shard %u: %s",
                      storageThreadPool_->getShardIdx(),
                      error_description(err));
      status_ = err;
      return;
    }

    // Mark rebuilding complete.
    RebuildingCompleteMetadata complete_metadata;
    rv = storageThreadPool_->getLocalLogStore().writeStoreMetadata(
        complete_metadata, options);
    if (rv != 0) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Could not write RebuildingCompleteMetadata for "
                      "shard %u: %s",
                      storageThreadPool_->getShardIdx(),
                      error_description(err));
      status_ = err;
      return;
    }
    status_ = E::OK;
  }

  void onDone() override {
    owner_->onMarkerWrittenForShard(
        storageThreadPool_->getShardIdx(), version_, status_);
  }

  void onDropped() override {
    ld_check(false);
  }
  bool isDroppable() const override {
    return false;
  }

 private:
  RebuildingCoordinator* owner_;
  lsn_t version_;
  Status status_{E::INTERNAL};
};

/**
 * A request to inform AllServerReadStreams running on a worker thread that a
 * shard was rebuilt. AllServerReadStreams will wake up all read streams that
 * were stalled waiting for this event to happen.
 */
class WakeUpServerReadStreamsRequest : public Request {
 public:
  WakeUpServerReadStreamsRequest(int worker_idx, uint32_t shard_idx)
      : Request(RequestType::WAKEUP_SERVER_READ_STREAMS),
        worker_idx_(worker_idx),
        shard_idx_(shard_idx) {}

  Request::Execution execute() override {
    ServerWorker::onThisThread()->serverReadStreams().onShardRebuilt(
        shard_idx_);
    return Execution::COMPLETE;
  }

  int getThreadAffinity(int /*nthreads*/) override {
    return worker_idx_;
  }

 private:
  int worker_idx_;
  uint32_t shard_idx_;
};

} // end of anonymous namespace

RebuildingCoordinator::RebuildingCoordinator(
    const std::shared_ptr<UpdateableConfig>& config,
    EventLogStateMachine* event_log,
    Processor* processor,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    UpdateableSettings<AdminServerSettings> admin_settings,
    ShardedLocalLogStore* sharded_store)
    : config_(config),
      event_log_(event_log),
      processor_(processor),
      rebuildingSettings_(rebuilding_settings),
      adminSettings_(admin_settings),
      shardedStore_(sharded_store) {}

int RebuildingCoordinator::start() {
  writer_ = std::make_unique<EventLogWriter>(event_log_);
  maintenance_log_writer_ =
      std::make_unique<maintenance::MaintenanceLogWriter>(processor_);

  myNodeId_ = getMyNodeID().index();

  populateDirtyShardCache(dirtyShards_);

  if (checkMarkers() != 0) {
    return -1;
  }

  class InitRequest : public Request {
   public:
    explicit InitRequest(RebuildingCoordinator* self)
        : Request(RequestType::REBUILDING_COORDINATOR_INIT_REQUEST),
          self_(self) {}
    Execution execute() override {
      self_->startOnWorkerThread();
      return Execution::COMPLETE;
    }
    int getThreadAffinity(int /* unused */) override {
      return 0;
    }
    RebuildingCoordinator* self_;
  };

  if (processor_) { // may be nullptr in tests
    std::unique_ptr<Request> req = std::make_unique<InitRequest>(this);
    processor_->postWithRetrying(req);
  }

  return 0;
}

void RebuildingCoordinator::subscribeToEventLogIfNeeded() {
  if (!started_) {
    if (config_->getLogsConfig()->isFullyLoaded()) {
      subscribeToEventLog();
      started_ = true;
    } else {
      ld_info("RebuildingCoordinator did not start yet because LogsConfig is "
              "not fully loaded yet");
    }
  }
}

void RebuildingCoordinator::startOnWorkerThread() {
  Worker* w = Worker::onThisThread();
  w->setRebuildingCoordinator(this);
  my_worker_id_ = w->idx_;

  // initialize the counter to 0. it will be updated based on the
  // rebuilding set.
  WORKER_STAT_SET(rebuilding_waiting_for_recoverable_shards, 0);

  scheduledRestarts_.reserve(numShards());
  for (shard_index_t s = 0; s < numShards(); ++s) {
    auto timer = std::make_unique<Timer>([self = this, shard = s] {
      self->restartForShard(shard, self->event_log_->getCurrentRebuildingSet());
    });
    scheduledRestarts_.emplace_back(std::move(timer));
  }

  subscribeToEventLogIfNeeded();

  rebuildingSettingsSubscription_ = rebuildingSettings_.subscribeToUpdates(
      [this, processor = w->processor_, worker_id = my_worker_id_] {
        // Forward the call back to RebuildingCoordinator's worker thread.
        std::unique_ptr<Request> rq = std::make_unique<FuncRequest>(
            my_worker_id_,
            WorkerType::GENERAL,
            RequestType::REBUILDING_COORDINATOR_SETTINGS_UPDATED,
            [this] {
              // Note that RebuildingCoordinator can't have been destroyed
              // because we're running on a worker thread, and worker threads
              // are destroyed before RebuildingCoordinator.
              noteRebuildingSettingsChanged();
            });
        // Note that, even during server shutdown, at this point processor can't
        // have been destroyed because RebuildingCoordinator::shutdown()
        // unsubscribes, and it happens before processor is destroyed.
        int rv = processor->postImportant(rq);
        if (rv != 0) {
          ld_check_eq(err, E::SHUTDOWN);
        }
      });

  nonAuthoratitiveRebuildingChecker_ =
      std::make_unique<NonAuthoritativeRebuildingChecker>(
          rebuildingSettings_, event_log_, myNodeId_);
}

void RebuildingCoordinator::shutdown() {
  // Among other things, make sure to destroy/cancel all timers here.
  // Otherwise the destructor will try to cancel the timer from a wrong thread.
  rebuildingSettingsSubscription_.unsubscribe();
  scheduledRestarts_.clear();
  handle_.reset();
  shardsRebuilding_.clear();
  writer_.reset();
  nonAuthoratitiveRebuildingChecker_.reset();
  planning_timer_.reset();
  shuttingDown_ = true;
}

void RebuildingCoordinator::noteConfigurationChanged() {
  // NOTE: we don't care about node config changes. A node is not removed from
  // the rebuilding set if it was removed from the config.

  subscribeToEventLogIfNeeded();

  for (auto& it : shardsRebuilding_) {
    ShardState& shard_state = it.second;
    if (shard_state.shardRebuilding != nullptr) {
      shard_state.shardRebuilding->noteConfigurationChanged();
    }
  }
}

void RebuildingCoordinator::noteRebuildingSettingsChanged() {
  for (auto& s : shardsRebuilding_) {
    if (s.second.shardRebuilding != nullptr) {
      s.second.shardRebuilding->noteRebuildingSettingsChanged();
    }

    // If global window size was decreased (e.g. global window got enabled) we
    // may want to write a SHARD_DONOR_PROGRESS event for some shards.
    notifyShardDonorProgress(
        s.first, s.second.myProgress, s.second.version, -1);
    // If global window size was increased (e.g. global window got disabled) we
    // may want to advance global window end.
    if (!restartIsScheduledForShard(s.first)) {
      trySlideGlobalWindow(s.first, event_log_->getCurrentRebuildingSet());
    }
  }
}

int RebuildingCoordinator::checkMarkers() {
  if (getMyNodeID().generation() <= 1) {
    for (uint32_t shard = 0; shard < numShards(); ++shard) {
      RebuildingCompleteMetadata metadata;
      LocalLogStore::WriteOptions options;
      LocalLogStore* store = shardedStore_->getByIndex(shard);
      if (store->acceptingWrites() != E::DISABLED) {
        notifyProcessorShardRebuilt(shard);
      }
      int rv = store->writeStoreMetadata(metadata, options);
      if (rv != 0) {
        ld_error("Could not write RebuildingCompleteMetadata for shard %u: %s",
                 shard,
                 error_description(err));
        if (store->acceptingWrites() != E::DISABLED) {
          // This shouldn't really happen with current LocalLogStore
          // implementations because a failed write transitions the store to
          // a disabled state.
          return -1;
        }
      }
    }
    return 0;
  }

  for (shard_index_t shard = 0; shard < numShards(); ++shard) {
    LocalLogStore* store = shardedStore_->getByIndex(shard);
    RebuildingCompleteMetadata meta;
    int rv = store->readStoreMetadata(&meta);
    if (rv == 0) {
      notifyProcessorShardRebuilt(shard);
    } else if (err == E::NOTFOUND) {
      ld_info("Did not find RebuildingCompleteMetadata for shard %u. Waiting "
              "for the shard to be rebuilt...",
              shard);

      // Request rebuilding of the shard.
      auto supervisor = processor_->rebuilding_supervisor_;
      ld_check(supervisor);
      supervisor->myShardNeedsRebuilding(shard);
    } else {
      // It's likely that the failing disk on which this shard resides has not
      // been repaired yet. Once the disk is repaired, logdeviced will be
      // restarted and we will try reading the marker again.
      ld_error("Error reading RebuildingCompleteMetadata for shard %u: %s",
               shard,
               error_description(err));
      if (store->acceptingWrites() != E::DISABLED) {
        return -1;
      }
    }
  }

  return 0;
}

void RebuildingCoordinator::populateDirtyShardCache(DirtyShardMap& map) {
  for (uint32_t shard = 0; shard < numShards(); ++shard) {
    RebuildingRangesMetadata meta;
    LocalLogStore* store = shardedStore_->getByIndex(shard);
    int rv = store->readStoreMetadata(&meta);
    if (rv != 0) {
      if (err != E::NOTFOUND) {
        ld_error("Could not read RebuildingRangesMetadata for shard %u: %s",
                 shard,
                 error_description(err));
        ld_check(store->acceptingWrites() == E::DISABLED);
      }
      // If we can't read from the shard, we'll be doing a full rebuild.
      // There's no point in claiming we have dirty state to publish/rebuild.
      processor_->markShardClean(shard);
    } else if (!meta.empty()) {
      map[shard] = meta;
    } else {
      processor_->markShardClean(shard);
    }
  }
}

RebuildingCoordinator::~RebuildingCoordinator() {
  // shutdown() must have ensured that all ShardRebuilding objects are destroyed
  // from the correct worker thread.
  ld_check(shardsRebuilding_.empty());
}

void RebuildingCoordinator::writeMarkerForShard(uint32_t shard, lsn_t version) {
  ld_info("Writing marker for shard %u, version %s",
          shard,
          lsn_to_string(version).c_str());
  auto task =
      std::make_unique<WriteShardRebuildingCompleteMetadataTask>(this, version);
  auto task_queue =
      ServerWorker::onThisThread()->getStorageTaskQueueForShard(shard);
  task_queue->putTask(std::move(task));
}

void RebuildingCoordinator::onMarkerWrittenForShard(uint32_t shard,
                                                    lsn_t version,
                                                    Status status) {
  if (status != E::OK) {
    ld_error("Error writting RebuildingCompleteMetadata for shard %u.", shard);
    // It's likely that the failing disk on which this shard resides has not
    // been replaced yet. Once the disk is replaced, logdeviced will be
    // restarted and we will try writting the marker again.
    return;
  }

  // Write RemoveMaintenance request if ClusterMaintenanceStateMachine
  // is enabled
  if (adminSettings_->enable_cluster_maintenance_state_machine) {
    writeRemoveMaintenance(ShardID(myNodeId_, shard));
  }
  notifyProcessorShardRebuilt(shard);
  notifyAckMyShardRebuilt(shard, version);
  wakeUpReadStreams(shard);
}

void RebuildingCoordinator::wakeUpReadStreams(uint32_t shard) {
  const int nworkers = processor_->getWorkerCount(WorkerType::GENERAL);
  for (int i = 0; i < nworkers; ++i) {
    std::unique_ptr<Request> req =
        std::make_unique<WakeUpServerReadStreamsRequest>(i, shard);
    processor_->postWithRetrying(req);
  }
}

void RebuildingCoordinator::notifyProcessorShardRebuilt(uint32_t shard) {
  if (!processor_->isDataMissingFromShard(shard) && !myShardIsDirty(shard)) {
    ld_warning("Shard %u was taking writes while its rebuilding was in "
               "progress. This can lead to underreplicated records "
               "(see task t10343616).",
               shard);
  }

  processor_->markShardAsNotMissingData(shard);
}

void RebuildingCoordinator::onShardRebuildingComplete(uint32_t shard_idx) {
  auto& shard_state = getShardState(shard_idx);

  /*
   * We are done rebuilding the shard. If the setting
   * disable-data-log-rebuilding was enabled, then we just skipped over the data
   * logs that have a retention specified. In that case, we want to delay
   * sending the SHARD_IS_REBULT message until the data logs expire. The timer
   * below handles that for us.
   */

  auto now = RecordTimestamp::now();
  RecordTimestamp maxBacklogTS =
      RecordTimestamp(shard_state.rebuilding_started_ts) +
      shard_state.max_rebuild_by_retention_backlog;

  auto delay_ms = std::chrono::milliseconds::zero();
  if (maxBacklogTS > now) {
    // Planning stage requested a delay.
    ld_check(rebuildingSettings_->disable_data_log_rebuilding);
    ld_check(shard_state.rebuilding_started_ts.count());
    delay_ms = maxBacklogTS - now;
  }

  // Stagger the messages only if the setting is enabled
  const auto& delay = rebuildingSettings_->shard_is_rebuilt_msg_delay;
  if (delay.hi.count()) {
    std::chrono::seconds rand_delay(
        folly::Random::rand32(delay.lo.count(), delay.hi.count()));
    delay_ms += rand_delay;
  }

  if (delay_ms.count()) {
    ld_check(!shard_state.shardIsRebuiltDelayTimer.isActive());

    maxBacklogTS += delay_ms;
    ld_info("%srequested a delay until %s for shard %u, "
            "before declaring the shard as rebuilt. Delaying "
            "SHARD_IS_REBUILT message by %ld ms",
            maxBacklogTS > now ? "delaying for retention: planning "
                               : "delaying to stagger: ",
            maxBacklogTS.toString().c_str(),
            shard_idx,
            delay_ms.count());

    shard_state.shardIsRebuiltDelayTimer.activate(delay_ms);
  } else {
    notifyShardRebuiltCB(shard_idx);
  }
}

void RebuildingCoordinator::notifyShardRebuiltCB(uint32_t shard_idx) {
  auto& shard_state = getShardState(shard_idx);

  ld_check(shard_state.participating);
  ld_check(shard_state.logsWithPlan.empty());

  notifyShardRebuilt(
      shard_idx, shard_state.version, shard_state.isAuthoritative);

  shard_state.shardRebuilding.reset();
  shard_state.planner.reset();
  shard_state.logsWithPlan.clear();
  shard_state.participating = false;
  shard_state.progressStat.setValue((int64_t)1e6);

  shard_state.max_rebuild_by_retention_backlog =
      std::chrono::milliseconds::zero();
  shard_state.rebuilding_started_ts = std::chrono::milliseconds::zero();
  shard_state.shardIsRebuiltDelayTimer.cancel();
}

void RebuildingCoordinator::trySlideGlobalWindow(
    uint32_t shard,
    const EventLogRebuildingSet& set) {
  auto& shard_state = getShardState(shard);

  if (!shard_state.participating) {
    return;
  }

  const RecordTimestamp::duration global_window =
      rebuildingSettings_->global_window;

  const auto* rsi = set.getForShardOffset(shard);
  ld_check(rsi);

  // Recompute the minimum next timestamp across all nodes.
  RecordTimestamp min_next_timestamp = RecordTimestamp::max();
  for (auto& n : rsi->donor_progress) {
    min_next_timestamp =
        std::min(min_next_timestamp, RecordTimestamp(n.second));
  }

  // Calculate new_global_window_end = min_next_timestamp + global_window,
  // but avoid overflow.
  RecordTimestamp new_global_window_end;
  if (global_window == RecordTimestamp::duration::max()) {
    // Treat global_window = max() as infinity.
    new_global_window_end = RecordTimestamp::max();
  } else if (min_next_timestamp == RecordTimestamp::min()) {
    // If some donors haven't made any progress yet, don't bother moving global
    // window from min() to min() + window.
    new_global_window_end = RecordTimestamp::min();
  } else if (min_next_timestamp.toMilliseconds().count() > 0 &&
             RecordTimestamp::max() - min_next_timestamp < global_window) {
    // Addition would overflow, clamp to max().
    new_global_window_end = RecordTimestamp::max();
  } else {
    new_global_window_end = min_next_timestamp + global_window;
  }

  if (new_global_window_end == shard_state.globalWindowEnd) {
    // The global window is not slid.
    // Note that if the window size setting is decreased at runtime we _do_
    // decrease the window end. This allows enabling global window on the fly.
    return;
  }

  RecordTimestamp old_window_end = shard_state.globalWindowEnd;
  shard_state.globalWindowEnd = new_global_window_end;
  ld_info(
      "Moving global window from %s to %s for shard %u and rebuilding set %s",
      format_time(old_window_end).c_str(),
      format_time(shard_state.globalWindowEnd).c_str(),
      shard,
      shard_state.rebuildingSet->describe().c_str());
  PER_SHARD_STAT_SET(getStats(),
                     rebuilding_global_window_end,
                     shard,
                     shard_state.globalWindowEnd.toMilliseconds().count());
  if (old_window_end != RecordTimestamp::min()) {
    PER_SHARD_STAT_ADD(
        getStats(), rebuilding_global_window_slide_num, shard, 1);
    PER_SHARD_STAT_ADD(
        getStats(),
        rebuilding_global_window_slide_total,
        shard,
        RecordTimestamp(shard_state.globalWindowEnd - old_window_end)
            .toMilliseconds()
            .count());
  }

  if (shard_state.shardRebuilding != nullptr) {
    shard_state.shardRebuilding->advanceGlobalWindow(
        shard_state.globalWindowEnd);
  }
}

void RebuildingCoordinator::onShardDonorProgress(
    node_index_t /*donor_node_idx*/,
    uint32_t shard_idx,
    RecordTimestamp /* next_ts */,
    lsn_t version,
    const EventLogRebuildingSet& set) {
  auto it_shard = shardsRebuilding_.find(shard_idx);
  if (it_shard == shardsRebuilding_.end()) {
    // We don't care about that information because we already finished
    // rebuilding the shard.
    return;
  }
  auto& shard_state = it_shard->second;

  if (shard_state.version != version || !shard_state.participating) {
    // This means the donor node sent this event before it received a
    // SHARD_NEEDS_REBUILD or SHARD_ABORT_REBUILD event.
    return;
  }

  // We may be able to slide the global timestamp window.
  trySlideGlobalWindow(shard_idx, set);
}

bool RebuildingCoordinator::shouldRebuildMetadataLogs(uint32_t shard_idx) {
  const auto& nodes_configuration = config_->getNodesConfiguration();
  const auto& nodes =
      nodes_configuration->getStorageMembership()->getMetaDataNodeIndices();

  // Don't schedule metadata logs for rebuilding if this node is not in their
  // nodeset.
  if (std::find(nodes.begin(), nodes.end(), myNodeId_) == nodes.end()) {
    return false;
  }

  // Otherwise, schedule them if at least one node in the rebuilding set is in
  // the metadata log nodeset.
  auto& shard_state = getShardState(shard_idx);
  for (auto& i : shard_state.rebuildingSet->shards) {
    if (std::find(nodes.begin(), nodes.end(), i.first.node()) != nodes.end()) {
      return true;
    }
  }
  return false;
}

bool RebuildingCoordinator::restartIsScheduledForShard(uint32_t shard_idx) {
  if (shard_idx >= numShards()) {
    return false;
  }

  auto& timer = scheduledRestarts_[shard_idx];
  ld_check(timer);
  return timer->isActive();
}

void RebuildingCoordinator::scheduleRestartForShard(uint32_t shard_idx) {
  if (shard_idx >= numShards()) {
    ld_error("Received request to rebuild shard %u, but there are"
             " only %lu shards",
             shard_idx,
             numShards());
    return;
  }

  auto& timer = scheduledRestarts_[shard_idx];
  ld_check(timer);
  timer->activate(rebuildingSettings_->rebuilding_restarts_grace_period);
  ld_info("Scheduling a restart for shard %u after %lums",
          shard_idx,
          rebuildingSettings_->rebuilding_restarts_grace_period.count());
}

void RebuildingCoordinator::restartForShard(uint32_t shard_idx,
                                            const EventLogRebuildingSet& set) {
  if (shard_idx >= numShards()) {
    ld_error("Received request to rebuild shard %u, but there are"
             " only %lu shards",
             shard_idx,
             numShards());
    return;
  }

  if (shardsRebuilding_.count(shard_idx)) {
    auto& shard_state = getShardState(shard_idx);
    ld_check(shard_state.restartVersion <= set.getLastSeenLSN());
    if (shard_state.restartVersion == set.getLastSeenLSN()) {
      // Event log state was updated but last seen LSN didn't increase.
      // We are probably here because of a bug in ReplicatedStateMachine causing
      // the callback to be called more than once for the same state version.
      // Let's not restart rebuilding.
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Not restarting rebuilding for shard %u because "
                      "restartVersion has not changed: %s",
                      shard_idx,
                      lsn_to_string(set.getLastSeenLSN()).c_str());
      // ld_check(false); // TODO(T17286647): add this assert back.
      return;
    }

    // Cancel the current rebuilding.
    abortShardRebuilding(shard_idx);
  }

  const auto* rsi = set.getForShardOffset(shard_idx);
  if (rsi == nullptr) {
    // The shard is not in rebuilding set anymore, presumably because
    // the rebuilding was acked. Remove the shard's state.
    shardsRebuilding_.erase(shard_idx);
    return;
  }

  std::shared_ptr<RebuildingSet> rebuildingSet =
      std::make_shared<RebuildingSet>();
  rebuildingSet->all_dirty_time_intervals = rsi->all_dirty_time_intervals;
  if (!rebuildingSet->all_dirty_time_intervals.empty()) {
    normalizeTimeRanges(shard_idx, rebuildingSet->all_dirty_time_intervals);
  }

  bool my_shard_draining = false;

  for (auto& node : rsi->nodes_) {
    if (!node.second.acked) {
      ShardID shard(node.first, shard_idx);
      rebuildingSet->shards.emplace(
          shard,
          RebuildingNodeInfo(node.second.dc_dirty_ranges, node.second.mode));
      if (node.second.auth_status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        rebuildingSet->empty.insert(shard);
      }
    }

    if (node.first == myNodeId_) {
      my_shard_draining = node.second.drain;
    }
  }

  ld_check(!rebuildingSet->shards.empty());

  // Create a new ShardState object.
  shardsRebuilding_[shard_idx] = ShardState{};
  auto& shard_state = getShardState(shard_idx);
  shard_state.rebuildingSet = rebuildingSet;
  shard_state.restartVersion = set.getLastSeenLSN();
  shard_state.recoverableShards =
      set.getForShardOffset(shard_idx)->num_recoverable_;
  shard_state.rebuildingSetContainsMyself =
      rebuildingSet->shards.find(ShardID(myNodeId_, shard_idx)) !=
      rebuildingSet->shards.end();
  shard_state.version = rsi->version;
  shard_state.rebuilding_started_ts =
      set.getLastSeenShardNeedsRebuildTS(shard_idx);
  shard_state.progressStat.assign(
      getStats(), &PerShardStats::rebuilding_progress_ppm, shard_idx, 0);

  // Install a delay timer to support the feature to skip rebuilding data logs
  // as well as to stagger sending SHARD_IS_REBUILT messages. If the
  // disable_data_log_rebuilding setting is enabled, the timer just delays the
  // SHARD_IS_REBUILT message until after all the data on this shard has
  // expired. In addition the timer is used to delay sending the messages by a
  // random time period in the range specified in the setting
  // shard_is_rebuilt_msg_delay.
  auto cb = [self = this, shard = shard_idx]() {
    self->notifyShardRebuiltCB(shard);
  };

  if (rebuildingSettings_->disable_data_log_rebuilding ||
      rebuildingSettings_->shard_is_rebuilt_msg_delay.hi.count()) {
    shard_state.shardIsRebuiltDelayTimer.assign(cb);
  }

  if (shard_state.rebuildingSetContainsMyself) {
    // Increment rebuilding_set_contains_myself stat.
    shard_state.rebuildingSetContainsMyselfStat.assign(
        getStats(), &PerShardStats::rebuilding_set_contains_myself, shard_idx);
    auto& s = rebuildingSet->shards.at(ShardID(myNodeId_, shard_idx));
    if (s.mode == RebuildingMode::RESTORE && s.dc_dirty_ranges.empty()) {
      // Increment full_restore_set_contains_myself stat if we're in RESTORE
      // mode and not mini-rebuilding.
      shard_state.restoreSetContainsMyselfStat.assign(
          getStats(),
          &PerShardStats::full_restore_set_contains_myself,
          shard_idx);
    }
  }

  if (shouldAcknowledgeRebuilding(shard_idx, set)) {
    writeMarkerForShard(shard_idx, shard_state.version);
  } else if (shard_state.rebuildingSetContainsMyself) {
    // One of my shards is being rebuilt... Let's check several things:
    // 1/ If the shard is actually functioning, has the marker, is not being
    //    drained, and is not dirty, we can abort the rebuilding;
    // 2/ If the shard is actually functioning, has the marker, is being drained
    //    in RESTORE mode, and is not dirty, the drain can be restarted in
    //    RELOCATE mode as we did not lose all data;
    // 3/ If the shard is rebuilding in RESTORE mode, is actually functioning
    //    but has no marker (e.g. a broken disk was replaced with a new one), do
    //    not abort rebuilding because data was lost, but mark the data as
    //    unrecoverable. This will make readers unstall if they were stalled
    //    hoping the data would be recovered due to the current rebuilding being
    //    non-authoritative.  Readers may in that case see dataloss.
    const bool is_restore =
        rebuildingSet->shards.at(ShardID(myNodeId_, shard_idx)).mode ==
        RebuildingMode::RESTORE;
    if (myShardHasDataIntact(shard_idx)) {
      // I am in the rebuilding set and my data is intact
      // Write a remove maintenance if I am rebuilding in
      // restore mode and ClusterMaintenanceStateMachine
      // is enabled.
      if (adminSettings_->enable_cluster_maintenance_state_machine) {
        writeRemoveMaintenance(ShardID(myNodeId_, shard_idx));
      }

      if (!my_shard_draining) {
        // 1/. The data is intact and the user does not wish to drain this
        // shard, abort rebuilding. abortForMyShard() will check for dirtiness
        // and respond appropriately.

        abortForMyShard(shard_idx,
                        rsi->version,
                        set.getNodeInfo(myNodeId_, shard_idx),
                        "data is intact");
      } else if (is_restore && !myShardIsDirty(shard_idx)) {
        // 2/. The data is intact. Restart the drain in RELOCATE mode.
        // Write RemoveMaintenance request if ClusterMaintenanceStateMachine
        // is enabled
        ld_info("The data on my shard %u is intact, restart rebuilding for "
                "this shard to continue the drain in RELOCATE mode.",
                shard_idx);
        auto f = SHARD_NEEDS_REBUILD_Header::RELOCATE |
            SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
        restartForMyShard(shard_idx, f, nullptr, rsi->version);
      }
    } else if (is_restore && shouldMarkMyShardUnrecoverable(shard_idx)) {
      // 3/ We should mark the data unrecoverable.
      ld_info("Notifying the event log that my shard %u is unrecoverable.",
              shard_idx);
      auto* node_info = set.getNodeInfo(myNodeId_, shard_idx);
      if (!node_info->dc_dirty_ranges.empty()) {
        // The cluster was performing a ranged rebuild for this shard.
        // Since all data is lost, convert to a full shard rebuild.
        if (adminSettings_->enable_cluster_maintenance_state_machine) {
          // request an internal maintenance
          auto delta = std::make_unique<maintenance::MaintenanceDelta>();
          delta->set_apply_maintenances(
              {maintenance::MaintenanceLogWriter::
                   buildMaintenanceDefinitionForRebuilding(
                       ShardID(myNodeId_, shard_idx),
                       "Triggered by RebuildingCoordinator")});
          maintenance_log_writer_->writeDelta(std::move(delta));

        } else {
          restartForMyShard(shard_idx, 0);
        }
      }
      markMyShardUnrecoverable(shard_idx);
    }
    // Dirty, recoverable, shards remain in RESTORE mode since some records
    // may be missing.
  }

  if (!rsi->donor_progress.count(myNodeId_)) {
    // I am not a donor node for this rebuilding, either because I'm in
    // rebuilding set or because I've already finished the rebuilding.
    // Do not create ShardRebuilding state machine. Our job is just to wait for
    // all donors to notify they finished to rebuild the shard so that we can
    // acknowledge our shard was rebuilt.

    if (set.isDonor(myNodeId_, shard_idx)) {
      // I used to be a donor but now I'm done. Report 100% progress.
      shard_state.progressStat.setValue((int64_t)1e6);
    } else {
      // I'm not a donor. Report "N/A" progress.
      shard_state.progressStat.setValue(0);
    }
    return;
  }

  auto settings = rebuildingSettings_.get();

  shard_state.participating = true;
  shard_state.globalWindowEnd = RecordTimestamp::min();
  shard_state.myProgress = RecordTimestamp::min();
  shard_state.lastReportedProgress = RecordTimestamp::min();
  shard_state.progressStat.setValue(1);
  trySlideGlobalWindow(shard_idx, set);

  RebuildingPlanner::Parameters params{
      .rebuild_metadata_logs =
          !rebuildUserLogsOnly_ && shouldRebuildMetadataLogs(shard_idx),
      .min_timestamp = rsi->all_dirty_time_intervals.begin()->lower(),
      .version = shard_state.version};

  requestPlan(shard_idx, params, *shard_state.rebuildingSet);
}

void RebuildingCoordinator::normalizeTimeRanges(uint32_t shard_idx,
                                                RecordTimeIntervals& rtis) {
  auto& store =
      ServerWorker::onThisThread()
          ->processor_->sharded_storage_thread_pool_->getByIndex(shard_idx)
          .getLocalLogStore();
  store.normalizeTimeRanges(rtis);
}

void RebuildingCoordinator::writeRemoveMaintenance(ShardID shard) {
  // We should remove maintenance only for our own shard
  ld_check(shard.node() == myNodeId_);
  auto delta = std::make_unique<maintenance::MaintenanceDelta>();
  delta->set_remove_maintenances(
      maintenance::MaintenanceLogWriter::buildRemoveMaintenancesRequest(
          shard, "Data intact"));
  maintenance_log_writer_->writeDelta(std::move(delta));
}

void RebuildingCoordinator::requestPlan(shard_index_t shard_idx,
                                        RebuildingPlanner::Parameters params,
                                        RebuildingSet rebuilding_set) {
  if (!requested_plans_) {
    requested_plans_ = std::make_unique<RequestedPlans>();
  }
  ld_debug("Plan requested for shard_idx=%hu version=%s rebuilding_set=%s",
           shard_idx,
           lsn_to_string(params.version).c_str(),
           rebuilding_set.describe().c_str());
  requested_plans_->params[shard_idx] = params;
  requested_plans_->rebuildingSets[shard_idx] = std::move(rebuilding_set);
  activatePlanningTimer();
}

void RebuildingCoordinator::cancelRequestedPlans(shard_index_t shard_idx) {
  if (requested_plans_) {
    requested_plans_->params.erase(shard_idx);
    requested_plans_->rebuildingSets.erase(shard_idx);
  }
}

void RebuildingCoordinator::activatePlanningTimer() {
  if (!planning_timer_) {
    planning_timer_ = std::make_unique<LibeventTimer>(
        Worker::onThisThread()->getEventBase(),
        [self = this] { self->executePlanningRequests(); });
  }
  if (!planning_timer_->isActive()) {
    planning_timer_->activate(rebuildingSettings_->planner_scheduling_delay);
  }
}

void RebuildingCoordinator::executePlanningRequests() {
  if (!requested_plans_) {
    return; /* nothing to do */
  }
  auto params = std::move(requested_plans_->params);
  auto rebuildingSets = std::move(requested_plans_->rebuildingSets);
  requested_plans_.reset();

  if (params.size() == 0) {
    return; // nothing to do
  }

  auto planner = createRebuildingPlanner(params, rebuildingSets);

  std::vector<std::string> params_str;
  for (auto& kv : params) {
    ld_check(rebuildingSets.count(kv.first));
    auto& rb_set = rebuildingSets.at(kv.first);
    params_str.push_back(
        folly::sformat("[shard_idx:{}, version:{}, rebuilding_set:{}]",
                       kv.first,
                       lsn_to_string(kv.second.version),
                       rb_set.describe()));
  }
  ld_debug("Executing the following planning requests: %s",
           folly::join(", ", params_str).c_str());

  for (auto& kv : params) {
    auto shard = kv.first;
    auto& shard_state = getShardState(shard);
    if (shard_state.planner) {
      shard_state.planner->abortShardIdx(shard);
    }
    shard_state.planningStartTime = SteadyTimestamp::now();
    shard_state.planner = planner;
  }
  planner->start();
}

std::shared_ptr<RebuildingPlanner>
RebuildingCoordinator::createRebuildingPlanner(
    RebuildingPlanner::ParametersPerShard params,
    RebuildingSets rebuildingSets) {
  return std::make_shared<RebuildingPlanner>(
      std::move(params),
      std::move(rebuildingSets),
      rebuildingSettings_,
      config_,
      numShards(),
      /* rebuilding_internal_logs = */ !rebuildUserLogsOnly_,
      this);
}

std::unique_ptr<ShardRebuildingInterface>
RebuildingCoordinator::createShardRebuilding(
    shard_index_t shard,
    lsn_t version,
    lsn_t restart_version,
    std::shared_ptr<const RebuildingSet> rebuilding_set,
    UpdateableSettings<RebuildingSettings> rebuilding_settings) {
  if (rebuilding_settings->enable_v2) {
    return std::make_unique<ShardRebuildingV2>(shard,
                                               version,
                                               restart_version,
                                               rebuilding_set,
                                               rebuilding_settings,
                                               processor_->getMyNodeID(),
                                               this);
  } else {
    return std::make_unique<ShardRebuildingV1>(shard,
                                               version,
                                               restart_version,
                                               rebuilding_set,
                                               shardedStore_->getByIndex(shard),
                                               rebuilding_settings,
                                               config_,
                                               this);
  }
}

void RebuildingCoordinator::abortShardRebuilding(uint32_t shard_idx) {
  cancelRequestedPlans(shard_idx);
  auto it_shard = shardsRebuilding_.find(shard_idx);
  if (it_shard == shardsRebuilding_.end()) {
    // We are not rebuilding this shard.
    return;
  }

  auto& shard_state = getShardState(shard_idx);

  if (!shard_state.isAuthoritative && shard_state.recoverableShards > 0) {
    WORKER_STAT_DECR(rebuilding_waiting_for_recoverable_shards);
  }

  shard_state.shardRebuilding.reset();
  if (shard_state.planner) {
    shard_state.planner->abortShardIdx(shard_idx);
  }
  shard_state.planner.reset();
  shard_state.logsWithPlan.clear();
  shard_state.waitingForMorePlans = 0;
  shard_state.participating = false;
  shard_state.isAuthoritative = true;

  shard_state.max_rebuild_by_retention_backlog =
      std::chrono::milliseconds::zero();
  shard_state.rebuilding_started_ts = std::chrono::milliseconds::zero();
  shard_state.shardIsRebuiltDelayTimer.cancel();
}

void RebuildingCoordinator::onShardMarkUnrecoverable(
    node_index_t /*node_idx*/,
    uint32_t shard_idx,
    const EventLogRebuildingSet& set) {
  auto it_shard = shardsRebuilding_.find(shard_idx);
  if (it_shard == shardsRebuilding_.end()) {
    return;
  }

  auto& shard_state = it_shard->second;
  if (shouldAcknowledgeRebuilding(shard_idx, set)) {
    // Issue a storage task to write a RebuildingCompleteMetadata marker
    // locally. Once the task comes back we will write SHARD_ACK_REBUILT to the
    // event log.
    writeMarkerForShard(shard_idx, shard_state.version);
  } else {
    if (!shard_state.isAuthoritative && shard_state.recoverableShards > 0) {
      WORKER_STAT_DECR(rebuilding_waiting_for_recoverable_shards);
    }
    shard_state.recoverableShards =
        set.getForShardOffset(shard_idx)->num_recoverable_;
  }
}

void RebuildingCoordinator::onShardIsRebuilt(node_index_t donor_node_idx,
                                             uint32_t shard_idx,
                                             lsn_t version,
                                             const EventLogRebuildingSet& set) {
  const auto* rsi = set.getForShardOffset(shard_idx);
  if (!rsi) {
    // We don't care about this shard because we are not rebuilding it or we
    // already rebuilt it.
    return;
  }

  auto it_shard = shardsRebuilding_.find(shard_idx);
  ld_check(it_shard != shardsRebuilding_.end());
  auto& shard_state = it_shard->second;

  if (shard_state.version != version) {
    // This means the donor node sent this event before it received a
    // SHARD_NEEDS_REBUILD or SHARD_ABORT_REBUILD event.
    return;
  }

  if (donor_node_idx == myNodeId_) {
    // This is a SHARD_IS_REBUILT message that we sent ourself. We should not
    // have any running ShardRebuilding at this point. However, this message may
    // have been sent by this node before it crashed in which case it's possible
    // that we are just catching up reading the event log and have running
    // ShardRebuilding state machine. Abort them.
    // TODO (#13606244): This is not fully correct, probably better to use
    //                   local checkpoint instead.
    abortShardRebuilding(shard_idx);
    shard_state.progressStat.setValue((int64_t)1e6);
  }

  if (shouldAcknowledgeRebuilding(shard_idx, set)) {
    // Issue a storage task to write a RebuildingCompleteMetadata marker
    // locally. Once the task comes back we will write SHARD_ACK_REBUILT to the
    // event log.
    writeMarkerForShard(shard_idx, shard_state.version);
  }

  if (rsi->donor_progress.empty()) {
    return;
  }

  // We may be able to slide the global timestamp window.
  trySlideGlobalWindow(shard_idx, set);
}

bool RebuildingCoordinator::shouldAcknowledgeRebuilding(
    uint32_t shard_idx,
    const EventLogRebuildingSet& set) {
  auto& shard_state = getShardState(shard_idx);

  if (!shard_state.rebuildingSetContainsMyself) {
    return false;
  }

  auto const* shard = set.getNodeInfo(myNodeId_, shard_idx);
  if (!shard) {
    return false;
  }

  if (shard->auth_status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    // Clear any dirty range data since the shard has gone to the empty
    // state. We'll persist this change when we write the rebuilding complete
    // marker which could be only after processing a SHARD_UNDRAIN.
    dirtyShards_.erase(shard_idx);
    if (shard->drain) {
      ld_info("Not ready to ack rebuilding of shard %u with version %s, "
              "rebuilding set %s because this shard is drained. Write a "
              "SHARD_UNDRAIN message to allow this shard to ack rebuilding and "
              "take writes again.",
              shard_idx,
              lsn_to_string(shard_state.version).c_str(),
              shard_state.rebuildingSet->describe().c_str());
      shard_state.waitingForUndrainStat.assign(
          getStats(), &PerShardStats::shard_waiting_for_undrain, shard_idx);
      return false;
    }
    return true;
  }

  if (shard->donors_remaining.empty()) {
    // Dirty shards are always authoritatative.
    // Ack once rebuilt authoritatively or the data necessary to
    // rebuild authoritatively has been marked unrecoverable.
    auto const* shard_rebuilding_info = set.getForShardOffset(shard_idx);
    ld_check(shard_rebuilding_info != nullptr);
    if (!shard->dc_dirty_ranges.empty() &&
        shard->auth_status == AuthoritativeStatus::FULLY_AUTHORITATIVE &&
        shard_rebuilding_info != nullptr &&
        shard_rebuilding_info->num_recoverable_ == 0) {
      ld_check(!shard->drain);
      return true;
    }
    ld_info("Rebuilding of my shard %u completed non authoritatively so not "
            "acking. Rebuilding can be acked once all shards in the "
            "rebuilding set are marked unrecoverable or enough shards in the "
            "rebuilding set come back with their data intact.",
            shard_idx);
  }
  return false;
}

void RebuildingCoordinator::onRetrievedPlanForLog(
    logid_t log,
    const uint32_t shard_idx,
    std::unique_ptr<RebuildingPlan> log_plan,
    bool is_authoritative,
    lsn_t version) {
  ld_assert(shardsRebuilding_.find(shard_idx) != shardsRebuilding_.end());
  auto& shard_state = getShardState(shard_idx);
  ld_check(version == shard_state.version);
  ld_check(shard_state.waitingForMorePlans);
  ld_check(shard_state.shardRebuilding == nullptr);
  ld_check(!shard_state.logsWithPlan.count(log));

  if (!is_authoritative && shard_state.isAuthoritative &&
      shard_state.recoverableShards > 0) {
    WORKER_STAT_INCR(rebuilding_waiting_for_recoverable_shards);
  }

  shard_state.isAuthoritative &= is_authoritative;

  ld_check(log_plan);
  ld_check(!log_plan->epochsToRead.empty());

  shard_state.logsWithPlan.emplace(log, std::move(log_plan));
}

void RebuildingCoordinator::onLogsEnumerated(
    uint32_t shard_idx,
    lsn_t version,
    std::chrono::milliseconds max_rebuild_by_retention_backlog) {
  ld_info("All plans for logs were retrieved for shard %u in version %s. "
          "Planning stage requested a delay of %ld ms",
          shard_idx,
          lsn_to_string(version).c_str(),
          max_rebuild_by_retention_backlog.count());

  auto& shard_state = getShardState(shard_idx);

  ld_check(version == shard_state.version);
  ld_check(shard_state.waitingForMorePlans);
  ld_check(shard_state.max_rebuild_by_retention_backlog ==
           std::chrono::milliseconds::zero());
  shard_state.max_rebuild_by_retention_backlog =
      max_rebuild_by_retention_backlog;
}

void RebuildingCoordinator::onFinishedRetrievingPlans(uint32_t shard_idx,
                                                      lsn_t version) {
  auto& shard_state = getShardState(shard_idx);
  ld_check(version == shard_state.version);
  ld_check(shard_state.waitingForMorePlans);
  shard_state.waitingForMorePlans = 0;
  shard_state.planner.reset();

  ld_info("All plans for logs were retrieved for shard %u in version %s",
          shard_idx,
          lsn_to_string(version).c_str());

  // Remove logs that are not in config anymore.
  auto config = config_->get();
  size_t total_epoch_ranges = 0;
  for (auto it = shard_state.logsWithPlan.begin();
       it != shard_state.logsWithPlan.end();) {
    if (!config->getLogGroupByIDShared(it->first)) {
      it = shard_state.logsWithPlan.erase(it);
    } else {
      total_epoch_ranges +=
          boost::icl::interval_count(it->second->epochsToRead);
      ++it;
    }
  }

  double planning_seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(
          SteadyTimestamp::now() - shard_state.planningStartTime)
          .count();

  if (shard_state.logsWithPlan.empty()) {
    ld_info(
        "Got empty rebuild plan for shard %u in %.3fs with rebuilding set: %s",
        shard_idx,
        planning_seconds,
        shard_state.rebuildingSet->describe().c_str());
    onShardRebuildingComplete(shard_idx);
  } else {
    ld_info("Got rebuilding plan (%lu epoch ranges in %lu logs) for shard %u "
            "in %.3fs. Starting rebuilding. Rebuilding set: %s",
            total_epoch_ranges,
            shard_state.logsWithPlan.size(),
            shard_idx,
            planning_seconds,
            shard_state.rebuildingSet->describe().c_str());
    shard_state.shardRebuilding =
        createShardRebuilding(shard_idx,
                              shard_state.version,
                              shard_state.restartVersion,
                              shard_state.rebuildingSet,
                              rebuildingSettings_);
    shard_state.shardRebuilding->advanceGlobalWindow(
        shard_state.globalWindowEnd);
    shard_state.shardRebuilding->start(std::move(shard_state.logsWithPlan));
  }
}

void RebuildingCoordinator::onShardUndrain(node_index_t node_idx,
                                           uint32_t shard_idx,
                                           const EventLogRebuildingSet& set) {
  if (node_idx != getMyNodeID().index()) {
    // We don't care about this event if it's not for this node.
    return;
  }

  ShardID shard(node_idx, shard_idx);

  const bool shard_is_rebuilding = shardsRebuilding_.count(shard_idx) != 0;
  if (!shard_is_rebuilding ||
      shardsRebuilding_[shard_idx].rebuildingSet->shards.count(shard) == 0) {
    ld_error(
        "Received SHARD_UNDRAIN for shard %u with node_idx=%u but %u "
        "is not in the rebuilding set %s. Ignoring.",
        shard_idx,
        node_idx,
        node_idx,
        shard_is_rebuilding
            ? shardsRebuilding_[shard_idx].rebuildingSet->describe().c_str()
            : "{}");
    return;
  }

  auto& shard_state = shardsRebuilding_[shard_idx];

  // node_idx is in the rebuilding set && node_idx == getMyNodeID(), so this
  // should be true.
  ld_check(shard_state.rebuildingSetContainsMyself);

  if (myShardHasDataIntact(shard_idx)) {
    if (myShardIsDirty(shard_idx)) {
      // We still have dirty ranges and so must complete a time ranged rebuild.
      auto ds_kv = dirtyShards_.find(shard_idx);
      restartForMyShard(shard_idx, 0, &ds_kv->second);
    } else {
      // We just requested to cancel the ongoing drain on this node, and this
      // node has its data intact. This means we can simply abort rebuilding.
      abortForMyShard(shard_idx,
                      shard_state.version,
                      set.getNodeInfo(myNodeId_, shard_idx),
                      "undrain and data is intact");
    }
    return;
  }

  // Marking the shard as undrained may make it possible to ack rebuilding,
  // check that.
  if (shouldAcknowledgeRebuilding(shard_idx, set)) {
    // Issue a storage task to write a RebuildingCompleteMetadata marker
    // locally. Once the task comes back we will write SHARD_ACK_REBUILT to the
    // event log.
    writeMarkerForShard(shard_idx, shard_state.version);
  }
}

void RebuildingCoordinator::onShardAckRebuilt(node_index_t node_idx,
                                              uint32_t shard_idx,
                                              lsn_t version) {
  auto it_shard = shardsRebuilding_.find(shard_idx);
  if (it_shard == shardsRebuilding_.end()) {
    // We don't care about this shard because we are not rebuilding it or we
    // already rebuilt it.
    return;
  }
  auto& shard_state = it_shard->second;

  if (shard_state.version != version) {
    // This means the node sent this event before it could received a
    // SHARD_NEEDS_REBUILD event for the same shard.
    return;
  }

  ShardID shard(node_idx, shard_idx);

  if (shard_state.rebuildingSet->shards.find(shard) ==
      shard_state.rebuildingSet->shards.end()) {
    ld_error("Received SHARD_ACK_REBUILT for shard %u with node_idx=%u, "
             "version=%s but %u is not in the rebuilding set %s. Ignoring.",
             shard_idx,
             node_idx,
             lsn_to_string(version).c_str(),
             node_idx,
             shard_state.rebuildingSet->describe().c_str());
    return;
  }

  auto rebuildingSet =
      std::make_shared<RebuildingSet>(*shard_state.rebuildingSet);
  rebuildingSet->shards.erase(shard);
  shard_state.rebuildingSet = rebuildingSet;

  if (rebuildingSet->shards.empty()) {
    abortShardRebuilding(shard_idx);
    shardsRebuilding_.erase(it_shard);
  } else if (node_idx == myNodeId_) {
    // Decrement stat rebuilding_set_contains_myself after getting our own ack,
    // without waiting for other rebuilding nodes to ack.
    ld_check(shard_state.rebuildingSetContainsMyself);
    shard_state.rebuildingSetContainsMyselfStat.reset();
  }
}

void RebuildingCoordinator::onEventLogTrimmed(lsn_t hi) {
  // The event log was trimmed. If there are active shard rebuildings, this
  // means we were reading a backlog in the event log and these rebuildings
  // actually completed earlier, so we should just abort them all.

  for (auto& s : shardsRebuilding_) {
    auto& shard_state = s.second;
    ld_check(shard_state.version <= hi);
    abortShardRebuilding(s.first);
  }
  shardsRebuilding_.clear();
}

void RebuildingCoordinator::onUpdate(const EventLogRebuildingSet& set,
                                     const EventLogRecord* delta,
                                     lsn_t version) {
  if (shuttingDown_) {
    return;
  }

  last_seen_event_log_version_ = version;

  if (first_update_) {
    // The EventLog RSM releases its first update once it has caught up (read
    // the tail LSN that was discovered upon subscribing to the event log). Now
    // that we have processed something aproximating the current state of the
    // cluster, emit SHARD_NEEDS_REBUILD events for any shards that were left
    // dirty by an unsafe shutdown.
    first_update_ = false;
    publishDirtyShards(set);
  }

  if (!delta) {
    // We don't have a delta, just restart all rebuildings with the new
    // rebuilding set.
    for (auto& shard : set.getRebuildingShards()) {
      scheduleRestartForShard(shard.first);
    }
    // Also schedule restarts (actually aborts) for shards that disappeared
    // from rebuilding set, presumably because they were acked.
    for (auto& kv : shardsRebuilding_) {
      if (set.getForShardOffset(kv.first) == nullptr) {
        scheduleRestartForShard(kv.first);
      }
    }
    return;
  }
  switch (delta->getType()) {
    case EventType::SHARD_NEEDS_REBUILD: {
      const auto ptr = static_cast<const SHARD_NEEDS_REBUILD_Event*>(delta);
      scheduleRestartForShard(ptr->header.shardIdx);
    } break;
    case EventType::SHARD_ABORT_REBUILD: {
      const auto ptr = static_cast<const SHARD_ABORT_REBUILD_Event*>(delta);
      scheduleRestartForShard(ptr->header.shardIdx);
    } break;
    case EventType::SHARD_IS_REBUILT: {
      const auto ptr = static_cast<const SHARD_IS_REBUILT_Event*>(delta);
      if (!restartIsScheduledForShard(ptr->header.shardIdx)) {
        onShardIsRebuilt(ptr->header.donorNodeIdx,
                         ptr->header.shardIdx,
                         ptr->header.version,
                         set);
      }
    } break;
    case EventType::SHARD_DONOR_PROGRESS: {
      const auto ptr = static_cast<const SHARD_DONOR_PROGRESS_Event*>(delta);
      if (!restartIsScheduledForShard(ptr->header.shardIdx)) {
        RecordTimestamp next_ts(
            std::chrono::milliseconds(ptr->header.nextTimestamp));
        onShardDonorProgress(ptr->header.donorNodeIdx,
                             ptr->header.shardIdx,
                             next_ts,
                             ptr->header.version,
                             set);
      }
    } break;
    case EventType::SHARD_ACK_REBUILT: {
      const auto ptr = static_cast<const SHARD_ACK_REBUILT_Event*>(delta);
      if (!restartIsScheduledForShard(ptr->header.shardIdx)) {
        onShardAckRebuilt(
            ptr->header.nodeIdx, ptr->header.shardIdx, ptr->header.version);
      }
    } break;
    case EventType::SHARD_UNDRAIN: {
      const auto ptr = static_cast<const SHARD_UNDRAIN_Event*>(delta);
      if (!restartIsScheduledForShard(ptr->header.shardIdx)) {
        onShardUndrain(ptr->header.nodeIdx, ptr->header.shardIdx, set);
      }
    } break;
    case EventType::SHARD_UNRECOVERABLE: {
      const auto ptr = static_cast<const SHARD_UNRECOVERABLE_Event*>(delta);
      if (!restartIsScheduledForShard(ptr->header.shardIdx)) {
        onShardMarkUnrecoverable(
            ptr->header.nodeIdx, ptr->header.shardIdx, set);
      }
    } break;
    default:
      // We don't care about any other event.
      break;
  }
}

void RebuildingCoordinator::publishDirtyShards(
    const EventLogRebuildingSet& set) {
  if (!rebuildingSettings_->rebuild_dirty_shards) {
    ld_info("Publishing dirty shard state to the event log is disabled.");
    return;
  }

  ld_info("Publishing dirty shards.");
  for (auto& ds_kv : dirtyShards_) {
    auto shard_idx = ds_kv.first;
    if (!myShardHasDataIntact(shard_idx)) {
      // Action should already have been taken to schedule a full
      // rebuild of this shard.
      continue;
    }

    ld_check(!ds_kv.second.empty());
    auto info = set.getNodeInfo(myNodeId_, shard_idx);
    if (info) {
      // If rebuilding completed while this node was down, the
      // dirty state is no longer relevant. We should have already
      // scheduled a task to ack the rebuild.
      if (info->auth_status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        continue;
      }

      // If a drain is active, we ignore dirty state and allow
      // the drain of all data to proceed. Any rebuilding range
      // data will be cleared once we transition to AUTHORITATIVE_EMTPY.
      // If the drain is cancelled before completion, the non-empty
      // range data will cause us to perform a ranged rebuild.
      if (info->drain) {
        ld_info("Shard %u: Draining. Not publishing dirty state: %s",
                shard_idx,
                toString(info->dc_dirty_ranges).c_str());
        if (info->mode == RebuildingMode::RELOCATE) {
          // Convert to a RESTORE drain since we are missing some
          // of our data. This is required unless/until we improve
          // the donor SCD filter to understand dirty ranges.
          if (adminSettings_->enable_cluster_maintenance_state_machine) {
            // request an internal maintenance
            auto delta = std::make_unique<maintenance::MaintenanceDelta>();
            delta->set_apply_maintenances(
                {maintenance::MaintenanceLogWriter::
                     buildMaintenanceDefinitionForRebuilding(
                         ShardID(myNodeId_, shard_idx),
                         "Triggered by RebuildingCoordinator")});
            maintenance_log_writer_->writeDelta(std::move(delta));
          } else {
            ld_info("Shard %u: Converting drain from RELOCATE to RESTORE",
                    shard_idx);
            restartForMyShard(shard_idx, 0);
          }
        }
        continue;
      }

      // If the time ranges all match, the cluster is already performing
      // the desired rebuild operation.
      if (info->dc_dirty_ranges == ds_kv.second.getDCDirtyRanges()) {
        ld_info("Shard %u: Current dirty ranges already published: %s",
                shard_idx,
                toString(info->dc_dirty_ranges).c_str());
        continue;
      }
    }
    // My shard's data is intact, any previously requested internal
    // maintenance should be removed.
    if (adminSettings_->enable_cluster_maintenance_state_machine) {
      writeRemoveMaintenance(ShardID(myNodeId_, shard_idx));
    }
    restartForMyShard(shard_idx, 0, &ds_kv.second);
  }
}

void RebuildingCoordinator::abortCleanedShards(const EventLogRebuildingSet& set,
                                               DirtyShardMap& cleaned_shards) {
  // Abort any of my shards that are in cleaned_shards and are
  // listed in the EventLogRebuildingSet as performing a time-ranged
  // rebuild.
  for (auto ds_kv : cleaned_shards) {
    const auto* my_node_info = set.getNodeInfo(myNodeId_, ds_kv.first);
    if (my_node_info != nullptr && !my_node_info->dc_dirty_ranges.empty()) {
      abortForMyShard(ds_kv.first,
                      set.getForShardOffset(ds_kv.first)->version,
                      my_node_info,
                      "Shard has been marked clean");
    }
  }
}

void RebuildingCoordinator::onDirtyStateChanged() {
  auto cleaned_shards = dirtyShards_;
  dirtyShards_.clear();
  populateDirtyShardCache(dirtyShards_);
  for (auto it = cleaned_shards.begin(); it != cleaned_shards.end();) {
    if (dirtyShards_.find(it->first) == dirtyShards_.end()) {
      ++it;
    } else {
      it = cleaned_shards.erase(it);
    }
  }
  auto rebuilding_set = event_log_->getCurrentRebuildingSet();
  abortCleanedShards(rebuilding_set, cleaned_shards);
  publishDirtyShards(rebuilding_set);
}

lsn_t RebuildingCoordinator::getLastSeenEventLogVersion() const {
  return last_seen_event_log_version_;
}

RebuildingCoordinator::ShardState&
RebuildingCoordinator::getShardState(uint32_t shard_idx) {
  ld_check(shardsRebuilding_.find(shard_idx) != shardsRebuilding_.end());
  return shardsRebuilding_[shard_idx];
}

const RebuildingCoordinator::ShardState&
RebuildingCoordinator::getShardState(uint32_t shard_idx) const {
  const auto it = shardsRebuilding_.find(shard_idx);
  ld_check(it != shardsRebuilding_.end());
  return it->second;
}

void RebuildingCoordinator::abortForMyShard(
    uint32_t shard,
    lsn_t version,
    const EventLogRebuildingSet::NodeInfo* node_info,
    const char* reason) {
  auto ds_kv = dirtyShards_.find(shard);
  if (ds_kv != dirtyShards_.end()) {
    ld_info("Request to abort rebuilding of my shard %u because: %s. "
            "But shard is dirty. Downgrading to time ranged rebuild.",
            shard,
            reason);
    ld_info(
        "EventLogRebuildingSet NodeInfo: %s", node_info->toString().c_str());
    ld_info("Local dirty ranges: %s", ds_kv->second.toString().c_str());

    ld_check(node_info);
    if (node_info &&
        ds_kv->second.getDCDirtyRanges() == node_info->dc_dirty_ranges) {
      ld_info("Cluster already rebuilding the correct dirty ranges for "
              "my shard %u. Converting abort into no-op.",
              shard);
    } else {
      restartForMyShard(shard, 0, &ds_kv->second);
    }
    return;
  }

  ld_info("Aborting rebuilding of my shard %u because: %s.", shard, reason);
  auto event =
      std::make_unique<SHARD_ABORT_REBUILD_Event>(myNodeId_, shard, version);
  writer_->writeEvent(std::move(event));
}

void RebuildingCoordinator::restartForMyShard(uint32_t shard,
                                              SHARD_NEEDS_REBUILD_flags_t f,
                                              RebuildingRangesMetadata* rrm,
                                              lsn_t conditional_version) {
  if (f & SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION) {
    ld_check(conditional_version != LSN_INVALID);
  }

  if (!rebuildingSettings_->allow_conditional_rebuilding_restarts) {
    // TODO(T22614431): conditional restart of rebuilding is gated as some
    // clients that run the EventLogStateMachine may be too old. It is meant
    // to be enabled by default and this logic removed once all clients are
    // updated.
    conditional_version = LSN_INVALID;
    f &= ~SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION;
  }

  std::string source = "N" + std::to_string(myNodeId_);
  auto event = std::make_unique<SHARD_NEEDS_REBUILD_Event>(
      SHARD_NEEDS_REBUILD_Header{myNodeId_,
                                 shard,
                                 source,
                                 "RebuildingCoordinator",
                                 f,
                                 conditional_version},
      rrm);
  writer_->writeEvent(std::move(event));
}

void RebuildingCoordinator::markMyShardUnrecoverable(uint32_t shard) {
  auto event = std::make_unique<SHARD_UNRECOVERABLE_Event>(myNodeId_, shard);
  writer_->writeEvent(std::move(event));
}

void RebuildingCoordinator::notifyShardRebuilt(uint32_t shard,
                                               lsn_t version,
                                               bool is_authoritative) {
  auto& s_state = getShardState(shard);
  ld_check(s_state.participating);
  ld_check(s_state.logsWithPlan.empty());

  auto event = std::make_unique<SHARD_IS_REBUILT_Event>(
      myNodeId_,
      shard,
      version,
      is_authoritative ? 0 : SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE);
  writer_->writeEvent(std::move(event));
}

void RebuildingCoordinator::notifyAckMyShardRebuilt(uint32_t shard,
                                                    lsn_t version) {
  auto event =
      std::make_unique<SHARD_ACK_REBUILT_Event>(myNodeId_, shard, version);
  writer_->writeEvent(std::move(event));
  dirtyShards_.erase(shard);
  processor_->markShardClean(shard);
}

void RebuildingCoordinator::notifyShardDonorProgress(uint32_t shard,
                                                     RecordTimestamp next_ts,
                                                     lsn_t version,
                                                     double progress_estimate) {
  auto it_shard = shardsRebuilding_.find(shard);
  ld_check(it_shard != shardsRebuilding_.end());
  auto& shard_state = it_shard->second;
  std::chrono::milliseconds window_size = rebuildingSettings_->global_window;

  if (progress_estimate > 0) {
    shard_state.progressStat.setValue(
        std::max(1l, (int64_t)ceil(progress_estimate * 1e6)));
  }

  if (window_size == std::chrono::milliseconds::max()) {
    // Global window is disabled, no need to write SHARD_DONOR_PROGRESS.
    return;
  }

  // Note that we can't return early if next_ts == myProgress, because
  // window size setting may have been decreased.
  shard_state.myProgress = std::max(shard_state.myProgress, next_ts);

  // To avoid flooding the event log, report progress only if we moved by
  // at least 1/3 of global window size since previous report.
  std::chrono::milliseconds threshold = window_size / 3;
  // timestamp_to_slide_window = lastReportedProgress + threshold,
  // but avoiding overflow. (No you can't swap threshold and
  // lastReportedProgress because lastReportedProgress can be negative.)
  RecordTimestamp timestamp_to_slide_window{
      threshold +
      std::min(std::chrono::milliseconds::max() - threshold,
               shard_state.lastReportedProgress.toMilliseconds())};

  if (shard_state.myProgress <= timestamp_to_slide_window) {
    return;
  }

  shard_state.lastReportedProgress = shard_state.myProgress;

  auto event = std::make_unique<SHARD_DONOR_PROGRESS_Event>(
      myNodeId_,
      shard,
      shard_state.myProgress.toMilliseconds().count(),
      version);
  writer_->writeEvent(std::move(event));
}

bool RebuildingCoordinator::myShardHasDataIntact(uint32_t shard) const {
  LocalLogStore* store = shardedStore_->getByIndex(shard);
  return !processor_->isDataMissingFromShard(shard) &&
      store->acceptingWrites() != E::DISABLED;
}

bool RebuildingCoordinator::myShardIsDirty(uint32_t shard) const {
  auto ds_kv = dirtyShards_.find(shard);
  return ds_kv != dirtyShards_.end();
}

bool RebuildingCoordinator::shouldMarkMyShardUnrecoverable(
    uint32_t shard) const {
  LocalLogStore* store = shardedStore_->getByIndex(shard);
  // If the shard does not have a marker but is available, it means it has no
  // data.
  return processor_->isDataMissingFromShard(shard) &&
      store->acceptingWrites() != E::DISABLED;
}

StatsHolder* RebuildingCoordinator::getStats() {
  if (Worker::onThisThread(false)) {
    return Worker::stats();
  } else {
    // We are shutting down.
    return nullptr;
  }
}

size_t RebuildingCoordinator::numShards() {
  return shardedStore_->numShards();
}

void RebuildingCoordinator::subscribeToEventLog() {
  auto cb = [&](const EventLogRebuildingSet& set,
                const EventLogRecord* delta,
                lsn_t version) {
    ld_check(Worker::onThisThread()->idx_ == my_worker_id_);
    onUpdate(set, delta, version);
  };

  ld_check(event_log_);
  handle_ = event_log_->subscribe(cb);
  ld_info("Subscribed to EventLog");
}

NodeID RebuildingCoordinator::getMyNodeID() {
  return processor_->getMyNodeID();
}

void RebuildingCoordinator::getDebugInfo(
    InfoRebuildingShardsTable& table) const {
  for (auto& s : shardsRebuilding_) {
    auto& shard_state = s.second;
    int64_t logs_waiting_for_plan = 0;
    if (shard_state.planner != nullptr) {
      // Use -1 to indicate that we don't know how many logs there are yet.
      logs_waiting_for_plan = shard_state.planner->isEnumerationComplete()
          ? static_cast<int64_t>(shard_state.planner->getNumRemainingLogs())
          : -1l;
    }
    table.next()
        .set<0>(s.first)
        .set<1>(shard_state.rebuildingSet->describe(
            std::numeric_limits<size_t>::max()))
        .set<2>(shard_state.version)
        .set<3>(shard_state.globalWindowEnd.toMilliseconds())
        .set<5>(logs_waiting_for_plan)
        .set<13>(shard_state.participating);

    if (shard_state.shardRebuilding != nullptr) {
      shard_state.shardRebuilding->getDebugInfo(table);
    }
  }
}

std::function<void(InfoRebuildingLogsTable&)>
RebuildingCoordinator::beginGetLogsDebugInfo() const {
  ld_check(rebuildingSettings_->enable_v2);
  std::vector<std::function<void(InfoRebuildingLogsTable&)>> funcs;
  for (auto& s : shardsRebuilding_) {
    auto& shard_state = s.second;
    if (shard_state.shardRebuilding != nullptr) {
      auto sr =
          dynamic_cast<ShardRebuildingV2*>(shard_state.shardRebuilding.get());
      ld_check(sr != nullptr);
      funcs.push_back(sr->beginGetLogsDebugInfo());
    }
  }
  return [funcs](InfoRebuildingLogsTable& table) {
    for (auto& f : funcs) {
      f(table);
    }
  };
}

RecordTimestamp
RebuildingCoordinator::getGlobalWindowEnd(uint32_t shard) const {
  return getShardState(shard).globalWindowEnd;
}

std::set<uint32_t> RebuildingCoordinator::getLocalShardsRebuilding() {
  std::set<uint32_t> res;
  for (auto& s : shardsRebuilding_) {
    auto& nodes = s.second.rebuildingSet->shards;
    if (nodes.find(ShardID(myNodeId_, s.first)) != nodes.end()) {
      res.insert(s.first);
    }
  }
  return res;
}
}} // namespace facebook::logdevice
