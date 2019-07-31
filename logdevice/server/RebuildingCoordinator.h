/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <queue>

#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/event_log/EventLogWriter.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"
#include "logdevice/server/rebuilding/NonAuthoritativeRebuildingChecker.h"
#include "logdevice/server/rebuilding/RebuildingPlanner.h"

/**
 * @file RebuildingCoordinator coordinates all LogRebuilding state machines.
 */

namespace facebook { namespace logdevice {

class ShardedLocalLogStore;

class RebuildingCoordinator : public RebuildingPlanner::Listener,
                              public RebuildingCoordinatorInterface,
                              public ShardRebuildingInterface::Listener {
 public:
  RebuildingCoordinator(
      const std::shared_ptr<UpdateableConfig>& config,
      EventLogStateMachine* event_log,
      Processor* processor,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      UpdateableSettings<AdminServerSettings> admin_settings,
      ShardedLocalLogStore* sharded_store);

  /**
   * Do not rebuild the metadata logs. Used by tests.
   */
  void rebuildUserLogsOnly() {
    rebuildUserLogsOnly_ = true;
  }

  /**
   * Initialize data members. Done here instead of in the constructor because
   * this calls virtual functions.
   * @return 0 on success, -1 on failure.
   */
  int start();

  /**
   * start() posts a requests to the worker that owns this state machine.
   * This takes care of the initialization that needs to happen on the worker
   * itself.
   */
  void startOnWorkerThread();

  /**
   * Called on shutdown. Destroy internal data structures and ensure that no
   * more work is started.
   * Must be called exactly once before destructor.
   * This is separate from destructor because shutdown() is called from the
   * worker thread on which RebuildingCoordinator lives, while destructor is
   * called from shutdown thread.
   */
  void shutdown() override;

  /**
   * Called when the cluster's configuration has changed.
   * The purpose of this method is to handle a cluster being shrunk while
   * rebuilding is in progress. The necessary steps are:
   *
   * 1/ find all nodes that are in the donor set for a shard but were removed
   * from the config and act as if they had sent a SHARD_IS_REBUILT message for
   * it.
   *
   * 2/ find all logs that have been removed from the config but are found in
   * activeLogs and stop LogRebuilding State machines
   */
  void noteConfigurationChanged() override;

  /**
   * Called when RebuildingSettings is updated.
   */
  void noteRebuildingSettingsChanged();

  ~RebuildingCoordinator() override;

  /**
   * Abort the current rebuilding for shard `shard_idx` and restart it with the
   * new rebuilding set defined in `set`.
   *
   * Called after a grace period when scheduleRestartForShard() is called.
   * A rebuilding restart usually happens because:
   * - We receive the initial rebuilding set (@see onInitialStateAvailable);
   * - We receive a new SHARD_NEEDS_REBUILD event;
   * - We receive a new SHARD_ABORT_REBUILD_Event.
   *
   * @param shard_idx Shard offset for which to restart rebuilding.
   * @param set       Rebuilding set to consider.
   */
  void restartForShard(uint32_t shard_idx, const EventLogRebuildingSet& set);

  /**
   * Write to the event log to notify that we slid our local timestamp window.
   *
   * If this donor node was the most behind among all donor nodes in the
   * cluster, this will cause every node to slide its view of the global
   * timestamp window and make more progress.
   *
   * @param shard    Shard for which we slid the local timestamp window.
   * @param ts       Timestamp of the next record to be rebuilt.
   * @param version  LSN of the last SHARD_NEEDS_REBUILD event log record that
   *                 was taken into account for rebuilding this shard.
   * @param progress_estimate  A number between 0 and 1 estimating the
   *                 fraction of work done so far. Exported as a stat.
   *                 -1 if not available; in this case stat is left unchanged.
   */
  void notifyShardDonorProgress(uint32_t shard,
                                RecordTimestamp next_ts,
                                lsn_t version,
                                double progress_estimate) override;

  /*
   * Called when RebuildingPlanner completes for a log.
   *
   * @param log          Log for which the request completed.
   * @param shard_idx    Shard that concerns this plan.
   * @param until_lsn    LSN up to which this log should be rebuilt.
   * @pararm log_plan    Indicates which epochs each donor shard must rebuild
   *                     (grouped by shard).
   * @param version      This function asserts that this is equal to the current
   *                     rebuilding version of the log's shard.
   */
  void onRetrievedPlanForLog(logid_t log,
                             uint32_t shard_idx,
                             std::unique_ptr<RebuildingPlan> log_plan,
                             bool is_authoritative,
                             lsn_t version) override;

  /**
   * Called when the local logs have been enumerated.
   *
   * @param shard_idx                         Shard index.
   * @param version                           Current shard rebuilding version.
   * @param max_rebuild_by_retention_backlog  Max backlog retention on this
   *                                          shard.
   *
   */
  void onLogsEnumerated(uint32_t shard,
                        lsn_t version,
                        std::chrono::milliseconds maxBacklogDuration) override;

  /**
   * Called when RebuildingPlanner has completed for all logs so we can move on
   * and finish rebuilding.
   *
   * @param shard_idx  Shard index.
   * @param version    Current shard rebuilding version.
   */
  void onFinishedRetrievingPlans(uint32_t shard, lsn_t version) override;

  /**
   * Called when we complete rebuilding of shard_id. Destroys the corresponding
   * ShardState object.
   *
   * @param shard_idx Shard that we rebuilt.
   */
  void onShardRebuildingComplete(uint32_t shard_idx) override;

  /**
   * @param table Filled with debug information about the state of rebuilding
   *              for each shard. Used by admin commands.
   */
  void getDebugInfo(InfoRebuildingShardsTable& table) const;

  /**
   * Returns a function that fills out the per-log debug info table.
   * The function needs to be called on a non-worker thread.
   * Requires rebuilding-v2 to be enabled in settings.
   * See ShardRebuildingV2::beginGetLogsDebugInfo() for more details.
   */
  std::function<void(InfoRebuildingLogsTable&)> beginGetLogsDebugInfo() const;

  RecordTimestamp getGlobalWindowEnd(uint32_t shard) const;

  /**
   * Called by the storage task that writes a RebuildingCompleteMetadata.
   *
   * @param shard   Shard for which we tried to write the marker.
   * @param version Rebuilding version at the time the storage task was issued.
   * @param status  - E::OK if the marker was successfully written;
   *                - E::LOCAL_LOG_STORE_WRITE if there was an error.
   */
  void onMarkerWrittenForShard(uint32_t shard, lsn_t version, Status status);

  /**
   * Returns a set of local shards (i.e. on this node) that are being rebuilt.
   */
  std::set<uint32_t> getLocalShardsRebuilding();

  lsn_t getLastSeenEventLogVersion() const override;

  /**
   * Called by external entities when the dirty state changes. Currently only
   * used by an admin command
   */
  void onDirtyStateChanged() override;

 protected:
  using DirtyShardMap =
      std::unordered_map<shard_index_t, RebuildingRangesMetadata>;

  // The following may be overridden by tests.

  virtual size_t numShards();

  virtual StatsHolder* getStats();

  /**
   * If this node's generation is 1, write RebuildingCheckpointMetadata for each
   * shard and mark all shards as rebuilt in Processor so that we can
   * immediately start accepting reads and writes.
   *
   * If this node's generation is >1, try and retrieve the
   * RebuildingCheckpointMetadata for each shard. For each shard:
   *
   * - If the marker does exist, mark the shard as rebuilt in Processor;
   * - If the marker does not exist, we don't mark the shard as rebuilt yet.
   *   Once it is rebuilt, we will write the RebuildingCheckpointMetadata before
   *   we send SHARD_ACK_REBUILT in the event log, and we will mark the shard as
   *   rebuilt in Processor.
   *
   * @return 0 on success, or -1 on failure.
   */
  virtual int checkMarkers();

  /**
   * Called once during startup to perform the blocking metadata read calls
   * necessary to fill dirtyShards_.
   */
  virtual void populateDirtyShardCache(DirtyShardMap&);

  /**
   * Called once we are synchronized with the cluster rebuilding state
   * (the state as it existed when we subscribed to the event
   * log) to emit time ranged SHARD_NEEDS_REBUILD records for any shards
   * that are marked dirty due to an unsafe shutdown.
   *
   * It is also called anytime the dirty shard state changes due to a node
   * local event (e.g. from processing a "rebuilding mark_dirty" admin
   * command).
   */
  virtual void publishDirtyShards(const EventLogRebuildingSet& set);

  /*
   * Called anytime a dirty shard is marked clean due to a node local
   * event (e.g. from processing a "rebuilding mark_clean" admin command).
   */
  virtual void abortCleanedShards(const EventLogRebuildingSet& set,
                                  DirtyShardMap& cleaned_shards);
  /**
   * Issue a storage task to write a RebuildingCompleteMetadata marker in the
   * local log store of shard `shard`.
   *
   * @param shard   Shard for which to write a marker.
   * @param version Rebuilding version for the shard. We keep track of this
   *                version so that we can discard the result of the task when
   *                it came back if the version changed in the mean time.
   */
  virtual void writeMarkerForShard(uint32_t shard, lsn_t version);

  /**
   * Called after our shard `shard` was rebuilt. Wake up any read stream that
   * was waiting for this shard to be rebuilt.
   */
  virtual void wakeUpReadStreams(uint32_t shard);

  /**
   * Tell Processor that shard `shard` is rebuilt.
   */
  virtual void notifyProcessorShardRebuilt(uint32_t shard);

  virtual void subscribeToEventLog();

  virtual NodeID getMyNodeID();

  void requestPlan(shard_index_t shard_idx,
                   RebuildingPlanner::Parameters params,
                   RebuildingSet rebuilding_set);
  void cancelRequestedPlans(shard_index_t shard_idx);
  void executePlanningRequests();

  virtual std::shared_ptr<RebuildingPlanner>
  createRebuildingPlanner(RebuildingPlanner::ParametersPerShard params,
                          RebuildingSets rebuildingSets);

  virtual std::unique_ptr<ShardRebuildingInterface> createShardRebuilding(
      shard_index_t shard,
      lsn_t version,
      lsn_t restart_version,
      std::shared_ptr<const RebuildingSet> rebuilding_set,
      UpdateableSettings<RebuildingSettings> rebuilding_settings);

  /**
   * Write to the event log to notify that one of my shard's data is
   * unrecoverable.
   */
  virtual void markMyShardUnrecoverable(uint32_t shard);

  /**
   * Request restarting rebuilding of one of my shards by writing a
   * SHARD_NEEDS_REBUILD event to the event log.
   *
   * if `f & CONDITIONAL_ON_VERSION`, conditional_version must be !=
   * LSN_INVALID.
   */
  virtual void restartForMyShard(uint32_t shard,
                                 SHARD_NEEDS_REBUILD_flags_t f,
                                 RebuildingRangesMetadata* rrm = nullptr,
                                 lsn_t conditional_version = LSN_INVALID);

  /**
   * Request aborting rebuilding of one of my shards by writing a
   * SHARD_ABORT_REBUILD event to the event log.
   */
  virtual void abortForMyShard(uint32_t shard,
                               lsn_t version,
                               const EventLogRebuildingSet::NodeInfo* node_info,
                               const char* reason);

  /**
   * A CB that is either invoked immediately if rebuild is done
   * or deffered if a delay was requested.
   *
   * @param shard            Shard that we rebuilt.
   */
  virtual void notifyShardRebuiltCB(uint32_t shard);

  /**
   * Write to the event log to notify other nodes that we rebuilt a shard.
   *
   * @param shard            Shard that we rebuilt.
   * @param version          LSN of the last SHARD_NEEDS_REBUILD event log
   *                         record.
   *                         that taken into account for rebuilding this shard.
   * @param is_authoritative True if the rebuilding was authoritative.
   */
  virtual void notifyShardRebuilt(uint32_t shard,
                                  lsn_t version,
                                  bool is_authoritative);

  /**
   * Write to the event log to notify that this node acknowledged that its shard
   * was rebuilt.
   *
   * @param shard Shard that we acknowledge was rebuilt.
   * @param version  LSN of the last SHARD_NEEDS_REBUILD event log record that
   *                 was taken into account for rebuilding this shard.
   */
  virtual void notifyAckMyShardRebuilt(uint32_t shard, lsn_t version);

  /**
   * Called when rebuilding was started in RESTORE mode for one of my shards.
   * Returns whether or not we find out that the data on this shard is intact,
   * in which case we should do one of the following:
   * 1/ If the user wished to drain this node in RELOCATE mode, restart
   *    rebuilding in RELOCATE mode;
   * 2/ If the user wished to drain this node in RESTORE mode, do nothing;
   * 3/ If the user did not wish to drain this node, just abort rebuilding.
   */
  virtual bool myShardHasDataIntact(uint32_t shard_idx) const;

  /**
   * If true, some of the data for this shard was lost due to an unsafe
   * shutdown and this data has not been rebuilt.
   */
  virtual bool myShardIsDirty(uint32_t shard) const;

  /**
   * Called when rebuilding was started in RESTORE mode for one of my shards.
   * Returns whether or not we should mark our shard unrecoverable because we
   * see it as functioning but it does not contain any marker, which means it's
   * been wiped.
   */
  virtual bool shouldMarkMyShardUnrecoverable(uint32_t shard_idx) const;

  /**
   * @return true if a restart is scheduled for the given shard.
   */
  virtual bool restartIsScheduledForShard(uint32_t shard_idx);

  /**
   * Schedule a rebuilding restart for shard `shard_idx`.
   * This will call schedule a timer to call `restartForShard()` after some time
   * determined by the `rebuilding-restarts-grace-period` setting.
   */
  virtual void scheduleRestartForShard(uint32_t shard_idx);

  /**
   * Have the LocalLogStore adjust our rebuilding ranges such that we cannot
   * miss any records even if the timestamps in this log are not strictly
   * increasing with increasing LSN.
   */
  virtual void normalizeTimeRanges(uint32_t shard_idx,
                                   RecordTimeIntervals& rtis);

  virtual void activatePlanningTimer();

 private:
  // Called when the rebuilding set changed. If `delta` != nullptr, only make
  // the necessary changes described by `delta`, otherwise, restart all
  // rebuildings using `set`.
  void onUpdate(const EventLogRebuildingSet& set,
                const EventLogRecord* delta,
                lsn_t version);

  bool rebuildUserLogsOnly_ = false;

  std::shared_ptr<UpdateableConfig> config_;

  EventLogStateMachine* event_log_;

  std::unique_ptr<EventLogStateMachine::SubscriptionHandle> handle_;

  Processor* processor_; // May be null in tests.

  worker_id_t my_worker_id_ = WORKER_ID_INVALID;

  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  UpdateableSettings<AdminServerSettings> adminSettings_;
  UpdateableSettings<RebuildingSettings>::SubscriptionHandle
      rebuildingSettingsSubscription_;

  ShardedLocalLogStore* shardedStore_;

  struct RequestedPlans {
    RebuildingPlanner::ParametersPerShard params;
    RebuildingSets rebuildingSets;
  };

  std::unique_ptr<RequestedPlans> requested_plans_;
  std::unique_ptr<LibeventTimer> planning_timer_;

  /**
   * State maintained per shard.
   */
  struct ShardState {
    // LSN of the last SHARD_NEEDS_REBUILD we considered. This identifies the
    // current rebuilding state machine and changes each time we rewind it
    // because the rebuilding set is modified following receiving a
    // SHARD_NEEDS_REBUILD record.
    lsn_t version{LSN_INVALID};

    // Changed each time we call restartForShard(). Used for checking staleness
    // of requests sent by LogRebuilding state machines over to us. When
    // restartForShard() is called, we bump this version which makes these stale
    // request created prior to the restart be discarded. Using this instead of
    // `version` is more reliable because it covers the case where the state
    // machine is restarted while the version was not bumped, which may happen
    // rarely if we fast-forward reading the event log because we receive a
    // snapshot.
    lsn_t restartVersion{LSN_INVALID};

    // Utility used to build a plan for each log on each shard. A plan describes
    // what needs to be rebuilt (which LSN to rebuild up to, which nodesets to
    // rebuild).
    // In order to determine which LSN to rebuild up to, this utility sends
    // GET_SEQ_STATE requests to sequencers until they give us all `untilLsn`s
    // and release all records up to it.
    std::shared_ptr<RebuildingPlanner> planner;
    SteadyTimestamp planningStartTime = SteadyTimestamp::max();

    int waitingForMorePlans{1};

    // Logs that received a nonempty RebuildingPlan.
    // Cleared after planning is done.
    std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> logsWithPlan;

    // End of the global timestamp window for this shard.
    RecordTimestamp globalWindowEnd = RecordTimestamp::min();
    // How far rebuilding has progressed on this node. This gets reported in
    // SHARD_DONOR_PROGRESS messages periodically.
    RecordTimestamp myProgress = RecordTimestamp::min();
    // Last SHARD_DONOR_PROGRESS message we've written out.
    RecordTimestamp lastReportedProgress = RecordTimestamp::min();

    // Set of nodes that we are rebuilding this shard for.
    std::shared_ptr<const RebuildingSet> rebuildingSet;

    // Set to true if rebuildingSet contains myself. When this happens this
    // ShardState object is not destroyed immediately when we finish rebuilding
    // the shard but is destroyed after we acknowledged that all donors rebuilt
    // it.
    bool rebuildingSetContainsMyself{false};
    // RAII helpers to decrease stats when ShardState is destroyed.
    PerShardStatToken rebuildingSetContainsMyselfStat;
    PerShardStatToken restoreSetContainsMyselfStat;
    PerShardStatToken waitingForUndrainStat;
    PerShardStatToken progressStat;

    // true iff our node is a donor for this rebuilding and hasn't finished
    // rebuilding yet. When all LogRebuildings finish, this is set to false
    // but ShardState stays alive until we read SHARD_ACK_REBUILD from
    // event log - this is needed to have the correct
    // rebuildingSet if we see other SHARD_NEEDS_REBUILD events.
    bool participating{false};

    bool isAuthoritative{true};
    size_t recoverableShards{0};

    // The object responsible for the actual re-replication.
    // Created after RebuildingPlanner finishes for all logs.
    std::unique_ptr<ShardRebuildingInterface> shardRebuilding;

    // The max backlog duration of all the logs with retention on this shard
    std::chrono::milliseconds max_rebuild_by_retention_backlog{0};

    // Timestamp of the latest SHARD_NEEDS_REBUILT message we received.
    std::chrono::milliseconds rebuilding_started_ts{0};

    // Timer that fires when all data under rebuild has expired.
    // The timer is only activated when disable-data-log-rebuilt
    // setting is set. The SHARD_IS_REBUILT message is sent when the timer
    // fires.
    Timer shardIsRebuiltDelayTimer;
  };

  bool shuttingDown_{false};

  void subscribeToEventLogIfNeeded();

  bool started_{false};

  /**
   * True if we are waiting for initial EventLogRebuildingSet information.
   */
  bool first_update_{true};

  /**
   * LSN from the last EventLogStateMachine callback we've received.
   */
  lsn_t last_seen_event_log_version_ = LSN_INVALID;

  /**
   * Map a shard id to a ShardState object.
   * There is an entry here for each shard being rebuilt.
   */
  std::unordered_map<uint32_t, ShardState> shardsRebuilding_;

  std::vector<std::unique_ptr<Timer>> scheduledRestarts_;

  /**
   * Cache of RebuildingRangesMetadata fully populated at startup. Ranges
   * are directly cleared by the RebuildingCoordinator when it acks
   * rebuilding is complete.
   *
   * RebuildingRangesMetadata is stable after LocalLogStore construction
   * for the duration of our process lifetime. The one exception is clearing
   * all ranges once rebuilding completes. By caching the data in the
   * RebuildingCoordinator, we can reference it from a Worker thread without
   * stalls.
   */
  DirtyShardMap dirtyShards_;

  node_index_t myNodeId_{-1};

  /**
   * EventLogWriter to write records to the event log
   */
  std::unique_ptr<EventLogWriter> writer_;

  /**
   * MaintenanceLogWriter to write records to the maintenance
   * log
   */
  std::unique_ptr<maintenance::MaintenanceLogWriter> maintenance_log_writer_;

  /**
   * @return True if we should rebuild metadata logs given the rebuilding set.
   * Check if this node is in the metadata nodeset, and if yes, if any node in
   * the rebuilding set is in the metadata nodeset.
   */
  bool shouldRebuildMetadataLogs(uint32_t shard_idx);

  /**
   * @return true If we should acknowledge rebuilding of shard_idx because all
   * donors rebuilt my shard and my shard has been marked as repaired in the
   * event log.
   */
  bool shouldAcknowledgeRebuilding(uint32_t shard_idx,
                                   const EventLogRebuildingSet& set);

  /**
   * Abort ShardRebuilding state machine currently running for the given shard.
   */
  void abortShardRebuilding(uint32_t shard_idx);

  /**
   * Called when a donor node finished rebuilding a shard.
   *
   * This is called each time a SHARD_IS_REBUILT event is read from the event
   * log.
   *
   * @param donor_node_idx Donor node that finished rebuilding a shard.
   * @param shard_idx      Shard that was rebuilt.
   * @param version        Rebuilding version. @see ShardState::version.
   */
  void onShardIsRebuilt(node_index_t donor_node_idx,
                        uint32_t shard_idx,
                        lsn_t version,
                        const EventLogRebuildingSet& set);
  /**
   * Actual callback used to send the SHARD_IS_REBUILT message.
   *
   * The SHARD_IS_REBULT message may be delayed if the planning
   * stage requested it, based on the 'disable_data_log_rebuilding'
   * setting. This callback is either called immediately after
   * onshardIsRebuilt or after a delay.
   */
  void notifyShardIsRebuilt();
  /**
   * Called when a shard has been marked unrecoverable in the event log.
   * If we are in the rebuilding set, check if this changes anything and enables
   * us to acknowledge rebuilding.
   */
  void onShardMarkUnrecoverable(node_index_t node_idx,
                                uint32_t shard_idx,
                                const EventLogRebuildingSet& set);

  /**
   * Called when a shard is marked as repaired in the event log.
   *
   * @param node_idx  Node for which a shard is repaired.
   * @param shard_idx Shard that was repaired.
   */
  void onShardUndrain(node_index_t node_idx,
                      uint32_t shard_idx,
                      const EventLogRebuildingSet& set);

  /**
   * Called when a node in the rebuilding set acknowledges that its shard was
   * rebuilt.
   *
   * @param node_idx       Node in the rebuilding set that acknowledges the
   *                       rebuilding.
   * @param shard_idx      Shard that was rebuilt.
   * @param version        Rebuilding version. @see ShardState::version.
   */
  void onShardAckRebuilt(node_index_t node_idx,
                         uint32_t shard_idx,
                         lsn_t version);

  /**
   * Called when a donor node informs that it made progress rebuilding a shard.
   *
   * The donor node inform of the next timestamp it needs to rebuild. All nodes
   * read the event log and deduce a global timestamp window whose left bound is
   * the minimum of all last timestamps reported by each donor node and the
   * right bound is the left bound plus the window length.
   *
   * @param donor_node_idx  Donor node that made progress.
   * @param shard_idx       Shard for which the node made progress.
   * @param next_ts         Timestamp of the next record to be rebuilt by the
   *                        donor node.
   * @param version         Rebuilding version. @see ShardState::version.
   */
  void onShardDonorProgress(node_index_t node_idx,
                            uint32_t shard_idx,
                            RecordTimestamp next_ts,
                            lsn_t version,
                            const EventLogRebuildingSet& set);

  /**
   * Called when event log was trimmed. This means that all rebuildings
   * completed and were acknowledged, so we should abort whatever rebuilding
   * we were doing and return to quiescent state.
   */
  void onEventLogTrimmed(lsn_t hi);

  /**
   * Called when we get more information on the progress made by other donor
   * nodes either because the donor node informed that it slid its local window
   * or because it informed it finished rebuilding the shard.
   *
   * Update ShardState::globalWindowEnd such that the global window's lower
   * bound matches the next timestamp of the donor node that's the most behind.
   *
   * @param shard                 Shard for which we are trying to slide the
   *                              global timestamp window.
   * @param EventLogRebuildingSet object used to retrieve the progress of each
   *        donor.
   */
  void trySlideGlobalWindow(uint32_t shard, const EventLogRebuildingSet& set);

  /**
   * Return a reference to the ShardState object for shard `shard_idx`.
   * This function asserts that the ShardState object exists in
   * `shardsRebuilding_`.
   */
  ShardState& getShardState(uint32_t shard_idx);
  const ShardState& getShardState(uint32_t shard_idx) const;

  // A helper method to write a thrift:;RemoveMaintenanceRequest to
  // maintenance log
  void writeRemoveMaintenance(ShardID shard);

  std::unique_ptr<NonAuthoritativeRebuildingChecker>
      nonAuthoratitiveRebuildingChecker_;

  friend class RebuildingCoordinatorTest;
  friend class MockedRebuildingCoordinator;
};

}} // namespace facebook::logdevice
