/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <mutex>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/LogTailAttributes.h"
#include "logdevice/server/RebuildingLogEnumerator.h"
#include "logdevice/server/rebuilding/RebuildingPlan.h"

namespace facebook { namespace logdevice {

/**
 * RebuildingPlanner is responsible for retrieving a RebuildingPlan for each log
 * on each local donor shard.
 *
 * A RebuildingPlan is the set of epoch ranges that the LogRebuilding state
 * machine on each shard will be responsible for rebuilding.
 *
 * This state machine starts off retrieving the LSN up to which the log has to
 * be rebuilt using SyncSequencerRequest. This state machine also uses the
 * WAIT_RELEASED and WAIT_PURGED arguments of SyncSequencerRequest has we don't
 * want the local shard to touch records that belong to unclean epochs and/or
 * have not been purged by purging.
 *
 * In order to retrieve the set of epochs that will need to be rebuilt, this
 * state machine reads the metadata log of the data log. For each epoch, we
 * check whether the storage set intersects with the rebuilding set. If it does
 * not, the epoch is not added to the plan.
 *
 * We then find the list of local shards that should be donors by looking if
 * they belong to the storage set.
 *
 * This state machine is also responsible for determining if the current
 * rebuilding is authoritative or not. An authoritative rebuilding is a
 * rebuilding that guarantees that all records will be left sufficiently
 * replicated when it completes. If replication factor is 3 and 3 or more shards
 * spanning more than 3 nodes are in the rebuilding set, then we know that
 * rebuilding is not authoritative. The event log state machine uses this
 * information to make decisions on when to transition a shard's authoritative
 * status to AUTHORITATIVE_EMPTY.
 *
 * ## A note about Flexible Log Sharding
 *
 * Currently RebuildingCoordinator runs one RebuildingPlanner per local shard.
 * This is due to the current architecture allowing one separate rebuilding
 * state machine per shard offset (all nodes in the cluster currently have the
 * same number of shards). With FLS, we want to make it possible to have
 * heterogeneous number of shards in the cluster, or we want to make it possible
 * for a storage set to contains more than one shard per node.
 * In order to support that, RebuildingCoordinator will be changed such that it
 * runs one and only one RebuildingPlanner, and the resulting plan is used to
 * define which logs are rebuilt by which local shards.
 *
 * The interface of RebuildingPlanner is compatible with that. Currently
 * RebuildingCoordinator only uses RebuildingPlanner for the set of logs that
 * are for a specific local shard according to getLegacyShardIndexForLog().
 * Then, RebuildingCoordinator asserts that the resulting plan only contains
 * entries for that shard.
 * In the future, RebuildingCoordinator will run only one RebuildingPlanner for
 * all shards at the beginning of rebuilding.
 */

// Schedule a SyncSequencerRequest for all the given logs.
// Lives on the worker thread on which constructor was called.
class RebuildingPlanner : public RebuildingLogEnumerator::Listener {
 public:
  using EnumerationResults = RebuildingLogEnumerator::Results;
  using LogPlan =
      std::unordered_map<shard_index_t, std::unique_ptr<RebuildingPlan>>;
  using ParametersPerShard = RebuildingLogEnumerator::ParametersPerShard;
  using Parameters = RebuildingLogEnumerator::Parameters;

  class Listener {
   public:
    virtual ~Listener() {}

    // Called once we have rebuilding plan for a log.
    virtual void onRetrievedPlanForLog(logid_t log,
                                       uint32_t shard,
                                       std::unique_ptr<RebuildingPlan> log_plan,
                                       bool is_authoritative,
                                       lsn_t version) = 0;

    // Used to pass up the ms until expiration of the log with the
    // highest retention backlog on this shard.
    virtual void onLogsEnumerated(
        uint32_t shard,
        lsn_t version,
        std::chrono::milliseconds max_rebuild_by_retention_backlog) = 0;

    // Tell listener to not expect new plans.
    // May destroy the RebuildingPlanner.
    virtual void onFinishedRetrievingPlans(uint32_t shard, lsn_t version) = 0;
  };

  RebuildingPlanner(ParametersPerShard parameters,
                    RebuildingSets rebuilding_sets,
                    UpdateableSettings<RebuildingSettings> rebuilding_settings,
                    std::shared_ptr<UpdateableConfig> config,
                    uint32_t max_num_shards,
                    bool rebuild_internal_logs,
                    Listener* listener);
  virtual void start();
  virtual ~RebuildingPlanner();

  void onLogsEnumerated(
      EnumerationResults results,
      std::chrono::milliseconds max_rebuild_by_retention_backlog) override;

  void abortShardIdx(shard_index_t shard);

  bool isEnumerationComplete();
  size_t getNumRemainingLogs();

 private:
  friend class SyncSequencerRequestAdapter;
  friend class RebuildingCoordinatorTest;

  // State maintained for each log as we are reading the metadata log to build a
  // plan for it.
  struct LogState {
    LogPlan plan;
    lsn_t until_lsn;
    NodeID seq;
    std::unordered_set<shard_index_t> non_auth_shards;
    std::unique_ptr<ExponentialBackoffTimer> timer;
  };
  std::unordered_map<logid_t, LogState, logid_t::Hash> log_states_;

  ParametersPerShard parameters_;
  RebuildingSets rebuildingSets_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  Listener* listener_;
  std::unique_ptr<RebuildingLogEnumerator> log_enumerator_;

  // Used to ensure we have a callback called on this Worker thread when the
  // SyncSequencerRequest completes.
  WorkerCallbackHelper<RebuildingPlanner> callbackHelper_;

  size_t inFlight_{0};
  std::vector<logid_t> remaining_;
  std::unordered_map<logid_t, RecordTimestamp, logid_t::Hash> next_timestamps_;
  int64_t last_reported_num_logs_to_plan_{0};

  // Looks at queue to see if some requests are ready to be sent.
  // May destroy `this`.
  void maybeSendMoreRequests();

  void sendSyncSequencerRequest(logid_t logid);

  // Calls the onRetrievedPlanForLog() method of the listener.
  void
  onSyncSequencerComplete(logid_t logid,
                          std::shared_ptr<const EpochMetaDataMap> metadata_map);

  std::vector<shard_index_t> findDonorShards(EpochMetaData& metadata) const;

  /**
   * Check if rebuilding of the given EpochMetaData will be authoritative.
   * Rebuliding is authoritative if it is known it won't miss any records, ie
   * there is no record whose copyset can possibly be a subset of the rebuilding
   * set.
   */
  bool rebuildingIsAuthoritative(const EpochMetaData& metadata) const;

  /**
   * Check if the rebuilding set is too big such that it will not be possible to
   * re-replicate data for the given EpochMetaData.
   */
  bool rebuildingSetTooBig(const EpochMetaData& metadata) const;

  /**
   * Process metadata for the epoch interval [epoch_first, epoch_last] (all
   * inclusive).
   */
  void computePlanForEpochInterval(logid_t logid,
                                   std::shared_ptr<EpochMetaData> metadata,
                                   epoch_t epoch_first,
                                   epoch_t epoch_last);

  /**
   * Start a WaitForPurgesRequest to wait for purging to complete for all shards
   * that are part of the RebuildingPlan.
   */
  void waitForPurges(logid_t logid);

  /**
   * Called when we completed the state machine for a log.
   */
  void onComplete(logid_t logid);

  /**
   * Tell shards to stop waiting for plans.
   */
  void notifyFinishedDeliveringPlans();

  /**
   * Returns a comma separated string with the description of the rebuilding
   * sets for indices passed as argument.
   */
  std::string rebuildingSetsDescription(std::vector<shard_index_t>& shard_idxs);
};

}} // namespace facebook::logdevice
