/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS
#include "logdevice/server/rebuilding/RebuildingPlanner.h"

#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/protocol/RELEASE_Message.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"
#include "logdevice/server/rebuilding/WaitForPurgesRequest.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"

namespace facebook { namespace logdevice {

class SyncSequencerRequestAdapter : public SyncSequencerRequest {
 public:
  SyncSequencerRequestAdapter(logid_t logid,
                              SyncSequencerRequest::Callback cb,
                              WeakRef<RebuildingPlanner> ref)
      : SyncSequencerRequest(
            logid,
            SyncSequencerRequest::WAIT_RELEASED |
                (!MetaDataLog::isMetaDataLog(logid)
                     ? SyncSequencerRequest::INCLUDE_HISTORICAL_METADATA
                     : 0), // Include historical epoch metadata for non-metadata
                           // logs.
            cb,
            GetSeqStateRequest::Context::REBUILDING_SEQ_ACTIVATOR),
        ref_(std::move(ref)),
        logid_(logid) {
    // If log is not in config, stop SyncSequencerRequest and skip rebuilding
    // the log.
    complete_if_log_not_found_ = true;
    // If we don't have access to the log, keep trying forever. We don't have
    // anything better to do, and the access error is probably due to
    // a configuration mistake.
    complete_if_access_denied_ = false;
  }

  bool isCanceled() const override {
    return !ref_;
  }

 private:
  WeakRef<RebuildingPlanner> ref_;
  logid_t logid_;
};

RebuildingPlanner::RebuildingPlanner(
    lsn_t version,
    shard_index_t shard,
    RebuildingSet rebuilding_set,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    std::shared_ptr<UpdateableConfig> config,
    Options options,
    uint32_t num_shards,
    Listener* listener)
    : version_(version),
      rebuildingSet_(std::move(rebuilding_set)),
      rebuildingSettings_(rebuilding_settings),
      listener_(listener),
      shard_(shard),
      callbackHelper_(this) {
  ld_check(listener);

  log_enumerator_ = std::make_unique<RebuildingLogEnumerator>(
      config, shard_, version, rebuilding_settings, options, num_shards, this);
}

RebuildingPlanner::~RebuildingPlanner() {
  /* clean-up stats */
  STAT_SUB(
      Worker::stats(), num_logs_rebuilding, last_reported_num_logs_to_plan_);
}

size_t RebuildingPlanner::getNumRemainingLogs() {
  return remaining_.size() + inFlight_;
}

void RebuildingPlanner::start() {
  log_enumerator_->start();
}

void RebuildingPlanner::onLogsEnumerated(
    uint32_t shard_idx,
    lsn_t version,
    std::unordered_map<logid_t, RecordTimestamp> logs,
    std::chrono::milliseconds max_rebuild_by_retention_backlog) {
  ld_check(version == version_);

  listener_->onLogsEnumerated(
      shard_, version_, max_rebuild_by_retention_backlog);

  if (logs.empty()) {
    // No logs to rebuild in this shard.
    ld_info("There are no logs to rebuild in shard %u", shard_idx);
    listener_->onFinishedRetrievingPlans(shard_, version_);
    // `this` may be destroyed here.
    return;
  }

  remaining_.reserve(logs.size());
  for (auto& kv : logs) {
    remaining_.push_back(kv.first);
  }

  next_timestamps_ = std::move(logs);

  // for tests
  std::sort(remaining_.begin(), remaining_.end());

  ld_info("Starting rebuilding of shard %u with rebuilding set: %s",
          shard_idx,
          rebuildingSet_.describe().c_str());

  maybeSendMoreRequests();
}

void RebuildingPlanner::maybeSendMoreRequests() {
  const auto max = rebuildingSettings_->max_get_seq_state_in_flight;

  while (!remaining_.empty() && inFlight_ < max) {
    logid_t logid = remaining_.back();
    remaining_.pop_back();

    sendSyncSequencerRequest(logid);
    ++inFlight_;
  }

  /* bump stats */
  int64_t nLogsToPlan = getNumRemainingLogs();
  STAT_ADD(Worker::stats(),
           num_logs_rebuilding,
           nLogsToPlan - last_reported_num_logs_to_plan_);
  last_reported_num_logs_to_plan_ = nLogsToPlan;

  if (nLogsToPlan == 0) {
    listener_->onFinishedRetrievingPlans(shard_, version_);
    // `this` may be destroyed here.
  }
}

void RebuildingPlanner::sendSyncSequencerRequest(logid_t logid) {
  auto callback_ticket = callbackHelper_.ticket();
  auto cb = [=](Status st,
                NodeID seq,
                lsn_t next_lsn,
                std::unique_ptr<LogTailAttributes> /*tail_attributes*/,
                std::shared_ptr<const EpochMetaDataMap> metadata_map,
                std::shared_ptr<TailRecord> /*tail_record*/) {
    callback_ticket.postCallbackRequest([=](RebuildingPlanner* planner) {
      if (!planner) {
        ld_debug("SyncSequencerRequest finished after RebuildingPlanner was "
                 "destroyed");
        return;
      }

      // We did not define a timeout, so SyncSequencerRequest should
      // eventually succeed, be aborted because log was removed
      // from config or return E::NOTFOUND.
      ld_check_in(st, ({E::OK, E::CANCELLED, E::NOTFOUND}));

      LogState& log_state = log_states_[logid];
      // If it's metadata log, and the corresponding data log is not in config
      // anymore, rebuild the metadata log anyway. This way if the log is
      // re-added to config, its metadata log won't be underreplicated.
      if (st == E::OK ||
          (st == E::NOTFOUND && MetaDataLog::isMetaDataLog(logid))) {
        if (st == E::OK) {
          ld_check(next_lsn != LSN_INVALID);
          ld_check(seq.isNodeID());
          log_state.until_lsn = next_lsn - 1;
          log_state.seq = seq;
        } else {
          ld_info("Log %lu is not in config, but its metadata log %lu has some "
                  "records. Will rebuild the metadata log until LSN_MAX, and "
                  "use fake node ID (N0:1) for seals.",
                  MetaDataLog::dataLogID(logid).val(),
                  logid.val());
          log_state.until_lsn = LSN_MAX;
          log_state.seq = NodeID(0, 1);
        }
        planner->onSyncSequencerComplete(logid, std::move(metadata_map));
      } else {
        // logid was not found, remove it.
        ld_warning("Error in SyncSequencerRequest for log %lu: %s.",
                   logid.val(),
                   error_name(st));
        log_state.plan.clear(); // Empty plan, nothing to rebuild.
        onComplete(logid);
      }
    });
  };

  std::unique_ptr<Request> rq = std::make_unique<SyncSequencerRequestAdapter>(
      logid, cb, callbackHelper_.getHolder().ref());
  ld_debug("Posting a new SyncSequencerRequest(id:%" PRIu64 ") for "
           "log:%lu",
           (uint64_t)rq->id_,
           logid.val_);
  Worker::onThisThread()->processor_->postWithRetrying(rq);
}

void RebuildingPlanner::onSyncSequencerComplete(
    logid_t logid,
    std::shared_ptr<const EpochMetaDataMap> metadata_map) {
  LogState& log_state = log_states_[logid];

  /* We are now going to process every interval for the epoch range
   * [EPOCH_MIN,epoch_max] */
  epoch_t epoch_max = std::min(lsn_to_epoch(log_state.until_lsn), EPOCH_MAX);

  if (MetaDataLog::isMetaDataLog(logid)) {
    // For metadata logs, the config should have the metadata.
    auto cfg = Worker::getConfig();
    auto meta_storage_set = EpochMetaData::nodesetToStorageSet(
        cfg->serverConfig()->getMetaDataNodeIndices(),
        logid,
        *cfg->serverConfig());
    auto meta_log = cfg->serverConfig()->getMetaDataLogGroup();
    auto metadata = std::make_shared<EpochMetaData>(
        meta_storage_set,
        ReplicationProperty::fromLogAttributes(meta_log->attrs()));
    ld_check(metadata->isValid());
    processMetadataForEpochInterval(
        logid, std::move(metadata), EPOCH_MIN, epoch_max);
  } else {
    ld_check(metadata_map != nullptr);
    epoch_t effective_until = metadata_map->getEffectiveUntil();
    ld_check(epoch_max == EPOCH_MAX || effective_until >= epoch_max);

    // We now iterate through the metadata map provided by the sequencer. Every
    // iteration covers a certain range of epochs.
    for (auto t : *metadata_map) {
      epoch_t epoch_first, epoch_last;

      std::tie(epoch_first, epoch_last) = t.first;
      const EpochMetaData& metadata = t.second;

      if (epoch_first <= epoch_max) {
        // i.e. if we have an intersection.
        processMetadataForEpochInterval(
            logid,
            std::make_shared<EpochMetaData>(metadata),
            epoch_first,
            std::min(epoch_last, epoch_max));
      }
    }
  }
}

std::vector<shard_index_t>
RebuildingPlanner::findDonorShards(EpochMetaData& metadata) const {
  std::vector<shard_index_t> donors;

  auto cfg = Worker::getConfig();

  // First, remove from the storage set any node that's no longer in the config.
  // This may happen if the cluster was shrunk.
  StorageSet shards_ = metadata.shards;
  shards_.erase(std::remove_if(shards_.begin(),
                               shards_.end(),
                               [&](ShardID shard) {
                                 const auto& node =
                                     cfg->serverConfig()->getNode(shard.node());
                                 return !node || !node->isReadableStorageNode();
                               }),
                shards_.end());
  metadata.setShards(shards_);

  const StorageSet& shards = metadata.shards;

  bool intersect = std::any_of(shards.begin(), shards.end(), [this](ShardID s) {
    return rebuildingSet_.shards.find(s) != rebuildingSet_.shards.end();
  });
  if (!intersect) {
    // The rebuilding set does not intersect with this storage set.
    return donors;
  }

  // For each donor shard on this node, check if it should be a donor for this
  // EpochMetaData.
  node_index_t nid = cfg->serverConfig()->getMyNodeID().index();
  const auto node = cfg->serverConfig()->getNode(nid);
  ld_check(node);
  for (shard_index_t shard = 0; shard < node->getNumShards(); ++shard) {
    ShardID donor = ShardID(nid, shard);
    if (std::find(shards.begin(), shards.end(), donor) != shards.end()) {
      donors.push_back(shard);
    }
  }

  return donors;
}

bool RebuildingPlanner::rebuildingIsAuthoritative(
    const EpochMetaData& metadata) const {
  auto cfg = Worker::getConfig()->serverConfig();
  const auto& rebuilding_set = rebuildingSet_.shards;

  // Create a FailureDomainNodeSet where the attribute is whether or not the
  // node is rebuilding in RESTORE mode...
  FailureDomainNodeSet<bool> f(metadata.shards, cfg, metadata.replication);

  // ... Set the attribute for each node being rebuilt that is currently
  // unavailable, but may return with intact data later.  This is all nodes
  // being rebuilt in RESTORE mode except those that have reached
  // AUTHORITATIVE_EMPTY status (previously rebuilt or administratively
  // declared UNRECOVERABLE) or are in the rebuilding set for a time-ranged
  // rebuild where we assume the node is back up and any data it is missing
  // is permanently lost.
  for (auto& n : rebuilding_set) {
    if (n.second.mode == RebuildingMode::RESTORE &&
        n.second.dc_dirty_ranges.empty() &&
        !rebuildingSet_.empty.count(n.first) && f.containsShard(n.first)) {
      f.setShardAttribute(n.first, true);
    }
  }
  // ... and check if we can replicate amongst such nodes, which would mean
  // rebuilding may miss some records and thus is not authoritative.
  if (f.canReplicate(true)) {
    return false;
  } else {
    return true;
  }
}

bool RebuildingPlanner::rebuildingSetTooBig(
    const EpochMetaData& metadata) const {
  auto cfg = Worker::getConfig()->serverConfig();

  // Create a FailureDomainNodeSet where the attribute is whether or not a
  // storage shard can receive stores...
  FailureDomainNodeSet<bool> f(metadata.shards, cfg, metadata.replication);
  for (ShardID s : metadata.shards) {
    auto kv = rebuildingSet_.shards.find(s);
    // Assume shards that aren't rebuilding or are rebuilding just for some
    // time ranges can receive data.
    bool accepting_stores = kv == rebuildingSet_.shards.end() ||
        !kv->second.dc_dirty_ranges.empty();
    f.setShardAttribute(s, accepting_stores);
  }

  // ... and verify that we can replicate amongst them.  If we cannot
  // replicate because too many shards are rebuilding, we should skip the
  // epoch otherwise this state machine will stay stuck forever unless
  // rebuilding is aborted.
  return !f.canReplicate(true);
}

void RebuildingPlanner::processMetadataForEpochInterval(
    logid_t logid,
    std::shared_ptr<EpochMetaData> metadata,
    epoch_t epoch_first,
    epoch_t epoch_last) {
  auto it = log_states_.find(logid);
  ld_check(it != log_states_.end());
  LogState& log_state = it->second;

  auto cfg = Worker::getConfig();

  auto donors = findDonorShards(*metadata);
  if (!donors.empty()) {
    if (rebuildingSetTooBig(*metadata)) {
      RATELIMIT_ERROR(
          std::chrono::seconds(1),
          1,
          "Cannot rebuild records in epochs [%u, %u] of log %lu because "
          "rebuilding set is too big (%s), which prevents data to be "
          "replicated to %s. Skipping this epoch and marking this rebuilding "
          "non authoritative.",
          epoch_first.val_,
          epoch_last.val_,
          logid.val_,
          rebuildingSet_.describe().c_str(),
          metadata->replication.toString().c_str());
      log_state.isAuthoritative = false;
    } else {
      for (shard_index_t shard : donors) {
        if (!log_state.plan.count(shard)) {
          auto next_ts = next_timestamps_.at(logid);
          log_state.plan[shard] = std::make_unique<RebuildingPlan>(next_ts);
        }
        log_state.plan[shard]->addEpochRange(epoch_first, epoch_last, metadata);
      }
      if (!rebuildingIsAuthoritative(*metadata)) {
        RATELIMIT_WARNING(
            std::chrono::seconds(1),
            1,
            "Rebuilding of records in epochs [%u, %u] of log %lu is non "
            "authoritative with rebuilding set (%s).",
            epoch_first.val_,
            epoch_last.val_,
            logid.val_,
            rebuildingSet_.describe().c_str());
        log_state.isAuthoritative = false;
      }
    }
  }

  if (epoch_last >= std::min(lsn_to_epoch(log_state.until_lsn), EPOCH_MAX)) {
    // This was the last epoch metadata.
    waitForPurges(logid);
  }
}

void RebuildingPlanner::waitForPurges(logid_t logid) {
  auto callback_ticket = callbackHelper_.ticket();
  auto cb = [=](Status st) {
    callback_ticket.postCallbackRequest([=](RebuildingPlanner* planner) {
      if (!planner) {
        ld_debug("WaitForPurgesRequest finished after RebuildingPlanner was "
                 "destroyed");
        return;
      }
      ld_check(st == E::OK || st == E::NOTFOUND);
      // We can get E::NOTFOUND if the log was removed from the config.
      // If the log was removed from the config, RebuildingCoordinator should
      // abort rebuilding for it anyway.
      // If the log is a metadata log (and thus the corresponding data log was
      // removed from the config), the full content of the log should be rebuilt
      // anyway.
      onComplete(logid);
    });
  };

  LogState& log_state = log_states_[logid];

  if (log_state.plan.empty()) {
    // No need to wait for purging. The plan is empty.
    onComplete(logid);
    return; // `this` is destroyed here.
  }

  if (log_state.until_lsn == LSN_MAX) {
    // until_lsn will be set to LSN_MAX if we should just rebuild the whole log
    // without worrying about data being released or purged.
    onComplete(logid);
    return; // `this` is destroyed here.
  }

  ld_check(log_state.seq.isNodeID());

  folly::small_vector<shard_index_t> shards_to_purge;
  shards_to_purge.reserve(log_state.plan.size());
  for (const auto& p : log_state.plan) {
    shards_to_purge.push_back(p.first);
  }

  std::unique_ptr<Request> rq =
      std::make_unique<WaitForPurgesRequest>(logid,
                                             log_state.seq,
                                             log_state.until_lsn,
                                             std::move(shards_to_purge),
                                             cb,
                                             rebuildingSettings_);
  ld_debug("Posting a new WaitForPurgesRequest(id:%" PRIu64 ") for "
           "log:%lu",
           (uint64_t)rq->id_,
           logid.val_);
  Worker::onThisThread()->processor_->postWithRetrying(rq);
}

void RebuildingPlanner::onComplete(logid_t logid) {
  ld_check(log_states_.count(logid));
  LogState& log_state = log_states_[logid];
  ld_check(inFlight_ > 0);
  --inFlight_;

  LogPlan plan = std::move(log_state.plan);
  for (auto& p : plan) {
    p.second->untilLSN = log_state.until_lsn;
    p.second->sequencerNodeID = log_state.seq;
  }
  listener_->onRetrievedPlanForLog(
      logid, shard_, std::move(plan), log_state.isAuthoritative, version_);
  log_states_.erase(logid);
  maybeSendMoreRequests();
}

}} // namespace facebook::logdevice
