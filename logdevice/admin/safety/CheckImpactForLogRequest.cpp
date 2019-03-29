/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/CheckImpactForLogRequest.h"

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"

using namespace facebook::logdevice::configuration;
namespace facebook { namespace logdevice {

CheckImpactForLogRequest::CheckImpactForLogRequest(
    logid_t log_id,
    std::chrono::milliseconds timeout,
    ShardAuthoritativeStatusMap shard_status,
    ShardSet op_shards,
    StorageState target_storage_state,
    SafetyMargin safety_margin,
    bool check_metadata_nodeset,
    WorkerType worker_type,
    Callback callback)
    : Request(RequestType::CHECK_IMPACT_FOR_LOG),
      log_id_(log_id),
      timeout_(timeout),
      shard_status_(std::move(shard_status)),
      op_shards_(std::move(op_shards)),
      target_storage_state_(target_storage_state),
      safety_margin_(std::move(safety_margin)),
      check_metadata_nodeset_(check_metadata_nodeset),
      worker_type_(worker_type),
      callback_(std::move(callback)) {
  if (!check_metadata_nodeset_) {
    ld_check(log_id != LOGID_INVALID);
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  }

  ld_check(callback_ != nullptr);
}

CheckImpactForLogRequest::~CheckImpactForLogRequest() {
  ld_check(current_worker_.val_ == -1 ||
           current_worker_ == Worker::onThisThread()->idx_);
}

WorkerType CheckImpactForLogRequest::getWorkerTypeAffinity() {
  return worker_type_;
}

Request::Execution CheckImpactForLogRequest::execute() {
  if (check_metadata_nodeset_) {
    checkMetadataNodeset();
  } else {
    ld_debug("CheckImpactForLogRequest::execute for log %lu", log_id_.val_);

    // Worker thread on which the request is running
    current_worker_ = Worker::onThisThread()->idx_;

    fetchHistoricalMetadata();
  }

  return Execution::CONTINUE;
}

std::tuple<bool, bool, NodeLocationScope>
CheckImpactForLogRequest::checkReadWriteAvailablity(
    const StorageSet& storage_set,
    const ReplicationProperty& replication_property) {
  bool safe_writes = true;
  bool safe_reads = true;
  NodeLocationScope fail_scope;

  // We always validate write availability issues, this is because the
  // target_storage_state cannot be READ_WRITE in this class. it will either be
  // READ_ONLY or DISABLED.
  ReplicationProperty replication_prop =
      extendReplicationWithSafetyMargin(replication_property, true);
  if (replication_prop.isEmpty()) {
    safe_writes = false;
  } else {
    safe_writes =
        checkWriteAvailability(storage_set, replication_prop, &fail_scope);
  }
  if (target_storage_state_ == StorageState::DISABLED) {
    replication_prop =
        extendReplicationWithSafetyMargin(replication_property, false);
    if (replication_prop.isEmpty()) {
      safe_reads = false;
    } else {
      safe_reads = checkReadAvailability(storage_set, replication_prop);
    }
  }
  return std::make_tuple(safe_reads, safe_writes, fail_scope);
}

void CheckImpactForLogRequest::checkMetadataNodeset() {
  auto config = Worker::onThisThread()->getConfiguration();
  auto metadatalogs_config = config->serverConfig()->getMetaDataLogsConfig();
  auto metadatalog_group = config->serverConfig()->getMetaDataLogGroup();
  if (!metadatalog_group) {
    complete(E::OK,
             Impact::ImpactResult::NONE,
             EPOCH_INVALID,
             {},
             {},
             ReplicationProperty());
    return;
  }
  ReplicationProperty replication_property =
      ReplicationProperty::fromLogAttributes(metadatalog_group->attrs());

  const shard_size_t n_shards = config->serverConfig()->getNumShards();
  int impact_result = Impact::ImpactResult::NONE;
  Impact::StorageSetMetadata storage_set_metadata;
  StorageSet storage_set;
  for (shard_size_t shard_id = 0; shard_id < n_shards; ++shard_id) {
    // TODO(T15517759): metadata log storage set should use ShardID.
    storage_set = EpochMetaData::nodesetToStorageSet(
        metadatalogs_config.metadata_nodes, shard_id);
    storage_set_metadata = getStorageSetMetadata(storage_set);

    NodeLocationScope fail_scope;
    bool safe_reads;
    bool safe_writes;
    std::tie(safe_reads, safe_writes, fail_scope) =
        checkReadWriteAvailablity(storage_set, replication_property);

    if (!safe_writes) {
      impact_result |= Impact::ImpactResult::WRITE_AVAILABILITY_LOSS;
    }
    if (!safe_reads) {
      impact_result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;
    }
    if (impact_result != Impact::ImpactResult::NONE) {
      break;
    }
  }

  complete(E::OK,
           impact_result,
           EPOCH_INVALID,
           std::move(storage_set),
           std::move(storage_set_metadata),
           replication_property);
}

void CheckImpactForLogRequest::complete(
    Status st,
    int impact_result,
    epoch_t error_epoch,
    StorageSet storage_set,
    Impact::StorageSetMetadata storage_set_metadata,
    ReplicationProperty replication) {
  // call user provided callback
  callback_(st,
            Impact::ImpactOnEpoch(log_id_,
                                  error_epoch,
                                  std::move(storage_set),
                                  std::move(storage_set_metadata),
                                  std::move(replication),
                                  impact_result));

  // destroy the request
  delete this;
}

void CheckImpactForLogRequest::fetchHistoricalMetadata() {
  nodeset_finder_ = std::make_unique<NodeSetFinder>(
      log_id_,
      timeout_,
      [this](Status st) {
        if (st != E::OK) {
          if (st == E::NOTINCONFIG || st == E::NOTFOUND) {
            // E::NOTINCONFIG - log not in config,
            // is ignored as it is possible due to config change
            // E::NOTFOUND - metadata not provisioned
            // is ignored as this means log is empty
            // We treat as it's safe for the above reasons
            complete(E::OK,
                     Impact::ImpactResult::NONE,
                     EPOCH_INVALID,
                     {},
                     {},
                     ReplicationProperty());
            return;
          }
          std::string message =
              folly::format(
                  "Fetching historical metadata for log {} FAILED: {}. ",
                  log_id_.val(),
                  error_description(st))
                  .str();
          complete(st);
          return; // `this` has been destroyed.
        }

        auto result = nodeset_finder_->getResult();
        for (const auto& interval : *result) {
          if (!onEpochMetaData(interval.second)) {
            // `this` was destroyed.
            return;
          }
        }
        complete(E::OK,
                 Impact::ImpactResult::NONE,
                 EPOCH_INVALID,
                 {},
                 {},
                 ReplicationProperty());
      },
      (read_epoch_metadata_from_sequencer_
           ? NodeSetFinder::Source::BOTH
           : NodeSetFinder::Source::METADATA_LOG));

  nodeset_finder_->start();
}

// returns empty ReplicationProperty if is impossible to satisfy
// resulting replication property
ReplicationProperty CheckImpactForLogRequest::extendReplicationWithSafetyMargin(
    const ReplicationProperty& replication_base,
    bool add) const {
  ReplicationProperty replication_new(replication_base);
  auto scope = NodeLocation::nextSmallerScope(NodeLocationScope::ROOT);
  int prev = 0;
  while (scope != NodeLocationScope::INVALID) {
    int replication = replication_base.getReplication(scope);
    // Do not consider the scope if the user did not specify a replication
    // factor for it
    if (replication != 0) {
      auto safety = safety_margin_.find(scope);
      if (safety != safety_margin_.end()) {
        if (add) {
          replication += safety->second;
        } else {
          replication -= safety->second;
        }
        if (replication <= 0) {
          // can't be satisfied. fail on this
          return ReplicationProperty();
        }
        // required to bypass ReplicationProperty validation
        // as lower scope can't be smaller
        int max_for_higher_domains = std::max(replication, prev);
        replication_new.setReplication(scope, max_for_higher_domains);
        prev = max_for_higher_domains;
      }
    }
    scope = NodeLocation::nextSmallerScope(scope);
  }
  return replication_new;
}

bool CheckImpactForLogRequest::onEpochMetaData(EpochMetaData metadata) {
  ld_check(Worker::onThisThread()->idx_ == current_worker_);
  ld_check(metadata.isValid());
  NodeLocationScope fail_scope;
  bool safe_writes;
  bool safe_reads;

  const auto since = metadata.h.effective_since.val_;
  const auto epoch = metadata.h.epoch.val_;

  std::tie(safe_reads, safe_writes, fail_scope) =
      checkReadWriteAvailablity(metadata.shards, metadata.replication);

  if (safe_writes && safe_reads) {
    ld_debug("for log %lu, epochs [%u, %u] is OK", log_id_.val_, since, epoch);
    return true;
  }

  // in case we can't replicate we return epoch & nodeset for it

  std::unordered_set<node_index_t> nodes_to_drain;
  for (const auto& shard_id : op_shards_) {
    nodes_to_drain.insert(shard_id.node());
  }

  std::string message;

  if (!safe_writes) {
    message =
        folly::format(
            "Drain on ({} shards, {} nodes) would cause "
            "loss of write availability for log {}, epochs [{}, {}], as in "
            "that storage set not enough {} domains would be "
            "available for writes. ",
            op_shards_.size(),
            nodes_to_drain.size(),
            log_id_.val(),
            since,
            epoch,
            NodeLocation::scopeNames()[fail_scope])
            .str();
  }

  if (!safe_reads) {
    message += folly::format(
                   "Disabling reads on ({} shards, {} nodes) would cause "
                   "loss of read availability for log {}, epochs [{}, {}], as "
                   "that storage would not constitute f-majority ",
                   op_shards_.size(),
                   nodes_to_drain.size(),
                   log_id_.val(),
                   since,
                   epoch)
                   .str();
  }

  int impact_result = 0;
  if (!safe_reads) {
    impact_result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;
  }
  if (!safe_writes) {
    impact_result |= Impact::ImpactResult::REBUILDING_STALL;
    // TODO #22911589 check do we lose write availablility
  }

  Impact::StorageSetMetadata storage_set_metadata =
      getStorageSetMetadata(metadata.shards);
  complete(E::OK,
           impact_result,
           metadata.h.effective_since,
           std::move(metadata.shards),
           std::move(storage_set_metadata),
           std::move(metadata.replication));
  return false;
}

bool CheckImpactForLogRequest::checkWriteAvailability(
    const StorageSet& storage_set,
    const ReplicationProperty& replication,
    NodeLocationScope* fail_scope) const {
  auto config = Worker::onThisThread()->getConfiguration();
  // We use FailureDomainNodeSet to determine if draining shards in
  // `op_shards_` would result in this StorageSet being unwritable. The boolean
  // attribute indicates for each node whether it will be able to take writes
  // after the drain. As such, a node can  take writes if:
  // * It's weight is > 0;
  // * it's not part  of `op_shards_`;
  // * it's FULLY_AUTHORITATIVE (ie it's not been drained or being drained,
  //   or it's not being rebuilt / in repair).

  // TODO #21954681 Add safety threshold x:
  // if N nodes are required to maintain write availability, make sure to
  // always have N+x nodes to have room for organic failures of x nodes
  FailureDomainNodeSet<bool> available_node_set(
      storage_set,
      *config->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      replication);

  for (const ShardID& shard : storage_set) {
    const auto& node = config->serverConfig()->getNode(shard.node());

    if (node && node->isWritableStorageNode()) {
      if (!op_shards_.count(shard)) {
        AuthoritativeStatus status = shard_status_.getShardStatus(shard);
        if (status == AuthoritativeStatus::FULLY_AUTHORITATIVE &&
            isAlive(shard.node())) {
          available_node_set.setShardAttribute(shard, true);
        }
      }
    }
  }
  return available_node_set.canReplicate(true, fail_scope);
}

bool CheckImpactForLogRequest::checkReadAvailability(
    const StorageSet& storage_set,
    const ReplicationProperty& replication) const {
  auto config = Worker::onThisThread()->getConfiguration();
  // We use FailureDomainNodeSet to determine if disabling reads for shards in
  // `op_shards_` would result in this StorageSet being unreadable.
  // The boolean attribute indicates for each node whether it will be able to
  // serve reads after the operation. As such, a node can serve reads if:
  // * It's weight is >= 0 (storage node);
  // * it's not part  of `op_shards_`;
  // Authoritative status is  set for all shards and it is taken into
  // account by isFmajority.
  // We should have f-majority of FULLY_AUTHORITATIVE (ie it's not been drained
  // or being drained, or it's not being rebuilt / in repair) shards, excluding
  // shards on which we are going to be stopped.

  FailureDomainNodeSet<bool> available_node_set(
      storage_set,
      *config->serverConfig()->getNodesConfigurationFromServerConfigSource(),
      replication);

  for (const ShardID& shard : storage_set) {
    const auto& node = config->serverConfig()->getNode(shard.node());

    if (node && node->isReadableStorageNode()) {
      AuthoritativeStatus status = shard_status_.getShardStatus(shard);
      if (shard_status_.shardIsTimeRangeRebuilding(
              shard.node(), shard.shard())) {
        // The shard has time-range rebuilding, we will lean on the safe-side
        // and mark this shard as UNAVAILABLE instead of FULLY_AUTHORITATIVE to
        // ensure we block operations that _may_ cause the mini-rebuilding to
        // stall.
        status = AuthoritativeStatus::UNAVAILABLE;
      }
      available_node_set.setShardAuthoritativeStatus(shard, status);
      if ((!op_shards_.count(shard)) && isAlive(shard.node())) {
        available_node_set.setShardAttribute(shard, true);
      }
    }
  }
  FmajorityResult health_state = available_node_set.isFmajority(true);

  // we treat NON_AUTHORITATIVE as unsafe, as we may increase damage,
  // i.e. more records will become unaccesible if stop more nodes
  auto res = health_state == FmajorityResult::AUTHORITATIVE_COMPLETE ||
      health_state == FmajorityResult::AUTHORITATIVE_INCOMPLETE;

  return res;
}

Impact::StorageSetMetadata
CheckImpactForLogRequest::getStorageSetMetadata(const StorageSet& storage_set) {
  auto config = Worker::onThisThread()->getConfiguration();
  Impact::StorageSetMetadata out;
  for (const auto& shard : storage_set) {
    const auto& node = config->serverConfig()->getNode(shard.node());
    out.push_back(Impact::ShardMetadata{
        .auth_status = shard_status_.getShardStatus(shard),
        .is_alive = isAlive(shard.node()),
        .storage_state = node->storage_attributes->state,
        .location = node->location});
  }
  return out;
}

bool CheckImpactForLogRequest::isAlive(node_index_t index) const {
  auto* cluster_state = Worker::getClusterState();
  if (cluster_state) {
    return cluster_state->isNodeAlive(index);
  } else {
    return true;
  }
}

}} // namespace facebook::logdevice
