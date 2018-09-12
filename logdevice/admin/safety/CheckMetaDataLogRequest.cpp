/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "CheckMetaDataLogRequest.h"

#include <boost/format.hpp>
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/server/FailureDetector.h"

namespace facebook { namespace logdevice {

CheckMetaDataLogRequest::CheckMetaDataLogRequest(
    logid_t log_id,
    std::chrono::milliseconds timeout,
    std::shared_ptr<Configuration> config,
    const ShardAuthoritativeStatusMap& shard_status,
    std::shared_ptr<ShardSet> op_shards,
    int operations,
    const SafetyMargin* safety_margin,
    bool check_metadata_nodeset,
    Callback callback)
    : Request(RequestType::CHECK_METADATA_LOG),
      log_id_(log_id),
      timeout_(timeout),
      config_(std::move(config)),
      shard_status_(shard_status),
      op_shards_(std::move(op_shards)),
      operations_(operations),
      safety_margin_(safety_margin),
      check_metadata_nodeset_(check_metadata_nodeset),
      callback_(std::move(callback)) {
  ld_check(op_shards_ != nullptr);
  if (!check_metadata_nodeset_) {
    ld_check(log_id != LOGID_INVALID);
    ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  }

  ld_check(callback_ != nullptr);
}

CheckMetaDataLogRequest::~CheckMetaDataLogRequest() {
  ld_check(current_worker_.val_ == -1 ||
           current_worker_ == Worker::onThisThread()->idx_);
}

Request::Execution CheckMetaDataLogRequest::execute() {
  if (check_metadata_nodeset_) {
    checkMetadataNodeset();
  } else {
    ld_debug("CheckMetaDataLogRequest::execute for log %lu", log_id_.val_);

    // Worker thread on which the request is running
    current_worker_ = Worker::onThisThread()->idx_;

    fetchHistoricalMetadata();
  }

  return Execution::CONTINUE;
}

std::tuple<bool, bool, NodeLocationScope>
CheckMetaDataLogRequest::checkReadWriteAvailablity(
    const StorageSet& storage_set,
    const ReplicationProperty& replication_property) {
  bool safe_writes = true;
  bool safe_reads = true;
  NodeLocationScope fail_scope;

  if (operations_ & Operation::DISABLE_WRITES) {
    ReplicationProperty replication_prop =
        extendReplicationWithSafetyMargin(replication_property, true);
    if (replication_prop.isEmpty()) {
      safe_writes = false;
    } else {
      safe_writes =
          checkWriteAvailability(storage_set, replication_prop, &fail_scope);
    }
  }
  if (operations_ & Operation::DISABLE_READS) {
    ReplicationProperty replication_prop =
        extendReplicationWithSafetyMargin(replication_property, false);
    if (replication_prop.isEmpty()) {
      safe_reads = false;
    } else {
      safe_reads = checkReadAvailability(storage_set, replication_prop);
    }
  }
  return std::make_tuple(safe_reads, safe_writes, fail_scope);
}

void CheckMetaDataLogRequest::checkMetadataNodeset() {
  auto metadatalogs_config = config_->serverConfig()->getMetaDataLogsConfig();
  auto metadatalog_group = config_->serverConfig()->getMetaDataLogGroup();
  if (!metadatalog_group) {
    complete(E::OK, Impact::ImpactResult::NONE, EPOCH_INVALID, {}, "");
    return;
  }
  ReplicationProperty replication_property =
      ReplicationProperty::fromLogAttributes(metadatalog_group->attrs());

  // TODO(T15517759): metadata log storage set should use ShardID.
  auto storage_set =
      EpochMetaData::nodesetToStorageSet(metadatalogs_config.metadata_nodes);

  NodeLocationScope fail_scope;
  bool safe_reads;
  bool safe_writes;
  std::tie(safe_reads, safe_writes, fail_scope) =
      checkReadWriteAvailablity(storage_set, replication_property);

  if (safe_writes && safe_reads) {
    ld_debug("It is safe to perform operations on metadata nodes");
    complete(E::OK, Impact::ImpactResult::NONE, EPOCH_INVALID, {}, "");
  } else {
    std::string message = "Operation would cause loss of read "
                          "or wite availability for metadata logs";

    ld_warning("ERROR: %s", message.c_str());
    complete(E::FAILED,
             Impact::ImpactResult::METADATA_LOSS,
             EPOCH_INVALID,
             std::move(storage_set),
             std::move(message));
  }
}

void CheckMetaDataLogRequest::complete(Status st,
                                       int impact_result,
                                       epoch_t error_epoch,
                                       StorageSet storage_set,
                                       std::string message) {
  // call user provided callback
  callback_(st,
            impact_result,
            log_id_,
            error_epoch,
            std::move(storage_set),
            std::move(message));

  // destroy the request
  delete this;
}

void CheckMetaDataLogRequest::fetchHistoricalMetadata() {
  nodeset_finder_ = std::make_unique<NodeSetFinder>(
      log_id_,
      timeout_,
      [this](Status st) {
        if (st != E::OK) {
          if (st == E::NOTINCONFIG || st == E::NOTFOUND) {
            complete(st, 0, EPOCH_INVALID, {}, "");
            return;
          }
          std::string message = boost::str(
              boost::format(
                  "Fetching historical metadata for log %lu FAILED: %s. ") %
              log_id_.val() % error_description(st));
          complete(st, Impact::ImpactResult::ERROR, EPOCH_INVALID, {}, message);
          return; // `this` has been destroyed.
        }

        auto result = nodeset_finder_->getResult();
        for (const auto& interval : *result) {
          if (!onEpochMetaData(interval.second)) {
            // `this` was destroyed.
            return;
          }
        }
        complete(E::OK, Impact::ImpactResult::NONE, EPOCH_INVALID, {}, "");
      },
      (read_epoch_metadata_from_sequencer_
           ? NodeSetFinder::Source::BOTH
           : NodeSetFinder::Source::METADATA_LOG));

  nodeset_finder_->start();
}

// returns empty ReplicationProperty if is impossible to satisfy
// resulting replicaiton property
ReplicationProperty CheckMetaDataLogRequest::extendReplicationWithSafetyMargin(
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
      auto safety = safety_margin_->find(scope);
      if (safety != safety_margin_->end()) {
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
        int max_for_higher_domians = std::max(replication, prev);
        replication_new.setReplication(scope, max_for_higher_domians);
        prev = max_for_higher_domians;
      }
    }
    scope = NodeLocation::nextSmallerScope(scope);
  }
  return replication_new;
}

bool CheckMetaDataLogRequest::onEpochMetaData(EpochMetaData metadata) {
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
  for (const auto& shard_id : *op_shards_) {
    nodes_to_drain.insert(shard_id.node());
  }

  std::string message;

  if (!safe_writes) {
    message = boost::str(
        boost::format(
            "Drain on (%u shards, %u nodes) would cause "
            "loss of write availability for log %lu, epochs [%u, %u], as in "
            "that storage set not enough %s domains would be "
            "available for writes. ") %
        op_shards_->size() % nodes_to_drain.size() % log_id_.val() % since %
        epoch % NodeLocation::scopeNames()[fail_scope].c_str());
  }

  if (!safe_reads) {
    message += boost::str(
        boost::format(
            "Disabling reads on (%u shards, %u nodes) would cause "
            "loss of read availability for log %lu, epochs [%u, %u], as "
            "that storage would not constitute f-majority ") %
        op_shards_->size() % nodes_to_drain.size() % log_id_.val() % since %
        epoch);
  }

  int impact_result = 0;
  if (!safe_reads) {
    impact_result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;
  }
  if (!safe_writes) {
    impact_result |= Impact::ImpactResult::REBUILDING_STALL;
    // TODO #22911589 check do we loose write availablility
  }

  complete(E::FAILED,
           impact_result,
           metadata.h.effective_since,
           std::move(metadata.shards),
           std::move(message));
  return false;
}

bool CheckMetaDataLogRequest::checkWriteAvailability(
    const StorageSet& storage_set,
    const ReplicationProperty& replication,
    NodeLocationScope* fail_scope) const {
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
      storage_set, config_->serverConfig(), replication);

  for (const ShardID& shard : storage_set) {
    const auto& node = config_->serverConfig()->getNode(shard.node());

    if (node && node->isWritableStorageNode()) {
      if (!op_shards_->count(shard)) {
        AuthoritativeStatus status = shard_status_.getShardStatus(shard);
        if (status == AuthoritativeStatus::FULLY_AUTHORITATIVE) {
          available_node_set.setShardAttribute(shard, true);
        }
      }
    }
  }
  return available_node_set.canReplicate(true, fail_scope);
}

bool CheckMetaDataLogRequest::checkReadAvailability(
    const StorageSet& storage_set,
    const ReplicationProperty& replication) const {
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
      storage_set, config_->serverConfig(), replication);

  for (const ShardID& shard : storage_set) {
    const auto& node = config_->serverConfig()->getNode(shard.node());

    if (node && node->isReadableStorageNode()) {
      AuthoritativeStatus status = shard_status_.getShardStatus(shard);
      available_node_set.setShardAuthoritativeStatus(shard, status);
      if ((!op_shards_ || !op_shards_->count(shard)) && isAlive(shard.node())) {
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

bool CheckMetaDataLogRequest::isAlive(node_index_t index) const {
  auto* cluster_state = Worker::getClusterState();
  if (cluster_state) {
    return cluster_state->isNodeAlive(index);
  } else {
    return true;
  }
}

}} // namespace facebook::logdevice
