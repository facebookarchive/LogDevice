/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/safety/SafetyCheckerUtils.h"

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"

using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice;

namespace facebook { namespace logdevice { namespace safety {

folly::Expected<Impact, Status> checkImpactOnLogs(
    const std::vector<logid_t>& log_ids,
    const std::shared_ptr<LogMetaDataFetcher::Results>& metadata,
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& sequencers,
    configuration::StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    bool internal_logs,
    bool abort_on_error,
    size_t error_sample_size,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state) {
  int impact_result_all = 0;
  std::vector<Impact::ImpactOnEpoch> affected_logs_sample;
  size_t logs_done = 0;
  bool internal_logs_affected = false;

  // Check other logs
  for (logid_t log_id : log_ids) {
    // We fail the safety check immediately if we cannot schedule internal
    // logs to be checked.
    auto result = checkImpactOnLog(log_id,
                                   metadata,
                                   shard_status,
                                   op_shards,
                                   sequencers,
                                   target_storage_state,
                                   safety_margin,
                                   nodes_config,
                                   cluster_state);
    logs_done++;
    if (result.hasError()) {
      // The operation failed. Possibly because we don't have metadata for
      // this log-id. This is critical.
      return folly::makeUnexpected(result.error());
    }
    const Impact::ImpactOnEpoch& epoch_impact = result.value();
    if (epoch_impact.impact_result > Impact::ImpactResult::NONE) {
      impact_result_all |= epoch_impact.impact_result;
      if (internal_logs) {
        internal_logs_affected = true;
      }
      if (affected_logs_sample.size() < error_sample_size) {
        affected_logs_sample.push_back(epoch_impact);
      }

      if (abort_on_error && affected_logs_sample.size() >= error_sample_size) {
        // We have enough error samples, let's return the result. We are not
        // setting the total_time here, leaving this to the caller to set since
        // the overall operation might be composed of multiple checks.
        break;
      }
    }
  }

  return Impact(impact_result_all,
                std::move(affected_logs_sample),
                internal_logs_affected,
                logs_done,
                std::chrono::seconds(0));
}

folly::Expected<Impact::ImpactOnEpoch, Status> checkImpactOnLog(
    logid_t log_id,
    const std::shared_ptr<LogMetaDataFetcher::Results>& metadata_cache,
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& /*sequencers*/,
    configuration::StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    ClusterState* cluster_state) {
  ld_assert(metadata_cache);
  if (metadata_cache->find(log_id) == metadata_cache->end()) {
    // We cannot find the epoch metadata for this log. This can have multiple
    // reasons:
    //   1. This is a newly added log, auto-log-provisioning didn't kick in yet.
    //   2. We didn't get all metadata for all logs (shouldn't happen).
    //   3. The metadata is stale and doesn't have that log yet.
    // We will assume that it's (1). So we will assume that it's a safe
    // operation.
    ld_warning("Cannot find metadata for log-id=%lu in metadata cache. It's "
               "likely that this log has been recently added but never "
               "provisioned. Assuming it's safe!",
               log_id.val_);
    return Impact::ImpactOnEpoch(log_id,
                                 EPOCH_INVALID,
                                 {},
                                 {},
                                 ReplicationProperty(),
                                 Impact::ImpactResult::NONE);
  }

  // We require that the node to be fully started if we are checking a normal
  // data log (not internal log)
  bool require_fully_started = !configuration::InternalLogs::isInternal(log_id);
  const LogMetaDataFetcher::Result& metadata_result = (*metadata_cache)[log_id];
  if (metadata_result.historical_metadata_status != E::OK) {
    // We were not able to fetch metadata for this log, in this case we will
    // fail since we can't establish confidence.
    ld_warning("Cannot finish safety check run because we failed to fetch "
               "metadata for log-id=%lu with status: %s. This might get fixed "
               "in the next cache refresh",
               log_id.val(),
               error_name(metadata_result.historical_metadata_status));
    return folly::makeUnexpected(metadata_result.historical_metadata_status);
  }

  ld_check(metadata_result.historical_metadata);
  auto metadata_map = metadata_result.historical_metadata->getMetaDataMap();
  ld_check(metadata_map);
  // for each epoch we validate, we fail on the first epoch that fails the
  // check.
  for (const auto& metadata_pair : *metadata_map) {
    ld_check(metadata_pair.second.isValid());
    const EpochMetaData& epoch_metadata = metadata_pair.second;
    bool safe_writes;
    bool safe_reads;

    std::tie(safe_reads, safe_writes) =
        checkReadWriteAvailablity(shard_status,
                                  op_shards,
                                  epoch_metadata.shards,
                                  target_storage_state,
                                  epoch_metadata.replication,
                                  safety_margin,
                                  nodes_config,
                                  cluster_state,
                                  require_fully_started);

    if (safe_writes && safe_reads) {
      continue;
    }

    // in case we can't replicate we return epoch & nodeset for it

    folly::F14FastSet<node_index_t> nodes_to_drain;
    for (const auto& shard_id : op_shards) {
      nodes_to_drain.insert(shard_id.node());
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
        getStorageSetMetadata(epoch_metadata.shards,
                              shard_status,
                              nodes_config,
                              cluster_state,
                              require_fully_started);

    // We don't scan more epochs, one failing epoch is enough for us.
    return Impact::ImpactOnEpoch(log_id,
                                 epoch_metadata.h.epoch,
                                 epoch_metadata.shards,
                                 std::move(storage_set_metadata),
                                 epoch_metadata.replication,
                                 impact_result);
  }

  // Everything is safe.
  return Impact::ImpactOnEpoch(log_id,
                               EPOCH_INVALID,
                               {},
                               {},
                               ReplicationProperty(),
                               Impact::ImpactResult::NONE);
}

Impact checkMetadataStorageSet(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const folly::F14FastSet<node_index_t>& /*sequencers*/,
    StorageState target_storage_state,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const nodes::NodesConfiguration>& nodes_config,
    ClusterState* cluster_state,
    size_t error_sample_size) {
  bool internal_logs_affected = false;
  // Convert the data log-id to metadata log-id
  ReplicationProperty replication_property =
      nodes_config->getMetaDataLogsReplication()->getReplicationProperty();

  std::vector<Impact::ImpactOnEpoch> impact_on_epochs;

  int impact_result = Impact::ImpactResult::NONE;
  int impact_result_all = 0;
  Impact::StorageSetMetadata storage_set_metadata;
  // We generate all possible storage sets for metadata logs and check until one
  // fails. This is for efficiency reasons. Even if some of these combinations
  // do not exist.
  //
  // NOTE: This assumes that all nodes one the cluster has the same number of
  // shards.
  //
  // TODO(T15517759): metadata log storage set should use ShardID.
  //
  const shard_size_t n_shards = nodes_config->getNumShards();

  for (shard_size_t shard_id = 0; shard_id < n_shards; ++shard_id) {
    StorageSet storage_set;
    storage_set = EpochMetaData::nodesetToStorageSet(
        nodes_config->getStorageMembership()->getMetaDataNodeIndices(),
        shard_id);

    /**
     * We only require that the node to be at least STARTING_UP for metadata
     * nodeset checks because they are excluded from the restrictions of this
     * gossip state.
     */
    storage_set_metadata =
        getStorageSetMetadata(storage_set,
                              shard_status,
                              nodes_config,
                              cluster_state,
                              /* require_fully_started = */ false);

    bool safe_reads;
    bool safe_writes;
    std::tie(safe_reads, safe_writes) =
        checkReadWriteAvailablity(shard_status,
                                  op_shards,
                                  storage_set,
                                  target_storage_state,
                                  replication_property,
                                  safety_margin,
                                  nodes_config,
                                  cluster_state,
                                  /* require_fully_started = */ false);

    if (!safe_writes) {
      impact_result |= Impact::ImpactResult::WRITE_AVAILABILITY_LOSS;
    }
    if (!safe_reads) {
      impact_result |= Impact::ImpactResult::READ_AVAILABILITY_LOSS;
    }

    if (impact_result > Impact::ImpactResult::NONE) {
      impact_result_all |= impact_result;
      internal_logs_affected = true;
      if (impact_on_epochs.size() < error_sample_size) {
        impact_on_epochs.push_back(
            Impact::ImpactOnEpoch(LOGID_INVALID,
                                  EPOCH_INVALID,
                                  std::move(storage_set),
                                  std::move(storage_set_metadata),
                                  replication_property,
                                  impact_result));
      }
    }
  }
  return Impact(impact_result_all,
                std::move(impact_on_epochs),
                internal_logs_affected,
                /* total_logs_checked = */ 0,
                std::chrono::seconds(0));
}

std::pair<bool, bool> checkReadWriteAvailablity(
    const ShardAuthoritativeStatusMap& shard_status,
    const ShardSet& op_shards,
    const StorageSet& storage_set,
    StorageState target_storage_state,
    const ReplicationProperty& replication_property,
    const SafetyMargin& safety_margin,
    const std::shared_ptr<const nodes::NodesConfiguration>& nodes_config,
    ClusterState* cluster_state,
    bool require_fully_started_nodes) {
  bool safe_writes;
  bool safe_reads = true;

  // We always validate write availability issues, this is because the
  // target_storage_state cannot be READ_WRITE in this class. it will either be
  // READ_ONLY or DISABLED.
  //
  // The replication_prop_for_writes is the safety margin added to the
  // replication property of the current epoch. It pushes the requirement for
  // canReplicate higher.
  //
  // if N nodes are required to maintain write availability, make sure to
  // always have N+x nodes to have room for organic failures of x nodes
  //

  // The replication_prop_for_reads is the safety margin minus the
  // replication property of the current epoch. It restricts the requirements
  // for f-majority check.
  //
  //
  // This check is optimised to reduce the number of attribute changes we are
  // doing to the FailureDomainNodeSet. This has the following assumptions:
  //   - A writable node is always readable. READ_WRITE > READ_ONLY.
  //   - We assume that nodes that are not FULLY_AUTHORITATIVE are not
  //   writable. This is to increase safety of operations. A node that is not
  //   FULLY_AUTHORITATIVE might be in repair or in maintenance. Better to
  //   avoid.
  //   - We always take into account the gossip status of the node. A node that
  //   is not ALIVE is not readable nor writable.
  //
  // The strategy:
  //   - We tag the writable shards first and test for write availability.
  //   - If we need to check for read availability, we will tag the 'readable'
  //   shards that were not tagged during the write check and run a f-majority
  //   check.

  // Write availability
  //
  // We use FailureDomainNodeSet to determine if draining shards in
  // `op_shards_` would result in this StorageSet being unwritable. The boolean
  // attribute indicates for each node whether it will be able to take writes
  // after the drain.

  auto check_writes =
      [&](FailureDomainNodeSet<bool>& failure_domains) mutable -> bool {
    // Check for write availability
    for (const ShardID& shard : storage_set) {
      // Writable shard in storage membership
      if (nodes_config->getStorageMembership()->canWriteToShard(shard)) {
        // We always set the authoritative status for all shards.
        AuthoritativeStatus status = shard_status.getShardStatus(shard);
        // If it's Dead, not fully-auth, or the shard we are disabling.
        // Then tag it as writable.
        if (!op_shards.count(shard) &&
            status == AuthoritativeStatus::FULLY_AUTHORITATIVE &&
            isAlive(cluster_state, shard.node(), require_fully_started_nodes)) {
          failure_domains.setShardAttribute(shard, true);
        }
      }
    }
    return failure_domains.canReplicate(true, nullptr);
  };

  // Read availability
  //
  // We use FailureDomainNodeSet to determine if disabling reads for shards in
  // `op_shards` would result in this StorageSet being unreadable.
  // The boolean attribute indicates for each node whether it will be able to
  // serve reads after the operation. As such, a node can serve reads if:
  //
  // Authoritative status is set for all shards and it is taken  into account by
  // isFmajority. We should have f-majority of FULLY_AUTHORITATIVE (ie it's not
  // been drained or being drained, or it's not being rebuilt / in repair)
  // shards, excluding shards on which we are going to be stopped.
  auto check_reads =
      [&](FailureDomainNodeSet<bool>& failure_domains) mutable -> bool {
    for (const ShardID& shard : storage_set) {
      if (nodes_config->getStorageMembership()->shouldReadFromShard(shard)) {
        // We always set the authoritative status for all shards.
        AuthoritativeStatus status = shard_status.getShardStatus(shard);

        bool is_mini_rebuilding = shard_status.shardIsTimeRangeRebuilding(
            shard.node(), shard.shard());
        if (is_mini_rebuilding) {
          // The shard has time-range rebuilding, we will lean on the
          // safe-side and mark this shard as UNAVAILABLE instead of
          // FULLY_AUTHORITATIVE to ensure we block operations that _may_
          // cause the mini-rebuilding to stall.
          status = AuthoritativeStatus::UNAVAILABLE;
        }
        // We should consider shards that are UNDERREPLICATION as UNAVAILABLE
        // since we don't want to make the recoverability worse by taking more
        // shards down. If we don't do that, the FailureDomainNodeSet will only
        // account for FULLY_AUTHORITATIVE and UNAVAILABLE.
        if (status == AuthoritativeStatus::UNDERREPLICATION) {
          status = AuthoritativeStatus::UNAVAILABLE;
        }
        failure_domains.setShardAuthoritativeStatus(shard, status);
        if (!op_shards.count(shard) &&
            isAlive(cluster_state, shard.node(), require_fully_started_nodes) &&
            // Any shard that is UNAVAILABLE (or UNDERREPLICATION) is tagged
            // here as well even if it's ALIVE.
            status != AuthoritativeStatus::UNAVAILABLE) {
          // We only tag the shards that we consider healthy.
          failure_domains.setShardAttribute(shard, true);
        } else if (is_mini_rebuilding) {
          // Explicitly set this shard to be false since this might have been
          // set to true by the write availability check. A shard in
          // mini-rebuilding is writable but not readable.
          failure_domains.setShardAttribute(shard, false);
        }
      }
    }
    FmajorityResult health_state = failure_domains.isFmajority(true);

    // we treat NON_AUTHORITATIVE as unsafe, as we may increase damage,
    // i.e. more records will become unaccesible if stop more nodes
    return health_state == FmajorityResult::AUTHORITATIVE_COMPLETE ||
        health_state == FmajorityResult::AUTHORITATIVE_INCOMPLETE;
  };

  if (!safety_margin.empty()) {
    // With safety margin, we have two different replications for reads and
    // writes.
    ReplicationProperty replication_for_writes =
        extendReplicationWithSafetyMargin(
            safety_margin, replication_property, true);

    ReplicationProperty replication_for_reads =
        extendReplicationWithSafetyMargin(
            safety_margin, replication_property, false);

    FailureDomainNodeSet<bool> available_node_set_for_writes(
        storage_set, *nodes_config, replication_for_writes);
    safe_writes = check_writes(available_node_set_for_writes);

    // We only check reads if necessary.
    if (target_storage_state == StorageState::DISABLED) {
      FailureDomainNodeSet<bool> available_node_set_for_reads(
          storage_set, *nodes_config, replication_for_reads);
      safe_reads = check_reads(available_node_set_for_reads);
    }
  } else {
    // No safety margin, use the same available_node_set
    FailureDomainNodeSet<bool> available_node_set(
        storage_set, *nodes_config, replication_property);
    safe_writes = check_writes(available_node_set);
    // We only check reads if necessary.
    if (target_storage_state == StorageState::DISABLED) {
      safe_reads = check_reads(available_node_set);
    }
  }

  return std::make_pair(safe_reads, safe_writes);
}

bool isAlive(ClusterState* cluster_state,
             node_index_t index,
             bool require_fully_started) {
  if (cluster_state && require_fully_started) {
    return cluster_state->isNodeFullyStarted(index);
  } else if (cluster_state) {
    return cluster_state->isNodeAlive(index);
  } else {
    return true;
  }
}

ReplicationProperty
extendReplicationWithSafetyMargin(const SafetyMargin& safety_margin,
                                  const ReplicationProperty& replication_base,
                                  bool add) {
  ReplicationProperty replication_new(replication_base);
  auto scope = NodeLocation::nextSmallerScope(NodeLocationScope::ROOT);
  int prev = 0;
  while (scope != NodeLocationScope::INVALID) {
    int replication = replication_base.getReplication(scope);
    // Do not consider the scope if the user did not specify a replication
    // factor for it
    if (replication != 0) {
      auto safety = safety_margin.find(scope);
      if (safety != safety_margin.end()) {
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

Impact::StorageSetMetadata getStorageSetMetadata(
    const StorageSet& storage_set,
    const ShardAuthoritativeStatusMap& shard_status,
    const std::shared_ptr<const nodes::NodesConfiguration>& nodes_config,
    ClusterState* cluster_state,
    bool require_fully_started) {
  Impact::StorageSetMetadata out;
  for (const auto& shard : storage_set) {
    // If the node doesn't exist anymore in the nodes configuration. We use
    // these defaults.
    StorageState storage_state;
    folly::Optional<NodeLocation> location = folly::none;

    const auto membership = nodes_config->getStorageMembership();
    if (membership->canWriteToShard(shard)) {
      storage_state = StorageState::READ_WRITE;
    } else if (membership->shouldReadFromShard(shard)) {
      storage_state = StorageState::READ_ONLY;
    } else {
      storage_state = StorageState::DISABLED;
    }

    const nodes::NodeServiceDiscovery* discovery =
        nodes_config->getNodeServiceDiscovery(shard.node());
    if (discovery) {
      location = discovery->location;
    }

    bool is_mini_rebuilding =
        shard_status.shardIsTimeRangeRebuilding(shard.node(), shard.shard());
    out.push_back(Impact::ShardMetadata{
        .auth_status = shard_status.getShardStatus(shard),
        .has_dirty_ranges = is_mini_rebuilding,
        .is_alive = isAlive(cluster_state, shard.node(), require_fully_started),
        .storage_state = storage_state,
        .location = location});
  }
  return out;
}

}}} // namespace facebook::logdevice::safety
