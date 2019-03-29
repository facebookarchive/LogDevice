/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/futures/Future.h>

#include "logdevice/admin/safety/SafetyAPI.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace maintenance {
// fwd declaration.
class ShardMaintenanceWorkflow;
class SequencerMaintenanceWorkflow;

/**
 * SafetyCheckScheduler is a long-living object that accepts a request to
 * perform a number of safety checks from several workflows at the same time.
 * The scheduler tries to figure out what is the possible combination of
 * storage state changes that are possible for a given set of groups of
 * operations.
 *
 * If some of the workflows return MaintenanceStatus::AWAITING_SAFETY_CHECK.
 * We need to schedule a safety check run for them, but the order and grouping
 * of these safety check runs is crucial.
 *
 * The goal is to ensure that maintenance groups are performed together on
 * single safety check run, and that safe operations are taken into account
 * into the subsequent tests. If the first group safety check passed and it
 * was requesting StorageState to be set to DISABLED. We then _assume_ that
 * these shards are already in this state in the following test within this
 * round of tests.
 *
 *
 * In order for the scheduler to do some tradeoffs, it needs to know some
 * metadata about the maintenance behind the safety check request. It will
 * help the scheduler decide on prioritizing some maintenances over.
 */
class SafetyCheckScheduler {
 public:
  /**
   * A structure holding the results of a safety checker run.
   */
  struct Result {
    folly::F14NodeMap<ShardID, std::shared_ptr<const Impact>> shard;
    folly::F14NodeMap<node_index_t, std::shared_ptr<const Impact>> sequencers;
  };

  SafetyCheckScheduler(UpdateableSettings<AdminServerSettings> settings);

  /**
   * Defines which value of ShardAuthoritativeStatusMap to be used by the safety
   * checker.
   */
  void
  setShardAuthoritativeStatusMap(const ShardAuthoritativeStatusMap& status_map);

  /**
   * Defines which value of NodesConfiguration to be used by the safety
   * checker.
   */
  void
  setNodesConfiguration(std::shared_ptr<const NodesConfiguration> nodes_config);

  /**
   * This is the main function of this class. It accepts a
   * ClusterMaintenanceWrapper which is able to answer information about the
   * groups and the workflows that are currently requesting safety check runs.
   *
   * The schedule function will do some attempts to group the maintenance
   * requests internally and return a Result object that contains information
   * about which state transitions are safe for shards and sequencers.
   */
  folly::SemiFuture<std::vector<Result>>
  schedule(const ClusterMaintenanceWrapper& maintenance_state,
           std::set<const ShardMaintenanceWorkflow&> shard_wf,
           std::set<const SequencerMaintenanceWorkflow&> seq_wf);
};
}}} // namespace facebook::logdevice::maintenance
