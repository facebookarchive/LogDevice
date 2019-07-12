/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>
#include <unordered_set>

#include <folly/FBVector.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/types_internal.h"

/**
 * @file EventLogRebuildingSet is a utility for keeping track of which (node id,
 * shard id) are currently rebuilding.
 *
 * This provides an update() method which should be called for each new record
 * read in the event log.
 */

namespace facebook { namespace logdevice {

namespace configuration { namespace nodes {
class NodesConfiguration;
}} // namespace configuration::nodes

class Configuration;

/**
 * Maintains a list of rebuilding shards.
 */
class EventLogRebuildingSet {
 public:
  using ts_type = std::chrono::milliseconds;

  struct NodeInfo {
    using set_of_nodes_t =
        folly::F14FastSet<node_index_t, Hash64<node_index_t>>;
    using map_from_node_to_lsn_t =
        folly::F14FastMap<node_index_t, lsn_t, Hash64<node_index_t>>;

    // LSN of the last SHARD_NEEDS_REBUILD event we received for this node.
    lsn_t version;
    // Mode we should be currently rebuilding with.
    RebuildingMode mode;

    std::string source;
    std::string details;

    // timestamp in milliseconds since epoch
    std::chrono::milliseconds rebuilding_started_ts{0};
    std::chrono::milliseconds rebuilding_completed_ts{0};
    std::chrono::milliseconds non_authoritative_since_ts{
        std::chrono::milliseconds::max()};

    // List of nodes that rebuilt this node's data, regardless of whether the
    // rebuilding was authoritative or not.
    map_from_node_to_lsn_t donors_complete;

    // List of nodes that rebuilt this node's data authoritatively.
    map_from_node_to_lsn_t donors_complete_authoritatively;

    // List of donors remaining. This is computed by
    // recomputeShardAuthoritativeStatus() which aggregates data from
    // `donors_complete`, `donors_complete_authoritatively` and the list of
    // storage nodes currently in the config.
    set_of_nodes_t donors_remaining;

    // DataClass/Time Ranges where this node may be missing data.
    // If !dc_dirty_ranges.empty(), then the node is up and we are
    // doing a partial rebuild.
    PerDataClassTimeRanges dc_dirty_ranges;

    // Authoritative status of this shard. This is computed by
    // recomputeShardAuthoritativeStatus() similarly.
    AuthoritativeStatus auth_status{AuthoritativeStatus::FULLY_AUTHORITATIVE};

    // If true, an administrative request has been made to retire the data on
    // this shard.  Rebuilding will continue until complete unless the drain
    // is administratively cancelled. For this reason, the logic that allows
    // a node/shard to self-cancel its rebuild when it rejoins the cluster
    // with its data intact is disabled when "drain" is true.
    //
    // If a draining shard is available, rebuilding is performed in RELOCATE
    // mode. This allows the draining shard to serve as a donor. If the shard
    // is down, or its data lost, rebuilding will be performed in RESTORE mode.
    //
    // Note: The RELOCATE<->RESTORE transition will occur any time the
    //       availability status of the shard changes. This can happen multiple
    //       times during a single drain.
    bool drain;

    // Whether or not there is a chance we can recover the data on that node.
    bool recoverable{false};

    // Whether the node acked rebuilding or not. The node should ack
    // rebuilding only after all donors rebuilt its data and
    // `drain`==false.
    bool acked{false};
    // At which LSN did the node ack.
    lsn_t ack_lsn{LSN_INVALID};
    // For which version did the node ack.
    lsn_t ack_version{LSN_INVALID};

    bool rebuildingIsNonAuthoritative() const;

    std::string toString() const;

    bool operator==(const NodeInfo& rhs) const;

    NodeInfo() = default;
  };

  struct RebuildingShardInfo {
    // LSN of the last SHARD_NEEDS_REBUILD event that affected `rebuilding_set`.
    // Donors restart rebuilding each time this changes.
    lsn_t version;
    // List of nodes for which there is state to maintain. Note that even nodes
    // that acked rebuilding are maintained here because the moment the node
    // acked helps determine if the node is supposed to be a donor for another
    // node. This RebuildingShardInfo object is erased once all nodes in
    // `nodes_` acked.
    folly::F14FastMap<node_index_t, NodeInfo> nodes_;
    // Track the progress of the donors for this rebuilding version. Reset when
    // the rebuliding version changes. RebuildingCoordinator uses this to slide
    // its local window according to global window deduced from the progress of
    // each donor. This is the union of `donors_remaining` for each node in
    // `nodes_`.
    folly::F14FastMap<node_index_t, ts_type> donor_progress;
    // The time intervals across all nodes to process.
    // NOTE: The intervals included here may differ between storage nodes
    //       since each storage node will exclude ranges on itself for which
    //       it cannot be a donor (e.g. the storage node has a dirty-time range
    //       or has shards that are being rebuilt by others in RESTORE mode).
    RecordTimeIntervals all_dirty_time_intervals;
    // Number of nodes in `nodes_` that acked.
    size_t num_acked_{0};
    // Number of nodes that are still being rebuilt but have their data
    // recoverable. More precisely, this is the number of nodes in `nodes_` that
    // have recoverable=True && acked=False.
    size_t num_recoverable_{0};

    // Set to true if rebuilding is non authoritative and there are recoverable
    // nodes. When this is true, we generate an alarm as this will cause readers
    // to get stuck until enough nodes come back with data or the oncall marks
    // all shards unrecoverable.
    bool waiting_for_recoverable_shards_{false};

    bool operator==(const RebuildingShardInfo& rhs) const;
  };

  void setShardAcked(node_index_t node, uint32_t shard, bool acked);
  void setShardRecoverable(node_index_t node, uint32_t shard, bool recoverable);
  void deleteShard(node_index_t node, uint32_t shard);

  void checkConsistency() const;

  EventLogRebuildingSet(lsn_t version,
                        const folly::Optional<NodeID>& my_node_id)
      : last_seen_lsn_(version),
        last_update_(version),
        my_node_id_(my_node_id) {}

  explicit EventLogRebuildingSet(const folly::Optional<NodeID>& my_node_id)
      : my_node_id_(my_node_id) {}

  // Copyable and assignable.
  EventLogRebuildingSet() = default;
  EventLogRebuildingSet(const EventLogRebuildingSet& rhs) = default;
  EventLogRebuildingSet(EventLogRebuildingSet&& rhs) = default;
  EventLogRebuildingSet& operator=(const EventLogRebuildingSet& rhs) = default;
  EventLogRebuildingSet& operator=(EventLogRebuildingSet&& rhs) = default;

  bool operator==(const EventLogRebuildingSet& rhs) const;

  const std::unordered_map<uint32_t, RebuildingShardInfo>&
  getRebuildingShards() const {
    return shards_;
  }

  /**
   * @return True if there is a non authoritative rebuilding and some shards
   * that are unavailable are still marked as recoverable. If this returns true,
   * this means readers may stall until either enough shards come back with data
   * intact or the oncall marks all unavailable shards unrecoverable to unblock
   * readers.
   *
   * Eitherway, when that happens, the oncall needs to be notified.
   * EventLogStateMachine bumps a counter when this returns true.
   */
  bool waitingForRecoverableShards() const;

  /**
   * Retrieve the state of rebuilding for a shard offset.
   *
   * @param shard Shard offset for which to get rebuilding info.
   * @return RebuildingShardInfo struct for that shard offset or nullptr if
   *         there is not rebuilding for this shard offset.
   */
  RebuildingShardInfo const* getForShardOffset(uint32_t shard) const;

  /**
   * Retrieve the state of rebuilding for a shard.
   *
   * @param node  Node on which there is a shard to retrieve the rebuilding
   *              state of.
   * @param shard Offset of the shard on the node.
   * @return NodeInfo struct for that shard or nullptr if
   *         there is not rebuilding for this shard.
   */
  NodeInfo const* getNodeInfo(node_index_t node, uint32_t shard) const;

  // Returns true if the rebuilding set is effectively empty for all shards.
  // This is the case if all shards are either acked, or not acked but rebuilt
  // completely (AUTHORITATIVE_EMPTY) and not in the config.
  bool canTrimEventLog(const configuration::nodes::NodesConfiguration&
                           nodes_configuration) const;

  // Returns rebuilding mode if we're rebuilding `shard` of `node`,
  // otherwise folly::none. Note that shards doing time-ranged rebuilding
  // only are not considered as rebuilding shards here.
  folly::Optional<RebuildingMode> isRebuildingFullShard(node_index_t node,
                                                        uint32_t shard) const {
    NodeInfo const* node_info = getNodeInfo(node, shard);
    if (!node_info || node_info->acked || !node_info->dc_dirty_ranges.empty()) {
      return folly::none;
    }
    return node_info->mode;
  }

  bool shardIsTimeRangeRebuilding(node_index_t node, uint32_t shard) const {
    NodeInfo const* node_info = getNodeInfo(node, shard);
    if (!node_info || node_info->dc_dirty_ranges.empty()) {
      return false;
    }
    return true;
  }

  RebuildingMode getRebuildingMode(node_index_t node, uint32_t shard) const {
    NodeInfo const* node_info = getNodeInfo(node, shard);
    if (!node_info) {
      return RebuildingMode::INVALID;
    }
    return node_info->mode;
  }

  // Returns true if the given node is participating or did participate as
  // a donor in the currently active rebuilding in the given shard.
  bool isDonor(node_index_t node, uint32_t shard) const;

  /**
   * Inform of a new record in the event log.
   * @param lsn         Lsn of the record (used for logging);
   * @param timestamp   Timestamp of the record
   * @param record      Record read from the event log.
   *
   * @param nodes_configuration  NodesConfiguration object used to
   *                             retrieve the list of potential donors for
   *                             rebuilding.
   *
   * @return 0 on success, or -1 and err set to E::FAILED.
   */
  int update(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  lsn_t getLastUpdate() const {
    return last_update_;
  }

  lsn_t getLastSeenLSN() const {
    return last_seen_lsn_;
  }

  std::chrono::milliseconds
  getLastSeenShardNeedsRebuildTS(shard_index_t shard) const {
    auto maxTS = RecordTimestamp::zero();
    const RebuildingShardInfo& s = shards_.find(shard)->second;
    for (auto node : s.nodes_) {
      const NodeInfo& n = node.second;
      maxTS.storeMax(RecordTimestamp(n.rebuilding_started_ts));
    }
    return maxTS.time_since_epoch();
  }

  bool empty() const {
    return shards_.empty();
  }

  /**
   * Usually done in response to trim gap in event log.
   * @param lsn  what to set `last_update_` to. Usually high end of trim gap.
   */
  void clear(lsn_t lsn) {
    ld_check(lsn > last_update_);
    shards_.clear();
    last_seen_lsn_ = lsn;
    last_update_ = lsn;
  }

  std::string toString() const;

  /**
   * @return the authoritative status of a shard.
   *
   * @param node
   * @param shard
   * @param storage_nodes If this function returns !=
   *                      AuthoritativeStatus::FULLY_AUTHORITATIVE or if that
   *                      shard is rebuilding in RELOCATE mode, this set will be
   *                      left with all the storage nodes that still have data
   *                      to rebuild for that shard.
   */
  AuthoritativeStatus
  getShardAuthoritativeStatus(node_index_t node,
                              uint32_t shard,
                              std::vector<node_index_t>& storage_nodes) const;

  /**
   * Create a ShardAuthoritativeStatusMap object from this rebuilding set.
   * @param nodes_configuration
   *                   Node configuration of the cluster. EventLogRebuildingSet
   *                   keeps track of the list of nodes that sent a
   *                   SHARD_IS_REBUILT event. This node configuration is used
   *                   to determined when rebuilding of a shard completed by
   *                   comparing the list of nodes that sent SHARD_IS_REBUILT
   *                   with it.
   */
  ShardAuthoritativeStatusMap toShardStatusMap(
      const configuration::nodes::NodesConfiguration& nodes_configuration)
      const;

  void recomputeAuthoritativeStatus(
      uint32_t shard,
      std::chrono::milliseconds timestamp,
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  void recomputeShardRebuildTimeIntervals(uint32_t shard);

 private:
  // Set of shards that are currently seen as rebuilding.
  std::unordered_map<uint32_t, RebuildingShardInfo> shards_;

  // Last lsn passed to update() or clear().
  lsn_t last_seen_lsn_ = LSN_INVALID;

  // LSN of the last event that affected rebuilding set, either
  // SHARD_NEEDS_REBUILD or SHARD_ACK_REBUILD.
  lsn_t last_update_ = LSN_INVALID;

  folly::Optional<NodeID> my_node_id_{folly::none};

  // Recomputes `NodeInfo::auth_status` and `NodeInfo::donors_remaining`.
  // @param out_comment
  //   A human-readable explanation for this transition.
  //   Should fit in the sentence:
  //   "N42:S15 is [not] transitioning from %s to %s because {*out_comment}"
  void recomputeShardAuthoritativeStatus(
      RebuildingShardInfo& shard_info,
      node_index_t nidx,
      std::chrono::milliseconds timestamp,
      NodeInfo& node_info,
      std::unordered_set<node_index_t> storage_nodes,
      std::string* out_comment) const;

  int onShardNeedsRebuild(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);
  int onShardAckRebuilt(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);
  int onShardIsRebuilt(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);
  int onShardAbortRebuild(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);
  int onShardUndrain(lsn_t lsn,
                     std::chrono::milliseconds timestamp,
                     const EventLogRecord& record);
  int onShardDonorProgress(lsn_t lsn,
                           std::chrono::milliseconds timestamp,
                           const EventLogRecord& record);
  int onShardUnrecoverable(
      lsn_t lsn,
      std::chrono::milliseconds timestamp,
      const EventLogRecord& record,
      const configuration::nodes::NodesConfiguration& nodes_configuration);

  friend class EventLogRebuildingSetCodec;
};

}} // namespace facebook::logdevice
