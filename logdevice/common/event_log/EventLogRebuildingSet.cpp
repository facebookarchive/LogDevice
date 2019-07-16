/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogRebuildingSet.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

int EventLogRebuildingSet::update(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  ld_check(lsn > last_update_);
  last_seen_lsn_ = lsn;
  auto type = record.getType();

  checkConsistency();

  int rv = -1;

  ld_debug("EventLogRebuildingSet %s, before: %s",
           logdevice::toString(type).c_str(),
           toString().c_str());

  if (type == EventType::SHARD_NEEDS_REBUILD) {
    rv = onShardNeedsRebuild(lsn, timestamp, record, nodes_configuration);
  } else if (type == EventType::SHARD_ACK_REBUILT) {
    rv = onShardAckRebuilt(lsn, timestamp, record, nodes_configuration);
  } else if (type == EventType::SHARD_IS_REBUILT) {
    rv = onShardIsRebuilt(lsn, timestamp, record, nodes_configuration);
  } else if (type == EventType::SHARD_ABORT_REBUILD) {
    rv = onShardAbortRebuild(lsn, timestamp, record, nodes_configuration);
  } else if (type == EventType::SHARD_UNDRAIN) {
    rv = onShardUndrain(lsn, timestamp, record);
  } else if (type == EventType::SHARD_DONOR_PROGRESS) {
    rv = onShardDonorProgress(lsn, timestamp, record);
  } else if (type == EventType::SHARD_UNRECOVERABLE) {
    rv = onShardUnrecoverable(lsn, timestamp, record, nodes_configuration);
  }

  if (rv == 0) {
    last_update_ = lsn;
  }

  ld_debug("EventLogRebuildingSet %s after: %s",
           logdevice::toString(type).c_str(),
           toString().c_str());

  return rv;
}

void EventLogRebuildingSet::setShardAcked(node_index_t node,
                                          uint32_t shard,
                                          bool acked) {
  ld_check(shards_.count(shard));
  RebuildingShardInfo& shard_info = shards_[shard];
  ld_check(shard_info.nodes_.count(node));
  NodeInfo& node_info = shard_info.nodes_[node];

  if (node_info.acked == acked) {
    return;
  }

  if (acked) {
    if (node_info.recoverable) {
      ld_check(shard_info.num_recoverable_ > 0);
      --shard_info.num_recoverable_;
    }
    ++shard_info.num_acked_;
    node_info.acked = true;
  } else {
    if (node_info.recoverable) {
      ++shard_info.num_recoverable_;
    }
    ld_check(shard_info.num_acked_ > 0);
    --shard_info.num_acked_;
    node_info.acked = false;
  }
}

void EventLogRebuildingSet::setShardRecoverable(node_index_t node,
                                                uint32_t shard,
                                                bool recoverable) {
  ld_check(shards_.count(shard));
  RebuildingShardInfo& shard_info = shards_[shard];
  ld_check(shard_info.nodes_.count(node));
  NodeInfo& node_info = shard_info.nodes_[node];

  if (node_info.recoverable == recoverable) {
    return;
  }

  if (recoverable) {
    if (!node_info.acked) {
      ++shard_info.num_recoverable_;
    }
    node_info.recoverable = true;
  } else {
    if (!node_info.acked) {
      ld_check(shard_info.num_recoverable_ > 0);
      --shard_info.num_recoverable_;
    }
    node_info.recoverable = false;
  }
}

void EventLogRebuildingSet::deleteShard(node_index_t node, uint32_t shard) {
  auto it_shard = shards_.find(shard);
  ld_check(it_shard != shards_.end());
  auto it_node = it_shard->second.nodes_.find(node);
  ld_check(it_node != it_shard->second.nodes_.end());
  RebuildingShardInfo& shard_info = it_shard->second;

  setShardAcked(node, shard, false);
  setShardRecoverable(node, shard, false);

  shard_info.nodes_.erase(it_node);
}

void EventLogRebuildingSet::checkConsistency() const {
  if (!folly::kIsDebug) {
    return;
  }
  for (auto it_shard : shards_) {
    const RebuildingShardInfo& shard_info = it_shard.second;
    size_t num_acked = 0;
    size_t num_recoverable = 0;
    size_t expected_num_acked = shard_info.num_acked_;
    size_t expected_num_recoverable = shard_info.num_recoverable_;
    for (auto it_node : shard_info.nodes_) {
      const NodeInfo& node_info = it_node.second;
      if (node_info.acked) {
        ++num_acked;
      } else if (node_info.recoverable) {
        ld_check(node_info.auth_status !=
                 AuthoritativeStatus::AUTHORITATIVE_EMPTY);
        ++num_recoverable;
      }
    }
    ld_check(num_acked == expected_num_acked);
    ld_check(num_recoverable == expected_num_recoverable);
  }
}

int EventLogRebuildingSet::onShardNeedsRebuild(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* ptr = dynamic_cast<const SHARD_NEEDS_REBUILD_Event*>(&record);
  ld_check(ptr);

  uint32_t shardIdx = ptr->header.shardIdx;
  node_index_t nodeIdx = ptr->header.nodeIdx;
  const auto flags = ptr->header.flags;

  // Check if we should discard this delta because it's a conditional write.
  if (flags & SHARD_NEEDS_REBUILD_Header::CONDITIONAL_ON_VERSION) {
    ld_check(ptr->header.conditional_version != LSN_INVALID);
    auto it = shards_.find(ptr->header.shardIdx);
    if (it == shards_.end()) {
      ld_info("Found event log record with lsn %s: %s, but event log is empty"
              "for this shard.",
              lsn_to_string(lsn).c_str(),
              record.describe().c_str());
      err = E::STALE;
      return -1;
    }
    if (it->second.version != ptr->header.conditional_version) {
      ld_info("Found event log record with lsn %s: %s, but rebuilding version "
              "does not match (%s). discarding.",
              lsn_to_string(lsn).c_str(),
              record.describe().c_str(),
              lsn_to_string(it->second.version).c_str());
      err = E::STALE;
      return -1;
    }
  }

  RebuildingMode requestedMode = flags & SHARD_NEEDS_REBUILD_Header::RELOCATE
      ? RebuildingMode::RELOCATE
      : RebuildingMode::RESTORE;

  // Remove the existing node entry if it was already in the rebuilding set.
  // Before that, gather the information that is sticky about that node entry,
  // which is whether or not the data is recoverable and whether or not the
  // intent is to drain it.
  bool is_recoverable = true;
  bool drain = false;
  folly::Optional<RebuildingMode> currentMode;
  auto it = shards_[shardIdx].nodes_.find(nodeIdx);
  if (it != shards_[shardIdx].nodes_.end()) {
    if (!it->second.dc_dirty_ranges.empty()) {
      // Switching from time-ranged rebuilding to a full rebuilding.
      // Reset recoverable to true since the time-ranged rebuilding implied that
      // the node was up and didn't lose all data.
    } else if (it->second.acked) {
      // The shard acked its previous rebuilding, so it's as good as not in
      // rebuilding set. Reset its recoverable to true.
    } else {
      // We're restarting a rebuilding. Preserve unrecoverability.
      is_recoverable = it->second.recoverable;
    }

    drain = it->second.drain;
    currentMode = it->second.mode;

    if (!(flags & SHARD_NEEDS_REBUILD_Header::FORCE_RESTART)) {
      if (flags & SHARD_NEEDS_REBUILD_Header::DRAIN) {
        // Do nothing if the user is requesting a drain but a drain is already
        // ongoing or already completed.
        if (drain) {
          if (it->second.auth_status ==
              AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
            ld_warning("Found event log record with lsn %s: %s, but this shard "
                       "is already drained. Discarding.",
                       lsn_to_string(lsn).c_str(),
                       record.describe().c_str());
          } else {
            ld_warning("Found event log record with lsn %s: %s, but this shard "
                       "is already being drained. Discarding.",
                       lsn_to_string(lsn).c_str(),
                       record.describe().c_str());
          }
          err = E::ALREADY;
          return -1;
        }
      } else {
        // Do nothing if rebuilding is requested but the shard either
        // (a) has already been rebuilt, or (b) is already being rebuilt with
        // the same time ranges. (a) may happen if there is a bug in
        // RebuildingSupervisor causing it to trigger rebuilding of a shard
        // already being rebuilt. (b) can currently happens a lot in some
        // circumstances when there are many deltas in the event log.
        // TODO (#T23077142):
        //   This is not really correct for time-ranged rebuildings: the node
        //   may crash again and re-request rebuilding with the same time
        //   ranges. Then rebuilding needs to be restarted to rebuild the newly
        //   lost data in the same ranges.
        if (currentMode.hasValue() && currentMode.value() == requestedMode &&
            ((flags & SHARD_NEEDS_REBUILD_Header::TIME_RANGED) == 0
                 ? it->second.auth_status !=
                     AuthoritativeStatus::FULLY_AUTHORITATIVE
                 : it->second.dc_dirty_ranges ==
                     ptr->time_ranges.getDCDirtyRanges())) {
          ld_warning("Found event log record with lsn %s: %s, but this shard "
                     "is already being rebuilt in this mode. Discarding.",
                     lsn_to_string(lsn).c_str(),
                     record.describe().c_str());
          err = E::ALREADY;
          return -1;
        }
      }
    }

    deleteShard(nodeIdx, shardIdx);
  }

  NodeInfo node_info;
  node_info.version = lsn;
  node_info.recoverable = false;
  node_info.acked = false;
  node_info.dc_dirty_ranges.clear();
  node_info.source = ptr->header.getSource();
  node_info.details = ptr->header.getDetails();
  node_info.rebuilding_started_ts = timestamp;

  // We make node_info.drain sticky but override it if DRAIN flag is set.
  node_info.drain = drain;
  node_info.mode = requestedMode;
  if (flags & SHARD_NEEDS_REBUILD_Header::DRAIN) {
    if (flags & SHARD_NEEDS_REBUILD_Header::RELOCATE) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     1,
                     "Rebuilding mode is set "
                     "when Drain flag is also set. Ignoring Rebuilding mode "
                     "from header");
    }
    // When drain flag is set, preserve the existing mode
    // if one exists. Otherwise, set mode to RELOCATE.
    node_info.mode =
        currentMode.hasValue() ? currentMode.value() : RebuildingMode::RELOCATE;
    node_info.drain = true;
  }

  if (flags & SHARD_NEEDS_REBUILD_Header::TIME_RANGED) {
    dd_assert(node_info.mode == RebuildingMode::RESTORE,
              "Time ranged rebuild request from N%d specifies RELOCATE mode, "
              "but should have been in RESTORE mode. "
              "Treating as a request for full rebuild.",
              nodeIdx);
    dd_assert(!node_info.drain,
              "Draining N%d should not have published "
              "a time ranged rebuild request. "
              "Treating as a request for full rebuild.",
              nodeIdx);
    if (node_info.mode == RebuildingMode::RESTORE && !node_info.drain) {
      node_info.dc_dirty_ranges = ptr->time_ranges.getDCDirtyRanges();
      // Dirty nodes are up and serve as donors, so they are already
      // "recovered".
      is_recoverable = false;
    }
  } else {
    ld_check(ptr->time_ranges.empty());
  }

  shards_[shardIdx].nodes_.insert(
      std::make_pair(nodeIdx, std::move(node_info)));
  shards_[shardIdx].version = lsn;
  shards_[shardIdx].donor_progress.clear();

  setShardRecoverable(nodeIdx, shardIdx, is_recoverable);

  recomputeAuthoritativeStatus(shardIdx, timestamp, nodes_configuration);

  recomputeShardRebuildTimeIntervals(shardIdx);

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardAckRebuilt(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* ptr = dynamic_cast<const SHARD_ACK_REBUILT_Event*>(&record);
  ld_check(ptr);

  auto it = shards_.find(ptr->header.shardIdx);
  if (it == shards_.end()) {
    ld_error("Found event log record with lsn %s: %s, but there is no "
             "matching event log record of type SHARD_NEEDS_REBUILD that "
             "precedes it.\n"
             "Current EventLogRebuildingSet: %s.",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str(),
             toString().c_str());
    err = E::FAILED;
    return -1;
  }

  RebuildingShardInfo& shard_info = it->second;

  if (ptr->header.version != shard_info.version) {
    // Discard because the version does not match.
    if (ptr->header.version > shard_info.version) {
      ld_error("Found event log record %s for a rebuilding version greater "
               "than the last version %s",
               record.describe().c_str(),
               lsn_to_string(shard_info.version).c_str());
    }
    err = E::FAILED;
    return -1;
  }

  auto it_node = shard_info.nodes_.find(ptr->header.nodeIdx);
  if (it_node == shard_info.nodes_.end()) {
    // The node already acknowledged. It's probably a duplicate record.
    err = E::FAILED;
    return -1;
  }

  NodeInfo& node_info = it_node->second;
  if (node_info.acked) {
    // The node already acknowledged. It's probably a duplicate record.
    err = E::FAILED;
    return -1;
  }

  if (node_info.drain) {
    // The node acknowledged rebuilding but a drain is still requested (the user
    // did not get a chance to write a SHARD_UNDRAIN event). This should not
    // happen as storage nodes should wait for that message. However there are
    // old versions of the server that may not wait for that message so do not
    // fail.
    ld_warning("Found event log record %s but the shard is being drained",
               record.describe().c_str());
  }
  node_info.ack_lsn = lsn;
  node_info.ack_version = ptr->header.version;

  if (!node_info.dc_dirty_ranges.empty() &&
      node_info.auth_status == AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    // If the node was in rebuilding set for time-ranged rebuilding and has now
    // acked, remove it from the rebuilding set
    ld_check(node_info.mode == RebuildingMode::RESTORE);
    deleteShard(ptr->header.nodeIdx, ptr->header.shardIdx);
  } else {
    setShardAcked(ptr->header.nodeIdx, ptr->header.shardIdx, true);
  }

  if (shard_info.num_acked_ == shard_info.nodes_.size()) {
    shards_.erase(ptr->header.shardIdx);
  }

  recomputeAuthoritativeStatus(
      ptr->header.shardIdx, timestamp, nodes_configuration);

  recomputeShardRebuildTimeIntervals(ptr->header.shardIdx);

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardIsRebuilt(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* ptr = dynamic_cast<const SHARD_IS_REBUILT_Event*>(&record);
  ld_check(ptr);

  const bool is_authoritative =
      !(ptr->header.flags & SHARD_IS_REBUILT_Header::NON_AUTHORITATIVE);

  uint32_t shard_idx = ptr->header.shardIdx;
  node_index_t node_idx = ptr->header.donorNodeIdx;

  auto it = shards_.find(shard_idx);
  if (it == shards_.end()) {
    ld_error("Found event log record with lsn %s: %s, but there is no "
             "matching event log record of type SHARD_NEEDS_REBUILD that "
             "precedes it",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str());
    err = E::FAILED;
    return -1;
  }

  RebuildingShardInfo& shard_info = shards_[shard_idx];

  bool found = false;
  for (auto& p : shard_info.nodes_) {
    if (p.second.version <= ptr->header.version) {
      found = true;
      p.second.donors_complete[node_idx] = ptr->header.version;
      if (is_authoritative) {
        p.second.donors_complete_authoritatively[node_idx] =
            ptr->header.version;
      }
    }
  }

  if (!found) {
    err = E::FAILED;
    return -1;
  }

  if (ptr->header.version == shard_info.version) {
    // This donor finished rebuilding for this version.
    shard_info.donor_progress.erase(node_idx);
  }

  recomputeAuthoritativeStatus(shard_idx, timestamp, nodes_configuration);

  recomputeShardRebuildTimeIntervals(shard_idx);

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardAbortRebuild(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* ptr = dynamic_cast<const SHARD_ABORT_REBUILD_Event*>(&record);
  ld_check(ptr);

  auto it = shards_.find(ptr->header.shardIdx);
  if (it == shards_.end()) {
    ld_error("Found event log record with lsn %s: %s, but there is no "
             "matching event log record of type SHARD_NEEDS_REBUILD that "
             "precedes it",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str());
    err = E::STALE;
    return -1;
  }

  RebuildingShardInfo& shard_info = it->second;

  if (ptr->header.version != LSN_INVALID &&
      ptr->header.version != shard_info.version) {
    // Discard because the version does not match.
    if (ptr->header.version > shard_info.version) {
      ld_error("Found event log record %s for a rebuilding version greater "
               "than the last version %s. Discarding.",
               record.describe().c_str(),
               lsn_to_string(shard_info.version).c_str());
    }
    err = E::FAILED;
    return -1;
  }

  auto it_node = shard_info.nodes_.find(ptr->header.nodeIdx);
  if (it_node == shard_info.nodes_.end()) {
    ld_warning("Found event log record with lsn %s: %s, but there is no "
               "matching event log record of type SHARD_NEEDS_REBUILD that "
               "precedes it",
               lsn_to_string(lsn).c_str(),
               record.describe().c_str());
    err = E::STALE;
    return -1;
  }

  deleteShard(ptr->header.nodeIdx, ptr->header.shardIdx);

  if (shard_info.num_acked_ == shard_info.nodes_.size()) {
    shards_.erase(ptr->header.shardIdx);
  } else {
    shard_info.version = lsn;
    shard_info.donor_progress.clear();
  }

  recomputeAuthoritativeStatus(
      ptr->header.shardIdx, timestamp, nodes_configuration);

  recomputeShardRebuildTimeIntervals(ptr->header.shardIdx);

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardUndrain(
    lsn_t lsn,
    std::chrono::milliseconds /* unused */,
    const EventLogRecord& record) {
  const auto* ptr = dynamic_cast<const SHARD_UNDRAIN_Event*>(&record);
  ld_check(ptr);

  auto node_id = ptr->header.nodeIdx;

  auto it = shards_.find(ptr->header.shardIdx);
  if (it == shards_.end()) {
    ld_warning("Found event log record with lsn %s: %s, but there is no "
               "matching event log record of type SHARD_NEEDS_REBUILD that "
               "precedes it",
               lsn_to_string(lsn).c_str(),
               record.describe().c_str());
    err = E::FAILED;
    return -1;
  }

  RebuildingShardInfo& shard_info = shards_[ptr->header.shardIdx];
  if (!shard_info.nodes_.count(ptr->header.nodeIdx)) {
    ld_warning("Found unexpected event log record with lsn %s: %s. This can "
               "happen either because there was no matching event of type "
               "SHARD_NEEDS_REBUILD or because node %d already acknowledged "
               "without waiting for this message",
               lsn_to_string(lsn).c_str(),
               record.describe().c_str(),
               node_id);
    err = E::FAILED;
    return -1;
  }

  if (!shard_info.nodes_[ptr->header.nodeIdx].drain) {
    ld_warning("Found event log record with lsn %s: %s but this shard is not "
               "being drained. Discarding.",
               lsn_to_string(lsn).c_str(),
               record.describe().c_str());
    err = E::FAILED;
    return -1;
  }

  shard_info.nodes_[ptr->header.nodeIdx].drain = false;

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardUnrecoverable(
    lsn_t lsn,
    std::chrono::milliseconds timestamp,
    const EventLogRecord& record,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto* ptr = dynamic_cast<const SHARD_UNRECOVERABLE_Event*>(&record);
  ld_check(ptr);

  auto node_id = ptr->header.nodeIdx;

  auto it = shards_.find(ptr->header.shardIdx);
  if (it == shards_.end()) {
    ld_error("Found event log record with lsn %s: %s, but there is no "
             "matching event log record of type SHARD_NEEDS_REBUILD that "
             "precedes it",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str());
    err = E::FAILED;
    return -1;
  }

  RebuildingShardInfo& shard_info = shards_[ptr->header.shardIdx];
  auto it_node = shard_info.nodes_.find(ptr->header.nodeIdx);
  if (it_node == shard_info.nodes_.end()) {
    ld_error("Found unexpected event log record with lsn %s: %s. This can "
             "happen either because there was no matching event of type "
             "SHARD_NEEDS_REBUILD or because node %d already acknowledged "
             "without waiting for this message",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str(),
             node_id);
    err = E::FAILED;
    return -1;
  }

  setShardRecoverable(ptr->header.nodeIdx, ptr->header.shardIdx, false);

  recomputeAuthoritativeStatus(
      ptr->header.shardIdx, timestamp, nodes_configuration);

  recomputeShardRebuildTimeIntervals(ptr->header.shardIdx);

  checkConsistency();

  return 0;
}

int EventLogRebuildingSet::onShardDonorProgress(
    lsn_t lsn,
    std::chrono::milliseconds /* unused */,
    const EventLogRecord& record) {
  const auto* ptr = dynamic_cast<const SHARD_DONOR_PROGRESS_Event*>(&record);
  ld_check(ptr);

  auto donor_node_id = ptr->header.donorNodeIdx;
  auto shard_id = ptr->header.shardIdx;

  if (!shards_.count(shard_id)) {
    ld_error("Found event log record with lsn %s: %s, but there is no "
             "matching event log record of type SHARD_NEEDS_REBUILD that "
             "precedes it",
             lsn_to_string(lsn).c_str(),
             record.describe().c_str());
    err = E::FAILED;
    return -1;
  }

  RebuildingShardInfo& shard_info = shards_[shard_id];

  if (shard_info.version != ptr->header.version) {
    err = E::FAILED;
    return -1;
  }

  if (!shard_info.donor_progress.count(donor_node_id)) {
    ld_warning("Node %u notifies us that it moved its local window for "
               "shard %u up to timestamp %ld but it is not expected to be a "
               "donor for this rebuilding version.",
               donor_node_id,
               shard_id,
               ptr->header.nextTimestamp);
    err = E::FAILED;
    return -1;
  }

  const auto cur_ts = shard_info.donor_progress[donor_node_id].count();

  if (ptr->header.nextTimestamp <= cur_ts) {
    ld_warning("Node %u notifies us that it moved its local window for "
               "shard %u up to timestamp %ld but it had previously moved it "
               "to a greater timestamp %ld",
               donor_node_id,
               shard_id,
               ptr->header.nextTimestamp,
               cur_ts);
    err = E::FAILED;
    return -1;
  }

  shard_info.donor_progress[donor_node_id] = ts_type(ptr->header.nextTimestamp);

  return 0;
}

void EventLogRebuildingSet::recomputeShardRebuildTimeIntervals(
    uint32_t shard_idx) {
  if (!shards_.count(shard_idx)) {
    return;
  }

  RebuildingShardInfo& shard = shards_[shard_idx];
  shard.all_dirty_time_intervals.clear();
  for (auto node_kv : shard.nodes_) {
    auto& node = node_kv.second;
    if (my_node_id_.hasValue() && node_kv.first == my_node_id_->index() &&
        node.mode == RebuildingMode::RESTORE) {
      // In RESTORE mode, we cannot be a donor for our own records.
      continue;
    }

    if (node.auth_status != AuthoritativeStatus::AUTHORITATIVE_EMPTY &&
        !node.acked &&
        (node.mode == RebuildingMode::RELOCATE ||
         node.dc_dirty_ranges.empty())) {
      // We have to scan the entire log space. Looking at other nodes
      // won't change that.
      shard.all_dirty_time_intervals.insert(allRecordTimeInterval());
      break;
    }

    // Exclude nodes performing a time-ranged rebuild that have completed
    // (no donors remain).  Unlike a node rebuilt in RELOCATE mode, these
    // nodes are FULLY_AUTHORITATIVE before and after all donors complete
    // since they continue to take stores. The node will be removed once it
    // acks the rebuilding.
    if (!node.donors_remaining.empty()) {
      for (auto dcr_kv : node.dc_dirty_ranges) {
        for (auto time_interval : dcr_kv.second) {
          shard.all_dirty_time_intervals += time_interval;
        }
      }
    }
  }
}

bool EventLogRebuildingSet::NodeInfo::
operator==(const EventLogRebuildingSet::NodeInfo& rhs) const {
  return version == rhs.version && mode == rhs.mode &&
      donors_complete == rhs.donors_complete &&
      donors_complete_authoritatively == rhs.donors_complete_authoritatively &&
      donors_remaining == rhs.donors_remaining &&
      dc_dirty_ranges == rhs.dc_dirty_ranges &&
      auth_status == rhs.auth_status && drain == rhs.drain &&
      recoverable == rhs.recoverable && acked == rhs.acked &&
      ack_lsn == rhs.ack_lsn && ack_version == rhs.ack_version;
}

std::string EventLogRebuildingSet::NodeInfo::toString() const {
  std::string s;

  if (acked) {
    s += "(acked)";
  }
  if (drain) {
    s += "(drain)";
  }
  s += logdevice::toString(dc_dirty_ranges);
  return s;
}

bool EventLogRebuildingSet::NodeInfo::rebuildingIsNonAuthoritative() const {
  // If not all nodes that completed rebuilding completed authoritatively, this
  // node is being rebuilt non authoritatively.
  return donors_complete.size() > donors_complete_authoritatively.size();
}

bool EventLogRebuildingSet::RebuildingShardInfo::
operator==(const EventLogRebuildingSet::RebuildingShardInfo& rhs) const {
  return version == rhs.version && nodes_ == rhs.nodes_ &&
      donor_progress == rhs.donor_progress &&
      all_dirty_time_intervals == rhs.all_dirty_time_intervals &&
      num_acked_ == rhs.num_acked_ && num_recoverable_ == rhs.num_recoverable_;
};

std::string EventLogRebuildingSet::toString() const {
  std::string s;
  s += "{";
  bool first = true;
  std::vector<const decltype(shards_)::value_type*> sorted_shards;
  for (auto& shard_kv : shards_) {
    sorted_shards.push_back(&shard_kv);
  }
  std::sort(sorted_shards.begin(), sorted_shards.end(), [](auto l, auto r) {
    return l->first < r->first;
  });
  for (auto it : sorted_shards) {
    if (!first) {
      s += ",";
    }
    first = false;
    s += "shard" + std::to_string(it->first) + "(" +
        lsn_to_string(it->second.version) + ")" + ":[";
    bool first_node = true;
    for (auto& node : it->second.nodes_) {
      if (!first_node) {
        s += ",";
      }
      first_node = false;
      s += std::to_string(node.first);
      s += node.second.toString();
    }
    s += "]";
  }
  s += "}";
  return s;
}

ShardAuthoritativeStatusMap EventLogRebuildingSet::toShardStatusMap(
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  ShardAuthoritativeStatusMap map(getLastUpdate());
  for (auto& shard : getRebuildingShards()) {
    for (auto& node : shard.second.nodes_) {
      if (!nodes_configuration.getStorageMembership()->hasNode(node.first)) {
        // ignore nodes that are not in the storagte membership
        continue;
      }
      map.setShardStatus(node.first,
                         shard.first,
                         node.second.auth_status,
                         !node.second.dc_dirty_ranges.empty());
    }
  }

  return map;
}

void EventLogRebuildingSet::recomputeAuthoritativeStatus(
    uint32_t shard,
    std::chrono::milliseconds timestamp,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  const auto& storage_membership = nodes_configuration.getStorageMembership();
  std::unordered_set<node_index_t> storage_nodes;
  for (const auto& n : *storage_membership) {
    if (storage_membership->hasShardShouldReadFrom(n)) {
      storage_nodes.insert(n);
    }
  }

  auto it_shard = shards_.find(shard);
  if (it_shard == shards_.end()) {
    return;
  }
  RebuildingShardInfo& shard_info = it_shard->second;

  shard_info.waiting_for_recoverable_shards_ = false;

  auto recompute_auth_status = [&]() {
    for (auto& it_node : shard_info.nodes_) {
      AuthoritativeStatus status_was = it_node.second.auth_status;
      bool donors_remaining_was_empty = it_node.second.donors_remaining.empty();
      std::string comment;

      recomputeShardAuthoritativeStatus(shard_info,
                                        it_node.first,
                                        timestamp,
                                        it_node.second,
                                        storage_nodes,
                                        &comment);

      // Clear the recoverable flag when the shard is empty.
      if (it_node.second.auth_status ==
          AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        setShardRecoverable(it_node.first, shard, false);
      }

      if (it_node.second.auth_status != status_was) {
        ld_info("%s transitioned from %s to %s because %s",
                ShardID(it_node.first, shard).toString().c_str(),
                logdevice::toString(status_was).c_str(),
                logdevice::toString(it_node.second.auth_status).c_str(),
                comment.c_str());
      } else if (!donors_remaining_was_empty &&
                 it_node.second.donors_remaining.empty() &&
                 it_node.second.auth_status !=
                     AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        ld_info("All donors finished rebuilding %s but it's not transitioning "
                "from %s to %s because %s",
                ShardID(it_node.first, shard).toString().c_str(),
                logdevice::toString(it_node.second.auth_status).c_str(),
                logdevice::toString(AuthoritativeStatus::AUTHORITATIVE_EMPTY)
                    .c_str(),
                comment.c_str());
      }
    }
  };

  size_t num_recoverable_prev = shard_info.num_recoverable_;
  recompute_auth_status();
  // recompute_auth_status() may have caused some shards to become
  // AUTHORITATIVE_EMPTY, which causes num_recoverable to decrease. If
  // num_recoverable_ reached zero, this means that there may be more shards
  // that can be transitioned to AUTHORITATIVE_EMPTY. Let's do another pass to
  // detect such shards.
  if (num_recoverable_prev > 0 && shard_info.num_recoverable_ == 0) {
    recompute_auth_status();
  }

  for (auto& it_node : shard_info.nodes_) {
    for (node_index_t nid : it_node.second.donors_remaining) {
      if (!shard_info.donor_progress.count(nid)) {
        shard_info.donor_progress[nid] = ts_type::min();
      }
    }
  }

  // compute non_authoritative_since_ts
  for (auto& n : shard_info.nodes_) {
    if (n.second.rebuildingIsNonAuthoritative()) {
      if (n.second.non_authoritative_since_ts ==
          std::chrono::milliseconds::max()) {
        // then we transitioned to non_authoritative status at time `timestamp'
        n.second.non_authoritative_since_ts = timestamp;
      }
    } else {
      n.second.non_authoritative_since_ts = std::chrono::milliseconds::max();
    }
  }
}

// This function contains the main logic that defines the authoritative status
// of nodes in the cluster according to the content of the event log as well as
// which node should be a donor for rebuilding.
//
// NOTE: this function re-populates `NodeInfo::donors_remaining` as well as
// `NodeInfo::auth_status` and is called after we receive any event that may
// cause them to be changed. It would be more efficient to do more fine-grain
// updates of these fields depending on which event we are receiving, however
// gathering all the logic into one function makes the code easier to reason
// about and less prone to bugs.
void EventLogRebuildingSet::recomputeShardAuthoritativeStatus(
    RebuildingShardInfo& shard_info,
    node_index_t nidx,
    std::chrono::milliseconds timestamp,
    NodeInfo& node_info,
    std::unordered_set<node_index_t> potential_donors,
    std::string* out_comment) const {
  ld_check(out_comment);
  node_info.donors_remaining.clear();

  // If the node acked, or is not rebuilt in RESTORE mode, consider it
  // FULLY_AUTHORITATIVE.
  if (node_info.acked) {
    node_info.auth_status = AuthoritativeStatus::FULLY_AUTHORITATIVE;
    *out_comment = "rebuilding was acked";
    return;
  }

  if (node_info.auth_status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
    // The node has already been establish AUTHORITATIVE_EMPTY, don't change
    // that.
    ld_check(node_info.dc_dirty_ranges.empty());
    node_info.auth_status = AuthoritativeStatus::AUTHORITATIVE_EMPTY;
    *out_comment = "it was already AUTHORITATIVE_EMPTY";
    return;
  }

  // `min_auth_complete_version` represents the minimum version for which a node
  // in `node_info.donors_complete_authoritatively` is actually considered as
  // authoritavely complete.
  lsn_t min_auth_complete_version = node_info.version;
  for (auto& n : shard_info.nodes_) {
    if (n.second.version > min_auth_complete_version &&
        !node_info.donors_complete_authoritatively.count(n.first)) {
      // Here, node n was put in the rebuilding set before completing
      // authoritatively. Any donor that rebuilt with a smaller version is not
      // considered authoritatively complete as it might have skipped some
      // records that would be rebuilt by node n.
      min_auth_complete_version = n.second.version;
    }
  }

  // Exclude authoritatively completed donors.
  for (auto& n : node_info.donors_complete_authoritatively) {
    if (n.second >= min_auth_complete_version) {
      potential_donors.erase(n.first);
    }
  }

  // Let's also remove all nodes that we know lost their data and can't rebuild
  // anything. These are the nodes rebuilding complete shards in RESTORE mode
  // that have not acked, or have acked *after* our shard was marked as needing
  // rebuilding or the nodes that have been rebuilt in relocate mode and are
  // now empty
  for (auto& n : shard_info.nodes_) {
    if ((n.second.mode == RebuildingMode::RESTORE &&
         n.second.dc_dirty_ranges.empty() &&
         (!n.second.acked || n.second.ack_lsn > node_info.version)) ||
        (n.second.mode == RebuildingMode::RELOCATE &&
         n.second.auth_status == AuthoritativeStatus::AUTHORITATIVE_EMPTY)) {
      potential_donors.erase(n.first);
    }
  }

  // A node can't be a donor to restore it's own data.
  // This excludes nodes restoring a time-range that would otherwise be
  // missed by the loop above.
  if (node_info.mode == RebuildingMode::RESTORE) {
    ld_check(potential_donors.count(nidx) == 0 ||
             !node_info.dc_dirty_ranges.empty());
    potential_donors.erase(nidx);
  }

  // Find all nodes that still have not rebuilt authoritatively and also have
  // not rebuilt for the current version.
  for (node_index_t n : potential_donors) {
    auto it = node_info.donors_complete.find(n);
    if (it == node_info.donors_complete.end() ||
        it->second < shard_info.version) {
      node_info.donors_remaining.insert(n);
    }
  }

  /**
   * Nodes performing a ranged rebuild are expected to be up, taking writes,
   * and informing readers of underreplication when delivering gaps from
   * dirty regions.
   */
  if (!node_info.dc_dirty_ranges.empty()) {
    ld_check(node_info.mode == RebuildingMode::RESTORE);
    node_info.auth_status = AuthoritativeStatus::FULLY_AUTHORITATIVE;
    *out_comment = "it's a time-range rebuilding";
    return;
  }

  if (potential_donors.empty()) {
    node_info.rebuilding_completed_ts = timestamp;
    node_info.auth_status = AuthoritativeStatus::AUTHORITATIVE_EMPTY;
    *out_comment = "all donors completed authoritatively";
    return;
  }

  if (node_info.mode == RebuildingMode::RELOCATE) {
    if (node_info.donors_remaining.empty()) {
      // Regardless of if some nodes could not complete rebuilding
      // authoritatively, if all donors completed and the node was being rebuilt
      // in RELOCATE mode, we know that for that node all records were rebuilt
      // since there is at least one copy of each record on it, so it is safe to
      // mark the node AUTHORITATIVE_EMPTY.
      node_info.rebuilding_completed_ts = timestamp;
      node_info.auth_status = AuthoritativeStatus::AUTHORITATIVE_EMPTY;
      *out_comment =
          "all donors completed (some non-authoritatively), and it's "
          "a RELOCATE rebuilding";
    } else {
      // We are draining this node in relocate mode. Either rebuilding completed
      // and in case its authoritative status is AUTHORITATIVE_EMPTY, or it did
      // not and it remains FULLY_AUTHORITATIVE.
      node_info.auth_status = AuthoritativeStatus::FULLY_AUTHORITATIVE;
      *out_comment = "not all donors completed";
    }
  } else { /* node_info.mode == RebuildingMode::RESTORE */
    // If all nodes that can rebuild rebuilt and none of the lost shards are
    // recoverable, report the node as authoritative empty.
    if (node_info.donors_remaining.empty() &&
        shard_info.num_recoverable_ == 0) {
      node_info.rebuilding_completed_ts = timestamp;
      node_info.auth_status = AuthoritativeStatus::AUTHORITATIVE_EMPTY;
      *out_comment = "all donors completed (some non-authoritatively), and all "
                     "shards are unrecoverable; expect data loss";
      return;
    }

    shard_info.waiting_for_recoverable_shards_ |=
        node_info.rebuildingIsNonAuthoritative() && node_info.recoverable;

    // If the node's data is recoverable, set its authoritative status to
    // UNAVAILABLE. Readers don't make any distinction between
    // FULLY_AUTHORITATIVE and UNAVAILABLE, which means that readers will stall
    // if they can't get an f-majority and at least one node is UNAVAILABLE.
    // Readers do not stall if they hear from all the nodes that are not
    // UNDERREPLICATION and AUTHORITATIVE_EMPTY.
    node_info.auth_status = node_info.recoverable
        ? AuthoritativeStatus::UNAVAILABLE
        : AuthoritativeStatus::UNDERREPLICATION;

    if (node_info.donors_remaining.empty()) {
      *out_comment =
          "donors completed non-authoritatively, and some shards are "
          "considered recoverable";
    } else {
      *out_comment =
          std::string("not all donors completed, and the shard is marked ") +
          (node_info.recoverable ? "recoverable" : "unrecoverable");
    }
  }
}

AuthoritativeStatus EventLogRebuildingSet::getShardAuthoritativeStatus(
    node_index_t node,
    uint32_t shard,
    std::vector<node_index_t>& storage_nodes) const {
  storage_nodes.clear();

  auto it_shard = shards_.find(shard);
  if (it_shard == shards_.end()) {
    return AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }

  const RebuildingShardInfo& shard_info = it_shard->second;
  auto it_node = shard_info.nodes_.find(node);
  if (it_node == shard_info.nodes_.end()) {
    return AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }

  const NodeInfo& node_info = it_node->second;
  storage_nodes = std::vector<node_index_t>(
      node_info.donors_remaining.begin(), node_info.donors_remaining.end());
  return node_info.auth_status;
}

bool EventLogRebuildingSet::operator==(const EventLogRebuildingSet& rhs) const {
  // Only verify the data that is serialized/deserialized.
  return shards_ == rhs.shards_;
}

bool EventLogRebuildingSet::waitingForRecoverableShards() const {
  for (const auto& shard : shards_) {
    if (shard.second.waiting_for_recoverable_shards_) {
      return true;
    }
  }
  return false;
}

EventLogRebuildingSet::RebuildingShardInfo const*
EventLogRebuildingSet::getForShardOffset(uint32_t shard) const {
  if (!shards_.count(shard)) {
    return nullptr;
  }
  return &shards_.find(shard)->second;
}

EventLogRebuildingSet::NodeInfo const*
EventLogRebuildingSet::getNodeInfo(node_index_t node, uint32_t shard) const {
  if (!shards_.count(shard)) {
    return nullptr;
  }
  const RebuildingShardInfo& shard_info = shards_.find(shard)->second;
  if (!shard_info.nodes_.count(node)) {
    return nullptr;
  }
  return &shard_info.nodes_.find(node)->second;
}

bool EventLogRebuildingSet::isDonor(node_index_t node, uint32_t shard) const {
  auto shards_it = shards_.find(shard);
  if (shards_it == shards_.end()) {
    return false;
  }
  const RebuildingShardInfo& shard_info = shards_it->second;
  if (shard_info.donor_progress.count(node)) {
    // It's a donor, and it hasn't completed yet.
    return true;
  }

  for (auto& kv : shard_info.nodes_) {
    const NodeInfo& node_info = kv.second;
    ld_check(!node_info.donors_remaining.count(node)); // would return above
    if (node_info.donors_complete.count(node)) {
      // It's a donor, and it's done with its donorship.
      return true;
    }
  }
  return false;
}

bool EventLogRebuildingSet::canTrimEventLog(
    const configuration::nodes::NodesConfiguration& nodes_configuration) const {
  const auto& storage_membership = nodes_configuration.getStorageMembership();
  for (const auto& shard : shards_) {
    for (const auto& node : shard.second.nodes_) {
      if (node.second.acked) {
        continue;
      }
      if (storage_membership->hasShardShouldReadFrom(node.first)) {
        return false;
      }
      if (node.second.auth_status != AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
        // The node is not in the config anymore or is not a storage node
        // anymore, but it's not been fully rebuilt yet.
        return false;
      }
    }
  }
  return true;
}

}} // namespace facebook::logdevice
