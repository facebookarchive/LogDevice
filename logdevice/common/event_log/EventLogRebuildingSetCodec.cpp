/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogRebuildingSetCodec.h"

#include <folly/container/F14Map.h>

namespace facebook { namespace logdevice {

template <typename T>
using offset_vector = std::vector<flatbuffers::Offset<T>>;

flatbuffers::Offset<event_log_rebuilding_set::Set>
EventLogRebuildingSetCodec::serialize(flatbuffers::FlatBufferBuilder& b,
                                      const EventLogRebuildingSet& set) {
  offset_vector<event_log_rebuilding_set::ShardInfo> shards;
  for (const auto& it_shard : set.getRebuildingShards()) {
    offset_vector<event_log_rebuilding_set::NodeInfo> nodes;
    for (const auto& it_node : it_shard.second.nodes_) {
      auto nodeInfo = EventLogRebuildingSetCodec::serialize(
          b, it_node.first, it_node.second);
      nodes.push_back(nodeInfo);
    }
    auto nodes_vector = b.CreateVector(nodes);

    offset_vector<event_log_rebuilding_set::DonorProgress> donors;
    for (const auto& it_donor : it_shard.second.donor_progress) {
      auto donorProgress = event_log_rebuilding_set::CreateDonorProgress(
          b, it_donor.first, it_donor.second.count());
      donors.push_back(donorProgress);
    }
    auto donors_vector = b.CreateVector(donors);

    auto shardInfo =
        event_log_rebuilding_set::CreateShardInfo(b,
                                                  it_shard.first,
                                                  it_shard.second.version,
                                                  nodes_vector,
                                                  donors_vector);
    shards.push_back(shardInfo);
  }

  auto shards_vector = b.CreateVector(shards);

  return event_log_rebuilding_set::CreateSet(b, shards_vector);
}

flatbuffers::Offset<event_log_rebuilding_set::NodeInfo>
EventLogRebuildingSetCodec::serialize(
    flatbuffers::FlatBufferBuilder& b,
    node_index_t nid,
    const EventLogRebuildingSet::NodeInfo& node) {
  offset_vector<event_log_rebuilding_set::DonorComplete> donors;
  for (const auto& it_donor : node.donors_complete) {
    auto donorsComplete = event_log_rebuilding_set::CreateDonorComplete(
        b, it_donor.first, it_donor.second);
    donors.push_back(donorsComplete);
  }

  offset_vector<event_log_rebuilding_set::DonorComplete> donors_auth;
  for (const auto& it_donor : node.donors_complete_authoritatively) {
    auto donorsComplete = event_log_rebuilding_set::CreateDonorComplete(
        b, it_donor.first, it_donor.second);
    donors_auth.push_back(donorsComplete);
  }

  offset_vector<event_log_rebuilding_set::DataClassTimeRanges> dc_time_ranges;
  for (const auto& it_dctr : node.dc_dirty_ranges) {
    offset_vector<event_log_rebuilding_set::RecordTimeRange> time_ranges;
    for (const auto& it_tr : it_dctr.second) {
      auto tr = event_log_rebuilding_set::CreateRecordTimeRange(
          b,
          it_tr.lower().toMilliseconds().count(),
          it_tr.upper().toMilliseconds().count());
      time_ranges.push_back(tr);
    }
    auto dc_tr = event_log_rebuilding_set::CreateDataClassTimeRanges(
        b, (data_class_index_t)it_dctr.first, b.CreateVector(time_ranges));
    dc_time_ranges.push_back(dc_tr);
  }

  return event_log_rebuilding_set::CreateNodeInfo(
      b,
      nid,
      node.version,
      (int)node.mode,
      b.CreateVector(donors),
      b.CreateVector(donors_auth),
      (int)node.auth_status,
      // This field used to have type RebuildingMode. 0 means "DRAIN in RELOCATE
      // mode" and -1 means "do not drain".
      node.drain ? 0 : -1,
      node.recoverable,
      node.acked,
      node.ack_lsn,
      node.ack_version,
      b.CreateVector(dc_time_ranges),
      b.CreateString(node.source),
      b.CreateString(node.details),
      node.rebuilding_started_ts.count(),
      node.rebuilding_completed_ts.count());
}

std::unique_ptr<EventLogRebuildingSet> EventLogRebuildingSetCodec::deserialize(
    const event_log_rebuilding_set::Set* set,
    lsn_t version,
    const folly::Optional<NodeID>& my_node_id) {
  auto res = std::make_unique<EventLogRebuildingSet>(version, my_node_id);

  auto shards = set->shards();
  if (!shards) {
    return res;
  }

  for (size_t i = 0; i < shards->Length(); ++i) {
    auto shard = shards->Get(i);
    uint32_t shard_idx = shard->idx();
    res->shards_[shard_idx] = deserialize(shard);
  }

  return res;
}

EventLogRebuildingSet::RebuildingShardInfo
EventLogRebuildingSetCodec::deserialize(
    const event_log_rebuilding_set::ShardInfo* shard_info) {
  folly::F14FastMap<node_index_t, EventLogRebuildingSet::NodeInfo> node_map;
  folly::F14FastMap<node_index_t, EventLogRebuildingSet::ts_type> donors;

  auto nodes = shard_info->nodes();
  if (nodes) {
    for (size_t i = 0; i < nodes->Length(); ++i) {
      auto node = nodes->Get(i);
      node_index_t nid = node->idx();
      node_map[nid] = deserialize(node);
    }
  }

  auto donor_progress = shard_info->donor_progress();
  if (donor_progress) {
    for (size_t i = 0; i < donor_progress->Length(); ++i) {
      auto donor = donor_progress->Get(i);
      node_index_t nid = donor->idx();
      donors[nid] = EventLogRebuildingSet::ts_type{donor->ts()};
    }
  }

  size_t num_acked = 0;
  size_t num_recoverable = 0;
  for (const auto& n : node_map) {
    if (n.second.acked) {
      ++num_acked;
    } else if (n.second.recoverable) {
      ++num_recoverable;
    }
  }

  auto s = EventLogRebuildingSet::RebuildingShardInfo{};
  s.version = shard_info->version();
  s.nodes_ = std::move(node_map);
  s.donor_progress = std::move(donors);
  s.num_acked_ = num_acked;
  s.num_recoverable_ = num_recoverable;
  return s;
}

EventLogRebuildingSet::NodeInfo EventLogRebuildingSetCodec::deserialize(
    const event_log_rebuilding_set::NodeInfo* node_info) {
  EventLogRebuildingSet::NodeInfo res;

  auto donor_complete = node_info->donors_complete();
  if (donor_complete) {
    for (size_t i = 0; i < donor_complete->Length(); ++i) {
      auto donor = donor_complete->Get(i);
      node_index_t nid = donor->idx();
      res.donors_complete[nid] = donor->version();
    }
  }

  auto donor_complete_auth = node_info->donors_complete_authoritatively();
  if (donor_complete_auth) {
    for (size_t i = 0; i < donor_complete_auth->Length(); ++i) {
      auto donor = donor_complete_auth->Get(i);
      node_index_t nid = donor->idx();
      res.donors_complete_authoritatively[nid] = donor->version();
    }
  }

  PerDataClassTimeRanges per_dc_dirty_ranges;
  auto per_dc_dirty_ranges_data = node_info->dc_dirty_ranges();
  if (per_dc_dirty_ranges_data) {
    bool unknown_data_class = false;
    for (size_t i = 0; i < per_dc_dirty_ranges_data->Length(); ++i) {
      auto dc_dirty_ranges = per_dc_dirty_ranges_data->Get(i);
      auto time_ranges = dc_dirty_ranges->time_ranges();
      if (time_ranges) {
        RecordTimeIntervals rtis;
        for (size_t j = 0; j < time_ranges->Length(); ++j) {
          auto range = time_ranges->Get(j);
          rtis.insert(RecordTimeInterval(
              RecordTimestamp::from(std::chrono::milliseconds(range->lower())),
              RecordTimestamp::from(
                  std::chrono::milliseconds(range->upper()))));
        }
        if (!rtis.empty()) {
          DataClass dc = (DataClass)dc_dirty_ranges->dc();
          if (dc >= DataClass::MAX) {
            ld_warning("Unknown DataClass %d in dirty time range data. "
                       "Performing full shard rebuild.",
                       (int)dc);
            unknown_data_class = true;
          } else {
            per_dc_dirty_ranges.insert(
                {(DataClass)dc_dirty_ranges->dc(), std::move(rtis)});
          }
        }
      }
    }
    if (unknown_data_class) {
      // Perform a full rebuild of the shard.
      per_dc_dirty_ranges.clear();
    }
  }

  res.version = node_info->version();
  res.mode = (RebuildingMode)node_info->mode();
  res.dc_dirty_ranges = std::move(per_dc_dirty_ranges);
  if (node_info->source()) {
    res.source = node_info->source()->str();
  }
  if (node_info->details()) {
    res.details = node_info->details()->str();
  }
  res.auth_status = (AuthoritativeStatus)node_info->auth_status();
  // This field used to have type RebuildingMode. 0 or 1 means "DRAIN in
  // RELOCATE or RESTORE mode" and -1 means "do not drain".  However, since
  // D4896350, we discard the user's intent to drain in RELOCATE or RESTORE mode
  // so this field can be considered a boolean.
  res.drain = node_info->drain() != -1;
  res.recoverable = node_info->recoverable();
  res.acked = node_info->acked();
  res.ack_lsn = node_info->ack_lsn();
  res.ack_version = node_info->ack_version();

  res.rebuilding_started_ts =
      std::chrono::milliseconds(node_info->rebuilding_started_ts_millis());
  res.rebuilding_completed_ts =
      std::chrono::milliseconds(node_info->rebuilding_completed_ts_millis());

  return res;
}

}} // namespace facebook::logdevice
