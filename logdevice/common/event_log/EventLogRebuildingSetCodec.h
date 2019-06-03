/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/event_log/EventLogRebuildingSet_generated.h"

/**
 * This file provides the interface for serializing EventLogRebuildingSet
 * snapshots using flatbuffers.
 */

namespace facebook { namespace logdevice {

class EventLogRebuildingSetCodec {
 public:
  static flatbuffers::Offset<event_log_rebuilding_set::Set>
  serialize(flatbuffers::FlatBufferBuilder& b,
            const EventLogRebuildingSet& set);

  static flatbuffers::Offset<event_log_rebuilding_set::NodeInfo>
  serialize(flatbuffers::FlatBufferBuilder& b,
            node_index_t nid,
            const EventLogRebuildingSet::NodeInfo& node);

  static std::unique_ptr<EventLogRebuildingSet>
  deserialize(const event_log_rebuilding_set::Set* set,
              lsn_t version,
              const folly::Optional<NodeID>& my_node_id);

  static EventLogRebuildingSet::RebuildingShardInfo
  deserialize(const event_log_rebuilding_set::ShardInfo* shard_info);

  static EventLogRebuildingSet::NodeInfo
  deserialize(const event_log_rebuilding_set::NodeInfo* node_info);
};

}} // namespace facebook::logdevice
