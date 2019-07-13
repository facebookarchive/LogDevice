/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/GOSSIP_Message.h"

#include <memory>

#include <folly/small_vector.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

GOSSIP_Message::GOSSIP_Message(NodeID this_node,
                               node_list_t node_list,
                               std::chrono::milliseconds instance_id,
                               std::chrono::milliseconds sent_time,
                               boycott_list_t boycott_list,
                               boycott_durations_list_t boycott_durations,
                               GOSSIP_Message::GOSSIP_flags_t flags,
                               uint64_t msg_id)
    : Message(MessageType::GOSSIP, TrafficClass::FAILURE_DETECTOR),
      node_list_(std::move(node_list)),
      gossip_node_(this_node),
      flags_(flags),
      instance_id_(instance_id),
      sent_time_(sent_time),
      num_boycotts_(boycott_list.size()),
      boycott_list_(std::move(boycott_list)),
      boycott_durations_list_(std::move(boycott_durations)),
      msg_id_(msg_id) {}

Message::Disposition GOSSIP_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/GOSSIP_onReceived.cpp; this should
  // never get called.
  std::abort();
}

void GOSSIP_Message::serialize(ProtocolWriter& writer) const {
  auto flags = flags_;
  node_list_t sorted_node_list;

  if (writer.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    // Old nodes expect nodes are fully in order
    // TODO: If deleting nodes from node_list_ is allowed later, not all node_id
    // may exist. In that case, dummy nodes need to be inserted in node_list_ to
    // assure all node_id exist between 0 and max.
    sorted_node_list = node_list_;
    std::sort(sorted_node_list.begin(), sorted_node_list.end());
  }

  writer.write((uint16_t)node_list_.size());
  writer.write(gossip_node_);
  writer.write(flags);

  if (writer.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    for (auto n : sorted_node_list) {
      writer.write(n.gossip_);
    }
  }
  writer.write(instance_id_);
  writer.write(sent_time_);
  if (writer.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    for (auto n : sorted_node_list) {
      writer.write(n.gossip_ts_);
    }
    if (flags & HAS_FAILOVER_LIST_FLAG) {
      for (auto n : sorted_node_list) {
        writer.write(n.failover_);
      }
    }
  }

  writeBoycottList(writer);
  writeBoycottDurations(writer);

  if (writer.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    if (flags & HAS_STARTING_LIST_FLAG) {
      // for backward compatibility
      starting_list_t starting_list;
      for (auto n : sorted_node_list) {
        if (n.is_node_starting_) {
          starting_list.push_back(NodeID(n.node_id_));
        }
      }
      writer.write((uint16_t)starting_list.size());
      for (auto& node : starting_list) {
        writer.write(node.index());
      }
    }
  }
  if (writer.proto() >=
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    writer.writeVector(node_list_);
  }
}

MessageReadResult GOSSIP_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<GOSSIP_Message> msg(new GOSSIP_Message());

  gossip_list_t gossip_list;
  gossip_ts_t gossip_ts;
  failover_list_t failover_list;
  std::unordered_set<node_index_t> is_starting;
  uint16_t num_nodes;
  uint16_t num_starting;

  reader.read(&num_nodes);
  reader.read(&msg->gossip_node_);
  reader.read(&msg->flags_);

  if (reader.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    reader.readVector(&gossip_list, num_nodes);
  }
  reader.read(&msg->instance_id_);
  reader.read(&msg->sent_time_);

  if (reader.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    reader.readVector(&gossip_ts, num_nodes);
    if (reader.ok() && (msg->flags_ & HAS_FAILOVER_LIST_FLAG)) {
      reader.readVector(&failover_list, num_nodes);
    } else {
      // dummy filling for backward compatibility
      failover_list =
          failover_list_t(num_nodes, std::chrono::milliseconds::zero());
    }
  }

  msg->readBoycottList(reader);
  msg->readBoycottDurations(reader);

  if (reader.proto() <
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    if (msg->flags_ & HAS_STARTING_LIST_FLAG) {
      // for backward compatibility
      reader.read(&num_starting);
      for (uint16_t i = 0; i < num_starting; i++) {
        node_index_t nidx;
        reader.read(&nidx);
        is_starting.insert(nidx);
      }
    }
  }

  if (reader.proto() >=
      Compatibility::ProtocolVersion::HASHMAP_SUPPORT_IN_GOSSIP) {
    reader.readVector(&msg->node_list_, num_nodes);
  } else {
    // In case of protocol before HASHMAP_SUPPORT_IN_GOSSIP,
    // node_id_ always increases linearly
    for (node_index_t i = 0; i < num_nodes; i++) {
      GOSSIP_Node gnode;
      gnode.node_id_ = i;
      gnode.gossip_ = gossip_list[i];
      gnode.gossip_ts_ = gossip_ts[i];
      gnode.failover_ = failover_list[i];
      gnode.is_node_starting_ = (is_starting.count(i) > 0) ? true : false;
      msg->node_list_.push_back(gnode);
    }
  }

  return reader.resultMsg(std::move(msg));
}

void GOSSIP_Message::onSent(Status /*st*/, const Address& /*to*/) const {
  // Receipt handler lives in server/GOSSIP_onSent.cpp; this should
  // never get called.
  std::abort();
}

void GOSSIP_Message::writeBoycottList(ProtocolWriter& writer) const {
  ld_check(boycott_list_.size() == num_boycotts_);
  writer.write(num_boycotts_);

  for (auto& boycott : boycott_list_) {
    writer.write(boycott.node_index);
    writer.write(boycott.boycott_in_effect_time);
    writer.write(boycott.boycott_duration);
    writer.write(boycott.reset);
  }
}

void GOSSIP_Message::readBoycottList(ProtocolReader& reader) {
  reader.read(&num_boycotts_);

  boycott_list_.resize(num_boycotts_);

  for (auto& boycott : boycott_list_) {
    reader.read(&boycott.node_index);
    reader.read(&boycott.boycott_in_effect_time);
    reader.read(&boycott.boycott_duration);
    reader.read(&boycott.reset);
  }
}

std::chrono::milliseconds GOSSIP_Message::getDefaultBoycottDuration() const {
  return Worker::settings().sequencer_boycotting.node_stats_boycott_duration;
}

// flattens the boycott durations and write them
void GOSSIP_Message::writeBoycottDurations(ProtocolWriter& writer) const {
  writer.write(boycott_durations_list_.size());
  for (const auto& d : boycott_durations_list_) {
    d.serialize(writer);
  }
}

// reads the flattened boycott durations and unflatten them
void GOSSIP_Message::readBoycottDurations(ProtocolReader& reader) {
  size_t list_size;
  reader.read(&list_size);
  boycott_durations_list_.resize(list_size);
  for (auto& d : boycott_durations_list_) {
    d.deserialize(reader);
  }
}

}} // namespace facebook::logdevice
