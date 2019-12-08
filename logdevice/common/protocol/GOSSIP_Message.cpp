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
                               uint64_t msg_id,
                               GOSSIP_Message::rsmtype_list_t rsm_types,
                               GOSSIP_Message::versions_node_list_t versions)
    : Message(MessageType::GOSSIP, TrafficClass::FAILURE_DETECTOR),
      node_list_(std::move(node_list)),
      gossip_node_(this_node),
      flags_(flags),
      instance_id_(instance_id),
      sent_time_(sent_time),
      num_boycotts_(boycott_list.size()),
      boycott_list_(std::move(boycott_list)),
      boycott_durations_list_(std::move(boycott_durations)),
      msg_id_(msg_id),
      rsm_types_(rsm_types),
      versions_(versions) {}

Message::Disposition GOSSIP_Message::onReceived(const Address& /*from*/) {
  // Receipt handler lives in server/GOSSIP_onReceived.cpp; this should
  // never get called.
  std::abort();
}

void GOSSIP_Message::serialize(ProtocolWriter& writer) const {
  auto flags = flags_;

  writer.write((uint16_t)node_list_.size());
  writer.write(gossip_node_);
  writer.write(flags);
  writer.write(instance_id_);
  writer.write(sent_time_);
  writeBoycottList(writer);
  writeBoycottDurations(writer);
  if (writer.proto() <
      Compatibility::ProtocolVersion::HEALTH_MONITOR_SUPPORT_IN_GOSSIP) {
    legacy_node_list_t legacy_node_list{};
    std::transform(node_list_.begin(),
                   node_list_.end(),
                   std::back_inserter(legacy_node_list),
                   [](GOSSIP_Node gossip_node) {
                     return static_cast<GOSSIP_Node_Legacy>(gossip_node);
                   });
    writer.writeVector(legacy_node_list);
  } else {
    writer.writeVector(node_list_);
    if (flags & HAS_VERSIONS) {
      writeVersions(writer);
    }
  }
}

MessageReadResult GOSSIP_Message::deserialize(ProtocolReader& reader) {
  std::unique_ptr<GOSSIP_Message> msg(new GOSSIP_Message());

  gossip_list_t gossip_list;
  gossip_ts_t gossip_ts;
  failover_list_t failover_list;
  std::unordered_set<node_index_t> is_starting;
  uint16_t num_nodes;

  reader.read(&num_nodes);
  reader.read(&msg->gossip_node_);
  reader.read(&msg->flags_);
  reader.read(&msg->instance_id_);
  reader.read(&msg->sent_time_);
  msg->readBoycottList(reader);
  msg->readBoycottDurations(reader);
  if (reader.proto() <
      Compatibility::ProtocolVersion::HEALTH_MONITOR_SUPPORT_IN_GOSSIP) {
    legacy_node_list_t legacy_node_list{};
    reader.readVector(&legacy_node_list, num_nodes);
    std::transform(legacy_node_list.begin(),
                   legacy_node_list.end(),
                   std::back_inserter(msg->node_list_),
                   [](GOSSIP_Node_Legacy gossip_node) {
                     return GOSSIP_Node{gossip_node};
                   });
  } else {
    reader.readVector(&msg->node_list_, num_nodes);
    if (msg->flags_ & HAS_VERSIONS ||
        msg->flags_ & HAS_DURABLE_SNAPSHOT_VERSIONS) {
      // For future compatibility deserialize messages with durable flag.
      // It will be discarded in this commit of the code, but will be supported
      // in future iteration(once Local snapshot store is implemented)
      msg->readVersions(reader, num_nodes);
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

void GOSSIP_Message::writeVersions(ProtocolWriter& writer) const {
  if (writer.proto() <
      Compatibility::ProtocolVersion::INCLUDE_VERSIONS_IN_GOSSIP) {
    return;
  }
  ld_check(rsm_types_.size() <= UINT8_MAX);

  uint8_t num_rsms_ = rsm_types_.size();
  writer.write(num_rsms_);
  for (const auto& rsm_type : rsm_types_) {
    writer.write(rsm_type);
  }

  for (const auto& node : versions_) {
    writer.write(node.node_id_);
    ld_check(node.rsm_versions_.size() == num_rsms_);
    for (const auto& rsm_ver : node.rsm_versions_) {
      writer.write(rsm_ver);
    }

    // Write NCM information
    for (size_t i = 0; i < node.ncm_versions_.size(); ++i) {
      writer.write(node.ncm_versions_[i]);
    }
  }
}

void GOSSIP_Message::readVersions(ProtocolReader& reader, uint16_t num_nodes) {
  if (reader.proto() <
      Compatibility::ProtocolVersion::INCLUDE_VERSIONS_IN_GOSSIP) {
    return;
  }

  uint8_t num_rsms_;
  reader.read(&num_rsms_);
  if (num_rsms_ == 0) {
    return;
  }

  rsm_types_.resize(num_rsms_);
  for (uint8_t i = 0; i < num_rsms_; ++i) {
    reader.read(&rsm_types_[i]);
  }

  versions_.resize(num_nodes);
  for (size_t i = 0; i < num_nodes; ++i) {
    reader.read(&versions_[i].node_id_);

    // Read RSM information
    versions_[i].rsm_versions_.resize(num_rsms_);
    for (uint8_t j = 0; j < num_rsms_; ++j) {
      reader.read(&versions_[i].rsm_versions_[j]);
    }

    // Read NCM information
    for (size_t k = 0; k < versions_[i].ncm_versions_.size(); ++k) {
      reader.read(&versions_[i].ncm_versions_[k]);
    }
  }
}

}} // namespace facebook::logdevice
