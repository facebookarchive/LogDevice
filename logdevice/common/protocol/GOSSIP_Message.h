/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include "logdevice/common/NodeHealthStatus.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/membership/types.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/sequencer_boycotting/Boycott.h"
#include "logdevice/common/sequencer_boycotting/BoycottAdaptiveDuration.h"

namespace facebook { namespace logdevice {

class FailureDetector;

struct GOSSIP_Node_Legacy {
  // Note: this is different from NodeID
  size_t node_id_;

  // How many gossip periods have passed before
  // hearing from the node (either directly or through a gossip message).
  uint32_t gossip_;

  // Instance id(timestamps)
  std::chrono::milliseconds gossip_ts_;

  // Either of the following 2 values:
  // a) 0 : the node is up b) the node's instance id(startup time in this
  // case) : the node requested failover
  std::chrono::milliseconds failover_;

  // The node is in starting state?
  bool is_node_starting_;

  bool operator<(const GOSSIP_Node_Legacy& a) const {
    return node_id_ < a.node_id_;
  }
};

struct GOSSIP_Node : public GOSSIP_Node_Legacy {
  // Health status of node
  NodeHealthStatus node_status_{NodeHealthStatus::UNDEFINED};

  bool operator<(const GOSSIP_Node& a) const {
    return node_id_ < a.node_id_;
  }
};

struct Versions_Node {
  size_t node_id_;
  // RSM Versions contained here follow the order of rsm types in
  // GOSSIP_Message::rsm_types_. This helps in reducing the Message size by
  // preventing the repetition of same information(i.e. rsm types) in each and
  // every Versions_Node
  std::vector<lsn_t> rsm_versions_;
  std::array<membership::MembershipVersion::Type, 3>
      ncm_versions_; // sorted by the order mentioned in
                     // FailureDetector::Node::ncm_versions_
  bool operator<(const Versions_Node& a) const {
    return node_id_ < a.node_id_;
  }
};

class GOSSIP_Message : public Message {
 public:
  using legacy_node_list_t = std::vector<GOSSIP_Node_Legacy>;
  using node_list_t = std::vector<GOSSIP_Node>;
  using gossip_list_t = std::vector<uint32_t>;
  using gossip_ts_t = std::vector<std::chrono::milliseconds>;
  using failover_list_t = std::vector<std::chrono::milliseconds>;
  using boycott_list_t = std::vector<Boycott>;
  using boycott_durations_list_t = std::vector<BoycottAdaptiveDuration>;
  using starting_list_t = std::vector<NodeID>;
  using GOSSIP_flags_t = uint8_t;
  using rsmtype_list_t = std::vector<logid_t>; // delta log of RSM
  using versions_node_list_t = std::vector<Versions_Node>;

  GOSSIP_Message()
      : Message(MessageType::GOSSIP, TrafficClass::FAILURE_DETECTOR),
        flags_(0),
        num_boycotts_(0) {}
  GOSSIP_Message(NodeID this_node,
                 node_list_t node_list,
                 std::chrono::milliseconds instance_id,
                 std::chrono::milliseconds sent_time,
                 boycott_list_t boycott_list,
                 boycott_durations_list_t boycott_durations,
                 GOSSIP_Message::GOSSIP_flags_t flags,
                 uint64_t msg_id,
                 GOSSIP_Message::rsmtype_list_t rsm_types,
                 GOSSIP_Message::versions_node_list_t versions);

  void serialize(ProtocolWriter&) const override;
  static Message::deserializer_t deserialize;

  Disposition onReceived(const Address& from) override;
  void onSent(Status st, const Address& to) const override;

  node_list_t node_list_;
  NodeID gossip_node_;
  GOSSIP_flags_t flags_;

  std::chrono::milliseconds instance_id_;
  // Used to measure delays on receiving side
  std::chrono::milliseconds sent_time_;

  // the amount of boycotts in the list
  uint8_t num_boycotts_;
  boycott_list_t boycott_list_;

  // The adaptive boycott durations
  boycott_durations_list_t boycott_durations_list_;

  // sequence number to match message when running onSent callback
  uint64_t msg_id_;

  // List of RSM delta log-ids
  rsmtype_list_t rsm_types_;
  // RSM and NCM versions
  versions_node_list_t versions_;

  // When set in flags_, indicates that the message includes the failover list.
  static const GOSSIP_flags_t HAS_FAILOVER_LIST_FLAG = 1 << 0;

  // Meant to notify other nodes that this node just came up.
  // All other information in gossip message should be ignored.
  static const GOSSIP_flags_t NODE_BRINGUP_FLAG = 1 << 1;

  // This flag is sent to close the following race:
  // Let's say we have N0, N1, N2 as our cluster nodes.
  // N0 is up since time T0, N1 comes up at T1, and N2 comes up at T2.
  // It is possible that when N2 came up, N1 was still in SUSPECT state
  // on both N0 and N1.
  // N2 sends Get-cluster-state to N0; N0 replies with N0=ALIVE, N1=DEAD.
  // N2 will move N1 to SUSPECT state on a subsequent gossip, which means
  // that N1's SUSPECT state will finish on N0 and N1 first as compared
  // to on N2. To fix this race, N1 will broadcast a suspect-state-finished
  // message, which is supposed to immediately move N1 into ALIVE on the
  // receiver.
  static const GOSSIP_flags_t SUSPECT_STATE_FINISHED = 1 << 2;

  // Node should not be considered as starting anymore (i.e. logsconfig should
  // be fully loaded).
  static const GOSSIP_flags_t STARTING_STATE_FINISHED = 1 << 3;

  // Flag to indicate that we have a starting list
  static const GOSSIP_flags_t HAS_STARTING_LIST_FLAG = 1 << 4;

  // Only one of the two flags can be set at any time
  // The message contains in-memory RSM versions on cluster nodes
  static const GOSSIP_flags_t HAS_VERSIONS = 1 << 5;
  // The message contains durable RSM versions(in local store) on cluster nodes
  static const GOSSIP_flags_t HAS_DURABLE_SNAPSHOT_VERSIONS = 1 << 6;

 private:
  // flattens the matrices and then writes them
  void writeBoycottList(ProtocolWriter& writer) const;
  // reads the flattened matrices and un-flattens them
  void readBoycottList(ProtocolReader& reader);
  // Gets the default boycotting duration from the settings
  std::chrono::milliseconds getDefaultBoycottDuration() const;

  // flattens the boycott durations and write them
  void writeBoycottDurations(ProtocolWriter& writer) const;
  // reads the flattened boycott durations and unflatten them
  void readBoycottDurations(ProtocolReader& reader);

  void writeStartingList(ProtocolWriter& writer) const;
  void readStartingList(ProtocolReader& reader);

  // Read and Write RSM and NCM versions
  void readVersions(ProtocolReader& reader, uint16_t num_nodes);
  void writeVersions(ProtocolWriter& writer) const;
};
}} // namespace facebook::logdevice
