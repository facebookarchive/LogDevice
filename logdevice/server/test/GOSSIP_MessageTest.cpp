/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/GOSSIP_Message.h"

#include <chrono>
#include <string>

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/sequencer_boycotting/BoycottAdaptiveDuration.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;

using unique_evbuffer =
    std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

using node_list_t = GOSSIP_Message::node_list_t;
using gossip_list_t = GOSSIP_Message::gossip_list_t;
using gossip_ts_t = GOSSIP_Message::gossip_ts_t;
using failover_list_t = GOSSIP_Message::failover_list_t;
using boycott_list_t = GOSSIP_Message::boycott_list_t;
using boycott_durations_list_t = GOSSIP_Message::boycott_durations_list_t;
using GOSSIP_flags_t = GOSSIP_Message::GOSSIP_flags_t;
using versions_list_t = GOSSIP_Message::versions_node_list_t;

namespace {
struct Params {
  explicit Params(uint16_t proto) : proto(proto) {}
  uint16_t proto;
  bool with_failover = false;
  bool with_boycott = false;
  bool with_starting = false;
  bool with_health_status = false;
  bool with_versions = false;
  std::string expected;
};
void checkGOSSIP_Node(const GOSSIP_Node& left, const GOSSIP_Node& right) {
  EXPECT_EQ(left.node_id_, right.node_id_);
  EXPECT_EQ(left.gossip_, right.gossip_);
  EXPECT_EQ(left.gossip_ts_, right.gossip_ts_);
  EXPECT_EQ(left.failover_, right.failover_);
  EXPECT_EQ(left.is_node_starting_, right.is_node_starting_);
  EXPECT_EQ(left.node_status_, right.node_status_);
};

void checkNodeList(const node_list_t& left, const node_list_t& right) {
  EXPECT_EQ(left.size(), right.size());
  if (left.size() != right.size()) {
    return;
  }
  auto lit = left.begin();
  auto rit = right.begin();
  while (lit != left.end() && rit != right.end()) {
    checkGOSSIP_Node(*lit, *rit);
    ++lit;
    ++rit;
  }
}

void checkVersionsNode(const Versions_Node& left, const Versions_Node& right) {
  EXPECT_EQ(left.node_id_, right.node_id_);
  EXPECT_EQ(left.rsm_versions_.size(), right.rsm_versions_.size());
  auto lit = left.rsm_versions_.begin();
  auto rit = right.rsm_versions_.begin();
  while (lit != left.rsm_versions_.end() && rit != right.rsm_versions_.end()) {
    EXPECT_EQ(*lit, *rit);
    ++lit;
    ++rit;
  }

  EXPECT_EQ(left.ncm_versions_.size(), right.ncm_versions_.size());
  EXPECT_EQ(right.ncm_versions_.size(), 3);
  auto ncml = left.ncm_versions_.begin();
  auto ncmr = right.ncm_versions_.begin();
  while (ncml != left.ncm_versions_.end() &&
         ncmr != right.ncm_versions_.end()) {
    EXPECT_EQ(*ncml, *ncmr);
    ++ncml;
    ++ncmr;
  }
};

void checkVersions(uint16_t proto,
                   const versions_list_t& left,
                   const versions_list_t& right,
                   GOSSIP_Message::rsmtype_list_t left_rsm_types,
                   GOSSIP_Message::rsmtype_list_t right_rsm_types,
                   GOSSIP_flags_t flags) {
  if (proto == 0 /* ignore protocol version */ ||
      proto >= Compatibility::ProtocolVersion::INCLUDE_VERSIONS_IN_GOSSIP) {
    EXPECT_EQ(left_rsm_types.size(), right_rsm_types.size());
    uint8_t num_rsms = left_rsm_types.size();
    if (!(flags & GOSSIP_Message::HAS_VERSIONS)) {
      EXPECT_EQ(num_rsms, 0);
    }
    auto left_rsm_type_it = left_rsm_types.begin();
    auto right_rsm_type_it = right_rsm_types.begin();
    while (left_rsm_type_it != left_rsm_types.end() &&
           right_rsm_type_it != right_rsm_types.end()) {
      EXPECT_EQ(*left_rsm_type_it, *right_rsm_type_it);
      ++left_rsm_type_it;
      ++right_rsm_type_it;
    }

    EXPECT_EQ(left.size(), right.size());
    auto lit = left.begin();
    auto rit = right.begin();
    while (lit != left.end() && rit != right.end()) {
      checkVersionsNode(*lit, *rit);
      ++lit;
      ++rit;
    }
  } else {
    EXPECT_EQ(right.size(), 0);
    EXPECT_EQ(right_rsm_types.size(), 0);
  }
}

void serializeAndDeserializeTest(Params params) {
  unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
    LD_EV(evbuffer_free)(ptr);
  });
  NodeID this_node{1};
  node_list_t node_list{{0, 1, 5ms, 0ms, 0}, {1, 2, 10ms, 0ms, 0}};
  std::chrono::milliseconds instance_id{1};
  std::chrono::milliseconds sent_time{1};

  GOSSIP_flags_t flags = 0;
  failover_list_t failover_list;
  if (params.with_failover) {
    flags = GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
    node_list[0].failover_ = 1ms;
    node_list[1].failover_ = 2ms;
  }

  if (params.with_starting) {
    node_list[1].is_node_starting_ = true;
    flags |= GOSSIP_Message::HAS_STARTING_LIST_FLAG;
  }

  boycott_list_t boycott_list;
  if (params.with_boycott) {
    boycott_list.emplace_back(Boycott{1, 1s, 30s});
    boycott_list.emplace_back(Boycott{2, 2s, 30s, true});
  }

  boycott_durations_list_t boycott_durations;
  if (params.with_boycott) {
    boycott_durations.emplace_back(
        1, 30min, 2h, 1min, 30s, 2, 60min, BoycottAdaptiveDuration::TS(1000h));
  }

  if (params.with_health_status) {
    node_list[0].node_status_ = NodeHealthStatus::HEALTHY;
    node_list[1].node_status_ = NodeHealthStatus::UNHEALTHY;
  }

  GOSSIP_Message::rsmtype_list_t rsm_types;
  GOSSIP_Message::versions_node_list_t versions_list;
  if (params.with_versions) {
    node_list[1].is_node_starting_ = true;
    flags |= GOSSIP_Message::HAS_VERSIONS;
    rsm_types.push_back(configuration::InternalLogs::MAINTENANCE_LOG_DELTAS);
    rsm_types.push_back(configuration::InternalLogs::CONFIG_LOG_DELTAS);
    rsm_types.push_back(configuration::InternalLogs::EVENT_LOG_DELTAS);
    versions_list.push_back({0,
                             {lsn_t(1), lsn_t(2), lsn_t(3)},
                             {membership::MembershipVersion::Type(101),
                              membership::MembershipVersion::Type(102),
                              membership::MembershipVersion::Type(103)}});
    versions_list.push_back({1,
                             {lsn_t(4), lsn_t(5), lsn_t(6)},
                             {membership::MembershipVersion::Type(104),
                              membership::MembershipVersion::Type(105),
                              membership::MembershipVersion::Type(106)}});
  }

  GOSSIP_Message msg(this_node,
                     node_list,
                     instance_id,
                     sent_time,
                     boycott_list,
                     boycott_durations,
                     flags,
                     0,
                     rsm_types,
                     versions_list);

  EXPECT_EQ(this_node, msg.gossip_node_);
  EXPECT_EQ(instance_id, msg.instance_id_);
  EXPECT_EQ(sent_time, msg.sent_time_);
  EXPECT_EQ(boycott_list, msg.boycott_list_);
  EXPECT_EQ(boycott_durations, msg.boycott_durations_list_);
  EXPECT_EQ(flags, msg.flags_);
  checkNodeList(node_list, msg.node_list_);
  checkVersions(0 /* ignore protocol checking */,
                versions_list,
                msg.versions_,
                rsm_types,
                msg.rsm_types_,
                flags);

  ProtocolWriter writer(msg.type_, evbuf.get(), params.proto);
  msg.serialize(writer);
  auto write_count = writer.result();

  ASSERT_GT(write_count, 0);
  size_t size = LD_EV(evbuffer_get_length)(evbuf.get());
  unsigned char* serialized = LD_EV(evbuffer_pullup)(evbuf.get(), -1);
  std::string serialized_hex = hexdump_buf(serialized, size);
  EXPECT_EQ(params.expected, serialized_hex);

  ProtocolReader reader(msg.type_, evbuf.get(), write_count, params.proto);
  std::unique_ptr<Message> deserialized_msg_base =
      GOSSIP_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  auto deserialized_msg =
      static_cast<GOSSIP_Message*>(deserialized_msg_base.get());
  EXPECT_EQ(this_node, deserialized_msg->gossip_node_);
  EXPECT_EQ(instance_id, deserialized_msg->instance_id_);
  EXPECT_EQ(sent_time, deserialized_msg->sent_time_);
  EXPECT_EQ(boycott_list, deserialized_msg->boycott_list_);
  EXPECT_EQ(boycott_durations, deserialized_msg->boycott_durations_list_);
  EXPECT_EQ(flags, deserialized_msg->flags_);
  checkNodeList(node_list, deserialized_msg->node_list_);
  checkVersions(params.proto,
                versions_list,
                deserialized_msg->versions_,
                rsm_types,
                deserialized_msg->rsm_types_,
                flags);
}
} // namespace

TEST(GOSSIP_MessageTest, SerializeAndDeserialize) {
  // last protocol before boycott info
  Params params{Compatibility::MIN_PROTOCOL_SUPPORTED};
  params.expected = "0200000001000001000000000000000100000000000000000000000000"
                    "0000000000000000000000010000000000000005000000000000000000"
                    "0000000000000000000000000000010000000000000002000000000000"
                    "000A0000000000000000000000000000000000000000000000";
  serializeAndDeserializeTest(params);

  params.with_failover = true;
  params.expected = "0200000001000101000000000000000100000000000000000000000000"
                    "0000000000000000000000010000000000000005000000000000000100"
                    "0000000000000000000000000000010000000000000002000000000000"
                    "000A0000000000000002000000000000000000000000000000";
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithBoycott) {
  Params params{Compatibility::MIN_PROTOCOL_SUPPORTED};
  params.with_boycott = true;
  params.expected =
      "020000000100000100000000000000010000000000000002010000CA9A3B000000003075"
      "000000000000000200009435770000000030750000000000000101000000000000000100"
      "40771B000000000000DD6D000000000060EA000000000000307500000000000002000000"
      "80EE360000000000000031512ECA0C000000000000000000000000000000000001000000"
      "000000000500000000000000000000000000000000000000000000000100000000000000"
      "02000000000000000A0000000000000000000000000000000000000000000000";
  serializeAndDeserializeTest(params);

  params.with_failover = true;
  params.expected =
      "020000000100010100000000000000010000000000000002010000CA9A3B000000003075"
      "000000000000000200009435770000000030750000000000000101000000000000000100"
      "40771B000000000000DD6D000000000060EA000000000000307500000000000002000000"
      "80EE360000000000000031512ECA0C000000000000000000000000000000000001000000"
      "000000000500000000000000010000000000000000000000000000000100000000000000"
      "02000000000000000A0000000000000002000000000000000000000000000000";
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithStarting) {
  Params params{Compatibility::ProtocolVersion::PROTOCOL_VERSION_LOWER_BOUND};
  params.with_starting = true;
  params.expected =
      "020000000100100100000000000000010000000000000000000000000000000000000000"
      "000000000100000000000000050000000000000000000000000000000000000000000000"
      "010000000000000002000000000000000A00000000000000000000000000000001000000"
      "00000000";
  serializeAndDeserializeTest(params);

  params.with_failover = true;
  params.expected =
      "020000000100110100000000000000010000000000000000000000000000000000000000"
      "000000000100000000000000050000000000000001000000000000000000000000000000"
      "010000000000000002000000000000000A00000000000000020000000000000001000000"
      "00000000";
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithNodeHealthStatus) {
  Params params{
      Compatibility::ProtocolVersion::HEALTH_MONITOR_SUPPORT_IN_GOSSIP};
  params.with_health_status = true;
  params.expected = "0200000001000001000000000000000100000000000000000000000000"
                    "0000000000000000000000010000000000000005000000000000000000"
                    "0000000000000000000001000000010000000000000002000000000000"
                    "000A0000000000000000000000000000000000000003000000";
  serializeAndDeserializeTest(params);

  params.with_failover = true;
  params.expected = "0200000001000101000000000000000100000000000000000000000000"
                    "0000000000000000000000010000000000000005000000000000000100"
                    "0000000000000000000001000000010000000000000002000000000000"
                    "000A0000000000000002000000000000000000000003000000";
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithVersions) {
  for (uint16_t p = Compatibility::MIN_PROTOCOL_SUPPORTED;
       p < Compatibility::INCLUDE_VERSIONS_IN_GOSSIP;
       p++) {
    Params params{p};
    params.with_versions = true;
    params.expected =
        "0200000001002001000000000000000100000000000000000000000000000000000000"
        "0000000000010000000000000005000000000000000000000000000000000000000000"
        "0000010000000000000002000000000000000A00000000000000000000000000000001"
        "00000000000000";
    serializeAndDeserializeTest(params);

    params.with_versions = false;
    params.expected =
        "0200000001000001000000000000000100000000000000000000000000000000000000"
        "0000000000010000000000000005000000000000000000000000000000000000000000"
        "0000010000000000000002000000000000000A00000000000000000000000000000000"
        "00000000000000";
    serializeAndDeserializeTest(params);
  }

  for (uint16_t p = Compatibility::INCLUDE_VERSIONS_IN_GOSSIP;
       p <= Compatibility::MAX_PROTOCOL_SUPPORTED;
       p++) {
    Params params{p};
    params.with_versions = true;
    params.expected =
        "0200000001002001000000000000000100000000000000000000000000000000000000"
        "0000000000010000000000000005000000000000000000000000000000000000000000"
        "0000010000000000000002000000000000000A00000000000000000000000000000001"
        "0000000000000003FBFFFFFFFFFFFF3FFDFFFFFFFFFFFF3FFFFFFFFFFFFFFF3F000000"
        "0000000000010000000000000002000000000000000300000000000000650000000000"
        "0000660000000000000067000000000000000100000000000000040000000000000005"
        "000000000000000600000000000000680000000000000069000000000000006A000000"
        "00000000";
    serializeAndDeserializeTest(params);

    params.with_versions = false;
    params.expected =
        "0200000001000001000000000000000100000000000000000000000000000000000000"
        "0000000000010000000000000005000000000000000000000000000000000000000000"
        "0000010000000000000002000000000000000A00000000000000000000000000000000"
        "00000000000000";
    serializeAndDeserializeTest(params);
  }
}
