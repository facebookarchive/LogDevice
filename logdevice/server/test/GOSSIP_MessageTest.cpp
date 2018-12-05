/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/GOSSIP_Message.h"

#include <chrono>

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/sequencer_boycotting/BoycottAdaptiveDuration.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;

using unique_evbuffer =
    std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

using gossip_list_t = GOSSIP_Message::gossip_list_t;
using gossip_ts_t = GOSSIP_Message::gossip_ts_t;
using failover_list_t = GOSSIP_Message::failover_list_t;
using suspect_matrix_t = GOSSIP_Message::suspect_matrix_t;
using boycott_list_t = GOSSIP_Message::boycott_list_t;
using boycott_durations_list_t = GOSSIP_Message::boycott_durations_list_t;
using starting_list_t = GOSSIP_Message::starting_list_t;
using GOSSIP_flags_t = GOSSIP_Message::GOSSIP_flags_t;

namespace {
struct Params {
  explicit Params(uint16_t proto) : proto(proto) {}

  uint16_t proto;
  bool with_suspect = false;
  bool with_failover = false;
  bool with_boycott = false;
  bool with_starting = false;
};

void serializeAndDeserializeTest(Params params) {
  unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
    LD_EV(evbuffer_free)(ptr);
  });
  NodeID this_node{1};
  gossip_list_t gossip_list{1, 2};
  gossip_ts_t gossip_ts{5ms, 10ms};
  std::chrono::milliseconds instance_id{1};
  std::chrono::milliseconds sent_time{1};

  GOSSIP_flags_t flags = 0;
  failover_list_t failover_list;
  if (params.with_failover) {
    flags = GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
    failover_list = {1ms, 2ms};
  }

  suspect_matrix_t suspect_matrix;
  if (params.with_suspect) {
    suspect_matrix = {{1, 0}, {0, 1}};
  }

  boycott_list_t boycott_list;
  if (params.with_boycott) {
    boycott_list.emplace_back(Boycott{1, 1s, 30s});
    boycott_list.emplace_back(Boycott{2, 2s, 30s, true});
  }

  boycott_durations_list_t boycott_durations;
  if (params.with_boycott) {
    boycott_durations.emplace_back(
        1, 30min, 2h, 1min, 30s, 2, 60min, std::chrono::system_clock::now());
  }

  starting_list_t starting_list;
  if (params.with_starting) {
    starting_list.emplace_back(0);
    starting_list.emplace_back(1);
  }

  GOSSIP_Message msg(this_node,
                     gossip_list,
                     instance_id,
                     sent_time,
                     gossip_ts,
                     failover_list,
                     suspect_matrix,
                     boycott_list,
                     boycott_durations,
                     starting_list,
                     flags);

  EXPECT_EQ(this_node, msg.gossip_node_);
  EXPECT_EQ(gossip_list, msg.gossip_list_);
  EXPECT_EQ(gossip_ts, msg.gossip_ts_);
  EXPECT_EQ(instance_id, msg.instance_id_);
  EXPECT_EQ(sent_time, msg.sent_time_);
  EXPECT_EQ(failover_list, msg.failover_list_);
  EXPECT_EQ(suspect_matrix, msg.suspect_matrix_);
  EXPECT_EQ(boycott_list, msg.boycott_list_);
  EXPECT_EQ(boycott_durations, msg.boycott_durations_list_);
  EXPECT_EQ(starting_list, msg.starting_list_);
  EXPECT_EQ(flags, msg.flags_);

  ProtocolWriter writer(msg.type_, evbuf.get(), params.proto);
  msg.serialize(writer);
  auto write_count = writer.result();

  ASSERT_GT(write_count, 0);

  ProtocolReader reader(msg.type_, evbuf.get(), write_count, params.proto);
  std::unique_ptr<Message> deserialized_msg_base =
      GOSSIP_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  auto deserialized_msg =
      static_cast<GOSSIP_Message*>(deserialized_msg_base.get());

  EXPECT_EQ(this_node, deserialized_msg->gossip_node_);
  EXPECT_EQ(gossip_list, deserialized_msg->gossip_list_);
  EXPECT_EQ(gossip_ts, deserialized_msg->gossip_ts_);
  EXPECT_EQ(instance_id, deserialized_msg->instance_id_);
  EXPECT_EQ(sent_time, deserialized_msg->sent_time_);
  EXPECT_EQ(failover_list, deserialized_msg->failover_list_);
  EXPECT_EQ(suspect_matrix, deserialized_msg->suspect_matrix_);
  EXPECT_EQ(boycott_list, deserialized_msg->boycott_list_);
  EXPECT_EQ(boycott_durations, deserialized_msg->boycott_durations_list_);
  EXPECT_EQ(flags, deserialized_msg->flags_);
}
} // namespace

TEST(GOSSIP_MessageTest, SerializeAndDeserialize) {
  // last protocol before boycott info
  Params params{Compatibility::MIN_PROTOCOL_SUPPORTED};
  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = false;
  params.with_failover = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  params.with_failover = true;
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithBoycott) {
  Params params{Compatibility::ADAPTIVE_BOYCOTT_DURATION};
  params.with_boycott = true;

  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = false;
  params.with_failover = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  params.with_failover = true;
  serializeAndDeserializeTest(params);
}

TEST(GOSSIP_MessageTest, SerializeAndDeserializeWithStarting) {
  Params params{Compatibility::ProtocolVersion::PROTOCOL_VERSION_LOWER_BOUND};
  params.with_starting = true;

  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = false;
  params.with_failover = true;
  serializeAndDeserializeTest(params);

  params.with_suspect = true;
  params.with_failover = true;
  serializeAndDeserializeTest(params);
}
