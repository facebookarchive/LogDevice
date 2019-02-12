/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/NODE_STATS_Message.h"

#include <folly/stats/BucketedTimeSeries.h>
#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/Address.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/stats/Stats.h"

using namespace facebook::logdevice;

class MockNODE_STATS_Message : public NODE_STATS_Message {
 public:
  MockNODE_STATS_Message(const NODE_STATS_Header& header,
                         std::vector<NodeID> ids,
                         append_list_t append_successes,
                         append_list_t append_fails)
      : NODE_STATS_Message(header, ids, append_successes, append_fails) {}

  MockNODE_STATS_Message() : NODE_STATS_Message() {}

  void sendReplyMessage(const Address& to) override {
    replied_to = to.asClientID();
  }

  StatsHolder* getStats() override {
    return &(MockNODE_STATS_Message::stats_holder);
  }

  ClientID replied_to = ClientID::INVALID;

  // 0 retention time = forever
  StatsHolder stats_holder{StatsParams{}.setNodeStatsRetentionTimeOnClients(
      std::chrono::seconds(0))};
};

namespace {
Address address(NodeID(1, 1));
}

TEST(NODE_STATS_MessageTest, SerializationAndDeserialization) {
  using unique_evbuffer =
      std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

  unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
    LD_EV(evbuffer_free)(ptr);
  });

  // currently no difference in versions, use w/e
  uint16_t proto = Compatibility::MIN_PROTOCOL_SUPPORTED;

  NODE_STATS_Header header{123 /*msg_id*/, 2 /*node count*/};
  std::vector<NodeID> ids{NodeID(3), NodeID(5)};
  std::vector<uint32_t> append_successes{1, 5000};
  std::vector<uint32_t> append_fails{123123, 4};
  NODE_STATS_Message msg(header, ids, append_successes, append_fails);

  EXPECT_EQ(header.msg_id, msg.header_.msg_id);
  EXPECT_EQ(header.num_nodes, msg.header_.num_nodes);
  EXPECT_EQ(ids, msg.ids_);
  EXPECT_EQ(append_successes, msg.append_successes_);
  EXPECT_EQ(append_fails, msg.append_fails_);

  ProtocolWriter writer(msg.type_, evbuf.get(), proto);

  msg.serialize(writer);
  auto write_count = writer.result();

  // make sure something was written
  ASSERT_GT(write_count, 0);

  std::unique_ptr<Message> deserialized_msg_base;
  ProtocolReader reader(msg.type_, evbuf.get(), write_count, proto);
  deserialized_msg_base = NODE_STATS_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  NODE_STATS_Message* deserialized_msg =
      dynamic_cast<NODE_STATS_Message*>(deserialized_msg_base.get());

  EXPECT_EQ(msg.header_.msg_id, deserialized_msg->header_.msg_id);
  EXPECT_EQ(msg.header_.num_nodes, deserialized_msg->header_.num_nodes);
  EXPECT_EQ(msg.ids_, deserialized_msg->ids_);
  EXPECT_EQ(msg.append_successes_, deserialized_msg->append_successes_);
  EXPECT_EQ(msg.append_fails_, deserialized_msg->append_fails_);
}

TEST(NODE_STATS_MessageTest, OnReceived) {
  MockNODE_STATS_Message msg;
  ClientID from{123};
  Address from_address(from);

  EXPECT_EQ(Message::Disposition::NORMAL, msg.onReceived(from_address));
  EXPECT_EQ(msg.replied_to, from);
}

TEST(NODE_STATS_MessageTest, OnReceivedFromNonClient) {
  MockNODE_STATS_Message msg;
  NodeID from{123, 1};
  Address from_address(from);

  EXPECT_EQ(Message::Disposition::ERROR, msg.onReceived(from_address));
  // didn't actually reply, still uses the initial value
  EXPECT_EQ(msg.replied_to, ClientID::INVALID);
}

TEST(NODE_STATS_MessageTest, OnReceivedStoresStats) {
  const unsigned int success_count_node_1 = 10;
  const unsigned int success_count_node_2 = 20;
  const unsigned int fail_count_node_1 = 3;
  const unsigned int fail_count_node_2 = 4;

  NODE_STATS_Header header;
  header.num_nodes = 2;

  std::vector<NodeID> ids{NodeID(1), NodeID(2)};
  NODE_STATS_Message::append_list_t append_successes{
      success_count_node_1, success_count_node_2};
  NODE_STATS_Message::append_list_t append_fails{
      fail_count_node_1, fail_count_node_2};

  MockNODE_STATS_Message msg{header, ids, append_successes, append_fails};
  ClientID from_client{123};
  Address from_address(from_client);

  msg.onReceived(from_address);

  /**
   * Iterate over all clients and nodes, and sum appends over all time for each
   * node. The reason for not summing with the member functions of
   * PerClientNodeTimeSeriesStats is that it uses an interval, which is a bit
   * flaky (even when not using buckets for some reason)
   *
   * In this test we don't care how well folly::BucketedTimeSeries works, but
   * simply that the values received are stored
   */
  auto sum_per_node = [&](auto getter_fn) {
    std::unordered_map<NodeID, uint32_t, NodeID::Hash> sum_map;
    auto buckets = msg.stats_holder.get().per_client_node_stats.rlock()->sum();
    for (const auto& bucket : buckets) {
      sum_map[bucket.node_id] += getter_fn(bucket.value);
    }

    return sum_map;
  };

  auto successes =
      sum_per_node([](const auto& value) { return value.successes; });
  auto fails = sum_per_node([](const auto& value) { return value.failures; });

  EXPECT_EQ(success_count_node_1, successes[ids[0]]);
  EXPECT_EQ(success_count_node_2, successes[ids[1]]);

  EXPECT_EQ(fail_count_node_1, fails[ids[0]]);
  EXPECT_EQ(fail_count_node_2, fails[ids[1]]);
}
