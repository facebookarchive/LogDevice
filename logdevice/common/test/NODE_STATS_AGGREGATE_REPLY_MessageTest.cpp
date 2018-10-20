/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_REPLY_Message.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace facebook::logdevice;

namespace {
void testSerializeDeserialize(uint16_t proto) {
  using unique_evbuffer =
      std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

  unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
    LD_EV(evbuffer_free)(ptr);
  });

  NODE_STATS_AGGREGATE_REPLY_Header header;
  header.msg_id = 1;
  header.node_count = 2;
  header.bucket_count = 3;
  header.separate_client_count = 1;

  // 2 nodes with 3 buckets
  boost::multi_array<BucketedNodeStats::SummedNodeStats, 2> sums{
      boost::extents[2][3]};
  sums[0][0] = {1, 1, 1};
  sums[0][1] = {2, 2, 2};
  sums[0][2] = {3, 3, 3};
  sums[1][0] = {4, 4, 4};
  sums[1][1] = {5, 5, 5};
  sums[1][2] = {6, 6, 6};

  boost::multi_array<BucketedNodeStats::ClientNodeStats, 3> clients{};
  clients.resize(boost::extents[2][3][1]);
  clients[0][0][0] = {7, 7};
  clients[0][1][0] = {8, 8};
  clients[0][2][0] = {9, 9};
  clients[1][0][0] = {10, 10};
  clients[1][1][0] = {11, 11};
  clients[1][2][0] = {12, 12};

  BucketedNodeStats stats;
  stats.node_ids = {NodeID{1}, NodeID{2}};
  stats.summed_counts->resize(boost::extents[2][3]);
  stats.client_counts->resize(boost::extents[2][3][1]);
  *stats.summed_counts = sums;
  *stats.client_counts = clients;

  NODE_STATS_AGGREGATE_REPLY_Message msg{header, std::move(stats)};

  EXPECT_EQ(header.msg_id, msg.header_.msg_id);
  EXPECT_EQ(header.node_count, msg.header_.node_count);
  EXPECT_EQ(header.bucket_count, msg.header_.bucket_count);
  EXPECT_EQ(header.separate_client_count, msg.header_.separate_client_count);
  EXPECT_THAT(msg.stats_.node_ids, testing::ElementsAre(NodeID{1}, NodeID{2}));
  EXPECT_EQ(sums, *msg.stats_.summed_counts);
  EXPECT_EQ(clients, *msg.stats_.client_counts);

  ProtocolWriter writer(msg.type_, evbuf.get(), proto);
  msg.serialize(writer);
  auto write_count = writer.result();

  ASSERT_GT(write_count, 0);

  std::unique_ptr<Message> deserialized_msg_base;
  ProtocolReader reader(msg.type_, evbuf.get(), write_count, proto);
  deserialized_msg_base =
      NODE_STATS_AGGREGATE_REPLY_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  auto deserialized_msg = static_cast<NODE_STATS_AGGREGATE_REPLY_Message*>(
      deserialized_msg_base.get());

  EXPECT_EQ(header.msg_id, deserialized_msg->header_.msg_id);
  EXPECT_EQ(header.node_count, deserialized_msg->header_.node_count);
  EXPECT_EQ(header.bucket_count, deserialized_msg->header_.bucket_count);
  EXPECT_THAT(deserialized_msg->stats_.node_ids,
              testing::ElementsAre(NodeID{1}, NodeID{2}));
  EXPECT_EQ(sums, *deserialized_msg->stats_.summed_counts);
  EXPECT_EQ(header.separate_client_count,
            deserialized_msg->header_.separate_client_count);
  EXPECT_EQ(clients, *deserialized_msg->stats_.client_counts);
}
} // namespace

TEST(NODE_STATS_AGGREGATE_REPLY_MessageTest, SerializeAndDeserialize) {
  testSerializeDeserialize(Compatibility::MIN_PROTOCOL_SUPPORTED);
}
