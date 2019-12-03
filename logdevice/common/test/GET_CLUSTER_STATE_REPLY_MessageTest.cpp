// Copyright 20019-present Facebook. All Rights Reserved.
#include "logdevice/common/protocol/GET_CLUSTER_STATE_REPLY_Message.h"

#include <vector>

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace facebook::logdevice;
using unique_evbuffer =
    std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

namespace {
struct Params {
  explicit Params(uint16_t proto) : proto(proto) {}

  uint16_t proto;
  bool with_health_status{false};
  std::string expected;
};
} // namespace

namespace facebook { namespace logdevice {
class GET_CLUSTER_STATE_REPLY_MessageTest {
 public:
  void
  checkPairVector(const std::vector<std::pair<node_index_t, uint16_t>>& left,
                  const std::vector<std::pair<node_index_t, uint16_t>>& right) {
    EXPECT_EQ(left.size(), right.size());
    if (left.size() != right.size()) {
      return;
    }
    auto lit = left.begin();
    auto rit = right.begin();
    while (lit != left.end() && rit != right.end()) {
      EXPECT_EQ(*lit, *rit);
      ++lit;
      ++rit;
    }
  }
  void serializeAndDeserializeTest(Params params) {
    unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
      LD_EV(evbuffer_free)(ptr);
    });

    std::vector<std::pair<node_index_t, uint16_t>> state_list{
        {0, 0}, {1, 0}, {2, 1}, {3, 2}, {4, 3}};
    std::vector<node_index_t> boycott_list{3};
    std::vector<std::pair<node_index_t, uint16_t>> status_list;
    GET_CLUSTER_STATE_REPLY_Message msg;
    msg.nodes_state_ = state_list;
    msg.boycotted_nodes_ = boycott_list;
    if (params.with_health_status) {
      status_list = {{0, 0}, {1, 1}, {2, 1}, {3, 2}, {4, 3}};
      msg.nodes_status_ = status_list;
    }

    checkPairVector(state_list, msg.nodes_state_);
    EXPECT_EQ(boycott_list, msg.boycotted_nodes_);
    checkPairVector(status_list, msg.nodes_status_);

    ProtocolWriter writer(msg.type_, evbuf.get(), params.proto);
    msg.serialize(writer);
    auto write_count = writer.result();

    ASSERT_GT(write_count, 0);
    size_t size = LD_EV(evbuffer_get_length)(evbuf.get());
    unsigned char* serialized = LD_EV(evbuffer_pullup)(evbuf.get(), -1);
    std::string serialized_hex = hexdump_buf(serialized, size);
    ASSERT_EQ(params.expected, serialized_hex);

    ProtocolReader reader(msg.type_, evbuf.get(), write_count, params.proto);
    std::unique_ptr<Message> deserialized_msg_base =
        GET_CLUSTER_STATE_REPLY_Message::deserialize(reader).msg;
    ASSERT_NE(nullptr, deserialized_msg_base);

    auto deserialized_msg = static_cast<GET_CLUSTER_STATE_REPLY_Message*>(
        deserialized_msg_base.get());
    checkPairVector(state_list, deserialized_msg->nodes_state_);
    EXPECT_EQ(boycott_list, deserialized_msg->boycotted_nodes_);
    checkPairVector(status_list, deserialized_msg->nodes_status_);
  }
};

}} // namespace facebook::logdevice

TEST(GET_CLUSTER_STATE_REPLY_MessageTest, SerializeAndDeserialize) {
  Params params{Compatibility::MIN_PROTOCOL_SUPPORTED};
  params.expected =
      "000000000000000000000500000000000000000001020302000000000000000300";
  GET_CLUSTER_STATE_REPLY_MessageTest msgtest;
  msgtest.serializeAndDeserializeTest(params);
}

TEST(GET_CLUSTER_STATE_REPLY_MessageTest, SerializeAndDeserializeWithHashMap) {
  Params params{Compatibility::ProtocolVersion::
                    NODE_STATUS_AND_HASHMAP_SUPPORT_IN_CLUSTER_STATE};
  params.expected = "0000000000000000000014000000000000000000000001000000020001"
                    "000300020004000300020000000000000003000000000000000000";
  GET_CLUSTER_STATE_REPLY_MessageTest msgtest;
  msgtest.serializeAndDeserializeTest(params);
  params.with_health_status = true;
  params.expected = "0000000000000000000014000000000000000000000001000000020001"
                    "0003000200040003000200000000000000030014000000000000000000"
                    "000001000100020001000300020004000300";
  msgtest.serializeAndDeserializeTest(params);
}
