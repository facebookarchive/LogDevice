/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/NODE_STATS_AGGREGATE_Message.h"

#include <gtest/gtest.h>

#include "event2/buffer.h"
#include "logdevice/common/libevent/compat.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace facebook::logdevice;

TEST(NODE_STATS_AGGREGATE_MessageTest, SerializeAndDeserialize) {
  using unique_evbuffer =
      std::unique_ptr<struct evbuffer, std::function<void(struct evbuffer*)>>;

  unique_evbuffer evbuf(LD_EV(evbuffer_new)(), [](auto ptr) {
    LD_EV(evbuffer_free)(ptr);
  });

  auto proto = Compatibility::MIN_PROTOCOL_SUPPORTED;

  NODE_STATS_AGGREGATE_Header header;
  header.msg_id = 1;
  header.bucket_count = 3;
  NODE_STATS_AGGREGATE_Message msg(header);

  EXPECT_EQ(header.msg_id, msg.header_.msg_id);
  EXPECT_EQ(header.bucket_count, msg.header_.bucket_count);

  ProtocolWriter writer(msg.type_, evbuf.get(), proto);
  msg.serialize(writer);
  auto write_count = writer.result();

  ASSERT_GT(write_count, 0);

  std::unique_ptr<Message> deserialized_msg_base;
  ProtocolReader reader(msg.type_, evbuf.get(), write_count, proto);
  deserialized_msg_base = NODE_STATS_AGGREGATE_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  auto deserialized_msg =
      static_cast<NODE_STATS_AGGREGATE_Message*>(deserialized_msg_base.get());

  EXPECT_EQ(header.msg_id, deserialized_msg->header_.msg_id);
  EXPECT_EQ(header.bucket_count, deserialized_msg->header_.bucket_count);
}
