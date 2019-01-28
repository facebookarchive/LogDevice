/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

#include <chrono>

#include <gtest/gtest.h>

#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;

void serializeAndDeserialize(uint64_t proto_version) {
  CONFIG_CHANGED_Header hdr = {1234,
                               config_version_t(4321),
                               NodeID(10, 2),
                               CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
                               CONFIG_CHANGED_Header::Action::UPDATE,
                               {}};
  std::string hash = "testhash";
  hash.copy(hdr.hash, hash.size());

  CONFIG_CHANGED_Message msg{hdr};
  EXPECT_EQ(1234, msg.getHeader().modified_time);
  EXPECT_EQ(config_version_t(4321), msg.getHeader().version);
  EXPECT_EQ(NodeID(10, 2), msg.getHeader().server_origin);
  EXPECT_EQ(CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
            msg.getHeader().config_type);
  EXPECT_EQ(CONFIG_CHANGED_Header::Action::UPDATE, msg.getHeader().action);
  EXPECT_EQ(0, hash.compare(0, hash.size(), std::string(msg.getHeader().hash)));

  std::string dest;
  ProtocolWriter writer(&dest, "", proto_version);
  msg.serialize(writer);
  ASSERT_GT(writer.result(), 0);

  auto destSlice = Slice::fromString(dest);
  ProtocolReader reader(destSlice, "", proto_version);
  std::unique_ptr<Message> deserialized_msg_base =
      CONFIG_CHANGED_Message::deserialize(reader).msg;
  ASSERT_NE(nullptr, deserialized_msg_base);

  auto deserialized_msg =
      static_cast<CONFIG_CHANGED_Message*>(deserialized_msg_base.get());
  const auto& deserialized_header = deserialized_msg->getHeader();

  EXPECT_EQ(1234, deserialized_header.modified_time);
  EXPECT_EQ(config_version_t(4321), deserialized_header.version);
  EXPECT_EQ(NodeID(10, 2), deserialized_header.server_origin);
  EXPECT_EQ(CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
            deserialized_header.config_type);
  EXPECT_EQ(CONFIG_CHANGED_Header::Action::UPDATE, deserialized_header.action);
  EXPECT_EQ(
      0, hash.compare(0, hash.size(), std::string(deserialized_header.hash)));
}

TEST(CONFIG_CHANGED_MessageTest, SerializationAndDeserialization) {
  serializeAndDeserialize(
      Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES);
}

TEST(CONFIG_CHANGED_MessageTest, LegacySerialization) {
  serializeAndDeserialize(
      Compatibility::ProtocolVersion::PROTOCOL_VERSION_LOWER_BOUND);
}
