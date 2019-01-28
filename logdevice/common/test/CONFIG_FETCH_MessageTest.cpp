/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

#include <chrono>

#include <gtest/gtest.h>

#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;

template <class T>
std::unique_ptr<T> tryRead(std::string data, uint16_t proto_version) {
  ProtocolReader reader(Slice::fromString(data), "", proto_version);
  std::unique_ptr<Message> deserialized_msg_base = T::deserialize(reader).msg;

  EXPECT_NE(deserialized_msg_base, nullptr);

  return std::unique_ptr<T>(static_cast<T*>(deserialized_msg_base.release()));
}

TEST(CONFIG_FETCH_MessageTest, SerializeAndDeserialize) {
  CONFIG_FETCH_Header header{
      request_id_t(3), CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG};
  CONFIG_FETCH_Message msg{header};

  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            msg.getHeader().config_type);

  std::string dest;
  ProtocolWriter writer(
      &dest, "", Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES);
  msg.serialize(writer);
  ASSERT_GT(writer.result(), 0);

  auto deserialized_msg = tryRead<CONFIG_FETCH_Message>(
      dest, Compatibility::ProtocolVersion::RID_IN_CONFIG_MESSAGES);

  EXPECT_EQ(request_id_t(3), deserialized_msg->getHeader().rid);
  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            deserialized_msg->getHeader().config_type);
}

TEST(CONFIG_FETCH_MessageTest, LegacySerializeAndDeserialize) {
  CONFIG_FETCH_Header header{
      request_id_t(3), CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG};
  CONFIG_FETCH_Message msg{header};

  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            msg.getHeader().config_type);

  std::string dest;
  ProtocolWriter writer(
      &dest, "", Compatibility::ProtocolVersion::PROTOCOL_VERSION_LOWER_BOUND);
  msg.serialize(writer);
  ASSERT_GT(writer.result(), 0);

  // Backward comptability test using a hex older than
  // ProtocolVersion::RID_IN_CONFIG_MESSAGES.
  EXPECT_EQ(std::string("01"), hexdump_buf(Slice::fromString(dest)));

  auto deserialized_msg = tryRead<CONFIG_FETCH_Message>(
      dest, Compatibility::ProtocolVersion::PROTOCOL_VERSION_LOWER_BOUND);

  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            deserialized_msg->getHeader().config_type);
}
