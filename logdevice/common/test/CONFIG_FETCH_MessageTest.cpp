/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

#include <chrono>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"
#include "logdevice/common/protocol/Compatibility.h"
#include "logdevice/common/protocol/FixedSizeMessage.h"
#include "logdevice/common/protocol/ProtocolReader.h"
#include "logdevice/common/protocol/ProtocolWriter.h"
#include "logdevice/common/test/TestUtil.h"

using namespace std::literals::chrono_literals;
using namespace facebook::logdevice;
using namespace ::testing;

template <class T>
std::unique_ptr<T> tryRead(std::string data, uint16_t proto_version) {
  ProtocolReader reader(Slice::fromString(data), "", proto_version);
  std::unique_ptr<facebook::logdevice::Message> deserialized_msg_base =
      T::deserialize(reader).msg;

  EXPECT_NE(deserialized_msg_base, nullptr);

  return std::unique_ptr<T>(static_cast<T*>(deserialized_msg_base.release()));
}

TEST(CONFIG_FETCH_MessageTest, SerializeAndDeserialize) {
  CONFIG_FETCH_Header header{
      request_id_t(3), CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG, 10};
  CONFIG_FETCH_Message msg{header};

  EXPECT_EQ(request_id_t(3), msg.getHeader().rid);
  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            msg.getHeader().config_type);
  EXPECT_EQ(10, msg.getHeader().my_version);

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
  EXPECT_EQ(10, deserialized_msg->getHeader().my_version);
}

TEST(CONFIG_FETCH_MessageTest, LegacySerializeAndDeserialize) {
  CONFIG_FETCH_Header header{
      request_id_t(3), CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG, 10};
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

  EXPECT_EQ(request_id_t(0), deserialized_msg->getHeader().rid);
  EXPECT_EQ(CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG,
            deserialized_msg->getHeader().config_type);
  EXPECT_EQ(0, deserialized_msg->getHeader().my_version);
}

struct CONFIG_FETCH_MessageMock : public CONFIG_FETCH_Message {
  using CONFIG_FETCH_Message::CONFIG_FETCH_Message;

  std::shared_ptr<Configuration> getConfig() override {
    return config;
  }

  NodeID getMyNodeID() const override {
    return NodeID(2, 1);
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() override {
    // TODO: migrate it to use NodesConfiguration with switchable source
    return config->serverConfig()
        ->getNodesConfigurationFromServerConfigSource();
  }

  int sendMessage(std::unique_ptr<CONFIG_CHANGED_Message> msg,
                  const Address& to) override {
    return sendMessage_(msg, to);
  }

  MOCK_METHOD2(sendMessage_,
               int(std::unique_ptr<CONFIG_CHANGED_Message>& msg, Address to));

  std::shared_ptr<Configuration> config;
};

void compareChangedMessages(std::unique_ptr<CONFIG_CHANGED_Message>& expected,
                            std::unique_ptr<CONFIG_CHANGED_Message>& got,
                            bool comparePayload) {
  EXPECT_EQ(expected->getHeader().status, got->getHeader().status);
  EXPECT_EQ(expected->getHeader().rid, got->getHeader().rid);
  EXPECT_EQ(
      expected->getHeader().modified_time, got->getHeader().modified_time);
  EXPECT_EQ(
      expected->getHeader().server_origin, got->getHeader().server_origin);
  EXPECT_EQ(expected->getHeader().config_type, got->getHeader().config_type);
  EXPECT_EQ(expected->getHeader().action, got->getHeader().action);
  EXPECT_EQ(expected->getHeader().version, got->getHeader().version);
  if (comparePayload) {
    EXPECT_EQ(expected->getConfigStr(), got->getConfigStr());
  }
}

TEST(CONFIG_FETCH_MessageTest, OnReceivedNodesConfiguration) {
  auto serialize = [](const std::shared_ptr<const NodesConfiguration>& cfg) {
    return configuration::nodes::NodesConfigurationCodec::serialize(
        *cfg, {true});
  };
  CONFIG_FETCH_MessageMock msg{
      CONFIG_FETCH_Header{
          request_id_t(4),
          CONFIG_FETCH_Header::ConfigType::NODES_CONFIGURATION,
      },
  };
  auto config = createSimpleConfig(3, 1);
  msg.config = config;

  // TODO: migrate it to use NodesConfiguration with switchable source
  auto expected = std::make_unique<CONFIG_CHANGED_Message>(
      CONFIG_CHANGED_Header{
          Status::OK,
          request_id_t(4),
          static_cast<uint64_t>(
              config->serverConfig()
                  ->getNodesConfigurationFromServerConfigSource()
                  ->getLastChangeTimestamp()
                  .time_since_epoch()
                  .count()),
          1,
          NodeID(2, 1),
          CONFIG_CHANGED_Header::ConfigType::NODES_CONFIGURATION,
          CONFIG_CHANGED_Header::Action::CALLBACK},
      serialize(config->serverConfig()
                    ->getNodesConfigurationFromServerConfigSource()));

  EXPECT_CALL(msg, sendMessage_(_, Address(NodeID(1, 1))))
      .WillOnce(
          Invoke([&](std::unique_ptr<CONFIG_CHANGED_Message>& got, Address) {
            compareChangedMessages(expected, got, true);
            return 0;
          }));

  EXPECT_EQ(CONFIG_FETCH_MessageMock::Disposition::NORMAL,
            msg.onReceived(Address(NodeID(1, 1))));
}

TEST(CONFIG_FETCH_MessageTest, OnReceivedNodesConfigurationConditional) {
  CONFIG_FETCH_MessageMock msg{
      CONFIG_FETCH_Header{
          request_id_t(4),
          CONFIG_FETCH_Header::ConfigType::NODES_CONFIGURATION,
          1,
      },
  };
  auto config = createSimpleConfig(3, 1);
  msg.config = config;

  auto expected = std::make_unique<CONFIG_CHANGED_Message>(
      CONFIG_CHANGED_Header{
          Status::UPTODATE,
          request_id_t(4),
          static_cast<uint64_t>(
              config->serverConfig()
                  ->getNodesConfigurationFromServerConfigSource()
                  ->getLastChangeTimestamp()
                  .time_since_epoch()
                  .count()),
          1,
          NodeID(2, 1),
          CONFIG_CHANGED_Header::ConfigType::NODES_CONFIGURATION,
          CONFIG_CHANGED_Header::Action::CALLBACK},
      "");

  EXPECT_CALL(msg, sendMessage_(_, Address(NodeID(1, 1))))
      .WillOnce(
          Invoke([&](std::unique_ptr<CONFIG_CHANGED_Message>& got, Address) {
            compareChangedMessages(expected, got, true);
            return 0;
          }));

  EXPECT_EQ(CONFIG_FETCH_MessageMock::Disposition::NORMAL,
            msg.onReceived(Address(NodeID(1, 1))));
}

TEST(CONFIG_FETCH_MessageTest, OnReceivedUpdate) {
  CONFIG_FETCH_MessageMock msg{
      CONFIG_FETCH_Header{CONFIG_FETCH_Header::ConfigType::MAIN_CONFIG},
  };
  auto config = createSimpleConfig(3, 1);
  msg.config = config;

  auto expected = std::make_unique<CONFIG_CHANGED_Message>(
      CONFIG_CHANGED_Header{Status::OK,
                            REQUEST_ID_INVALID,
                            static_cast<uint64_t>(config->serverConfig()
                                                      ->getMainConfigMetadata()
                                                      .modified_time.count()),
                            1,
                            config->serverConfig()->getServerOrigin(),
                            CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
                            CONFIG_CHANGED_Header::Action::UPDATE},
      "");

  EXPECT_CALL(msg, sendMessage_(_, Address(NodeID(1, 1))))
      .WillOnce(
          Invoke([&](std::unique_ptr<CONFIG_CHANGED_Message>& got, Address) {
            compareChangedMessages(expected, got, false);
            return 0;
          }));

  EXPECT_EQ(CONFIG_FETCH_MessageMock::Disposition::NORMAL,
            msg.onReceived(Address(NodeID(1, 1))));
}
