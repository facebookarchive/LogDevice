// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/server/ServerSettings.h"

#include <gtest/gtest.h>

using namespace facebook::logdevice;
using NodesConfigTagMapT = ServerSettings::NodesConfigTagMapT;

TEST(ServerSettingsTest, parseTags_empty) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("");
  NodesConfigTagMapT expected;

  EXPECT_EQ(actual, expected);
}

TEST(ServerSettingsTest, parseTags_one) {
  NodesConfigTagMapT actual =
      ServerSettings::parse_tags("key1:symbols)(*&^_-{}");
  NodesConfigTagMapT expected = {{"key1", "symbols)(*&^_-{}"}};

  EXPECT_EQ(actual, expected);
}

TEST(ServerSettingsTest, parseTags_many) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("K1:V1,K2:V2");
  NodesConfigTagMapT expected = {{"K1", "V1"}, {"K2", "V2"}};

  EXPECT_EQ(actual, expected);
}

TEST(ServerSettingsTest, parseTags_emptyValues) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("key1:,key3:");
  NodesConfigTagMapT expected = {{"key1", ""}, {"key3", ""}};

  EXPECT_EQ(actual, expected);
}

TEST(ServerSettingsTest, parseTags_colonsInValue) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("A:B:C,D:E:F:G");
  NodesConfigTagMapT expected = {{"A", "B:C"}, {"D", "E:F:G"}};

  EXPECT_EQ(actual, expected);
}

TEST(ServerSettingsTest, parseTags_validation_emptyKeys) {
  ASSERT_THROW(ServerSettings::parse_tags(":value"), std::exception);
}

TEST(ServerSettingsTest, parseTags_validation_emptyPair) {
  ASSERT_THROW(ServerSettings::parse_tags("key1:value1,"), std::exception);
}

TEST(ServerSettingsTest, parseTags_validation_arbitraryContent) {
  ASSERT_THROW(ServerSettings::parse_tags("I like turtles."), std::exception);
}

TEST(ServerSettingsTest, parseValuesPerNetPriority) {
  using ClientNetworkPriority =
      ServerSettings::NodeServiceDiscovery::ClientNetworkPriority;
  using PortMapT = std::map<ClientNetworkPriority, int>;
  using UnixSocketMapT = std::map<ClientNetworkPriority, std::string>;

  auto MEDIUM = ClientNetworkPriority::MEDIUM;
  auto HIGH = ClientNetworkPriority::HIGH;

  auto parse_unix_sockets = ServerSettings::parse_unix_sockets_per_net_priority;
  auto parse_ports = ServerSettings::parse_ports_per_net_priority;

  // empty values
  ASSERT_EQ((PortMapT{}), parse_ports(""));
  ASSERT_EQ((UnixSocketMapT{}), parse_unix_sockets(""));

  // single valid value
  ASSERT_EQ((PortMapT{{MEDIUM, 666}}), parse_ports("medium:666"));
  ASSERT_EQ(
      (UnixSocketMapT{{MEDIUM, "/666"}}), parse_unix_sockets("medium:/666"));

  // parse two values
  ASSERT_EQ((PortMapT{{MEDIUM, 666}, {HIGH, 420}}),
            parse_ports("medium:666,high:420"));
  ASSERT_EQ((UnixSocketMapT{{MEDIUM, "/666"}, {HIGH, "/420"}}),
            parse_unix_sockets("medium:/666,high:/420"));

  // invalid values
  ASSERT_THROW(parse_ports("medium:/420"), std::exception);
  ASSERT_THROW(parse_ports("medium:0"), std::exception);
  ASSERT_THROW(parse_ports("medium:-1"), std::exception);
  ASSERT_THROW(parse_ports("medium:65536"), std::exception);
  ASSERT_THROW(parse_unix_sockets("medium:whereAmI"), std::exception);

  // invalid priority name
  ASSERT_THROW(parse_ports("medium:1,high:2,whenItsReady:3"), std::exception);
  ASSERT_THROW(parse_unix_sockets("medium:/valid/path,high:/another/valid/"
                                  "path,whenItsReady:/hat/trick"),
               std::exception);
}
