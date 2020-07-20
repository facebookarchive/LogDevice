// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/server/ServerSettings.h"

#include <gtest/gtest.h>

using namespace ::testing;
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
