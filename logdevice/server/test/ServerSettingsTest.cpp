// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/server/ServerSettings.h"

#include <gtest/gtest.h>

using namespace ::testing;
using namespace facebook::logdevice;
using NodesConfigTagMapT = ServerSettings::NodesConfigTagMapT;

TEST(SeverSettingsTest, parseTags_empty) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("");
  NodesConfigTagMapT expected;

  EXPECT_EQ(actual, expected);
}

TEST(SeverSettingsTest, parseTags_one) {
  NodesConfigTagMapT actual = ServerSettings::parse_tags("key1:value1");
  NodesConfigTagMapT expected = {{"key1", "value1"}};

  EXPECT_EQ(actual, expected);
}

TEST(SeverSettingsTest, parseTags_many) {
  NodesConfigTagMapT actual =
      ServerSettings::parse_tags("key1:value1,key2:value2");
  NodesConfigTagMapT expected = {{"key1", "value1"}, {"key2", "value2"}};

  EXPECT_EQ(actual, expected);
}

TEST(SeverSettingsTest, parseTags_emptyValues) {
  NodesConfigTagMapT actual =
      ServerSettings::parse_tags("key1:,key2:value2,key3:");
  NodesConfigTagMapT expected = {
      {"key1", ""}, {"key2", "value2"}, {"key3", ""}};

  EXPECT_EQ(actual, expected);
}

TEST(SeverSettingsTest, parseTags_validation_emptyKeys) {
  ASSERT_THROW(ServerSettings::parse_tags(":value"), std::exception);
}

TEST(SeverSettingsTest, parseTags_validation_invalidSeparator) {
  ASSERT_THROW(ServerSettings::parse_tags("key1:value1&key2"), std::exception);
}

TEST(SeverSettingsTest, parseTags_validation_spaces) {
  ASSERT_THROW(ServerSettings::parse_tags("key 1:value"), std::exception);
}

TEST(SeverSettingsTest, parseTags_validation_arbitraryContent) {
  ASSERT_THROW(ServerSettings::parse_tags("I like turtles."), std::exception);
}
