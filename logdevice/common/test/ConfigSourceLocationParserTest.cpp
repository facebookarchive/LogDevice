/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConfigSourceLocationParser.h"

#include <gtest/gtest.h>

#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/configuration/ZookeeperConfigSource.h"

using namespace facebook::logdevice;

TEST(ConfigSourceLocationParserTest, TestAll) {
  std::vector<std::unique_ptr<ConfigSource>> sources;
  sources.push_back(std::make_unique<FileConfigSource>());
  sources.push_back(std::make_unique<ZookeeperConfigSource>(
      std::chrono::milliseconds(100), nullptr));

  std::string path;
  auto it = sources.end();

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "zk:logdevice/test");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("Zookeeper", (*it)->getName());
    EXPECT_EQ("logdevice/test", path);
  }

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "zookeeper:logdevice/test");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("Zookeeper", (*it)->getName());
    EXPECT_EQ("logdevice/test", path);
  }

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "file://logdevice/test");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("file", (*it)->getName());
    EXPECT_EQ("//logdevice/test", path);
  }

  {
    // File is the default
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "/logdevice/test");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("file", (*it)->getName());
    EXPECT_EQ("/logdevice/test", path);
  }

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "schema:/logdevice/test");
    ASSERT_EQ(sources.end(), it);
    EXPECT_EQ("", path);
  }

  {
    std::tie(it, path) = ConfigSourceLocationParser::parse(sources, "");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("file", (*it)->getName());
    EXPECT_EQ("", path);
  }

  {
    std::tie(it, path) = ConfigSourceLocationParser::parse(sources, "zk");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("file", (*it)->getName());
    EXPECT_EQ("zk", path);
  }

  {
    std::tie(it, path) = ConfigSourceLocationParser::parse(sources, "zk:");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("Zookeeper", (*it)->getName());
    EXPECT_EQ("", path);
  }

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "zk:{test: test}");
    ASSERT_NE(sources.end(), it);
    EXPECT_EQ("Zookeeper", (*it)->getName());
    EXPECT_EQ("{test: test}", path);
  }

  {
    std::tie(it, path) =
        ConfigSourceLocationParser::parse(sources, "10.0.0.1:8080");
    ASSERT_EQ(sources.end(), it);
    EXPECT_EQ("", path);
  }
}
