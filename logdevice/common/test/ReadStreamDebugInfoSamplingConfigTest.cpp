/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ReadStreamDebugInfoSamplingConfig.h"

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/settings/UpdateableSettings-details.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace ::testing;
using namespace configuration::all_read_streams_debug_config::thrift;
namespace {

AllReadStreamsDebugConfig buildConfig(std::string csid, int64_t deadline) {
  AllReadStreamsDebugConfig config;
  config.csid = csid;
  config.deadline = deadline;
  return config;
}

TEST(ReadStreamDebugInfoSamplingConfigTest, ConstructionNotFoundPlugin) {
  std::shared_ptr<PluginRegistry> plugin_registry = make_test_plugin_registry();

  std::unique_ptr<ReadStreamDebugInfoSamplingConfig> fetcher =
      std::make_unique<ReadStreamDebugInfoSamplingConfig>(
          plugin_registry, "asd: asd");

  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid1"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed(""));
}

TEST(ReadStreamDebugInfoSamplingConfigTest, Construction) {
  std::shared_ptr<PluginRegistry> plugin_registry = make_test_plugin_registry();

  AllReadStreamsDebugConfigs configs;
  configs.configs.push_back(buildConfig("test-csid", 123));
  std::string serialized_configs = "data: " +
      ThriftCodec::serialize<apache::thrift::SimpleJSONSerializer>(configs);

  std::unique_ptr<ReadStreamDebugInfoSamplingConfig> fetcher =
      std::make_unique<ReadStreamDebugInfoSamplingConfig>(
          plugin_registry, serialized_configs);

  EXPECT_TRUE(fetcher->isReadStreamDebugInfoSamplingAllowed(
      "test-csid", std::chrono::seconds(120)));

  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid1"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed(""));
}

TEST(ReadStreamDebugInfoSamplingConfigTest, ExpiredDeadline) {
  std::shared_ptr<PluginRegistry> plugin_registry = make_test_plugin_registry();

  AllReadStreamsDebugConfigs configs;
  configs.configs.push_back(buildConfig("test-csid", 1));
  std::string serialized_configs = "data: " +
      ThriftCodec::serialize<apache::thrift::SimpleJSONSerializer>(configs);

  std::unique_ptr<ReadStreamDebugInfoSamplingConfig> fetcher =
      std::make_unique<ReadStreamDebugInfoSamplingConfig>(
          plugin_registry, serialized_configs);

  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid1"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed(""));
}

TEST(ReadStreamDebugInfoSamplingConfigTest, MultipleConfigs) {
  std::shared_ptr<PluginRegistry> plugin_registry = make_test_plugin_registry();

  AllReadStreamsDebugConfigs configs;
  configs.configs.push_back(buildConfig("test-csid", 1));

  configs.configs.push_back(buildConfig("test-csid1", 123));
  std::string serialized_configs = "data: " +
      ThriftCodec::serialize<apache::thrift::SimpleJSONSerializer>(configs);

  std::unique_ptr<ReadStreamDebugInfoSamplingConfig> fetcher =
      std::make_unique<ReadStreamDebugInfoSamplingConfig>(
          plugin_registry, serialized_configs);

  EXPECT_TRUE(fetcher->isReadStreamDebugInfoSamplingAllowed(
      "test-csid1", std::chrono::seconds(120)));

  // Permission denied after deadline expiration
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed(
      "test-csid1", std::chrono::seconds(124)));

  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed("test-csid"));
  EXPECT_FALSE(fetcher->isReadStreamDebugInfoSamplingAllowed(""));
}
} // namespace
