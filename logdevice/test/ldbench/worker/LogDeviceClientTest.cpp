/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/ldbench/worker/LogDeviceClient.h"

#include <gtest/gtest.h>

#include "logdevice/test/utils/IntegrationTestUtils.h"

namespace facebook { namespace logdevice { namespace ldbench {

class LogDeviceClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    cluster = IntegrationTestUtils::ClusterFactory().setNumLogs(10).create(4);
  }
  void TearDown() override {}
  std::unique_ptr<IntegrationTestUtils::Cluster> cluster;
  std::unique_ptr<LogStoreClient> client;
};

/**
 * Creating a cluster is expensive
 * We try to reuse one cluster as much as possible
 */
TEST_F(LogDeviceClientTest, logOprations) {
  options.bench_name = "write";
  options.sys_name = "logdevice";
  options.config_path = cluster->getConfigPath();
  options.log_range_names.clear();
  std::vector<uint64_t> all_logs;
  options.use_buffered_writer = false;
  client = std::make_unique<LogDeviceClient>(options);
  EXPECT_TRUE(client);
  EXPECT_TRUE(client->getLogs(&all_logs));
  EXPECT_EQ(10, all_logs.size());
  client.reset();
  options.use_buffered_writer = true;
  client = std::make_unique<LogDeviceClient>(options);
  EXPECT_TRUE(client->append(1, "test payload", nullptr));
  client.reset();
  options.use_buffered_writer = false;
  client = std::make_unique<LogDeviceClient>(options);
  EXPECT_TRUE(client->append(1, "test payload", nullptr));
  client.reset();
}
}}} // namespace facebook::logdevice::ldbench
