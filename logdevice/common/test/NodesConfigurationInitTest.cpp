/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationInit.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include "logdevice/common/configuration/nodes/NodesConfigurationCodecFlatBuffers.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;

struct MockNodesConfigurationInit : public NodesConfigurationInit {
  ~MockNodesConfigurationInit() override = default;

  using NodesConfigurationInit::NodesConfigurationInit;

  void injectExtraSettings(Settings& settings) const override {
    settings.enable_initial_get_cluster_state = false;
  }

  FRIEND_TEST(NodesConfigurationInitTest, ConfigCreation);
};

TEST(NodesConfigurationInitTest, ConfigCreation) {
  MockNodesConfigurationInit init(nullptr);
  auto config = init.buildDummyServerConfig({
      "10.0.0.2:4440",
      "10.0.0.3:4440",
      "10.0.0.4:4440",
  });
  EXPECT_NE(nullptr, config);

  auto nodes =
      config->getServerConfig()->getNodesConfiguration()->getServiceDiscovery();
  EXPECT_EQ(3, nodes->numNodes());
  EXPECT_EQ("10.0.0.2:4440",
            nodes->getNodeAttributesPtr(node_index_t(0))->address.toString());
  EXPECT_EQ("10.0.0.3:4440",
            nodes->getNodeAttributesPtr(node_index_t(1))->address.toString());
  EXPECT_EQ("10.0.0.4:4440",
            nodes->getNodeAttributesPtr(node_index_t(2))->address.toString());
}

TEST(NodesConfigurationInitTest, InitTest) {
  // Create a dummy nodes configuration
  auto node_config = createSimpleNodesConfig(3);
  auto metadata_config = createMetaDataLogsConfig(node_config, 2, 1);
  node_config.generateNodesConfiguration(metadata_config, config_version_t(2));
  auto nodes_configuration = node_config.getNodesConfiguration();

  // Serialize it
  auto serialized =
      NodesConfigurationCodecFlatBuffers::serialize(*nodes_configuration);

  // Write it to the InMemoryStore
  auto store = std::make_unique<InMemNodesConfigurationStore>(
      "/foo", [](folly::StringPiece) { return vcs_config_version_t(2); });
  auto status = store->updateConfigSync(serialized, folly::none);
  EXPECT_EQ(Status::OK, status);

  MockNodesConfigurationInit init(std::move(store));
  auto fetched_node_config = std::make_shared<UpdateableNodesConfiguration>();
  int success = init.init(
      fetched_node_config, make_test_plugin_registry(), "data:10.0.0.2:4440");
  EXPECT_TRUE(success);
  ASSERT_NE(nullptr, fetched_node_config->get());
  EXPECT_EQ(vcs_config_version_t(2), fetched_node_config->get()->getVersion());
  EXPECT_EQ(3, fetched_node_config->get()->clusterSize());
}
