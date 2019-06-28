/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationInit.h"

#include <folly/futures/Future.h>
#include <folly/init/Init.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <gtest/gtest_prod.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::NodesConfigurationTestUtil;

namespace {

struct MockNodesConfigurationInit : public NodesConfigurationInit {
  ~MockNodesConfigurationInit() override = default;

  using NodesConfigurationInit::NodesConfigurationInit;

  void injectExtraSettings(Settings& settings) const override {
    settings.enable_initial_get_cluster_state = false;
  }

  FRIEND_TEST(NodesConfigurationInitTest, ConfigCreation);
};

class TimeControlledNCS : public NodesConfigurationStore {
 public:
  using Timestamp = ::Timestamp<std::chrono::steady_clock,
                                detail::Holder,
                                std::chrono::milliseconds>;
  TimeControlledNCS(
      std::string config,
      std::chrono::milliseconds unavailable_duration,
      folly::Optional<std::chrono::milliseconds> get_delay = folly::none)
      : config_(std::move(config)),
        available_since_(Timestamp::now() + unavailable_duration),
        get_delay_(std::move(get_delay)) {}

  void getConfig(value_callback_t cb,
                 folly::Optional<version_t> base_version = {}) const override {
    if (Timestamp::now() < available_since_) {
      cb(E::NOTREADY, "");
      return;
    }

    if (get_delay_.hasValue()) {
      folly::makeFuture()
          .delayed(get_delay_.value())
          .thenValue([mcb = std::move(cb), cfg = config_](auto&&) mutable {
            ld_info("Calling delayed callback");
            mcb(E::OK, std::move(cfg));
          });
    } else {
      cb(E::OK, config_);
    }
  }

  void getLatestConfig(value_callback_t cb) const override {
    getConfig(std::move(cb));
  }

  Status
  getConfigSync(std::string*,
                folly::Optional<version_t> base_version = {}) const override {
    ld_check(false);
    return E::INTERNAL;
  }

  void updateConfig(std::string,
                    folly::Optional<version_t>,
                    write_callback_t) override {
    ld_check(false);
  }

  Status updateConfigSync(std::string,
                          folly::Optional<version_t>,
                          version_t*,
                          std::string*) override {
    return E::INTERNAL;
  }

  void shutdown() override {}

 private:
  const std::string config_;
  const Timestamp available_since_;
  const folly::Optional<std::chrono::milliseconds> get_delay_;
};

TEST(NodesConfigurationInitTest, ConfigCreation) {
  MockNodesConfigurationInit init(nullptr, UpdateableSettings<Settings>());
  auto config = init.buildBootstrappingServerConfig({
      "10.0.0.2:4440",
      "10.0.0.3:4440",
      "10.0.0.4:4440",
  });
  EXPECT_NE(nullptr, config);

  auto nodes = config->getServerConfig()
                   ->getNodesConfigurationFromServerConfigSource()
                   ->getServiceDiscovery();
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
  auto serialized = NodesConfigurationCodec::serialize(*nodes_configuration);

  // Write it to the InMemoryStore
  auto store = std::make_unique<InMemNodesConfigurationStore>(
      "/foo", [](folly::StringPiece) { return vcs_config_version_t(2); });
  auto status = store->updateConfigSync(serialized, folly::none);
  EXPECT_EQ(Status::OK, status);

  MockNodesConfigurationInit init(
      std::move(store), UpdateableSettings<Settings>());
  auto fetched_node_config = std::make_shared<UpdateableNodesConfiguration>();
  int success = init.init(
      fetched_node_config, make_test_plugin_registry(), "data:10.0.0.2:4440");
  EXPECT_TRUE(success);
  ASSERT_NE(nullptr, fetched_node_config->get());
  EXPECT_EQ(vcs_config_version_t(2), fetched_node_config->get()->getVersion());
  EXPECT_EQ(3, fetched_node_config->get()->clusterSize());
}

TEST(NodesConfigurationInitTest, Retry) {
  auto nodes_configuration = provisionNodes();
  auto serialized = NodesConfigurationCodec::serialize(*nodes_configuration);

  Settings settings(create_default_settings<Settings>());
  settings.nodes_configuration_init_timeout = std::chrono::seconds(10);
  settings.nodes_configuration_init_retry_timeout =
      chrono_expbackoff_t<std::chrono::milliseconds>(
          std::chrono::milliseconds(50), std::chrono::milliseconds(200));

  // store will be unavailable for 2 seconds
  auto store =
      std::make_unique<TimeControlledNCS>(serialized, std::chrono::seconds(2));

  MockNodesConfigurationInit init(
      std::move(store), UpdateableSettings<Settings>(settings));
  auto fetched_node_config = std::make_shared<UpdateableNodesConfiguration>();
  int success = init.init(
      fetched_node_config, make_test_plugin_registry(), "data:10.0.0.2:4440");
  EXPECT_TRUE(success);
  ASSERT_NE(nullptr, fetched_node_config->get());
  EXPECT_EQ(*nodes_configuration, *fetched_node_config->get());
}

TEST(NodesConfigurationInitTest, Timeout) {
  auto nodes_configuration = provisionNodes();
  auto serialized = NodesConfigurationCodec::serialize(*nodes_configuration);

  Settings settings(create_default_settings<Settings>());
  settings.nodes_configuration_init_timeout = std::chrono::seconds(2);
  settings.nodes_configuration_init_retry_timeout =
      chrono_expbackoff_t<std::chrono::milliseconds>(
          std::chrono::milliseconds(50), std::chrono::milliseconds(200));

  // store will be unavailable for 300 seconds
  auto store = std::make_unique<TimeControlledNCS>(
      serialized, std::chrono::seconds(300));

  MockNodesConfigurationInit init(
      std::move(store), UpdateableSettings<Settings>(settings));
  auto fetched_node_config = std::make_shared<UpdateableNodesConfiguration>();
  int success = init.init(
      fetched_node_config, make_test_plugin_registry(), "data:10.0.0.2:4440");
  EXPECT_FALSE(success);
  ASSERT_EQ(nullptr, fetched_node_config->get());
}

TEST(NodesConfigurationInitTest, WithLongDurationCallback) {
  auto nodes_configuration = provisionNodes();
  auto serialized = NodesConfigurationCodec::serialize(*nodes_configuration);

  Settings settings(create_default_settings<Settings>());
  settings.nodes_configuration_init_timeout = std::chrono::seconds(1);
  settings.nodes_configuration_init_retry_timeout =
      chrono_expbackoff_t<std::chrono::milliseconds>(
          std::chrono::milliseconds(50), std::chrono::milliseconds(200));

  // store will be immediately available, however callbacks will be delayed 2s
  auto store = std::make_unique<TimeControlledNCS>(
      serialized, std::chrono::seconds(0), std::chrono::milliseconds(2000));

  MockNodesConfigurationInit init(
      std::move(store), UpdateableSettings<Settings>(settings));
  auto fetched_node_config = std::make_shared<UpdateableNodesConfiguration>();
  int success = init.init(
      fetched_node_config, make_test_plugin_registry(), "data:10.0.0.2:4440");
  // we should still success after 2 seconds despite that the config init
  // timeout is 1s
  EXPECT_TRUE(success);
  ASSERT_NE(nullptr, fetched_node_config->get());
  EXPECT_EQ(*nodes_configuration, *fetched_node_config->get());
}

} // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv);
  return RUN_ALL_TESTS();
}
