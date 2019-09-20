/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/FileBasedNodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientProcessor.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::IntegrationTestUtils;

class NodesConfigurationPublisherIntegrationTest : public IntegrationTestBase {
  void SetUp() override {
    IntegrationTestBase::SetUp();

    // Initialize the cluster
    cluster_ = IntegrationTestUtils::ClusterFactory{}
                   .doNotSyncServerConfigToNodesConfiguration()
                   .create(5);

    // Initialize a NCS pointing to the same data as the cluster to be able
    // to mutate it during the test.
    ncs_ = std::make_unique<FileBasedNodesConfigurationStore>(
        NodesConfigurationStoreFactory::getDefaultConfigStorePath(
            NodesConfigurationStoreFactory::NCSType::File,
            "" /* doesn't matter */),
        cluster_->getNCSPath(),
        NodesConfigurationCodec::extractConfigVersion);
    ASSERT_NE(nullptr, ncs_);

    // Build a client with NCM enabled and a seed server pointing to a node in
    // the cluster.
    {
      auto settings = std::unique_ptr<ClientSettings>(ClientSettings::create());
      settings->set("enable-nodes-configuration-manager", "true");
      settings->set("admin-client-capabilities", "true");
      // Increase the polling interval of the config to detect config changes
      // faster.
      settings->set("file-config-update-interval", "100ms");
      settings->set(
          "nodes-configuration-manager-store-polling-interval", "10ms");
      settings->set(
          "nodes-configuration-file-store-dir", cluster_->getNCSPath());
      client_ = cluster_->createIndependentClient(
          getDefaultTestTimeout(), std::move(settings));
    }
  }

 public:
  std::unique_ptr<Cluster> cluster_;
  std::unique_ptr<NodesConfigurationStore> ncs_;
  std::shared_ptr<Client> client_;
};

TEST_F(NodesConfigurationPublisherIntegrationTest, Publish) {
  auto client_impl = dynamic_cast<ClientImpl*>(client_.get());
  ASSERT_NE(nullptr, client_impl);
  auto processor = client_impl->getProcessorPtr();
  auto updateable_config = processor->config_;

  auto do_and_wait_for_notification = [&](std::function<void()> f,
                                          uint64_t target_version) {
    Semaphore sem;
    auto subscription =
        updateable_config->updateableNodesConfiguration()->subscribeToUpdates(
            [&]() {
              if (updateable_config->getNodesConfiguration()
                      ->getVersion()
                      .val() == target_version) {
                // Only post to the semaphore when we get the target version
                // we are looking for. This will prevent flakiness from late
                // subscriptions and config sync re-publishes.
                sem.post();
              }
              return;
            });
    f();
    return sem.timedwait(std::chrono::seconds(5)) == 0;
  };

  {
    // Initial Checks
    // Let's make sure that our assumption is correct about the inital value of
    // the settings.
    ASSERT_FALSE(processor->settings()
                     ->use_nodes_configuration_manager_nodes_configuration);
    wait_until("NCM to catchup", [&]() {
      return updateable_config->getNodesConfigurationFromNCMSource()
                 ->getVersion() == vcs_config_version_t(1);
    });
    EXPECT_EQ(1,
              updateable_config->getNodesConfigurationFromServerConfigSource()
                  ->getVersion()
                  .val());
    EXPECT_EQ(updateable_config->getServerConfig()
                  ->getNodesConfigurationFromServerConfigSource(),
              updateable_config->getNodesConfiguration());
  }

  {
    // Let's modify the server config and make sure that a subscription is
    // invoked
    auto new_server_config =
        updateable_config->getServerConfig()->withVersion(config_version_t(2));
    auto notified = do_and_wait_for_notification(
        [&]() {
          cluster_->writeConfig(new_server_config.get(),
                                updateable_config->getLogsConfig().get(),
                                true);
        },
        2);
    EXPECT_TRUE(notified);
    EXPECT_EQ(2, processor->getNodesConfiguration()->getVersion().val());
  }

  {
    // Modifying the NCS shouldn't trigger a config publish.
    auto new_nodes_configuration =
        updateable_config->getNodesConfigurationFromNCMSource()->withVersion(
            vcs_config_version_t(20));

    auto notified = do_and_wait_for_notification(
        [&]() {
          auto serialized =
              NodesConfigurationCodec::serialize(*new_nodes_configuration);
          ASSERT_EQ(Status::OK,
                    ncs_->updateConfigSync(std::move(serialized), folly::none));
        },
        20);
    EXPECT_FALSE(notified);
    EXPECT_EQ(2, processor->getNodesConfiguration()->getVersion().val());
  }

  {
    // Switching the source of truth to the NCM should trigger a config publish.
    auto notified = do_and_wait_for_notification(
        [&]() {
          client_->settings().set(
              "use-nodes-configuration-manager-nodes-configuration", "true");
        },
        20);
    EXPECT_TRUE(notified);
    EXPECT_EQ(20, processor->getNodesConfiguration()->getVersion().val());
  }

  {
    // Modifying the NCS should trigger a config publish.
    auto new_nodes_configuration =
        updateable_config->getNodesConfigurationFromNCMSource()->withVersion(
            vcs_config_version_t(21));

    auto notified = do_and_wait_for_notification(
        [&]() {
          auto serialized =
              NodesConfigurationCodec::serialize(*new_nodes_configuration);
          ASSERT_EQ(Status::OK,
                    ncs_->updateConfigSync(std::move(serialized), folly::none));
        },
        21);
    EXPECT_TRUE(notified);
    EXPECT_EQ(21, processor->getNodesConfiguration()->getVersion().val());
  }

  {
    // Switcing the source of truth back to the ServerConfig should trigger a
    // config publish.
    auto notified = do_and_wait_for_notification(
        [&]() {
          client_->settings().set(
              "use-nodes-configuration-manager-nodes-configuration", "false");
        },
        2);
    EXPECT_TRUE(notified);
    EXPECT_EQ(2, processor->getNodesConfiguration()->getVersion().val());
  }
}
