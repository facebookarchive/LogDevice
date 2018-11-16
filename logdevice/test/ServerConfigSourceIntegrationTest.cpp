/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class ServerConfigSourceIntegrationTest : public IntegrationTestBase {};

/**
 * Start a client with a ServerConfigSource using multiple seed hosts. Kill all
 * but the last host in the list. Check that the config the client retrieves is
 * correct.
 */
TEST_F(ServerConfigSourceIntegrationTest, Basic) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(3);

  auto new_server_config =
      cluster->getConfig()->getServerConfig()->withVersion(config_version_t(2));
  cluster->writeServerConfig(new_server_config.get());
  cluster->waitForConfigUpdate();

  std::string config_path = "server:";
  auto nodes = new_server_config->getNodes();
  for (node_index_t index = nodes.size() - 1; index >= 0; index--) {
    auto& node = nodes.at(index);
    config_path += node.address.toString();
    if (index != 0) {
      config_path += ',';
      // Kill all but the last node in the list
      cluster->getNode(index).kill();
    }
  }

  std::shared_ptr<Client> client =
      ClientFactory()
          .setSetting("on-demand-logs-config", true)
          .create(config_path);
  ASSERT_TRUE((bool)client);

  auto client_config =
      checked_downcast<ClientImpl*>(client.get())->getProcessor().config_;
  EXPECT_EQ(client_config->getServerConfig()->toString(),
            new_server_config->toString());
}

/**
 * Start a cluster with a config version at 0 and a client with a config
 * version at 1. Update the config version in the file to 1, but set the config
 * update interval high enough to guarantee that the server configs will not
 * receive the update. Modify the client config so that the client treats it as
 * a config sent from a server. Send an append to a server and check that the
 * server fetched the client.
 */
TEST_F(ServerConfigSourceIntegrationTest, StaleServerConfigFetchFromClient) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     // Ensure that no nodes update their configs from polling
                     .setParam("--file-config-update-interval", "1000000ms")
                     .create(1);

  // The default version for the cluster is 1
  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();
  EXPECT_EQ(config_version_t(1), cluster_config->serverConfig()->getVersion());

  std::shared_ptr<UpdateableConfig> client_config =
      std::make_shared<UpdateableConfig>(
          std::make_shared<UpdateableServerConfig>(
              cluster_config->serverConfig()->copy()),
          std::make_shared<UpdateableLogsConfig>(cluster_config->logsConfig()),
          std::make_shared<UpdateableZookeeperConfig>(
              cluster_config->zookeeperConfig()));
  // Set the client config version to 2, so it's higher than the cluster's
  client_config->get()->serverConfig()->setVersion(config_version_t(2));
  // Pretend this config is from the server
  client_config->get()->serverConfig()->setServerOrigin(NodeID(0, 1));

  // Update cluster config version
  // Since the polling interval for the nodes is large, they should not receive
  // this update
  auto new_server_config =
      cluster_config->serverConfig()->withVersion(config_version_t(2));
  cluster->writeConfig(
      new_server_config.get(), cluster_config->logsConfig().get());

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-config-synchronization", true));
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      std::chrono::seconds(1),
      std::move(client_settings),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  // Make an appendSync() call. The server config should detect that its config
  // is stale and fetch the new config from the client. However, since the
  // client did not receive the config from a server, the server cannot trust
  // the CONFIG_CHANGED message. This means that it will fetch the config from
  // the source.
  char data[20];
  client->appendSync(logid_t(1), Payload(data, sizeof data));
  // Send a second append to guarantee that the server will have received the
  // CONFIG_CHANGED message by the time we check its config and stats
  client->appendSync(logid_t(1), Payload(data, sizeof data));

  wait_until([&]() -> bool {
    std::string reply = cluster->getNode(0).sendCommand("info config");
    std::string config_str = reply.substr(0, reply.length() - 5);
    auto updated_config = Configuration::fromJson(config_str, nullptr, nullptr);
    ld_check(updated_config);
    return client_config->get()->serverConfig()->getVersion() ==
        updated_config->serverConfig()->getVersion();
  });
  EXPECT_LT(0, cluster->getNode(0).stats()["config_changed_update"]);
}

/**
 * Start a cluster with a config version at 0 and a client with a config
 * version at 1. Update the config version in the file to 1, but set the config
 * update interval high enough to guarantee that the server configs will not
 * receive the update. Without having received a config from a server, the
 * client config should not be trusted by the server. Send an append to a
 * server and check that the server fetched the latest config from the source.
 */
TEST_F(ServerConfigSourceIntegrationTest, StaleServerConfigFetchFromSource) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     // Ensure that no nodes update their configs from polling
                     .setParam("--file-config-update-interval", "1000000ms")
                     .create(1);

  // The default version for the cluster is 1
  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();
  EXPECT_EQ(config_version_t(1), cluster_config->serverConfig()->getVersion());

  std::shared_ptr<UpdateableConfig> client_config =
      std::make_shared<UpdateableConfig>(
          std::make_shared<UpdateableServerConfig>(
              cluster_config->serverConfig()->copy()),
          std::make_shared<UpdateableLogsConfig>(cluster_config->logsConfig()),
          std::make_shared<UpdateableZookeeperConfig>(
              cluster_config->zookeeperConfig()));
  // Set the client config version to 2, so it's higher than the cluster's
  client_config->get()->serverConfig()->setVersion(config_version_t(2));

  // Update cluster config version
  // Since the polling interval for the nodes is large, they should not receive
  // this update
  auto new_server_config =
      cluster_config->serverConfig()->withVersion(config_version_t(2));
  cluster->writeConfig(
      new_server_config.get(), cluster_config->logsConfig().get());

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-config-synchronization", true));
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      std::chrono::seconds(1),
      std::move(client_settings),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  // Make an appendSync() call. The server config should detect that its config
  // is stale and fetch the new config from the client. However, since the
  // client did not receive the config from a server, the server cannot trust
  // the CONFIG_CHANGED message. This means that it will fetch the config from
  // the source.
  //
  // NOTE: the fetch from config source would typically be asynchronous, but
  // since we're using a FileConfigSource, the config is updated immediately.
  char data[20];
  client->appendSync(logid_t(1), Payload(data, sizeof data));
  // Send a second append to guarantee that the server will have received the
  // CONFIG_CHANGED message by the time we check its config and stats
  client->appendSync(logid_t(1), Payload(data, sizeof data));
  wait_until([&]() -> bool {
    std::string reply = cluster->getNode(0).sendCommand("info config");
    std::string config_str = reply.substr(0, reply.length() - 5);
    auto updated_config = Configuration::fromJson(config_str, nullptr, nullptr);
    ld_check(updated_config);
    return client_config->get()->serverConfig()->getVersion() ==
        updated_config->serverConfig()->getVersion();
  });
  EXPECT_EQ(0, cluster->getNode(0).stats()["config_changed_update"]);
  EXPECT_LT(
      0, cluster->getNode(0).stats()["config_changed_ignored_not_trusted"]);
}
