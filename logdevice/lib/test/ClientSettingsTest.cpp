/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/ClientSettings.h"

#include <algorithm>
#include <chrono>
#include <fstream>
#include <memory>

#include <boost/filesystem.hpp>
#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"

using namespace facebook::logdevice;
namespace {
// Helper method, extracts a Settings instance from ClientSettings
static UpdateableSettings<Settings> settings(const ClientSettings& inst) {
  return static_cast<const ClientSettingsImpl&>(inst).getSettings();
}
static UpdateableSettings<Settings>
settings(const std::unique_ptr<ClientSettings>& ptr) {
  return settings(*ptr);
}

static std::shared_ptr<Client>
create_client(std::unique_ptr<ClientSettings>&& settings) {
  // NOTE: assumes test is being run from top-level fbcode dir
  std::string config_path =
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf");
  return ClientFactory()
      .setClientSettings(std::move(settings))
      .create(config_path);
}
} // namespace

TEST(ClientSettingsTest, Basic) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("enable-logsconfig-manager", "false");
  ASSERT_EQ(0, client_settings->set("sendbuf-kb", "123"));
  ASSERT_EQ(123, settings(client_settings)->tcp_sendbuf_kb);

  auto client = create_client(std::move(client_settings));

  // Check that we can change settings on an existing Client
  client->settings().set("sendbuf-kb", "456");
  ASSERT_EQ(456, settings(client->settings())->tcp_sendbuf_kb);

  // Check that the int overload works
  client->settings().set("sendbuf-kb", 789);
  ASSERT_EQ(789, settings(client->settings())->tcp_sendbuf_kb);
}

/**
 * Should be able to create a Client with nullptr settings
 */
TEST(ClientSettingsTest, Default) {
  auto client = create_client(nullptr);
  ASSERT_EQ(-1, settings(client->settings())->tcp_sendbuf_kb);
}

TEST(ClientSettingsTest, Errors) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  EXPECT_EQ(-1, client_settings->set("blahinvalidoption", "foo"));
  EXPECT_EQ(E::UNKNOWN_SETTING, err);

  EXPECT_EQ(-1, client_settings->set(" blah invalid option", "foo"));
  EXPECT_EQ(E::UNKNOWN_SETTING, err);

  // Value should be numeric
  EXPECT_EQ(-1, client_settings->set("sendbuf-kb", "foo"));
  EXPECT_EQ(E::INVALID_PARAM, err);
}

/**
 * on-demand-logs-config should force an alternative logs config to be used
 * instead a local one
 */
TEST(ClientSettingsTest, OnDemandLogsConfig) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  client_settings->set("on-demand-logs-config", true);
  EXPECT_TRUE(settings(client_settings)->on_demand_logs_config);

  auto client = create_client(std::move(client_settings));
  const ClientImpl* clientImpl = static_cast<ClientImpl*>(client.get());
  // A RemoteLogsConfig should be used
  auto config = clientImpl->getConfig().get();
  EXPECT_FALSE(config->get()->logsConfig()->isLocal());
}

/**
 * verifies that a setting updated via ClientSettings::set gets propagated to
 * the client workers
 */
TEST(ClientSettingsTest, MethodUpdateableSettings) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  // A local copy we keep to set updateable settings on
  std::unique_ptr<ClientSettings> local_copy(new ClientSettingsImpl(
      dynamic_cast<ClientSettingsImpl&>(*client_settings)));
  ASSERT_TRUE((bool)local_copy.get());

  ASSERT_EQ(0, client_settings->set("checksum-bits", "32"));
  EXPECT_EQ(32, settings(client_settings)->checksum_bits);

  auto client = create_client(std::move(client_settings));
  ClientImpl* clientImpl = static_cast<ClientImpl*>(client.get());

  std::vector<int> settings_read =
      run_on_all_workers(&clientImpl->getProcessor(),
                         [&]() { return Worker::settings().checksum_bits; });

  ASSERT_EQ(
      settings_read.size(), clientImpl->getProcessor().getAllWorkersCount());
  for (int i = 0; i < settings_read.size(); ++i) {
    ASSERT_EQ(32, settings_read[i]);
  }

  // Updating the setting live in the local copy
  ASSERT_EQ(0, local_copy->set("checksum-bits", "64"));
  EXPECT_EQ(64,
            dynamic_cast<ClientSettingsImpl&>(clientImpl->settings())
                .getSettings()
                ->checksum_bits);
  settings_read = run_on_all_workers(&clientImpl->getProcessor(), [&]() {
    return Worker::settings().checksum_bits;
  });

  ASSERT_EQ(
      settings_read.size(), clientImpl->getProcessor().getAllWorkersCount());
  for (int i = 0; i < settings_read.size(); ++i) {
    ASSERT_EQ(64, settings_read[i]);
  }
}

/**
 * verifies that a setting updated via ClientSettings::set gets propagated to
 * the client workers
 */
TEST(ClientSettingsTest, ConfigUpdateableSettings) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  // Copying the config to a temporary location so we can modify it later
  char configPath[] = "/tmp/ClientSettingsTest.conf.XXXXXX";
  int fd = mkstemp(configPath);
  ASSERT_NE(fd, -1);
  close(fd);

  SCOPE_EXIT {
    unlink(configPath);
  };
  {
    std::ifstream src(TEST_CONFIG_FILE("sample_no_ssl.conf"), std::ios::binary);
    std::ofstream dst(configPath, std::ios::binary);
    dst << src.rdbuf();
  }

  auto client = ClientFactory()
                    .setSetting("enable-logsconfig-manager", "false")
                    .setSetting("checksum-bits", "32")
                    .setSetting("file-config-update-interval", "10ms")
                    .create(std::string("file:") + configPath);
  ClientImpl* clientImpl = dynamic_cast<ClientImpl*>(client.get());
  ASSERT_TRUE((bool)clientImpl);
  EXPECT_EQ(32, settings(client->settings())->checksum_bits);

  std::vector<int> settings_read =
      run_on_all_workers(&clientImpl->getProcessor(),
                         [&]() { return Worker::settings().checksum_bits; });

  ASSERT_EQ(
      settings_read.size(), clientImpl->getProcessor().getAllWorkersCount());
  for (int i = 0; i < settings_read.size(); ++i) {
    ASSERT_EQ(32, settings_read[i]);
  }

  // Setting up the config update listener on the client
  Semaphore sem;
  auto handle = client->subscribeToConfigUpdates([&]() { sem.post(); });

  // Updating the setting via modifying the config
  auto config = Configuration::fromJsonFile(configPath);
  ASSERT_TRUE((bool)config);
  auto& server_settings_config = const_cast<ServerConfig::SettingsConfig&>(
      config->serverConfig()->getServerSettingsConfig());
  auto& client_settings_config = const_cast<ServerConfig::SettingsConfig&>(
      config->serverConfig()->getClientSettingsConfig());
  server_settings_config["checksum-bits"] = "0";
  client_settings_config["checksum-bits"] = "64";
  overwriteConfigFile(configPath, config->toString());

  // Waiting for config to be updated
  sem.wait(); // ServerConfig
  sem.wait(); // LogsConfig

  EXPECT_EQ(64,
            dynamic_cast<ClientSettingsImpl&>(clientImpl->settings())
                .getSettings()
                ->checksum_bits);
  settings_read = run_on_all_workers(&clientImpl->getProcessor(), [&]() {
    return Worker::settings().checksum_bits;
  });

  ASSERT_EQ(
      settings_read.size(), clientImpl->getProcessor().getAllWorkersCount());
  for (int i = 0; i < settings_read.size(); ++i) {
    ASSERT_EQ(64, settings_read[i]);
  }
}

// Test getting a setting from the ClientSettings itself
TEST(ClientSettingsTest, GetConfig) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  ASSERT_EQ(0, client_settings->set("sendbuf-kb", "123"));
  EXPECT_EQ(123, settings(client_settings)->tcp_sendbuf_kb);
  EXPECT_EQ("123", client_settings->get("sendbuf-kb"));
}

// Test getting a setting that doesn't exist from the ClientSettings
TEST(ClientSettingsTest, GetConfigNonExistentSetting) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  EXPECT_FALSE(client_settings->get("thisisanonexistentsetting"));
}

static bool
isInList(std::pair<std::string, std::string> pair,
         const std::vector<std::pair<std::string, std::string>>& settings) {
  return std::find(settings.begin(), settings.end(), pair) != settings.end();
}

TEST(ClientSettingsTest, GetAllSettings) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  ASSERT_EQ(0, client_settings->set("sendbuf-kb", "123"));
  ASSERT_EQ(0, client_settings->set("checksum-bits", "64"));
  ASSERT_EQ(0, client_settings->set("file-config-update-interval", "10ms"));

  const auto settings = client_settings->getAll();
  EXPECT_TRUE(isInList(std::make_pair("sendbuf-kb", "123"), settings));
  EXPECT_TRUE(isInList(std::make_pair("checksum-bits", "64"), settings));
  EXPECT_TRUE(isInList(
      std::make_pair("file-config-update-interval", "10ms"), settings));
}

TEST(ClientSettingsTest, GetDefaultValue) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  EXPECT_EQ("false", client_settings->get("server"));
  EXPECT_EQ("cores", client_settings->get("num-workers"));
}

TEST(ClientSettingsTest, ParseNumWorkers) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());

  // Invalid test cases
  ASSERT_EQ(-1, client_settings->set("num-workers", "derps*1.4"));
  ASSERT_EQ(-1, client_settings->set("num-workers", "ncores*abc"));
  ASSERT_EQ(-1, client_settings->set("num-workers", "*1.4"));
  ASSERT_EQ(-1, client_settings->set("num-workers", "cores*"));

  // Valid test case
  ASSERT_EQ(0, client_settings->set("num-workers", "4"));
  EXPECT_EQ(4, settings(client_settings)->num_workers);
  ASSERT_EQ(0, client_settings->set("num-workers", "ncores"));
  EXPECT_EQ(std::min<int>(boost::thread::physical_concurrency(), MAX_WORKERS),
            settings(client_settings)->num_workers);
  ASSERT_EQ(0, client_settings->set("num-workers", "cores*1.4"));
  EXPECT_EQ(
      std::min<int>(boost::thread::physical_concurrency() * 1.4, MAX_WORKERS),
      settings(client_settings)->num_workers);
  ASSERT_EQ(0, client_settings->set("num-workers", "cores*0.5"));
  EXPECT_EQ(
      std::min<int>(boost::thread::physical_concurrency() * 0.5, MAX_WORKERS),
      settings(client_settings)->num_workers);
}
