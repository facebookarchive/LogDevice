/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/common/LogsConfigApiRequest.h"
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"
#include "logdevice/common/configuration/logs/LogsConfigDeltaTypes.h"
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::logsconfig;

class LogsConfigIntegrationTest : public IntegrationTestBase {};

class ParametrizedLogsConfigIntegrationTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<bool /*disable_get_cluster_state*/> {
};

TEST_P(ParametrizedLogsConfigIntegrationTest, ConnectionHandling) {
  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_singleWriter(false);
  internal_log_attrs.set_replicationFactor(3);
  internal_log_attrs.set_extraCopies(0);
  internal_log_attrs.set_syncedCopies(0);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .setConfigLogAttributes(internal_log_attrs)
                     .create(5);

  std::unique_ptr<ClientSettings> settings(ClientSettings::create());
  if (GetParam()) {
    settings->set("enable-initial-get-cluster-state", "false");
  }
  settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      std::chrono::seconds(60), std::move(settings));
  std::unique_ptr<client::Directory> dir = client->makeDirectorySync(
      "/my_logs", false, client::LogAttributes().with_replicationFactor(22));
  ASSERT_NE(nullptr, dir);

  cluster->shutdownNodes({1, 2});
  // We are not trying to change the tree because we cannot activate the
  // internal logs sequencer if we lost the majority of the cluster
  wait_until("Directory is visible on another node", [&]() {
    std::unique_ptr<client::Directory> dir2 =
        client->getDirectorySync("/my_logs");
    return bool(dir2);
  });
}

INSTANTIATE_TEST_CASE_P(ParametrizedLogsConfigIntegrationTest,
                        ParametrizedLogsConfigIntegrationTest,
                        ::testing::Bool());

TEST_F(LogsConfigIntegrationTest, NotSupported) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(2);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_EQ(nullptr, dir);
  ASSERT_EQ(E::NOTSUPPORTED, err);
}

TEST_F(LogsConfigIntegrationTest, MakeDirectory) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  std::unique_ptr<client::Directory> dir = client->makeDirectorySync(
      "/my_logs", false, client::LogAttributes().with_replicationFactor(22));

  ASSERT_NE(nullptr, dir);
  ASSERT_EQ("/my_logs/", dir->getFullyQualifiedName());
  ASSERT_EQ(22, dir->attrs().replicationFactor().value());

  auto dir2 =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_EQ(E::EXISTS, err);
  ASSERT_EQ(nullptr, dir2);

  auto dir3 = client->makeDirectorySync("/my_logs/test_logs/another_one", true);

  ASSERT_NE(nullptr, dir3);
  ASSERT_EQ("another_one", dir3->name());
  ASSERT_EQ("/my_logs/test_logs/another_one/", dir3->getFullyQualifiedName());
  ASSERT_TRUE(dir3->version() > dir->version());
  ASSERT_EQ(22, dir3->attrs().replicationFactor().value());
}

TEST_F(LogsConfigIntegrationTest, MakeLogGroup) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  std::unique_ptr<client::LogGroup> lg1 = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(100)),
      client::LogAttributes().with_replicationFactor(2),
      false);
  ASSERT_NE(nullptr, lg1);
  ASSERT_EQ(2, lg1->attrs().replicationFactor().value());

  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));

  auto lg2 = client->makeLogGroupSync(
      "/my_logs/log1",
      logid_range_t(logid_t(200), logid_t(400)),
      client::LogAttributes().with_replicationFactor(2),
      false);
  ASSERT_NE(nullptr, lg2);
  ASSERT_TRUE(lg2->version() > lg1->version());
  client->syncLogsConfigVersion(lg2->version());
  std::unique_ptr<client::Directory> dir = client->getDirectorySync("/");

  ASSERT_NE(nullptr, dir);
  ASSERT_EQ(1, dir->logs().size());
  ASSERT_EQ(1, dir->children().size());
  auto log_group = dir->logs().at("log1");
  ASSERT_NE(nullptr, log_group);
  ASSERT_EQ(logid_range_t(logid_t(1), logid_t(100)), log_group->range());
  ASSERT_EQ(2, log_group->attrs().replicationFactor().value());
  ASSERT_EQ("/log1", log_group->getFullyQualifiedName());
  auto subdir = dir->children().at("my_logs").get();
  ASSERT_EQ(1, subdir->logs().size());
  ASSERT_EQ("/my_logs/", subdir->getFullyQualifiedName());
  ASSERT_EQ(
      "/my_logs/log1", subdir->logs().at("log1")->getFullyQualifiedName());
}

TEST_F(LogsConfigIntegrationTest, Rename) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  std::unique_ptr<client::Directory> dir = client->makeDirectorySync(
      "/my_logs", false, client::LogAttributes().with_replicationFactor(22));
  ASSERT_NE(nullptr, dir);
  std::unique_ptr<client::LogGroup> lg = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(100)),
      client::LogAttributes().with_replicationFactor(2),
      false);
  ASSERT_NE(nullptr, lg);

  // cannot rename root
  ASSERT_FALSE(client->renameSync("/", "/test"));
  ASSERT_FALSE(client->renameSync("/my_logs", "/"));

  // NOT FOUND
  ASSERT_FALSE(client->renameSync("/test", "/test1"));
  ASSERT_EQ(E::NOTFOUND, err);

  // EXISTS
  ASSERT_FALSE(client->renameSync("/my_logs", "/log1"));
  ASSERT_EQ(E::EXISTS, err);

  // OK
  ASSERT_TRUE(client->renameSync("/log1", "/log2"));

  auto lg1 = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(200), logid_t(400)),
      client::LogAttributes().with_replicationFactor(3),
      false);
  ASSERT_NE(nullptr, lg1);

  auto lg2 = client->makeLogGroupSync(
      "/log2",
      logid_range_t(logid_t(600), logid_t(800)),
      client::LogAttributes().with_replicationFactor(3),
      false);
  ASSERT_EQ(nullptr, lg2);
  ASSERT_EQ(E::EXISTS, err);

  std::unique_ptr<client::LogGroup> log2_back =
      client->getLogGroupSync("/log2");

  ASSERT_NE(nullptr, log2_back);
  ASSERT_EQ("/log2", log2_back->getFullyQualifiedName());
  ASSERT_EQ(logid_range_t(logid_t(1), logid_t(100)), log2_back->range());
  ASSERT_EQ(2, log2_back->attrs().replicationFactor().value());
}

TEST_F(LogsConfigIntegrationTest, Remove) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  uint64_t version;
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs/your_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));
  ASSERT_NE(nullptr,
            client->makeLogGroupSync(
                "/log1",
                logid_range_t(logid_t(1), logid_t(100)),
                client::LogAttributes().with_replicationFactor(2),
                false));
  ASSERT_NE(nullptr,
            client->makeLogGroupSync(
                "/my_logs/your_logs/log1",
                logid_range_t(logid_t(101), logid_t(120)),
                client::LogAttributes().with_replicationFactor(2),
                false));

  ASSERT_FALSE(client->removeDirectorySync("/my_logs/your_logs", false));
  ASSERT_TRUE(
      client->removeDirectorySync("/my_logs/your_logs", true, &version));
  client->syncLogsConfigVersion(version);
  ASSERT_EQ(nullptr, client->getDirectorySync("/my_logs/your_logs"));

  ASSERT_FALSE(client->removeLogGroupSync("/log5", &version));
  client->syncLogsConfigVersion(version);

  ASSERT_TRUE(client->removeLogGroupSync("/log1", &version));
  client->syncLogsConfigVersion(version);
  ASSERT_EQ(nullptr, client->getLogGroupSync("/log1"));
}

TEST_F(LogsConfigIntegrationTest, Defaults) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  ASSERT_EQ(logsconfig::DefaultLogAttributes().maxWritesInFlight().value(),
            client->getDirectorySync("/")->attrs().maxWritesInFlight().value());

  auto my_logs = client->makeDirectorySync(
      "/my_logs", false, client::LogAttributes().with_replicationFactor(22));
  ASSERT_NE(nullptr, my_logs);
  client->syncLogsConfigVersion(my_logs->version());

  auto dir1 = client->getDirectorySync("/my_logs");
  ASSERT_NE(nullptr, dir1);
  ASSERT_EQ(logsconfig::DefaultLogAttributes().maxWritesInFlight().value(),
            dir1->attrs().maxWritesInFlight().value());
  ASSERT_EQ(22, dir1->attrs().replicationFactor().value());

  auto my_logs_log1 = client->makeLogGroupSync(
      "/my_logs/log1",
      logid_range_t(logid_t(101), logid_t(120)),
      client::LogAttributes().with_replicationFactor(2),
      false);
  ASSERT_NE(nullptr, my_logs_log1);
  client->syncLogsConfigVersion(my_logs_log1->version());
  auto lg1 = client->getLogGroupSync("/my_logs/log1");
  ASSERT_NE(nullptr, lg1);

  ASSERT_EQ(logsconfig::DefaultLogAttributes().maxWritesInFlight().value(),
            lg1->attrs().maxWritesInFlight().value());
  ASSERT_EQ(2, lg1->attrs().replicationFactor().value());

  auto my_logs_log2 =
      client->makeLogGroupSync("/my_logs/duper/log2",
                               logid_range_t(logid_t(201), logid_t(220)),
                               client::LogAttributes(),
                               true);
  ASSERT_NE(nullptr, my_logs_log2);
  client->syncLogsConfigVersion(my_logs_log2->version());

  auto dir2 = client->getDirectorySync("/my_logs/duper");
  ASSERT_NE(nullptr, dir2);
  ASSERT_EQ(22, dir2->attrs().replicationFactor().value());
  ASSERT_TRUE(dir2->attrs().replicationFactor().isInherited());
  auto lg2 = client->getLogGroupSync("/my_logs/duper/log2");
  ASSERT_NE(nullptr, lg2);
  ASSERT_EQ(22, lg2->attrs().replicationFactor().value());
}

TEST_F(LogsConfigIntegrationTest, SetAttributes) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));
  ASSERT_NE(nullptr, client->makeDirectorySync("/my_logs/your_logs", false));
  ASSERT_NE(
      nullptr,
      client->makeLogGroupSync("/my_logs/your_logs/log1",
                               logid_range_t(logid_t(101), logid_t(120))));

  uint64_t version;
  ASSERT_TRUE(client->setAttributesSync(
      "/my_logs", client::LogAttributes().with_replicationFactor(3), &version));
  client->syncLogsConfigVersion(version);
  ASSERT_EQ(3,
            client->getDirectorySync("/my_logs")
                ->attrs()
                .replicationFactor()
                .value());

  ASSERT_EQ(3,
            client->getDirectorySync("/my_logs/your_logs")
                ->attrs()
                .replicationFactor()
                .value());

  ASSERT_TRUE(client->setAttributesSync(
      "/my_logs/your_logs/log1",
      client::LogAttributes().with_replicationFactor(3),
      &version));
  client->syncLogsConfigVersion(version);

  ASSERT_EQ(3,
            client->getLogGroupSync("/my_logs/your_logs/log1")
                ->attrs()
                .replicationFactor()
                .value());
}

TEST_F(LogsConfigIntegrationTest, SetLogRange) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));
  ASSERT_NE(nullptr, client->makeDirectorySync("/my_logs/your_logs", false));
  ASSERT_NE(
      nullptr,
      client->makeLogGroupSync("/my_logs/your_logs/log1",
                               logid_range_t(logid_t(101), logid_t(120))));
  ASSERT_NE(
      nullptr,
      client->makeLogGroupSync("/my_logs/your_logs/log2",
                               logid_range_t(logid_t(150), logid_t(180))));

  ASSERT_FALSE(client->setLogGroupRangeSync(
      "/my_logs/your_logs/log1", logid_range_t(logid_t(101), logid_t(220))));
  ASSERT_EQ(E::ID_CLASH, err);

  uint64_t version;
  ASSERT_TRUE(
      client->setLogGroupRangeSync("/my_logs/your_logs/log1",
                                   logid_range_t(logid_t(101), logid_t(149)),
                                   &version));
  client->syncLogsConfigVersion(version);

  auto lg = client->getLogGroupSync("/my_logs/your_logs/log1");
  ASSERT_NE(nullptr, lg);
  ASSERT_EQ(logid_t(101), lg->range().first);
  ASSERT_EQ(logid_t(149), lg->range().second);
}

std::string random_string(size_t length) {
  const std::string charset = "0123456789"
                              "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                              "abcdefghijklmnopqrstuvwxyz";
  auto randchar = [&]() -> char {
    const size_t max_index = (charset.length() - 1);
    return charset[folly::Random::rand32(max_index)];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

TEST_F(LogsConfigIntegrationTest, TooBigConfig) {
  LogAttributes::ExtrasMap extras;
  extras["clowntown"] = random_string(4 * 1024 * 1024);
  LogAttributes log_attrs =
      DefaultLogAttributes().with_replicationFactor(2).with_extras(extras);

  auto clusterFactory =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().setParam(
          "--logsconfig-snapshotting", "false");

  clusterFactory.useDefaultTrafficShapingConfig(false);
  auto cluster = clusterFactory.create(2);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "false");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));

  for (int i = 0; i < 30; i++) {
    std::string name =
        "/this is a pretty big log name that has a number " + std::to_string(i);
    ASSERT_NE(
        nullptr,
        client->makeLogGroupSync(std::move(name),
                                 logid_range_t(logid_t(i + 1), logid_t(i + 1)),
                                 log_attrs));
  }

  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_EQ(30, root1->logs().size());
}

TEST_F(LogsConfigIntegrationTest, ClientTest) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ClientImpl* raw_client = static_cast<ClientImpl*>(client.get());
  auto updateable_config = raw_client->getConfig();
  auto local_config1 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(5, local_config1->size());
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22)));
  ASSERT_NE(nullptr, client->makeDirectorySync("/my_logs/your_logs", false));
  auto lg = client->makeLogGroupSync(
      "/my_logs/your_logs/log1", logid_range_t(logid_t(101), logid_t(120)));
  ASSERT_NE(nullptr, lg);

  ASSERT_TRUE(client->syncLogsConfigVersion(lg->version()));
  auto local_config2 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(25, local_config2->size());
}

TEST_F(LogsConfigIntegrationTest, notifyOnLogsConfigVersionTest) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", true));
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ClientImpl* raw_client = static_cast<ClientImpl*>(client.get());
  auto updateable_config = raw_client->getConfig();
  // give it a couple of seconds to recover
  auto local_config1 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(5, local_config1->size());
  for (int i = 0; i < 100; i++) {
    std::string prefix("my_logs");
    auto dir = client->makeDirectorySync(
        prefix + std::to_string(i),
        false,
        client::LogAttributes().with_replicationFactor(22));
    ASSERT_NE(nullptr, dir);
    ASSERT_TRUE(client->syncLogsConfigVersion(dir->version()));
  }
}

TEST_F(LogsConfigIntegrationTest, TextConfigUpdaterIsDisabled) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_with_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .setParam("--file-config-update-interval", "10ms")
                     .deferStart()
                     .create(*config);
  uint64_t version;
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_EQ(0, root1->logs().size());
  ASSERT_EQ(1, root1->version());
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_NE(nullptr, dir);

  auto your_logs = client->makeDirectorySync("/my_logs/your_logs", false);
  ASSERT_NE(nullptr, your_logs);
  client->syncLogsConfigVersion(your_logs->version());
  auto root2 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root2);
  ASSERT_TRUE(root2->version() > root1->version());
  ASSERT_EQ(1, root2->children().count("my_logs"));
  // Let's do some changes to the Logs section in the config file
  Semaphore sem;
  auto handle = client->subscribeToConfigUpdates([&]() { sem.post(); });

  auto original_log_group = config->localLogsConfig()->getLogGroup("/sublog1");
  ASSERT_NE(nullptr, original_log_group);
  config->localLogsConfig()->replaceLogGroup(
      "/sublog1",
      original_log_group->withLogAttributes(
          client::LogAttributes().with_replicationFactor(2)));
  cluster->writeConfig(cluster->getConfig()->get()->serverConfig().get(),
                       config->logsConfig().get(),
                       false);
  ld_info("Waiting for Client to pick up the config changes");
  sem.wait();
  // one more change
  config->localLogsConfig()->replaceLogGroup(
      "/sublog1",
      original_log_group->withLogAttributes(
          client::LogAttributes().with_replicationFactor(3)));
  cluster->writeConfig(cluster->getConfig()->get()->serverConfig().get(),
                       config->logsConfig().get(),
                       false);

  ld_info("Waiting for Client to pick up the config changes");
  sem.wait();

  auto root3 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root3);
  ASSERT_EQ(root2->version(), root3->version());
  ASSERT_EQ(1, root3->children().count("my_logs"));
  // Make sure that the client-loaded RSM also has the correct config version
  ClientImpl* raw_client = static_cast<ClientImpl*>(client.get());
  auto updateable_config = raw_client->getConfig();
  auto local_config1 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(root3->version(), local_config1->getVersion());
}

// The public facing client API should return NOTFOUND on internal and
// metadata logs. The LogsConfigAPI itself should be able to handle it though
// which is tested in LogsConfigAPIMetadataLogs.
TEST_F(LogsConfigIntegrationTest, GetByIDForInternalLogs) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();

  ASSERT_EQ(
      nullptr, client->getLogGroupByIdSync(logid_t(9223372036854775809ul)));

  ASSERT_EQ(nullptr,
            client->getLogGroupByIdSync(
                configuration::InternalLogs::MAINTENANCE_LOG_DELTAS));
  ASSERT_EQ(E::NOTFOUND, err);
}

TEST_F(LogsConfigIntegrationTest, LogsConfigAPIMetadataLogs) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;
  auto processor = make_test_processor(settings, cluster->getConfig());

  Semaphore sem;
  auto callback = [&](Status st, uint64_t, std::string) {
    ASSERT_EQ(Status::OK, st);
    sem.post();
  };

  std::unique_ptr<Request> req = std::make_unique<LogsConfigApiRequest>(
      LOGS_CONFIG_API_Header::Type::GET_LOG_GROUP_BY_ID,
      std::to_string(9223372036854775809ul),
      std::chrono::seconds(5),
      LogsConfigApiRequest::MAX_ERRORS,
      10,
      callback);

  processor->postRequest(req);
  sem.wait();
}

TEST_F(LogsConfigIntegrationTest, ConfigManagerFromServerSettings) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_with_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--file-config-update-interval", "10ms")
                     .deferStart()
                     .create(*config);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client);
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_EQ(0, root1->logs().size());
}

TEST_F(LogsConfigIntegrationTest, StartServerWithNoLogsSection) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_without_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--file-config-update-interval", "10ms")
                     .deferStart()
                     .create(*config);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_NE(nullptr, dir);
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_LT(1, root1->version());
  ASSERT_EQ(0, root1->logs().size());
}

TEST_F(LogsConfigIntegrationTest, StartClientWithNoLogsSection) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_without_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--file-config-update-interval", "10ms")
                     .enableLogsConfigManager()
                     .deferStart()
                     .create(*config);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_NE(nullptr, dir);
  client->syncLogsConfigVersion(dir->version());
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_EQ(0, root1->logs().size());
  ASSERT_LT(1, root1->version());
  // Make sure that the client-loaded RSM also has the correct config version
  ClientImpl* raw_client = static_cast<ClientImpl*>(client.get());
  client->syncLogsConfigVersion(root1->version());
  auto updateable_config = raw_client->getConfig();
  auto local_config1 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(root1->version(), local_config1->getVersion());
}

TEST_F(LogsConfigIntegrationTest, StartWithNoLogsSectionCLI) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_without_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--file-config-update-interval", "10ms")
                     .enableLogsConfigManager()
                     .deferStart()
                     .create(*config);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_NE(nullptr, dir);
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_LT(1, root1->version());
  ASSERT_EQ(0, root1->logs().size());
}

TEST_F(LogsConfigIntegrationTest, RemoveLogsSection) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("logsconfig_manager_with_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .setParam("--file-config-update-interval", "10ms")
                     .deferStart()
                     .create(*config);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "false");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  auto root1 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root1);
  ASSERT_EQ(1, root1->version());
  ASSERT_EQ(0, root1->logs().size());
  auto dir =
      client->makeDirectorySync("/my_logs", false, client::LogAttributes());
  ASSERT_NE(nullptr, dir);
  auto dir2 = client->makeDirectorySync("/my_logs/your_logs", false);
  ASSERT_NE(nullptr, dir2);
  client->syncLogsConfigVersion(dir2->version());

  auto root2 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root2);
  ASSERT_TRUE(root2->version() > root1->version());
  ASSERT_EQ(1, root2->children().count("my_logs"));
  auto original_log_group = config->localLogsConfig()->getLogGroup("/sublog1");
  ASSERT_NE(nullptr, original_log_group);
  config->localLogsConfig()->replaceLogGroup(
      "/sublog1",
      original_log_group->withLogAttributes(
          client::LogAttributes().with_replicationFactor(2)));

  Semaphore sem;
  auto handle = client->subscribeToConfigUpdates([&]() { sem.post(); });
  cluster->writeConfig(cluster->getConfig()->get()->serverConfig().get(),
                       config->logsConfig().get(),
                       false);
  ld_info("Waiting for Client to pick up the config changes");
  sem.wait();

  auto root3 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root3);
  ASSERT_EQ(root2->version(), root3->version());
  ASSERT_EQ(1, root3->children().count("my_logs"));
  // Make sure that the client-loaded RSM also has the correct config version
  ClientImpl* raw_client = static_cast<ClientImpl*>(client.get());
  auto updateable_config = raw_client->getConfig();
  auto local_config1 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(root2->version(), local_config1->getVersion());

  // Readding an empty logs section to the config shouldn't crash neither the
  // server nor the client
  //
  configuration::LocalLogsConfig empty_logs_config;
  cluster->writeConfig(cluster->getConfig()->get()->serverConfig().get(),
                       &empty_logs_config,
                       false);
  ld_info("Waiting for Client to pick up the config changes");
  sem.wait();

  auto root4 = client->getDirectorySync("/");
  ASSERT_NE(nullptr, root4);
  ASSERT_EQ(root2->version(), root4->version());
  ASSERT_EQ(1, root4->children().count("my_logs"));
  // Make sure that the client-loaded RSM also has the correct config version
  raw_client = static_cast<ClientImpl*>(client.get());
  auto local_config2 = updateable_config->getLocalLogsConfig();
  ASSERT_EQ(root2->version(), local_config2->getVersion());
}

TEST_F(LogsConfigIntegrationTest, LogGroupById) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory().enableLogsConfigManager().create(
          3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();

  LogAttributes::ExtrasMap extras;
  extras["testing_something"] = "1234";
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/my_logs",
                false,
                client::LogAttributes().with_replicationFactor(22).with_extras(
                    extras)));
  ASSERT_NE(nullptr,
            client->makeLogGroupSync(
                "/my_logs/log1",
                logid_range_t(logid_t(1), logid_t(100)),
                client::LogAttributes().with_replicationFactor(2),
                false));

  auto my_logs_log2 =
      client->makeLogGroupSync("/my_logs/log2",
                               logid_range_t(logid_t(201), logid_t(300)),
                               client::LogAttributes(),
                               false);
  ASSERT_NE(nullptr, my_logs_log2);
  client->syncLogsConfigVersion(my_logs_log2->version());

  // Look for non-existing log groups
  auto log_group = client->getLogGroupByIdSync(logid_t(0));
  ASSERT_EQ(nullptr, log_group);
  ASSERT_EQ(E::NOTFOUND, err);
  log_group = client->getLogGroupByIdSync(logid_t(123));
  ASSERT_EQ(nullptr, log_group);
  ASSERT_EQ(E::NOTFOUND, err);

  // Look for lower edge of log groups
  log_group = client->getLogGroupByIdSync(logid_t(1));
  ASSERT_NE(nullptr, log_group);
  ASSERT_EQ("/my_logs/log1", log_group->getFullyQualifiedName());
  ASSERT_EQ(logid_range_t(logid_t(1), logid_t(100)), log_group->range());
  log_group = client->getLogGroupByIdSync(logid_t(201));
  ASSERT_NE(nullptr, log_group);
  ASSERT_EQ("/my_logs/log2", log_group->getFullyQualifiedName());

  // Look in middle/end of the log group
  log_group = client->getLogGroupByIdSync(logid_t(99));
  ASSERT_NE(nullptr, log_group);
  ASSERT_EQ("/my_logs/log1", log_group->getFullyQualifiedName());
  log_group = client->getLogGroupByIdSync(logid_t(100));
  ASSERT_NE(nullptr, log_group);
  ASSERT_EQ("/my_logs/log1", log_group->getFullyQualifiedName());

  // Check if the log flattened its structure
  auto extras_received = log_group->attrs().extras();
  ASSERT_TRUE(extras_received.hasValue());
  ASSERT_EQ("1234", extras_received.value().at("testing_something"));
  ASSERT_EQ(2, log_group->attrs().replicationFactor().value());
  log_group = client->getLogGroupByIdSync(logid_t(250));
  ASSERT_EQ(22, log_group->attrs().replicationFactor().value());
}

TEST_F(LogsConfigIntegrationTest, SetAttributesLogGroupClash) {
  int node_count = 3;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .enableLogsConfigManager()
                     .create(node_count);

  cluster->waitForRecovery();

  for (int node_index = 0; node_index < node_count; ++node_index) {
    IntegrationTestUtils::Node& node = cluster->getNode(node_index);
    node.waitUntilAvailable();
  }

  std::shared_ptr<Client> client = cluster->createClient();

  client->makeLogGroupSync("/testX",
                           logid_range_t(logid_t(1), logid_t(1)),
                           client::LogAttributes().with_replicationFactor(2),
                           false);

  client->setAttributesSync(
      "/", client::LogAttributes().with_replicationFactor(1));

  client->makeLogGroupSync("/testX log " + std::to_string(1),
                           logid_range_t(logid_t(1), logid_t(1)),
                           client::LogAttributes().with_replicationFactor(2),
                           false);
  ASSERT_EQ(E::ID_CLASH, err);

  for (int node_index = 0; node_index < node_count; ++node_index) {
    IntegrationTestUtils::Node& node = cluster->getNode(node_index);
    ASSERT_TRUE(node.isRunning());
  }
}

TEST_F(LogsConfigIntegrationTest, DisableEnableWorkflow) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .setParam("--file-config-update-interval", "10ms")
                     .deferStart()
                     .create(3);
  // provision internal logs
  ASSERT_EQ(0, cluster->provisionEpochMetaData(nullptr, true));
  cluster->start();

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  client_settings->set("on-demand-logs-config", "false");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      DEFAULT_TEST_TIMEOUT, std::move(client_settings));
  ASSERT_NE(nullptr, client->makeDirectorySync("/my_logs1"));

  auto update_lcm_config = [&](bool value) {
    auto config = cluster->getConfig();
    auto json = config->getServerConfig()->withIncrementedVersion()->toJson(
        config->getLogsConfig().get(), config->getZookeeperConfig().get());
    json["server_settings"]["enable-logsconfig-manager"] =
        json["client_settings"]["enable-logsconfig-manager"] =
            value ? "true" : "false";
    json["logs"] = value ? folly::dynamic::array() : nullptr;
    auto new_server_config = ServerConfig::fromJson(json);
    cluster->writeConfig(
        new_server_config.get(), config->getLogsConfig().get(), true);
  };

  update_lcm_config(false);
  std::this_thread::sleep_for(std::chrono::seconds(10));
  update_lcm_config(true);
  std::this_thread::sleep_for(std::chrono::seconds(10));

  err = Status::OK;
  auto dir = client->makeDirectorySync("/my_logs2");
  EXPECT_EQ(Status::OK, err);
  ASSERT_NE(nullptr, dir);
}
