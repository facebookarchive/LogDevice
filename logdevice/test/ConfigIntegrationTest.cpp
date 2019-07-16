/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>
#include <memory>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/EpochMetaDataCache.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/FileEpochStore.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/hash.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/lib/RemoteLogsConfig.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

class ConfigIntegrationTest : public IntegrationTestBase {};

// The LogsConfig is used in ClientReadStream on the Client. Using ReaderTest
// to test it end-to-end.
TEST_F(ConfigIntegrationTest, RemoteLogsConfigTest) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--file-config-update-interval", "10ms")
          // TODO(#8466255): remove.
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::NONE)
          .create(2);

  // Copying the config for the client so that it's not affected when we update
  // the server config
  std::string client_config_path(cluster->getConfigPath() + "_client");
  boost::filesystem::copy_file(cluster->getConfigPath(), client_config_path);

  auto client = ClientFactory()
                    .setSetting("file-config-update-interval", "10ms")
                    .setSetting("on-demand-logs-config", "true")
                    .setTimeout(testTimeout())
                    .create(client_config_path);

  ASSERT_TRUE((bool)client);
  auto client_impl = static_cast<ClientImpl*>(client.get());
  auto config = client_impl->getConfig()->get();

  // Check that on-demand-logs-config forced the alternative_logs_config to be
  // used
  ASSERT_FALSE(config->logsConfig()->isLocal());

  // Fetching by ID
  ld_info("Fetching by ID");
  auto log_cfg = config->getLogGroupByIDShared(logid_t(1));
  ASSERT_TRUE(log_cfg != nullptr);

  ld_info("Fetching by ID 2");
  // Testing the cache - we have to get the same entry for a 2nd request sent
  // in such a short while:
  auto log_cfg2 = config->getLogGroupByIDShared(logid_t(1));
  ASSERT_EQ(log_cfg.get(), log_cfg2.get());

  ld_info("Checking remote attributes");
  ASSERT_EQ("test_logs", log_cfg->name());
  ASSERT_EQ(logid_t(1), log_cfg->range().first);
  ASSERT_EQ(logid_t(2), log_cfg->range().second);

  ld_info("Fetching non-existent ID");

  // Fetching non-existent log ID
  log_cfg = config->getLogGroupByIDShared(logid_t(1234567890));
  ASSERT_EQ(nullptr, log_cfg);

  // Fetching by name
  ld_info("Fetching by name");

  std::pair<logid_t, logid_t> range =
      config->logsConfig()->getLogRangeByName("ns/test_logs");
  ASSERT_NE(LSN_INVALID, range.first.val_);
  ASSERT_NE(LSN_INVALID, range.second.val_);

  // By absolute path
  range = config->logsConfig()->getLogRangeByName("/ns/test_logs");
  ASSERT_NE(LSN_INVALID, range.first.val_);
  ASSERT_NE(LSN_INVALID, range.second.val_);

  std::unique_ptr<client::LogGroup> log_group =
      client->getLogGroupSync("ns/test_logs");
  ASSERT_TRUE(log_group);
  ASSERT_EQ(range, log_group->range());
  ASSERT_EQ("test_logs", log_group->name());
  ASSERT_EQ(log_cfg2->attrs().replicationFactor().asOptional(),
            log_group->attrs().replicationFactor().asOptional());
  ASSERT_EQ(log_cfg2->attrs().maxWritesInFlight(),
            log_group->attrs().maxWritesInFlight());

  ld_info("Async fetching by name");
  Semaphore sem;
  Status st;
  std::pair<logid_t, logid_t> range2;
  config->logsConfig()->getLogRangeByNameAsync(
      "ns/test_logs", [&](Status err, decltype(range) r) {
        st = err;
        range2 = r;
        sem.post();
      });
  sem.wait();

  ASSERT_EQ(E::OK, st);
  ASSERT_EQ(range, range2);

  ld_info("Fetching by name 2");
  // Repeating fetch to validate cache
  range2 = config->logsConfig()->getLogRangeByName("ns/test_logs");
  ASSERT_EQ(range, range2);

  ld_info("Fetching non-existent group by name");
  // Fetching non-existent log group
  auto range3 =
      config->logsConfig()->getLogRangeByName("non_existent_log_group_name");
  ASSERT_EQ(LSN_INVALID, range3.first.val_);
  ASSERT_EQ(LSN_INVALID, range3.second.val_);

  ld_info("Async fetching non-existent group by name");
  config->logsConfig()->getLogRangeByNameAsync(
      "non_existent_log_group_name", [&](Status err, decltype(range3) r) {
        st = err;
        range3 = r;
        sem.post();
      });
  sem.wait();
  ASSERT_EQ(E::NOTFOUND, st);
  ASSERT_EQ(LSN_INVALID, range3.first.val_);
  ASSERT_EQ(LSN_INVALID, range3.second.val_);

  ld_info("Fetching all named logs");
  // Fetching all named logs
  auto ranges = config->logsConfig()->getLogRangesByNamespace("");
  ASSERT_EQ(1, ranges.size());
  ASSERT_EQ("/ns/test_logs", ranges.begin()->first);
  ASSERT_EQ(range, ranges.begin()->second);

  ld_info("Fetching by namespace");
  // Fetching logs under namespace ns
  auto ranges2 = config->logsConfig()->getLogRangesByNamespace("ns");
  ASSERT_EQ(ranges, ranges2);

  ld_info("Async fetching by namespace");
  config->logsConfig()->getLogRangesByNamespaceAsync(
      "ns", [&](Status err, decltype(ranges2) r) {
        st = err;
        ranges2 = r;
        sem.post();
      });
  sem.wait();
  ASSERT_EQ(E::OK, st);
  ASSERT_EQ(ranges, ranges2);
  // Fetching again - the result should come from cache
  ld_info("Sync fetching by namespace 2");
  auto ranges3 = config->logsConfig()->getLogRangesByNamespace("ns");
  ASSERT_EQ(ranges, ranges3);

  // Fetching logs under non-existent namespace
  ld_info("Fetching non-existent namespace");
  ranges = config->logsConfig()->getLogRangesByNamespace("non_existent_ns");
  ASSERT_EQ(0, ranges.size());

  // Fetching logs under non-existent namespace
  ld_info("Async fetching non-existent namespace");
  config->logsConfig()->getLogRangesByNamespaceAsync(
      "non_existent_ns", [&](Status err, decltype(ranges) r) {
        st = err;
        ranges = r;
        sem.post();
      });
  sem.wait();
  ASSERT_EQ(E::NOTFOUND, st);
  ASSERT_EQ(0, ranges.size());

  // Modifying the config and writing it to the nodes in the cluster
  ld_info("Modifying the config");
  auto logs_config_changed =
      cluster->getConfig()->getLocalLogsConfig()->copyLocal();
  auto& logs =
      const_cast<logsconfig::LogMap&>(logs_config_changed->getLogMap());
  auto log_in_directory = logs.begin()->second;
  ASSERT_TRUE(logs_config_changed->replaceLogGroup(
      log_in_directory.getFullyQualifiedName(),
      log_in_directory.log_group->withName("modified_range_name")));

  ld_info("Waiting for config change");
  Semaphore config_change_sem;
  auto handle = client->subscribeToConfigUpdates([&]() {
    ld_info("Posting to config_change_sem");
    config_change_sem.post();
  });
  cluster->writeLogsConfig(logs_config_changed.get());

  // Waiting for the client to get a CONFIG_CHANGED message from the server -
  config_change_sem.wait();
  // only the logs config should have changed. check that the callback didn't
  // get called twice
  ASSERT_EQ(0, config_change_sem.value());

  auto config2 = client_impl->getConfig()->get();
  ASSERT_NE(config, config2);

  ld_info("Fetching log range by name");
  // This time the log config should be updated, as RemoteLogsConfig had to be
  // notified of the config change
  auto changed_range = config2->logsConfig()->getLogRangeByName("ns/test_logs");
  ASSERT_EQ(LSN_INVALID, changed_range.first.val_);
  ASSERT_EQ(LSN_INVALID, changed_range.second.val_);

  ld_info("Fetching modified log range by name");
  changed_range =
      config2->logsConfig()->getLogRangeByName("ns/modified_range_name");
  ASSERT_EQ(range, changed_range);

  auto get_target_node_id = [](Configuration* config) -> node_index_t {
    auto rlc =
        std::dynamic_pointer_cast<const RemoteLogsConfig>(config->logsConfig());
    auto target_node_info = rlc->getTargetNodeInfo();
    ld_check(target_node_info);
    NodeID target_node_id;
    {
      std::lock_guard<std::mutex> lock(target_node_info->mutex_);
      target_node_id = target_node_info->node_id_;
    }
    uint16_t index = target_node_id.index();
    return index;
  };

  ld_info("Killing the target node");
  // Killing the target node to force us to select another one
  auto node_index2 = get_target_node_id(config2.get());
  ASSERT_GE(node_index2, 0);
  ASSERT_LT(node_index2, 2);
  cluster->getNode(node_index2).kill();

  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Checking that the config had been reloaded
  auto config3 = client_impl->getConfig()->get();
  ASSERT_NE(config2, config3);

  // Requesting config info to trigger a reconnection
  ld_info("Request log range by name");
  config2->logsConfig()->getLogRangeByName("ns/test_logs");

  // Checking that now we are subscribed to another node
  auto node_index3 = get_target_node_id(config3.get());
  ASSERT_GE(node_index3, 0);
  ASSERT_LT(node_index3, 2);
  ASSERT_NE(node_index2, node_index3);

  // Checking that the config fetching still works
  ld_info("Request log range by name again");
  auto fetched_range =
      config2->logsConfig()->getLogRangeByName("ns/modified_range_name");
  ASSERT_EQ(changed_range, fetched_range);

  ld_info("Testing internal logs");
  log_cfg = config2->getLogGroupByIDShared(
      configuration::InternalLogs::CONFIG_LOG_DELTAS);
  ASSERT_TRUE(log_cfg != nullptr);
  ASSERT_EQ(1, log_cfg->attrs().replicationFactor().value());
}

TEST_F(ConfigIntegrationTest, RemoteLogsConfigWithLogsConfigManagerTest) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .enableLogsConfigManager()
                     .create(3);
  std::shared_ptr<Client> client = cluster->createIndependentClient();

  ld_info("Creating directories and log groups");
  ASSERT_NE(
      nullptr,
      client->makeDirectorySync(
          "/dir", false, client::LogAttributes().with_replicationFactor(5)));
  ASSERT_NE(nullptr,
            client->makeDirectorySync(
                "/dir/subdir",
                false,
                client::LogAttributes().with_replicationFactor(6)));
  ASSERT_NE(nullptr,
            client->makeLogGroupSync(
                "/dir/subdir/log1",
                logid_range_t(logid_t(1), logid_t(100)),
                client::LogAttributes().with_replicationFactor(2),
                false));
  ASSERT_NE(nullptr,
            client->makeLogGroupSync("/dir/subdir/log2",
                                     logid_range_t(logid_t(201), logid_t(300)),
                                     client::LogAttributes(),
                                     false));

  // Create a second client with on-demand-logs-config
  std::string client_config_path(cluster->getConfigPath() + "_client");
  boost::filesystem::copy_file(cluster->getConfigPath(), client_config_path);
  auto client2 = ClientFactory()
                     .setSetting("file-config-update-interval", "10ms")
                     .setSetting("on-demand-logs-config", "true")
                     .setTimeout(testTimeout())
                     .create(client_config_path);
  ASSERT_TRUE((bool)client2);
  auto client_impl = static_cast<ClientImpl*>(client2.get());
  auto config = client_impl->getConfig()->get();

  ASSERT_FALSE(config->logsConfig()->isLocal());

  ld_info("Fetching by ID");
  auto log_cfg = config->getLogGroupByIDShared(logid_t(1));
  ASSERT_TRUE(log_cfg != nullptr);
  ASSERT_EQ(2, log_cfg->attrs().replicationFactor().value());

  ld_info("Fetching by namespace");
  auto ranges = config->logsConfig()->getLogRangesByNamespace("");
  ASSERT_EQ(2, ranges.size());
  ASSERT_EQ("/dir/subdir/log1", ranges.begin()->first);
  ASSERT_EQ(logid_range_t(logid_t(1), logid_t(100)), ranges.begin()->second);
  ASSERT_EQ("/dir/subdir/log2", (++ranges.begin())->first);
  ASSERT_EQ(
      logid_range_t(logid_t(201), logid_t(300)), (++ranges.begin())->second);

  ld_info("Fetching by log group name");
  std::pair<logid_t, logid_t> range =
      config->logsConfig()->getLogRangeByName("/dir/subdir/log2");
  ASSERT_EQ(201, range.first.val());
  ASSERT_EQ(300, range.second.val());

  // Update a log group and see whether we can read the change
  // Wait for the CONFIG_CHANGED to propagate
  Semaphore config_change_sem;
  auto handle = client2->subscribeToConfigUpdates([&]() {
    ld_info("Received config update");
    config_change_sem.post();
  });
  ld_info("Changing logsconfig property");
  ASSERT_TRUE(client->setAttributesSync(
      "/dir/subdir/log1", client::LogAttributes().with_replicationFactor(3)));

  // Waiting for the client to get a CONFIG_CHANGED message from the server
  config_change_sem.wait();

  ld_info("Verifying logs config invalidation");
  // Check if RemoteLogsConfig got invalidated
  auto config2 = client_impl->getConfig()->get();
  ASSERT_NE(config, config2);

  // Check if new properties are propagated
  ld_info("Fetching updated logs config properties");
  log_cfg = config2->getLogGroupByIDShared(logid_t(1));
  ASSERT_TRUE(log_cfg != nullptr);
  ASSERT_EQ(3, log_cfg->attrs().replicationFactor().value());
}

TEST_F(ConfigIntegrationTest,
       RemoteLogsConfigWithSubscriptionAndConnectionFailedTest) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useTcp()
                     .setParam("--file-config-update-interval", "10ms")
                     .create(2);

  // Copying the config for the client so that it's not affected when we update
  // the server config
  std::string client_config_path(cluster->getConfigPath() + "_client");
  boost::filesystem::copy_file(cluster->getConfigPath(), client_config_path);

  auto client = ClientFactory()
                    .setSetting("file-config-update-interval", "10ms")
                    .setSetting("on-demand-logs-config", "true")
                    .setSetting("connect-throttle", "0s..0s")
                    .setTimeout(testTimeout())
                    .create(client_config_path);

  ASSERT_TRUE((bool)client);

  // Fetching by ID
  ld_info("Fetching by ID");
  auto range = client->getLogRangeByName("/ns/test_logs");
  ASSERT_EQ(range.first.val(), 1);
  ASSERT_EQ(range.second.val(), 2);

  ld_info("Waiting for config change");
  Semaphore config_change_sem;
  auto handle = client->subscribeToConfigUpdates([&]() {
    ld_info("Posting to config_change_sem");
    config_change_sem.post();
  });

  // close all client sockets on both nodes. This should trigger the onclose
  // callback for the GetLogInfoRequest socket
  for (auto& node : cluster->getNodes()) {
    node.second->sendCommand("close_socket --all-clients=true");
  }

  // Waiting for the client to get notified due to config invalidation
  config_change_sem.wait();
  // only the logs config should have changed. check that the callback didn't
  // get called twice
  ASSERT_EQ(0, config_change_sem.value());

  // Fetching ID again to cause connection to be re-established (config cache
  // should have been wiped by invalidation of the remote logs config)
  range = client->getLogRangeByName("/ns/test_logs");
  ASSERT_EQ(range.first.val(), 1);
  ASSERT_EQ(range.second.val(), 2);

  // close all client sockets again
  for (auto& node : cluster->getNodes()) {
    node.second->sendCommand("close_socket --all-clients=true");
  }

  // Make sure that the onclose callback is again executed.
  // Waiting for the client to get notified due to config invalidation
  config_change_sem.wait();
  // only the logs config should have changed. check that the callback didn't
  // get called twice
  ASSERT_EQ(0, config_change_sem.value());
}

TEST_F(ConfigIntegrationTest, ClientConfigSubscription) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--file-config-update-interval", "10ms")
          // Config synchronization makes the number of config updates vary
          .setParam("--enable-config-synchronization", "false")
          // TODO(#8466255): remove.
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::NONE)
          .create(2);
  auto client = ClientFactory()
                    .setSetting("file-config-update-interval", "10ms")
                    .setTimeout(testTimeout())
                    .create(cluster->getConfigPath());

  Semaphore sem;
  auto handle = client->subscribeToConfigUpdates([&]() {
    ld_info("Config updated");
    sem.post();
  });

  // Overwriting the config to trigger an update
  cluster->writeServerConfig(
      cluster->getConfig()->getServerConfig()->withIncrementedVersion().get());

  // Subscription callback currently gets invoked twice (once for server
  // config, once for logs)
  ld_info("Waiting for config change to be picked up ...");
  sem.wait();
  sem.wait();

  // Unsubscribe and assert that the subscription callback does not get called
  // on a further update
  handle.unsubscribe();

  // Overwriting the config to trigger an update
  cluster->writeServerConfig(
      cluster->getConfig()->getServerConfig()->withIncrementedVersion().get());

  ASSERT_EQ(0, sem.value());
}

TEST_F(ConfigIntegrationTest, IncludedConfigUpdated) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--file-config-update-interval", "10ms")
          // TODO(#8466255): remove.
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::NONE)
          .writeLogsConfigFileSeparately()
          .create(2);

  auto client = ClientFactory()
                    .setSetting("file-config-update-interval", "10ms")
                    .setTimeout(testTimeout())
                    .create(cluster->getConfigPath());

  boost::filesystem::path config_path(cluster->getConfigPath());
  auto included_path = config_path.parent_path() / "included_logs.conf";
  Semaphore sem;
  auto handle = client->subscribeToConfigUpdates([&]() { sem.post(); });

  auto changed_logs_config =
      cluster->getConfig()->getLocalLogsConfig()->copyLocal();

  for (int i = 0; i < 3; ++i) {
    ld_check(changed_logs_config->isLocal());
    auto& tree = const_cast<logsconfig::LogsConfigTree&>(
        changed_logs_config->getLogsConfigTree());
    auto& logs =
        const_cast<logsconfig::LogMap&>(changed_logs_config->getLogMap());
    auto log_in_directory = logs.begin()->second;
    auto new_log_id =
        logid_t(log_in_directory.log_group->range().first.val() + 1);
    bool res = changed_logs_config->replaceLogGroup(
        log_in_directory.getFullyQualifiedName(),
        log_in_directory.log_group->withRange(
            logid_range_t({new_log_id, new_log_id})));
    ASSERT_TRUE(res);
    // Setting the newer tree version
    tree.setVersion(tree.version() + 1);
    // writing new config
    int rv = cluster->writeConfig(
        /* server_cfg */ nullptr, changed_logs_config.get());
    ASSERT_EQ(0, rv);

    ld_info("Waiting for config change to be picked up ...");
    sem.wait();
    // server config hasn't changed. only logs config update should
    // initiate a callback.
    ASSERT_EQ(0, sem.value());
  }
  ASSERT_EQ(0, sem.value());
}

// Tries to fetch log config on a worker thread with synchronous methods.
// Verifies it fails.
TEST_F(ConfigIntegrationTest, ConfigSyncMethodFailure) {
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--file-config-update-interval", "10ms")
          // TODO(#8466255): remove.
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::NONE)
          .create(2);

  auto append_and_wait = [&](int iteration) {
    auto client = ClientFactory()
                      .setSetting("abort-on-failed-check", "true")
                      .setTimeout(testTimeout())
                      .create(cluster->getConfigPath());
    ASSERT_TRUE((bool)client);
    auto client_config =
        checked_downcast<ClientImpl*>(client.get())->getConfig()->get();

    Semaphore sem;
    auto get_log_config_cb = [&](Status, const DataRecord&) {
      switch (iteration) {
        case 0:
          client->getLogRangeByName("");
          break;
        case 1:
          client->getLogRangesByNamespace("");
          break;
        case 2:
          client->getLogGroupSync("");
          break;
        default:
          ld_check(false);
      }

      // An assert should fail before we get here
      sem.post();
    };

    char data[128];
    Payload payload(data, 1);
    client->append(logid_t(1), payload, get_log_config_cb);
    sem.wait();
  };
  for (int i = 0; i <= 2; ++i) {
    ld_info("Running iteration %d of the test", i);
    ASSERT_DEATH(append_and_wait(i), "");
  }
}

TEST_F(ConfigIntegrationTest, NumLogsConfiguredStat) {
  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(2);
  log_attrs.set_extraCopies(0);
  log_attrs.set_syncedCopies(0);
  log_attrs.set_maxWritesInFlight(100);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .setParam("--file-config-update-interval", "10ms")
          .setParam("--num-workers", "8")
          .setLogGroupName("test_range")
          .setLogAttributes(log_attrs)
          // TODO(#8466255): remove.
          .eventLogMode(
              IntegrationTestUtils::ClusterFactory::EventLogMode::NONE)
          .setNumLogs(4)
          .create(3);
  auto stats = cluster->getNode(0).stats();

  // 4 internal logs (config_log_deltas/snapshots) + 4 user logs.
  ASSERT_EQ(stats["num_logs_configured"], 8);

  // add an extra log to config
  const logid_t EXTRA_LOG_ID(271828182);
  std::unique_ptr<EpochMetaData> metadata;
  {
    std::shared_ptr<Configuration> current_config = cluster->getConfig()->get();
    auto new_logs_config = current_config->localLogsConfig()->copyLocal();
    auto logs_config = current_config->localLogsConfig();
    new_logs_config->insert(EXTRA_LOG_ID.val_, "test_range_2", log_attrs);
    auto new_config = std::make_shared<Configuration>(
        current_config->serverConfig(), std::move(new_logs_config));

    // provision epoch metadata in epoch store for the new log
    std::shared_ptr<NodeSetSelector> selector =
        NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL);
    auto provisioner = std::make_shared<CustomEpochMetaDataUpdater>(
        new_config,
        new_config->getNodesConfigurationFromServerConfigSource(),
        selector,
        true,
        true /* provision_if_empty */,
        false /* update_if_exists */);
    (*provisioner)(EXTRA_LOG_ID, metadata, /* MetaDataTracer */ nullptr);
    auto epoch_store = cluster->createEpochStore();
    int rv = static_cast<FileEpochStore*>(epoch_store.get())
                 ->provisionMetaDataLog(EXTRA_LOG_ID, provisioner);
    // Write the new config to the file and
    // update the in-memory UpdateableConfig.
    cluster->writeConfig(*new_config);
    cluster->waitForConfigUpdate();
  }
  stats = cluster->getNode(0).stats();
  ASSERT_EQ(stats["num_logs_configured"], 9);

  {
    boost::icl::right_open_interval<logid_t::raw_type> logid_interval(1, 2);
    auto new_logs_config = std::make_unique<configuration::LocalLogsConfig>();
    new_logs_config->insert(logid_interval, "test_range_3", log_attrs);
    cluster->writeLogsConfig(new_logs_config.get());
    cluster->waitForConfigUpdate();
    stats = cluster->getNode(0).stats();
    ASSERT_EQ(stats["num_logs_configured"], 5);
  }
}

void IntegrationTest_RunNamespaceTest(std::shared_ptr<Client> client,
                                      std::string delimiter,
                                      std::string ns = "") {
  ASSERT_TRUE((bool)client);

  LogsConfig::NamespaceRangeLookupMap ranges;
  LogsConfig::NamespaceRangeLookupMap subranges;
  std::string prefix = ns.empty() ? ns : ns + delimiter;

  std::string log_name = prefix + "sublog1";
  auto range = client->getLogRangeByName(log_name);
  ASSERT_EQ(1, range.first.val_);
  ASSERT_EQ(1, range.second.val_);
  std::unique_ptr<client::LogGroup> attributes =
      client->getLogGroupSync(log_name);
  ASSERT_NE(nullptr, attributes);
  std::string full_ns = attributes->getFullyQualifiedName();
  ASSERT_EQ(range, attributes->range());
  ASSERT_EQ("sublog1", attributes->name());
  ASSERT_EQ(delimiter + "ns1" + delimiter + "sublog1", full_ns);
  ASSERT_EQ(2, attributes->attrs().replicationFactor());
  ASSERT_EQ(10000, attributes->attrs().maxWritesInFlight());
  ranges[full_ns] = range;

  log_name = prefix + "ns2" + delimiter + "subsublog1";
  range = client->getLogRangeByName(log_name);
  ASSERT_EQ(2, range.first.val_);
  ASSERT_EQ(3, range.second.val_);
  attributes = client->getLogGroupSync(log_name);
  ASSERT_NE(nullptr, attributes);
  full_ns = attributes->getFullyQualifiedName();
  ASSERT_EQ(range, attributes->range());
  ASSERT_EQ("subsublog1", attributes->name());
  ASSERT_EQ(delimiter + "ns1" + delimiter + "ns2" + delimiter + "subsublog1",
            full_ns);
  ASSERT_EQ(3, attributes->attrs().replicationFactor());
  ASSERT_EQ(10000, attributes->attrs().maxWritesInFlight());
  ranges[full_ns] = range;

  log_name = prefix + "ns2" + delimiter + "subsublog2";
  range = client->getLogRangeByName(log_name);
  ASSERT_EQ(4, range.first.val_);
  ASSERT_EQ(4, range.second.val_);
  attributes = client->getLogGroupSync(log_name);
  ASSERT_NE(nullptr, attributes);
  full_ns = attributes->getFullyQualifiedName();
  ASSERT_EQ(range, attributes->range());
  ASSERT_EQ("subsublog2", attributes->name());
  ASSERT_EQ(delimiter + "ns1" + delimiter + "ns2" + delimiter + "subsublog2",
            full_ns);
  ASSERT_EQ(1, attributes->attrs().replicationFactor());
  ASSERT_EQ(10000, attributes->attrs().maxWritesInFlight());
  ranges[full_ns] = range;

  // This should fail because "log1" is not under "ns1"
  log_name = prefix + "log1";
  range = client->getLogRangeByName(log_name);
  ASSERT_EQ(LOGID_INVALID, range.first);
  ASSERT_EQ(LOGID_INVALID, range.second);
  attributes = client->getLogGroupSync(log_name);
  ASSERT_EQ(nullptr, attributes);

  // This should succeed as it queries "log1" from the root namespace
  range = client->getLogRangeByName(delimiter + "log1");
  ASSERT_EQ(95, range.first.val_);
  ASSERT_EQ(100, range.second.val_);
  attributes = client->getLogGroupSync(delimiter + "log1");
  ASSERT_NE(nullptr, attributes);
  full_ns = attributes->getFullyQualifiedName();
  ASSERT_EQ(range, attributes->range());
  ASSERT_EQ("log1", attributes->name());
  ASSERT_EQ(delimiter + "log1", full_ns);
  ASSERT_EQ(1, attributes->attrs().replicationFactor());
  ASSERT_EQ(10000, attributes->attrs().maxWritesInFlight());

  // supposed to fetch all logs under ns1
  auto fetched_ranges = client->getLogRangesByNamespace(ns);
  ASSERT_EQ(ranges, fetched_ranges);
}

// Testing how the client works with a config where logs are contained in
// namespaces
TEST_F(ConfigIntegrationTest, LogsConfigNamespaceTest) {
  auto config =
      Configuration::fromJsonFile(TEST_CONFIG_FILE("namespaced_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .create(*config);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  auto client =
      cluster->createClient(testTimeout(), std::move(client_settings));
  IntegrationTest_RunNamespaceTest(client, "/", "ns1");

  std::unique_ptr<client::LogGroup> attributes;
  std::string full_ns;
  auto range = client->getLogRangeByName("log1");
  ASSERT_EQ(95, range.first.val_);
  ASSERT_EQ(100, range.second.val_);
  attributes = client->getLogGroupSync("log1");
  ASSERT_NE(nullptr, attributes);
  full_ns = attributes->getFullyQualifiedName();
  ASSERT_EQ(range, attributes->range());
  ASSERT_EQ("log1", attributes->name());
  ASSERT_EQ("/log1", full_ns);
  ASSERT_EQ(1, attributes->attrs().replicationFactor());
  ASSERT_EQ(10000, attributes->attrs().maxWritesInFlight());
}

// Testing how the client works with a config where logs are contained in
// namespaces and a default namespace is specified
TEST_F(ConfigIntegrationTest, LogsConfigNamespaceDefaultTest) {
  auto config =
      Configuration::fromJsonFile(TEST_CONFIG_FILE("namespaced_logs.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .create(*config);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("default-log-namespace", "ns1"));
  auto client =
      cluster->createClient(testTimeout(), std::move(client_settings));
  IntegrationTest_RunNamespaceTest(client, "/");
}

// Testing how the client works with a config where logs are contained in
// namespaces, and the delimiter for namespaces is set to '#'
TEST_F(ConfigIntegrationTest, LogsConfigNamespaceDelimiterTest) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("namespaced_logs_delimiter.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .doPreProvisionEpochMetaData()
                     .create(*config);

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("default-log-namespace", "ns1"));
  auto client =
      cluster->createClient(testTimeout(), std::move(client_settings));
  IntegrationTest_RunNamespaceTest(client, "#");
}

/**
 * Expand the cluster and update the cluster config version without updating
 * the client config version. Send an append to a log with a sequencer running
 * on the new node. The configs should be synced after the appendSync(), so
 * both the client and server configs should have the same number of nodes. The
 * append should also succeed since the client config would have all the
 * information about the new node.
 */
TEST_F(ConfigIntegrationTest, ExpandWithVersionUpdate) {
  size_t num_logs = 5;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     .setNumLogs(num_logs)
                     .useHashBasedSequencerAssignment()
                     .create(1);

  // Must set version > 0 so that the first config advisory is sent
  cluster->writeServerConfig(cluster->getConfig()
                                 ->getServerConfig()
                                 ->withVersion(config_version_t(1))
                                 .get());
  cluster->waitForConfigUpdate();
  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();

  std::shared_ptr<UpdateableConfig> client_config =
      std::make_shared<UpdateableConfig>(
          std::make_shared<UpdateableServerConfig>(
              cluster_config->serverConfig()->copy()),
          std::make_shared<UpdateableLogsConfig>(cluster_config->logsConfig()),
          std::make_shared<UpdateableZookeeperConfig>(
              cluster_config->zookeeperConfig()));

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-config-synchronization", true));
  // To ensure there's only one connection to the target node to have
  // deterministic stat values
  ASSERT_EQ(0, client_settings->set("num-workers", "1"));
  // Creating a client through instantiating an instance of ClientImpl directly
  // makes it ignore the settings in the config, so we have to set this here
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", "false"));
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      getDefaultTestTimeout(),
      std::move(client_settings),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  std::string old_hash =
      client_config->get()->serverConfig()->getMainConfigMetadata().hash;

  // Remove a node from the nodes field in the client config
  cluster->expand(1);

  // Bump config version so that the update is propagated to the client.
  // It would probably make sense for expand() to do it instead.
  cluster->writeServerConfig(cluster->getConfig()
                                 ->getServerConfig()
                                 ->withVersion(config_version_t(2))
                                 .get());
  cluster->waitForConfigUpdate();

  // Get the updated cluster configuration
  cluster_config = cluster->getConfig()->get();

  // Look for a log with a sequencer running on node 1 to make sure that an
  // append to the new node does not fail
  int log_id = -1;
  size_t cluster_nodes_size = cluster_config->serverConfig()->getNodes().size();
  std::vector<double> weights(cluster_nodes_size, 1.0);
  for (int i = 0; i < num_logs; i++) {
    auto seq = hashing::weighted_ch(i, weights);
    if (seq == 1) {
      log_id = i;
      break;
    }
  }
  EXPECT_NE(log_id, -1);
  // With mismatched versions, the configs should sync after an
  // appendSync() call
  char data[20];
  lsn_t lsn = client->appendSync(logid_t(log_id), Payload(data, sizeof data));
  // With the client config updated, the append should succeed
  EXPECT_NE(LSN_INVALID, lsn);

  auto client_server_config = client_config->get()->serverConfig();
  auto cluster_server_config = cluster_config->serverConfig();
  // Configs should be synced
  size_t client_nodes_size = client_server_config->getNodes().size();
  EXPECT_EQ(client_nodes_size, cluster_nodes_size);
  EXPECT_EQ(
      client_server_config->getVersion(), cluster_server_config->getVersion());
  Stats stats =
      checked_downcast<ClientImpl*>(client.get())->stats()->aggregate();
  EXPECT_EQ(stats.config_changed_update, 1);
  EXPECT_EQ(stats.config_changed_ignored_uptodate, 0);
  // Make sure the config hash was updated in the metadata
  std::string new_hash = client_server_config->getMainConfigMetadata().hash;
  EXPECT_NE(old_hash, new_hash);
}

/**
 * if the client looses connection to all the servers, then reconnects
 * and then the config changes, it should receive a config update.
 * T19222939
 */
TEST_F(ConfigIntegrationTest, ConfigSyncAfterReconnect) {
  size_t num_logs = 5;
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     .setNumLogs(num_logs)
                     .useHashBasedSequencerAssignment()
                     .create(1);

  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();
  // Must set version > 0 so that the first config advisory is sent
  auto new_server_config =
      cluster_config->serverConfig()->withVersion(config_version_t(1));
  cluster->writeServerConfig(new_server_config.get());
  cluster->waitForConfigUpdate();

  std::shared_ptr<UpdateableConfig> client_config =
      std::make_shared<UpdateableConfig>(
          std::make_shared<UpdateableServerConfig>(new_server_config->copy()),
          std::make_shared<UpdateableLogsConfig>(cluster_config->logsConfig()),
          std::make_shared<UpdateableZookeeperConfig>(
              cluster_config->zookeeperConfig()));

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-config-synchronization", true));
  // To make sure only 1 connection is maintained to a node, which ensures
  // only 1 CONFIG_CHANGED message is received
  ASSERT_EQ(0, client_settings->set("num-workers", "1"));
  // Creating a client through instantiating an instance of ClientImpl directly
  // makes it ignore the settings in the config, so we have to set this here
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", "false"));
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      getDefaultTestTimeout(),
      std::move(client_settings),
      plugin_registry);
  ASSERT_TRUE((bool)client);

  std::string old_hash =
      client_config->get()->serverConfig()->getMainConfigMetadata().hash;

  // the cluster has only one node, but we're going to expand it to 2 nodes
  // so we look for a log id that will be sequenced by N0 so that both appends
  // before and after expand go to the same node
  int log_id = -1;
  std::vector<double> weights(2, 1.0);
  for (int i = 0; i < num_logs; i++) {
    auto seq = hashing::weighted_ch(i, weights);
    if (seq == 1) {
      log_id = i;
      break;
    }
  }
  EXPECT_NE(log_id, -1);

  // append a record to create connection to N0
  lsn_t lsn = client->appendSync(logid_t(log_id), Payload("foo", 3));
  EXPECT_NE(LSN_INVALID, lsn);

  // now restart N0
  cluster->getNode(0).kill();
  cluster->getNode(0).start();

  // now expand the cluster and bump config version to cause config to be
  // updated
  cluster->expand(1);

  cluster_config = cluster->getConfig()->get();
  new_server_config =
      cluster_config->serverConfig()->withVersion(config_version_t(2));
  cluster->writeServerConfig(new_server_config.get());
  cluster->waitForConfigUpdate();

  // Get the updated cluster configuration
  cluster_config = cluster->getConfig()->get();
  size_t cluster_nodes_size = cluster_config->serverConfig()->getNodes().size();

  // append another record. the client should reconnect to N1 and receive a
  // CONFIG_CHANGED message
  lsn = client->appendSync(logid_t(log_id), Payload("foo", 3));
  EXPECT_NE(LSN_INVALID, lsn);

  auto client_server_config = client_config->get()->serverConfig();
  auto cluster_server_config = cluster_config->serverConfig();
  // Configs should be synced
  size_t client_nodes_size = client_server_config->getNodes().size();
  EXPECT_EQ(client_nodes_size, cluster_nodes_size);
  EXPECT_EQ(
      client_server_config->getVersion(), cluster_server_config->getVersion());
  Stats stats =
      checked_downcast<ClientImpl*>(client.get())->stats()->aggregate();
  EXPECT_EQ(stats.config_changed_update, 1);
  EXPECT_EQ(stats.config_changed_ignored_uptodate, 0);
  // Make sure the config hash was updated in the metadata
  std::string new_hash = client_server_config->getMainConfigMetadata().hash;
  EXPECT_NE(old_hash, new_hash);
}

/**
 * Start a client with a config on the same version as the cluster config, then
 * expand the cluster without a version update.  After an appendSync(), the
 * cluster config should have more nodes than the client config since the
 * CONFIG_CHANGED_Message should not be sent if the versions match.
 */
TEST_F(ConfigIntegrationTest, ExpandWithoutVersionUpdate) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     .create(1);

  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();
  // Must set version > 0 so that the first config advisory is sent
  auto new_server_config =
      cluster_config->serverConfig()->withVersion(config_version_t(1));
  cluster->writeServerConfig(new_server_config.get());
  cluster->waitForConfigUpdate();

  std::shared_ptr<UpdateableConfig> client_config =
      std::make_shared<UpdateableConfig>(
          std::make_shared<UpdateableServerConfig>(
              cluster_config->serverConfig()->copy()),
          std::make_shared<UpdateableLogsConfig>(cluster_config->logsConfig()),
          std::make_shared<UpdateableZookeeperConfig>(
              cluster_config->zookeeperConfig()));

  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-config-synchronization", true));
  // Creating a client through instantiating an instance of ClientImpl directly
  // makes it ignore the settings in the config, so we have to set this here
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", "false"));
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

  // Expand the cluster by one node without updating the config version
  cluster->expand(1);
  cluster->waitForConfigUpdate();

  // Make an appendSync() call. Configs should not be synchronized after
  // append since the config versions are the same.
  char data[20];
  client->appendSync(logid_t(1), Payload(data, sizeof data));
  EXPECT_EQ(client_config->get()->serverConfig()->getVersion(),
            cluster_config->serverConfig()->getVersion());

  // Get the updated cluster configuration
  cluster_config = cluster->getConfig()->get();

  auto client_server_config = client_config->get()->serverConfig();
  auto cluster_server_config = cluster_config->serverConfig();
  // Client config should not be updated
  size_t client_nodes_size = client_server_config->getNodes().size();
  size_t cluster_nodes_size = cluster_server_config->getNodes().size();
  EXPECT_LT(client_nodes_size, cluster_nodes_size);
  Stats stats =
      checked_downcast<ClientImpl*>(client.get())->stats()->aggregate();
  EXPECT_EQ(stats.config_changed_update, 0);
  EXPECT_EQ(stats.config_changed_ignored_uptodate, 0);
}

TEST_F(ConfigIntegrationTest, MetaDataLog) {
  auto cluster = IntegrationTestUtils::ClusterFactory().create(1);

  const logid_t LOG_ID(949494);

  std::shared_ptr<Configuration> cluster_config = cluster->getConfig()->get();

  // Create a config with an extra log that the servers don't know about.
  std::shared_ptr<configuration::LocalLogsConfig> logs_config =
      checked_downcast<std::unique_ptr<configuration::LocalLogsConfig>>(
          cluster_config->localLogsConfig()->copy());

  logsconfig::LogAttributes log_attrs;
  log_attrs.set_replicationFactor(3);
  logs_config->insert(LOG_ID.val_, "test_log_log", log_attrs);

  auto client_config = std::make_shared<UpdateableConfig>();
  client_config->updateableServerConfig()->update(
      cluster_config->serverConfig());
  client_config->updateableLogsConfig()->update(logs_config);
  // Creating a client through instantiating an instance of ClientImpl directly
  // makes it ignore the settings in the config, so we have to set this here
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  ASSERT_EQ(0, client_settings->set("enable-logsconfig-manager", "false"));
  auto plugin_registry =
      std::make_shared<PluginRegistry>(getClientPluginProviders());
  std::shared_ptr<Client> client = std::make_shared<ClientImpl>(
      client_config->get()->serverConfig()->getClusterName(),
      client_config,
      "",
      "",
      this->testTimeout(),
      std::move(client_settings),
      plugin_registry);
  ASSERT_TRUE((bool)client);
  auto client_impl = static_cast<ClientImpl*>(client.get());
  auto config = client_impl->getConfig()->get();

  auto log_cfg = config->getLogGroupByIDShared(LOG_ID);
  ASSERT_NE(nullptr, log_cfg);
  // metadata log should exist but it will have different configuration
  auto log_cfg2 =
      config->getLogGroupByIDShared(MetaDataLog::metaDataLogID(LOG_ID));
  ASSERT_NE(nullptr, log_cfg2);
  ASSERT_FALSE(*log_cfg == *log_cfg2);
}

TEST_F(ConfigIntegrationTest, Stats) {
  size_t num_logs = 5;
  dbg::parseLoglevelOption("debug");
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--enable-config-synchronization")
                     .setNumLogs(num_logs)
                     .useHashBasedSequencerAssignment()
                     .create(1);

  std::shared_ptr<Configuration> old_config = cluster->getConfig()->get();
  auto new_server_config =
      old_config->serverConfig()->withVersion(config_version_t(1));
  Configuration new_config(
      std::move(new_server_config), old_config->logsConfig());
  std::string config_str = new_config.toString();
  ld_info("CFG: %s", config_str.c_str() + 2000);
  cluster->writeConfig(new_config);
  cluster->waitForConfigUpdate();

  // take a snapshot of the stats
  auto initial_stats = cluster->getNode(0).stats();

  // test #1: no change but overwrite config to trigger reload and verify that
  // config_update_same_version is incremented
  EXPECT_EQ(config_str, cluster->getConfig()->get()->toString());
  cluster->writeConfig(new_config);

  wait_until([&]() {
    auto tmp_stats = cluster->getNode(0).stats();
    return (initial_stats["config_update_same_version"] + 1) <=
        tmp_stats["config_update_same_version"];
  });

  auto stats = cluster->getNode(0).stats();
  EXPECT_LE((initial_stats["config_update_same_version"] + 1),
            stats["config_update_same_version"]);
  EXPECT_EQ(initial_stats["config_update_hash_mismatch"],
            stats["config_update_hash_mismatch"]);
  EXPECT_EQ(initial_stats["config_update_old_version"],
            stats["config_update_old_version"]);
  EXPECT_EQ(initial_stats["updated_config"], stats["updated_config"]);

  // test #2: change config without updating version. expect
  // config_update_hash_mismatch and updated_config to be incremented.
  cluster->expand(1);
  cluster->waitForConfigUpdate();

  auto stats2 = cluster->getNode(0).stats();
  EXPECT_LE(stats["config_update_same_version"],
            stats2["config_update_same_version"]);
  EXPECT_EQ((stats["config_update_hash_mismatch"] + 1),
            stats2["config_update_hash_mismatch"]);
  EXPECT_EQ(
      stats["config_update_old_version"], stats2["config_update_old_version"]);
  EXPECT_EQ((stats["updated_config"] + 1), stats2["updated_config"]);

  // test #3: change config and update version. expect updated_config
  // to be incremented
  cluster->expand(1);

  cluster->writeServerConfig(cluster->getConfig()
                                 ->getServerConfig()
                                 ->withVersion(config_version_t(2))
                                 .get());
  cluster->waitForConfigUpdate();

  auto stats3 = cluster->getNode(0).stats();
  EXPECT_LE(stats2["config_update_same_version"],
            stats3["config_update_same_version"]);
  EXPECT_EQ(stats2["config_update_hash_mismatch"] + 1,
            stats3["config_update_hash_mismatch"]);
  EXPECT_EQ(
      stats2["config_update_old_version"], stats3["config_update_old_version"]);
  EXPECT_EQ((stats2["updated_config"] + 2), stats3["updated_config"]);

  // test #4: change config with older version. expect
  // config_update_old_version to be incremented
  cluster->writeConfig(new_config, false);
  wait_until([&]() {
    auto tmp_stats = cluster->getNode(0).stats();
    return (stats3["config_update_old_version"] + 1) ==
        tmp_stats["config_update_old_version"];
  });

  auto stats4 = cluster->getNode(0).stats();
  EXPECT_LE(stats3["config_update_same_version"],
            stats4["config_update_same_version"]);
  EXPECT_EQ(stats3["config_update_hash_mismatch"],
            stats4["config_update_hash_mismatch"]);
  EXPECT_EQ((stats3["config_update_old_version"] + 1),
            stats4["config_update_old_version"]);
  EXPECT_EQ(stats3["updated_config"], stats4["updated_config"]);
  EXPECT_EQ(0, stats3["config_update_invalid"]);
}

TEST_F(ConfigIntegrationTest, InvalidConfigClientCreationTest) {
  std::string client_config_path = TEST_CONFIG_FILE("overlap1.conf");
  auto client = ClientFactory().create(client_config_path);

  // should return nullptr to indicate failure but no exceptions,
  // err should be set to E::INVALID_CONFIG
  ASSERT_EQ(nullptr, client);
  ASSERT_EQ(E::INVALID_CONFIG, err);
}

TEST_F(ConfigIntegrationTest, InvalidConfigClientCreationTest2) {
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  std::string client_config_path = TEST_CONFIG_FILE("sample_invalid1.conf");
  auto client = ClientFactory().create(client_config_path);

  // should return nullptr to indicate failure but no exceptions,
  // err should be set to E::INVALID_CONFIG
  ASSERT_EQ(nullptr, client);
  ASSERT_EQ(E::INVALID_CONFIG, err);
}

TEST_F(ConfigIntegrationTest, SslClientCertUnsetByDefault) {
  std::string client_config_path = TEST_CONFIG_FILE("sample_no_ssl.conf");
  auto client = ClientFactory().create(client_config_path);
  ASSERT_NE(nullptr, client);

  ClientImpl* client_impl = dynamic_cast<ClientImpl*>(client.get());
  ClientSettings& settings = client_impl->settings();

  auto certval = settings.get("ssl-cert-path");
  ASSERT_TRUE(certval.hasValue());
  ASSERT_EQ(certval.value(), "");

  auto keyval = settings.get("ssl-key-path");
  ASSERT_TRUE(keyval.hasValue());
  ASSERT_EQ(keyval.value(), "");

  auto loadval = settings.get("ssl-load-client-cert");
  ASSERT_TRUE(loadval.hasValue());
  ASSERT_EQ(loadval.value(), "false");
}
