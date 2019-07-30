/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>

#include <gtest/gtest.h>

#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/Record.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/AppendThread.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"
#include "logdevice/test/utils/ReaderThread.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::thrift;

class AdminAPILowLevelTest : public IntegrationTestBase {};

TEST_F(AdminAPILowLevelTest, BasicThriftClientCreation) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .useHashBasedSequencerAssignment()
                     .create(3);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  auto fbStatus = admin_client->sync_getStatus();
  ASSERT_EQ(facebook::fb303::cpp2::fb_status::ALIVE, fbStatus);
}

TEST_F(AdminAPILowLevelTest, LogTreeReplicationInfo) {
  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_singleWriter(false);
  internal_log_attrs.set_replicationFactor(2);
  internal_log_attrs.set_extraCopies(0);
  internal_log_attrs.set_syncedCopies(0);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setConfigLogAttributes(internal_log_attrs)
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .create(3);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  // Let's create some log groups
  std::unique_ptr<ClientSettings> settings(ClientSettings::create());
  settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      std::chrono::seconds(60), std::move(settings));

  cluster->waitForRecovery();

  auto lg1 = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(100)),
      client::LogAttributes()
          .with_replicateAcross({{NodeLocationScope::RACK, 3}})
          .with_backlogDuration(std::chrono::seconds(60)),
      false);

  ASSERT_TRUE(lg1);
  auto lg2 = client->makeLogGroupSync(
      "/log2",
      logid_range_t(logid_t(200), logid_t(300)),
      client::LogAttributes()
          .with_replicateAcross(
              {{NodeLocationScope::RACK, 2}, {NodeLocationScope::NODE, 3}})
          .with_backlogDuration(std::chrono::seconds(30)),
      false);
  ASSERT_TRUE(lg2);

  auto target_version = std::to_string(lg2->version());

  ReplicationInfo info;
  int give_up = 10;
  do {
    // let's keep trying until the server hits that version
    admin_client->sync_getReplicationInfo(info);
    ld_info("Still waiting for the server to sync to config version '%s'"
            " instead we have received '%s', retries left %i.",
            target_version.c_str(),
            info.get_version().c_str(),
            give_up);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    give_up--;
  } while (info.get_version() != target_version && give_up > 0);

  ASSERT_EQ(target_version, info.get_version());
  ASSERT_EQ(2, info.get_smallest_replication_factor());
  std::map<thrift::LocationScope, int32_t> narrowest_expected{
      {thrift::LocationScope::NODE, 2}};
  std::map<std::string, int32_t> narrowest_expected_legacy{{"NODE", 2}};
  ASSERT_EQ(narrowest_expected, info.get_narrowest_replication());
  ASSERT_EQ(narrowest_expected_legacy, info.get_narrowest_replication_legacy());
  auto tolerable_failure_domains = info.get_tolerable_failure_domains();
  ASSERT_EQ(
      thrift::LocationScope::NODE, tolerable_failure_domains.get_domain());
  ASSERT_EQ("NODE", tolerable_failure_domains.get_domain_legacy());
  ASSERT_EQ(1, tolerable_failure_domains.get_count());

  LogTreeInfo logtree;
  admin_client->sync_getLogTreeInfo(logtree);
  ASSERT_EQ(target_version, logtree.get_version());
  // 200 normal logs + 6 internal logs
  ASSERT_EQ(206, logtree.get_num_logs());
  ASSERT_EQ(60, logtree.get_max_backlog_seconds());
  ASSERT_TRUE(logtree.get_is_fully_loaded());
}

TEST_F(AdminAPILowLevelTest, TakeLogTreeSnapshot) {
  const int node_count = 3;
  logsconfig::LogAttributes internal_log_attrs;
  internal_log_attrs.set_singleWriter(false);
  internal_log_attrs.set_replicationFactor(2);
  internal_log_attrs.set_extraCopies(0);
  internal_log_attrs.set_syncedCopies(0);
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setConfigLogAttributes(internal_log_attrs)
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .create(node_count);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  // Let's create some log groups
  std::unique_ptr<ClientSettings> settings(ClientSettings::create());
  settings->set("on-demand-logs-config", "true");
  std::shared_ptr<Client> client = cluster->createIndependentClient(
      std::chrono::seconds(60), std::move(settings));

  cluster->waitForRecovery();

  auto lg1 = client->makeLogGroupSync(
      "/log1",
      logid_range_t(logid_t(1), logid_t(100)),
      client::LogAttributes()
          .with_replicateAcross({{NodeLocationScope::RACK, 3}})
          .with_backlogDuration(std::chrono::seconds(60)),
      false);

  ASSERT_TRUE(lg1);
  auto server_version = std::to_string(lg1->version());

  // Takes a log-tree snapshot regardless of the version. This shouldn't throw
  // exceptions.
  wait_until([&]() {
    LogTreeInfo logtree;
    admin_client->sync_getLogTreeInfo(logtree);
    return logtree.get_is_fully_loaded();
  });
  admin_client->sync_takeLogTreeSnapshot(0);
  auto unrealistic_version = 999999999999999999l;
  ASSERT_THROW(admin_client->sync_takeLogTreeSnapshot(unrealistic_version),
               thrift::StaleVersion);
}

TEST_F(AdminAPILowLevelTest, SettingsAPITest) {
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .setParam("--store-timeout", "1s..12s")
                     .useHashBasedSequencerAssignment()
                     .create(2);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  auto fbStatus = admin_client->sync_getStatus();
  ASSERT_EQ(facebook::fb303::cpp2::fb_status::ALIVE, fbStatus);

  thrift::SettingsResponse response;
  admin_client->sync_getSettings(response, thrift::SettingsRequest());
  ASSERT_NE(nullptr, admin_client);

  // For LogsConfig manager, we pass this as both a CLI argument and in CONFIG
  auto& logsconfig_setting = response.settings["enable-logsconfig-manager"];
  ASSERT_EQ("false", logsconfig_setting.currentValue);
  ASSERT_EQ("true", logsconfig_setting.defaultValue);
  ASSERT_EQ(
      "false",
      logsconfig_setting.sources.find(thrift::SettingSource::CONFIG)->second);
  ASSERT_EQ(
      "false",
      logsconfig_setting.sources.find(thrift::SettingSource::CLI)->second);
  ASSERT_TRUE(
      logsconfig_setting.sources.end() ==
      logsconfig_setting.sources.find(thrift::SettingSource::ADMIN_OVERRIDE));

  auto& store_timeout_setting = response.settings["store-timeout"];
  ASSERT_EQ(
      "1s..12s",
      store_timeout_setting.sources.find(thrift::SettingSource::CLI)->second);

  // Check setting filtering
  auto filtered_request = thrift::SettingsRequest();
  std::set<std::string> filtered_settings;
  filtered_settings.insert("rebuilding-local-window");
  filtered_settings.insert("store-timeout");

  thrift::SettingsResponse filtered_response;
  filtered_request.set_settings(std::move(filtered_settings));
  admin_client->sync_getSettings(filtered_response, filtered_request);

  // Check that not all settings are in the response
  ASSERT_TRUE(filtered_response.settings.find("enable-logsconfig-manager") ==
              filtered_response.settings.end());

  // Check that requested settings are in there
  auto& rebuilding_window_setting =
      filtered_response.settings["rebuilding-local-window"];
  ASSERT_EQ("60min", rebuilding_window_setting.currentValue);
  store_timeout_setting = filtered_response.settings["store-timeout"];
  ASSERT_EQ("1s..12s", store_timeout_setting.currentValue);
}

TEST_F(AdminAPILowLevelTest, LogGroupThroughputAPITest) {
  /**
   * Using one node to assure querying to a sequencer rather
   * than a data node.
   */
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .create(1);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  auto client = cluster->createClient();
  client->makeLogGroupSync("/log1",
                           logid_range_t(logid_t(1), logid_t(10)),
                           client::LogAttributes().with_replicationFactor(1),
                           false);

  client->makeLogGroupSync("/log2",
                           logid_range_t(logid_t(20), logid_t(30)),
                           client::LogAttributes().with_replicationFactor(1),
                           false);

  auto fbStatus = admin_client->sync_getStatus();
  ASSERT_EQ(facebook::fb303::cpp2::fb_status::ALIVE, fbStatus);

  // Start writers
  using namespace facebook::logdevice::IntegrationTestUtils;
  auto append_thread_lg1 = std::make_unique<AppendThread>(client, logid_t(1));
  auto append_thread_lg2 = std::make_unique<AppendThread>(client, logid_t(20));
  append_thread_lg1->start();
  append_thread_lg2->start();

  // Start readers
  std::unique_ptr<ClientSettings> client_settings(ClientSettings::create());
  auto reader_lg1 = std::make_unique<ReaderThread>(
      cluster->createClient(testTimeout(), std::move(client_settings)),
      logid_t(1));
  auto reader_lg2 = std::make_unique<ReaderThread>(
      cluster->createClient(testTimeout(), std::move(client_settings)),
      logid_t(20));
  reader_lg1->start();
  reader_lg2->start();

  wait_until([&]() {
    return reader_lg1->getNumRecordsRead() > 100 &&
        reader_lg2->getNumRecordsRead() > 100;
  });

  ld_info("Syncing reader to tail...");
  reader_lg1->syncToTail();
  reader_lg2->syncToTail();

  // Get throughput for log appends
  thrift::LogGroupThroughputResponse response;
  thrift::LogGroupThroughputRequest request;
  request.set_operation(thrift::LogGroupOperation::APPENDS);
  request.set_time_period({60});
  admin_client->sync_getLogGroupThroughput(response, request);
  ASSERT_TRUE(!response.get_throughput().empty());
  std::set<std::string> log_groups{"/log1", "/log2", "/config_log_deltas"};
  for (const auto& it : response.get_throughput()) {
    ASSERT_TRUE((bool)log_groups.count(it.first));
    ASSERT_EQ(thrift::LogGroupOperation::APPENDS, it.second.get_operation());
    ASSERT_TRUE(it.second.get_results()[0] > 0);
  }

  // Test log_group_name filtering
  request.set_log_group_name("/log2");
  admin_client->sync_getLogGroupThroughput(response, request);
  ASSERT_TRUE(!response.get_throughput().empty());
  for (const auto& it : response.get_throughput()) {
    ASSERT_EQ("/log2", it.first);
    ASSERT_EQ(thrift::LogGroupOperation::APPENDS, it.second.get_operation());
    ASSERT_TRUE(it.second.get_results()[0] > 0);
  }

  // Get throughput for log reads. Test interval request and filtering.
  request.set_operation(thrift::LogGroupOperation::READS);
  request.set_log_group_name("/log1");
  request.set_time_period({60, 300});
  admin_client->sync_getLogGroupThroughput(response, request);
  ASSERT_TRUE(!response.get_throughput().empty());
  for (const auto& it : response.get_throughput()) {
    ASSERT_EQ("/log1", it.first);
    ASSERT_EQ(thrift::LogGroupOperation::READS, it.second.get_operation());
    for (const auto& result : it.second.get_results()) {
      ASSERT_TRUE(result > 0);
    }
  }

  // Get throughput for log appends_out. Test interval request and filtering.
  request.set_operation(thrift::LogGroupOperation::APPENDS_OUT);
  request.set_log_group_name("/log1");
  request.set_time_period({60, 300});
  admin_client->sync_getLogGroupThroughput(response, request);
  ASSERT_TRUE(!response.get_throughput().empty());
  for (const auto& it : response.get_throughput()) {
    ASSERT_EQ("/log1", it.first);
    ASSERT_EQ(
        thrift::LogGroupOperation::APPENDS_OUT, it.second.get_operation());
    for (const auto& result : it.second.get_results()) {
      ASSERT_TRUE(result > 0);
    }
  }
}

TEST_F(AdminAPILowLevelTest, LogGroupCustomCountersAPITest) {
  /**
   * Using one node to assure querying to a sequencer rather
   * than a data node.
   */
  auto cluster = IntegrationTestUtils::ClusterFactory()
                     .enableLogsConfigManager()
                     .useHashBasedSequencerAssignment()
                     .create(1);

  cluster->waitUntilAllAvailable();
  auto admin_client = cluster->getNode(0).createAdminClient();
  ASSERT_NE(nullptr, admin_client);

  auto client = cluster->createClient();
  client->makeLogGroupSync("/log1",
                           logid_range_t(logid_t(1), logid_t(10)),
                           client::LogAttributes().with_replicationFactor(1),
                           false);
  client->makeLogGroupSync("/log2",
                           logid_range_t(logid_t(21), logid_t(30)),
                           client::LogAttributes().with_replicationFactor(1),
                           false);

  // Start writers
  using namespace facebook::logdevice::IntegrationTestUtils;
  auto append_thread_lg1 = std::make_unique<AppendThread>(client, logid_t(1));
  auto append_thread_lg2 = std::make_unique<AppendThread>(client, logid_t(21));

  AppendAttributes attr = AppendAttributes();
  attr.counters = std::map<uint8_t, int64_t>();
  attr.counters->insert(std::pair<uint8_t, int64_t>(0, 1));
  attr.counters->insert(std::pair<uint8_t, int64_t>(1, 1));
  append_thread_lg1->setAppendAttributes(attr);

  AppendAttributes attr2 = AppendAttributes();
  attr2.counters = std::map<uint8_t, int64_t>();
  attr2.counters->insert(std::pair<uint8_t, int64_t>(5, 1));
  attr2.counters->insert(std::pair<uint8_t, int64_t>(6, 1));
  append_thread_lg2->setAppendAttributes(attr2);

  append_thread_lg1->start();
  append_thread_lg2->start();

  wait_until([&]() {
    return append_thread_lg1->getNumRecordsAppended() > 100 &&
        append_thread_lg2->getNumRecordsAppended() > 100;
  });

  append_thread_lg1->stop();
  append_thread_lg2->stop();

  thrift::LogGroupCustomCountersRequest request;
  thrift::LogGroupCustomCountersResponse response;

  request.set_time_period(300);
  admin_client->sync_getLogGroupCustomCounters(response, request);

  auto counters = response.get_counters();
  ASSERT_TRUE(!counters.empty());

  std::set<std::string> log_groups{"/log1", "/log2", "/config_log_deltas"};
  for (const auto& it : counters) {
    ASSERT_TRUE((bool)log_groups.count(it.first));
  }
  auto keys = counters["/log1"];

  ASSERT_EQ(keys[0].key, 0);
  ASSERT_EQ(keys[1].key, 1);

  ASSERT_TRUE(keys[0].val > 0);
  ASSERT_TRUE(keys[1].val > 0);

  thrift::LogGroupCustomCountersRequest log2Request;
  thrift::LogGroupCustomCountersResponse log2Response;

  log2Request.set_log_group_path("/log2");
  admin_client->sync_getLogGroupCustomCounters(log2Response, log2Request);

  auto log2Counters = log2Response.get_counters();
  ASSERT_EQ(log2Counters.size(), 1);

  auto log2Keys = counters["/log2"];
  ASSERT_EQ(log2Keys[0].key, 5);
  ASSERT_EQ(log2Keys[1].key, 6);

  ASSERT_TRUE(log2Keys[0].val > 0);
  ASSERT_TRUE(log2Keys[1].val > 0);

  LogGroupCustomCountersRequest keyFilteredReq;
  LogGroupCustomCountersResponse keyFilteredRes;

  keyFilteredReq.set_keys(std::vector<int16_t>{0});
  admin_client->sync_getLogGroupCustomCounters(keyFilteredRes, keyFilteredReq);

  auto keyFilteredCounters = keyFilteredRes.get_counters();
  auto keyFilteredKeys = keyFilteredCounters["/log1"];
  ASSERT_EQ(keyFilteredKeys.size(), 1);
  ASSERT_EQ(keyFilteredKeys[0].key, 0);
  ASSERT_TRUE(keyFilteredKeys[0].val > 0);
}
