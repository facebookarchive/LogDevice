/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <atomic>
#include <chrono>
#include <cmath>
#include <memory>
#include <utility>
#include <vector>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/Reader.h"
#include "logdevice/include/Record.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/shadow/ShadowClient.h"
#include "logdevice/test/BufferedWriterTestUtils.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

#define LD_SHADOW_TEST_PREFIX LD_SHADOW_PREFIX "[inttest] "

using namespace std::chrono_literals;
using namespace facebook::logdevice;
using namespace facebook::logdevice::logsconfig;

static const int kNumNodes = 3;

static const std::string kTestLog = "/test_log";
static const logid_range_t kTestRange{logid_t{1}, logid_t{100}};
static const logsconfig::LogAttributes::Shadow kShadowAttrs{"dummy", 0.1};
static const std::pair<int, int> kShadowRatio{1, 10}; // 1/10 = 0.1

static const std::string kTestString = "SHADOW TEST 123";
static const int kNumClientThreads = 20;
static const double kRatioAccuracy = 0.01;

class ShadowIntegrationTest : public IntegrationTestBase {
 protected:
  using Cluster = std::unique_ptr<IntegrationTestUtils::Cluster>;

  void waitForSequencer(std::shared_ptr<Client> client, logid_t logid) {
    wait_until("sequencer is ready", [&]() {
      IntegrationTestUtils::SequencerState seq_state;
      Status st = IntegrationTestUtils::getSeqState(
          client.get(), logid, seq_state, true);
      return st == E::OK;
    });
  }

  void makeCluster() {
    factory_ = IntegrationTestUtils::ClusterFactory()
                   .enableLogsConfigManager()
                   .useHashBasedSequencerAssignment();

    ld_info(LD_SHADOW_TEST_PREFIX "Initializing ORIGIN cluster");
    cluster_ = factory_.create(kNumNodes);

    // Create origin cluster client
    auto client_settings =
        std::unique_ptr<ClientSettings>(ClientSettings::create());
    client_ = cluster_->createIndependentClient(
        DEFAULT_TEST_TIMEOUT, std::move(client_settings));
    client_settings_ = &client_->settings();

    // Set up log range on origin cluster
    LogAttributes attrs =
        LogAttributes().with_replicationFactor(1).with_shadow(kShadowAttrs);
    auto group = client_->makeLogGroupSync(kTestLog, kTestRange, attrs);
    ASSERT_TRUE(client_->syncLogsConfigVersion(group->version()));
    waitForSequencer(client_, logid_t{1});
  }

  void connectShadowCluster() {
    // Create shadow cluster
    ld_info(LD_SHADOW_TEST_PREFIX "Initializing SHADOW cluster");
    shadow_cluster_ = factory_.create(kNumNodes);
    std::string shadow_config = "file:" + shadow_cluster_->getConfigPath();

    // Set up equivalent log range on shadow cluster
    shadow_client_ = shadow_cluster_->createIndependentClient();
    LogAttributes shadow_log_attrs = LogAttributes().with_replicationFactor(1);
    auto group = shadow_client_->makeLogGroupSync(
        kTestLog, kTestRange, shadow_log_attrs);
    ASSERT_TRUE(shadow_client_->syncLogsConfigVersion(group->version()));
    waitForSequencer(shadow_client_, logid_t{1});

    // Update log attributes on origin cluster to configure shadowing
    logsconfig::LogAttributes::Shadow shadow_attrs{
        shadow_config, kShadowAttrs.ratio()};
    LogAttributes updated_log_attrs =
        client_->getLogGroupSync(kTestLog)->attrs().with_shadow(shadow_attrs);
    client_->setAttributesSync(kTestLog, updated_log_attrs);
  }

  void updateShadowRatio(double new_ratio) {
    LogAttributes original_log_attrs =
        client_->getLogGroupSync(kTestLog)->attrs();
    logsconfig::LogAttributes::Shadow new_shadow_attrs{
        original_log_attrs.shadow().value().destination(), new_ratio};
    LogAttributes updated_log_attrs =
        original_log_attrs.with_shadow(new_shadow_attrs);
    client_->setAttributesSync(kTestLog, updated_log_attrs);
  }

  Payload makePayload() {
    return Payload{kTestString.data(), kTestString.size()};
  }

  void assertShadowRatio(std::unique_ptr<Reader>& reader,
                         std::atomic_int& total_appends,
                         std::pair<int, int> ratio) {
    const int multiplier = 4;
    const double target_ratio = ratio.first * 1.0 / ratio.second;
    double current_ratio;
    int total_shadows = 0;
    std::vector<std::unique_ptr<DataRecord>> records;
    GapRecord gap;

    while (true) {
      ssize_t nrecords = reader->read(1, &records, &gap);
      if (nrecords < 0) { // gap
        continue;
      }

      if (nrecords == 1) {
        total_shadows++;
        EXPECT_EQ(records[0]->payload.toString(), kTestString);
        records.clear();
      }

      int total_appends_load = total_appends.load();
      if (total_appends_load == 0) {
        continue;
      }
      current_ratio = total_shadows * 1.0 / total_appends_load;
      RATELIMIT_INFO(
          250ms,
          1,
          LD_SHADOW_TEST_PREFIX
          "Current shadow ratio: %.4f / target: %.4f (%d shadows / %d appends)",
          current_ratio,
          target_ratio,
          total_shadows,
          total_appends_load);

      // Wait for a minimum number of appends before testing ratio
      if (total_appends_load > ratio.second * kNumClientThreads * multiplier &&
          std::fabs(current_ratio - target_ratio) < kRatioAccuracy) {
        break;
      }
    }

    ld_info(
        LD_SHADOW_TEST_PREFIX
        "Achieved target shadow ratio %.4f with final ratio %.4f (+/- %.4f)",
        target_ratio,
        current_ratio,
        std::fabs(current_ratio - target_ratio));
  }

  IntegrationTestUtils::ClusterFactory factory_;

  Cluster cluster_;
  std::shared_ptr<Client> client_;
  ClientSettings* client_settings_;

  Cluster shadow_cluster_;
  std::shared_ptr<Client> shadow_client_;
};

// For now, just make sure it doesn't crash
TEST_F(ShadowIntegrationTest, SanityCheck) {
  makeCluster();
  auto payload = makePayload();
  lsn_t lsn;

  // Test with shadow disabled
  lsn = client_->appendSync(logid_t{1}, payload);
  EXPECT_NE(lsn, LSN_INVALID) << err;

  // Test with shadow enabled
  client_settings_->set("traffic-shadow-enabled", "true");
  lsn = client_->appendSync(logid_t{1}, payload);
  EXPECT_NE(lsn, LSN_INVALID) << err;
  // Test again now log group is loaded
  lsn = client_->appendSync(logid_t{1}, payload);
  EXPECT_NE(lsn, LSN_INVALID) << err;
}

TEST_F(ShadowIntegrationTest, ShadowTraffic) {
  makeCluster();
  connectShadowCluster();

  // A small value forces all shadow client creations to go through
  // the retry path.
  client_settings_->set("shadow-client-creation-retry-interval", "1s");

  // Keep appending until shadow ratio is approximately right or test times out
  // This is best approach since no way to know when shadow client is loaded
  // Better way to do this?
  std::unique_ptr<Reader> reader = shadow_client_->createReader(1);
  reader->setTimeout(50ms);
  int rv = reader->startReading(logid_t{1}, LSN_OLDEST);
  EXPECT_EQ(rv, 0);

  const auto client_sleep = []() {
    return std::chrono::milliseconds(folly::Random::rand32(10, 100));
  };

  std::atomic_int total_appends{0};
  std::atomic_bool shutdown{false};
  std::vector<std::thread> client_threads;
  ld_info(LD_SHADOW_TEST_PREFIX "Starting writer threads");
  for (int i = 0; i < kNumClientThreads; i++) {
    client_threads.emplace_back([&]() {
      lsn_t lsn;
      Payload payload;
      while (!shutdown) {
        payload = makePayload();
        lsn = client_->appendSync(logid_t{1}, payload);
        EXPECT_NE(lsn, LSN_INVALID) << err;
        total_appends++;
        /* sleep override */
        std::this_thread::sleep_for(client_sleep());
      }
    });
  }

  client_settings_->set("traffic-shadow-enabled", "true");
  assertShadowRatio(reader, total_appends, kShadowRatio);

  // Update shadow ratio in real time
  total_appends = 0;
  std::pair<int, int> ratio{13, 20};
  updateShadowRatio(0.65);
  assertShadowRatio(reader, total_appends, ratio);

  // Finally make sure shadowing can be disabled
  total_appends = 0;
  client_settings_->set("traffic-shadow-enabled", "false");
  assertShadowRatio(reader, total_appends, {0, 10});

  shutdown = true;
  for (auto& thread : client_threads) {
    thread.join();
  }
}

TEST_F(ShadowIntegrationTest, ShadowBufferedWriterTraffic) {
  makeCluster();
  connectShadowCluster();

  // Keep appending until shadow ratio is approximately right or test times out
  // This is best approach since no way to know when shadow client is loaded
  // Better way to do this?
  std::unique_ptr<Reader> reader = shadow_client_->createReader(1);
  reader->setTimeout(50ms);
  int rv = reader->startReading(logid_t{1}, LSN_OLDEST);
  EXPECT_EQ(rv, 0);

  const auto client_sleep = []() {
    return std::chrono::milliseconds(folly::Random::rand32(10, 100));
  };

  std::atomic_int total_appends{0};
  std::atomic_bool shutdown{false};
  std::vector<std::thread> client_threads;
  ld_info(LD_SHADOW_TEST_PREFIX "Starting writer threads");
  for (int i = 0; i < kNumClientThreads; i++) {
    client_threads.emplace_back([&]() {
      Payload payload;
      TestCallback cb;
      auto writer = BufferedWriter::create(client_, &cb);
      const logid_t LOG_ID(1);
      size_t num_appends = 0;
      const int record_count_per_append = 10;

      while (!shutdown) {
        ASSERT_EQ(
            0, writer->append(LOG_ID, std::string(kTestString), NULL_CONTEXT));
        ++num_appends;
        if (num_appends % record_count_per_append == 0) {
          ASSERT_EQ(0, writer->flushAll());
          for (int j = 0; j < record_count_per_append; ++j) {
            cb.sem.wait();
          }
          /* sleep override */
          std::this_thread::sleep_for(client_sleep());
        }
        total_appends++;
      }
    });
  }

  client_settings_->set("traffic-shadow-enabled", "true");
  assertShadowRatio(reader, total_appends, kShadowRatio);

  // Update shadow ratio in real time
  total_appends = 0;
  std::pair<int, int> ratio{13, 20};
  updateShadowRatio(0.65);
  assertShadowRatio(reader, total_appends, ratio);

  // Finally make sure shadowing can be disabled
  total_appends = 0;
  client_settings_->set("traffic-shadow-enabled", "false");
  assertShadowRatio(reader, total_appends, {0, 10});

  shutdown = true;
  for (auto& thread : client_threads) {
    thread.join();
  }
}
