/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/shadow/Shadow.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <utility>

#include <gtest/gtest.h>

#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/include/debug.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/shadow/ShadowClient.h"

namespace facebook { namespace logdevice {

using namespace logsconfig;

struct TestLogData {
  std::string path;
  logid_range_t range;
  LogAttributes::Shadow attrs;
  std::pair<int, int> ratio;
};
static constexpr int kTestDataLen = 2;
static const std::array<TestLogData, kTestDataLen> kTestData{
    {{"/test_log",
      {logid_t{1}, logid_t{100}},
      {"logdevice.test", 0.1},
      {1, 10}}, // 1/10 = 0.1
     {"/test_log2",
      {logid_t{201}, logid_t{300}},
      {"logdevice.test2", 0.8},
      {4, 5}}}}; // 4/5 = 0.8

class MockShadowClientFactory : public ShadowClientFactory {
 public:
  explicit MockShadowClientFactory(std::string origin_name,
                                   UpdateableSettings<Settings> client_settings)
      : ShadowClientFactory(origin_name, nullptr, client_settings) {}
  ~MockShadowClientFactory() override = default;

  void start(std::chrono::milliseconds) override {}
  void shutdown() override {}
  int createAsync(const Shadow::Attrs& /*attrs*/,
                  bool /* is_a_retry */) override {
    return 0;
  }
};

class MockShadow : public Shadow {
 public:
  MockShadow(std::string origin_name,
             std::shared_ptr<UpdateableConfig> origin_config,
             UpdateableSettings<Settings> client_settings)
      : Shadow(origin_name,
               std::move(origin_config),
               client_settings,
               nullptr,
               std::make_unique<MockShadowClientFactory>(origin_name,
                                                         client_settings)) {}

  bool isReset() {
    return !last_used_range_.hasValue() && range_cache_.empty() &&
        shadow_map_.empty();
  }

  folly::Optional<logid_range_t> getLastUsedRange() {
    return last_used_range_;
  }

  bool isLogGroupLoaded(logid_range_t range) {
    return std::find(range_cache_.begin(), range_cache_.end(), range) !=
        range_cache_.end();
  }

  ShadowInfo getLoadedLogGroup(logid_range_t range) {
    return shadow_map_[range];
  }
};

class ShadowTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dbg::currentLevel = dbg::Level::DEBUG;
    config = std::make_shared<UpdateableConfig>();
    settings.reset(static_cast<ClientSettingsImpl*>(ClientSettings::create()));
    shadow =
        std::make_unique<MockShadow>("test", config, settings->getSettings());
  }

  void TearDown() override {
    // noop
  }

  void setShadowingEnabled(bool enabled) {
    settings->set("traffic-shadow-enabled", (enabled ? "true" : "false"));
  }

  // log range loading will happen in sync with LocalLogsConfig
  template <std::size_t N = kTestDataLen>
  void genLogsConfig(bool shadow_enabled = false,
                     const std::array<TestLogData, N>& testData = kTestData) {
    auto lc = std::make_shared<configuration::LocalLogsConfig>();
    auto root = lc->getRootNamespace();
    for (const auto& testLogData : testData) {
      logsconfig::LogAttributes attrs =
          logsconfig::LogAttributes().with_replicationFactor(3);
      if (shadow_enabled) {
        attrs = attrs.with_shadow(testLogData.attrs);
      }
      lc->insert(root, testLogData.range, testLogData.path, attrs);
    }
    config->updateableLogsConfig()->update(lc);
  }

  // tests ratio for first log range by default
  template <int n = 0>
  void assertShadowRatio(std::pair<int, int> ratio = kTestData[n].ratio) {
    const int loops = 3;
    int shadows = 0;
    for (int i = 0; i < ratio.second * loops; i++) {
      if (shadow->checkShadowConfig(kTestData[n].range.first)) {
        shadows++;
      } else {
        ASSERT_EQ(err, E::SHADOW_SKIP);
      }
    }
    ASSERT_EQ(shadows, ratio.first * loops);
  }

  std::shared_ptr<UpdateableConfig> config;
  std::unique_ptr<ClientSettingsImpl> settings;
  std::unique_ptr<MockShadow> shadow;
};

// Ensure shadowing can be enabled/disabled using client settings
TEST_F(ShadowTest, ClientDisableShadow) {
  // Shadowing should be disabled by default
  ASSERT_FALSE(settings->getSettings()->traffic_shadow_enabled);
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{0}));
  ASSERT_EQ(err, E::SHADOW_DISABLED);

  // Should respond to enabling in settings after initialization
  setShadowingEnabled(true);
  ASSERT_TRUE(settings->getSettings()->traffic_shadow_enabled);
  // Still false because no LogsConfig has been provided
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{0}));
  ASSERT_EQ(err, E::SHADOW_UNCONFIGURED);

  // Should respond to disabling after initialization
  setShadowingEnabled(false);
  ASSERT_FALSE(settings->getSettings()->traffic_shadow_enabled);
  ASSERT_TRUE(shadow->isReset());
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{0}));
  ASSERT_EQ(err, E::SHADOW_DISABLED);

  // Also can manually verify console outputs shadow enabled/disabled messages
}

// Test logid -> log range resolution with shadow unconfigured
TEST_F(ShadowTest, LogRangeResolutionNoShadow) {
  genLogsConfig(false);
  setShadowingEnabled(true);

  // Out of range logid
  // Console output - failure message
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{0}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  ASSERT_FALSE(shadow->isLogGroupLoaded(kTestData[0].range));

  // In range logid but shadow not configured
  // Console output - loaded but unconfigured message
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  ASSERT_TRUE(shadow->isLogGroupLoaded(kTestData[0].range));
  ASSERT_FALSE(shadow->getLoadedLogGroup(kTestData[0].range).attrs.hasValue());

  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{201}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  ASSERT_TRUE(shadow->isLogGroupLoaded(kTestData[1].range));
  ASSERT_FALSE(shadow->getLoadedLogGroup(kTestData[1].range).attrs.hasValue());
}

// Test logid -> log range resolution with shadow configured
TEST_F(ShadowTest, LogRangeResolutionWithShadow) {
  genLogsConfig(true);
  setShadowingEnabled(true);

  // In range logid with shadow configured
  // Console output - loaded message with shadow parameters
  // Fails first time, needs to load log group (happens in sync with
  // LocalLogsConfig)
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  ASSERT_TRUE(shadow->isLogGroupLoaded(kTestData[0].range));
  ASSERT_TRUE(shadow->getLoadedLogGroup(kTestData[0].range).attrs.hasValue());
  ASSERT_EQ(shadow->getLoadedLogGroup(kTestData[0].range).attrs.value(),
            kTestData[0].attrs);

  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{201}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  ASSERT_TRUE(shadow->isLogGroupLoaded(kTestData[1].range));
  ASSERT_TRUE(shadow->getLoadedLogGroup(kTestData[1].range).attrs.hasValue());
  ASSERT_EQ(shadow->getLoadedLogGroup(kTestData[1].range).attrs.value(),
            kTestData[1].attrs);

  // Should succeed now for configured ratio
  assertShadowRatio<0>();
  assertShadowRatio<1>();
}

// Test range caching
TEST_F(ShadowTest, LogRangeCacheTest) {
  genLogsConfig(true);
  setShadowingEnabled(true);

  ASSERT_FALSE(shadow->getLastUsedRange());

  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{201}));

  // checkShadowConfig still returns false because of ratio
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_TRUE(shadow->getLastUsedRange());
  ASSERT_EQ(shadow->getLastUsedRange(), kTestData[0].range);

  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{201}));
  ASSERT_TRUE(shadow->getLastUsedRange());
  ASSERT_EQ(shadow->getLastUsedRange(), kTestData[1].range);

  setShadowingEnabled(false);
  ASSERT_FALSE(shadow->getLastUsedRange());
}

// Test auto update of log ranges that have been previously appended to
TEST_F(ShadowTest, LogRangeAutoUpdate) {
  genLogsConfig(false);
  genLogsConfig(true);
  setShadowingEnabled(true);

  // This should not have updated anything
  ASSERT_FALSE(shadow->isLogGroupLoaded(kTestData[0].range));

  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_LOADING);
  assertShadowRatio();

  // Update config and make sure it propagates
  auto testDataCopy = kTestData;
  testDataCopy[0].attrs = {"logdevice.test3", 0.65};
  genLogsConfig(true, testDataCopy);
  assertShadowRatio({13, 20});
  genLogsConfig(false);
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_UNCONFIGURED);
  genLogsConfig(true);
  assertShadowRatio();

  // Should stop updating after disable, test twice to make sure doesn't load
  setShadowingEnabled(false);
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_DISABLED);
  genLogsConfig(false);
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_FALSE(shadow->checkShadowConfig(logid_t{1}));
  ASSERT_EQ(err, E::SHADOW_DISABLED);
}

// Make sure shadow client initializes without crashing
TEST(ShadowClientTest, ShadowClientInitAppend) {
  LogAttributes::Shadow shadowAttr{
      std::string("file:") + TEST_CONFIG_FILE("sample_no_ssl.conf"), 0.1};
  std::shared_ptr<ShadowClient> shadow_client = ShadowClient::create(
      "test", shadowAttr, std::chrono::seconds(10), nullptr);
  ASSERT_NE(shadow_client, nullptr);

  std::string payload_str = "test";
  Payload payload{payload_str.data(), payload_str.size()};
  int rv = shadow_client->append(logid_t{1}, payload, {}, false);
  ASSERT_EQ(rv, 0);
}

}} // namespace facebook::logdevice
