/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/FileConfigSource.h"

#include <chrono>
#include <cstdlib>
#include <thread>
#include <unistd.h>

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include "logdevice/common/FileConfigSourceThread.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/ConfigSubscriptionHandle.h"

using namespace facebook::logdevice;

// NOTE: file reading assumes the test is being run from the top-level fbcode
// dir

// Interval at which FileConfigSourceThread wakes up and re-reads configs
const std::chrono::milliseconds CONFIG_UPDATE_INTERVAL(10);

// Waits for FileConfigSourceThread to wake up and re-read configs
static void wait_for_config_update(FileConfigSourceThread* thread) {
  const std::chrono::seconds MAX_WAIT(5);
  using std::chrono::steady_clock;

  int64_t start_iters = thread->main_loop_iterations_;
  steady_clock::time_point tstart = steady_clock::now();
  while (true) {
    int64_t now_iters = thread->main_loop_iterations_;
    if (now_iters >= start_iters + 2) {
      // The counter went up by 2, which ensures that
      // FileConfigSourceThread completed at least one full iteration of
      // its loop since this function started.
      break;
    }

    auto elapsed = steady_clock::now() - tstart;
    if (elapsed >= MAX_WAIT) {
      FAIL() << "wait_for_config_update() wait time exceeded MAX_WAIT\n";
    }

    /* sleep override */
    std::this_thread::sleep_for(CONFIG_UPDATE_INTERVAL);
  }
}

class FileConfigSourceTest : public ::testing::Test {
  void SetUp() override {
    dbg::assertOnData = true;
  }
};

static void readerThreadImpl(std::shared_ptr<UpdateableConfig>,
                             FileConfigSourceThread*);
static std::atomic<bool> reader_thread_waiting_;
static std::atomic<bool> writer_thread_waiting_;

// Helper method to create an UpdateableConfig tracking a file using
// FileConfigSource
static std::pair<std::shared_ptr<UpdateableConfig>, FileConfigSourceThread*>
create_tracked_config(const std::string& path) {
  auto config = UpdateableConfig::createEmpty();
  auto updater = std::make_unique<TextConfigUpdater>(config);
  auto source = std::make_unique<FileConfigSource>(CONFIG_UPDATE_INTERVAL);
  FileConfigSourceThread* thread = source->thread();
  ld_check(thread != nullptr);
  updater->registerSource(std::move(source));
  int rv = updater->load("file:" + path, nullptr);
  if (rv != 0) {
    return std::make_pair(nullptr, nullptr);
  }
  // FileConfigSource is blocking so load() should have already populated the
  // config with a valid config
  if (!config->get()) {
    return std::make_pair(nullptr, nullptr);
  }
  config->updateableServerConfig()->setUpdater(std::move(updater));
  config->updateableLogsConfig()->setUpdater(std::move(updater));
  return std::make_pair(config, thread);
}

/**
 * Creates an auto-updating config, starts a reader thread, modifies the
 * configuration file, verifies that the reader thread picks up the new
 * config.
 */
TEST_F(FileConfigSourceTest, Simple) {
  char configPath[] = "/tmp/FileConfigSourceTest.conf.XXXXXX";
  int fd = mkstemp(configPath);
  ASSERT_NE(fd, -1);
  close(fd);

  SCOPE_EXIT {
    unlink(configPath);
  };

  // Write the initial config with generation 1.
  writeSimpleConfig(configPath, 1);

  // Create the auto-updating config.
  std::shared_ptr<UpdateableConfig> config;
  FileConfigSourceThread* thread;
  std::tie(config, thread) = create_tracked_config(configPath);
  ASSERT_NE(nullptr, config);
  ASSERT_NE(nullptr, config->get());

  reader_thread_waiting_.store(true);
  writer_thread_waiting_.store(true);

  // Start a reader thread.  It will check the config, wait a while and check
  // again.
  std::thread readerThread(readerThreadImpl, config, thread);

  while (writer_thread_waiting_.load()) {
    // sleep for a while until readerThreadImpl() checks the config
    /* sleep override */
    std::this_thread::sleep_for(CONFIG_UPDATE_INTERVAL);
  }

  // Write the new config with generation 2.
  writeSimpleConfig(configPath, 2);
  reader_thread_waiting_.store(false);

  readerThread.join();
}

TEST_F(FileConfigSourceTest, MultipleConfigs) {
  char configPath[] = "/tmp/MultipleConfigurationTest.conf.XXXXXX";
  int fd = mkstemp(configPath);
  ASSERT_NE(fd, -1);
  close(fd);

  SCOPE_EXIT {
    unlink(configPath);
  };

  // Write the initial config with generation 1.
  writeSimpleConfig(configPath, 1);
  std::string configPathStr = configPath;

  // Create the auto-updating configs.
  std::shared_ptr<UpdateableConfig> client1_config, client2_config;
  FileConfigSourceThread *thread1, *thread2;
  std::tie(client1_config, thread1) = create_tracked_config(configPath);
  std::tie(client2_config, thread2) = create_tracked_config(configPath);
  ASSERT_NE(nullptr, client1_config);
  ASSERT_NE(nullptr, client2_config);

  writeSimpleConfig(configPath, 3);

  wait_for_config_update(thread1);
  EXPECT_EQ(
      3, client1_config->get()->serverConfig()->getNodes().at(0).generation);
  wait_for_config_update(thread2);
  EXPECT_EQ(
      3, client2_config->get()->serverConfig()->getNodes().at(0).generation);
}

static void readerThreadImpl(std::shared_ptr<UpdateableConfig> config,
                             FileConfigSourceThread* thread) {
  std::shared_ptr<Configuration> c1 = config->get();
  EXPECT_EQ(1, c1->serverConfig()->getNodes().at(0).generation);

  // Try again immediately, expect exactly the same config
  std::shared_ptr<Configuration> c2 = config->get();
  EXPECT_EQ(c1->serverConfig(), c2->serverConfig());
  EXPECT_EQ(c1->logsConfig(), c2->logsConfig());

  writer_thread_waiting_.store(false);
  while (reader_thread_waiting_.load()) {
    // wait until writeSimpleConfig() is called
    /* sleep override */
    std::this_thread::sleep_for(CONFIG_UPDATE_INTERVAL);
  }

  wait_for_config_update(thread);

  // Verify that the config was updated
  std::shared_ptr<Configuration> c3 = config->get();
  EXPECT_EQ(2, c3->serverConfig()->getNodes().at(0).generation);
}

TEST_F(FileConfigSourceTest, SubscribeToUpdates) {
  char configPath[] = "/tmp/FileConfigSourceTest.conf.XXXXXX";
  int fd = mkstemp(configPath);
  ASSERT_NE(fd, -1);
  close(fd);

  SCOPE_EXIT {
    unlink(configPath);
  };

  // Write the initial config with generation 1.
  writeSimpleConfig(configPath, 1);

  // Create the auto-updating config.
  std::shared_ptr<UpdateableConfig> config;
  FileConfigSourceThread* thread;
  std::tie(config, thread) = create_tracked_config(configPath);
  ASSERT_NE(nullptr, config);

  int callback_count = 0;

  auto handle = config->updateableServerConfig()->subscribeToUpdates([&]() {
    ld_info("Callback invoked");
    ++callback_count;
  });
  ld_info("Subscribed to updates");

  wait_for_config_update(thread);
  ASSERT_EQ(0, callback_count); // no calls when config hasn't changed

  writeSimpleConfig(configPath, 2);
  wait_for_config_update(thread);
  // Because of how writeSimpleConfig() overwrites the timestamps, the config
  // may be updated 1 or 2 times.  Since the Aug 2016 config refactoring, each
  // update of the full config file triggers two subscription notifications
  // (one each for the server and logs config), but because we ignore logs
  // section since enable_logsconfig_manager is enabled by default.
  ASSERT_TRUE(callback_count == 1 || callback_count == 2)
      << "Callback count should be 1 or 2, actual " << callback_count;
  int prev_count = callback_count;

  handle.unsubscribe();
  ld_info("Unsubscribed from updates");

  writeSimpleConfig(configPath, 3);
  wait_for_config_update(thread);
  ASSERT_EQ(prev_count, callback_count); // unsubscribed, should be unchanged
}
