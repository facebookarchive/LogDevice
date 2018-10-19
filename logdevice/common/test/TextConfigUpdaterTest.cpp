/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/TextConfigUpdater.h"

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/test/TestUtil.h"

// NOTE: file reading assumes the test is being run from the top-level fbcode
// dir

using namespace facebook::logdevice;

// Helper method to create an UpdateableConfig tracking a file using
// FileConfigSource
static std::shared_ptr<UpdateableConfig>
create_tracked_config(const std::string& path) {
  auto config = std::make_shared<UpdateableConfig>();
  auto settings = create_default_settings<Settings>();
  settings.enable_logsconfig_manager = false;
  auto updater = std::make_shared<TextConfigUpdater>(config, settings);
  updater->registerSource(std::make_unique<FileConfigSource>());
  int rv = updater->load("file:" + path, nullptr);
  if (rv != 0) {
    return nullptr;
  }
  // FileConfigSource is blocking so load() should have already populated the
  // config with a valid config
  if (!config->get()) {
    return nullptr;
  }
  config->updateableServerConfig()->setUpdater(updater);
  config->updateableLogsConfig()->setUpdater(updater);
  return config;
}

/**
 * Tries to reads the compressed config included_logs.conf.gz (and the
 * included included_logs.inc.gz).
 *
 * The two files were generated with:
 * % cat included_logs.conf | sed -e 's/logs.inc/logs.inc.gz/' | gzip >
 * included_logs.conf.gz
 * % cat included_logs.inc | gzip > included_logs.inc.gz
 */
TEST(TextConfigUpdaterTest, GzipCompressed) {
  auto updateable_config =
      create_tracked_config(TEST_CONFIG_FILE("included_logs.conf.gz"));
  ld_check(updateable_config);
  auto config = updateable_config->get();
  ASSERT_NE(nullptr, config);
  ASSERT_NE(nullptr, config->serverConfig());
  ASSERT_NE(nullptr, config->logsConfig());
  ASSERT_EQ(6, config->localLogsConfig()->getLogMap().size());
}

TEST(TextConfigUpdaterTest, CompressedCorrupted) {
  auto updateable_config = create_tracked_config(
      TEST_CONFIG_FILE("included_logs.conf.corrupted.gz"));
  ASSERT_EQ(nullptr, updateable_config);
}
