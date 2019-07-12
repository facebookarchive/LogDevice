/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/plugin/DynamicPluginLoader.h"

#include <folly/FileUtil.h>
#include <folly/experimental/io/FsUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/plugin/test/DummyLoggerPlugin.h"

namespace {

using namespace ::testing;
using namespace facebook::logdevice;

using std::literals::string_literals::operator""s;

class MockDynamicPluginLoader : public DynamicPluginLoader {
 public:
  MOCK_CONST_METHOD0(readEnv, folly::Optional<std::string>());
};

TEST(DynamicPluginLoaderTest, getPlugins) {
  {
    // libdummyplugin.so is expected to be in the same location of the
    // binary.
    auto plugin_path =
        folly::fs::executable_path().remove_filename() / "libdummyplugin.so";
    ASSERT_TRUE(boost::filesystem::exists(plugin_path));
    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv())
        .Times(1)
        .WillOnce(Return(plugin_path.string()));
    auto plugins = loader.getPlugins();
    EXPECT_EQ(1, plugins.size());
    auto& plugin = plugins[0];
    auto dplugin = dynamic_cast<DummyLogger*>(plugin.get());
    ASSERT_NE(nullptr, dplugin);
    ASSERT_EQ("dummy_logger_plugin", dplugin->identifier());
  }

  {
    // Pointing to the binary dir should also discover the shared lib.
    auto binary_dir = folly::fs::executable_path().remove_filename();
    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv())
        .Times(1)
        .WillOnce(Return(binary_dir.string()));
    auto plugins = loader.getPlugins();
    EXPECT_EQ(1, plugins.size());
    auto& plugin = plugins[0];
    auto dplugin = dynamic_cast<DummyLogger*>(plugin.get());
    ASSERT_NE(nullptr, dplugin);
    ASSERT_EQ("dummy_logger_plugin", dplugin->identifier());
  }

  {
    // Env variable not set or empty should be fine
    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv()).Times(1).WillOnce(Return(folly::none));
    EXPECT_EQ(0, loader.getPlugins().size());
  }

  {
    // Empty Dir shouldn't load anything
    auto tmpD = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path();
    create_directories(tmpD);

    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv()).Times(1).WillOnce(Return(tmpD.string()));
    EXPECT_EQ(0, loader.getPlugins().size());
  }

  {
    // A directory ending with ".so" should be ignored.
    auto tmpD = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path();
    create_directories(tmpD);
    create_directories(tmpD / "dir.so");

    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv()).Times(1).WillOnce(Return(tmpD.string()));
    EXPECT_EQ(0, loader.getPlugins().size());
  }
}

TEST(DynamicPluginLoaderTest, getPluginsFailures) {
  {
    // A not found file
    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv()).Times(1).WillOnce(Return("notfound"s));
    ASSERT_THROW(loader.getPlugins(), std::runtime_error);
  }

  {
    // An explicitly passed file that's not a shared library should throw
    auto tmpF = boost::filesystem::temp_directory_path() /
        boost::filesystem::unique_path();
    folly::writeFile("not an so"s, tmpF.c_str());

    MockDynamicPluginLoader loader;
    EXPECT_CALL(loader, readEnv()).Times(1).WillOnce(Return(tmpF.string()));
    ASSERT_THROW(loader.getPlugins(), std::runtime_error);
  }
}

} // namespace
