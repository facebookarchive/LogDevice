/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <gtest/gtest.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/settings/Validators.h"
#include "logdevice/common/test/TestUtil.h"

namespace facebook { namespace logdevice {

enum class TestEnum : char { VALUE1 = 0, VALUE2 };

std::istream& operator>>(std::istream& in, TestEnum& val) {
  std::string token;
  in >> token;
  if (token == "value1") {
    val = TestEnum::VALUE1;
  } else if (token == "value2") {
    val = TestEnum::VALUE2;
  } else {
    in.setstate(std::ios::failbit);
  }
  return in;
}

struct Bundle1 : public SettingsBundle {
  int setting_1;
  chrono_interval_t<std::chrono::milliseconds> setting_2;
  int setting_3;
  TestEnum setting_4;

  const char* getName() const override {
    return "Settings1";
  }

  void defineSettings(SettingEasyInit& init) override {
    using namespace SettingFlag;
    init("bundle-1-setting-1",
         &setting_1,
         "42",
         nullptr,
         "This is a test setting",
         SERVER);

    init("bundle-1-setting-2",
         &setting_2,
         "100ms..1s",
         [](chrono_interval_t<std::chrono::milliseconds>) {},
         "This is another test setting",
         SERVER);

    init("bundle-1-setting-3",
         &setting_3,
         "333",
         [](int) {},
         "This is another test setting that cannot be changed at run-time",
         SERVER | REQUIRES_RESTART);

    init("bundle-1-setting-4",
         &setting_4,
         "value2",
         [](TestEnum) {},
         "This is another test setting",
         SERVER);
  }

 private:
  // Only UpdateableSettings can create this bundle.
  Bundle1() {}
  friend class UpdateableSettingsRaw<Bundle1>;
};

struct Bundle2 : public SettingsBundle {
  int setting_1;
  bool setting_2;
  int setting_3;
  int setting_4;
  int setting_5;

  const char* getName() const override {
    return "Settings2";
  }

  void defineSettings(SettingEasyInit& init) override {
    using namespace SettingFlag;
    init("bundle-2-setting-1",
         &setting_1,
         "1337",
         nullptr,
         "This is a test setting",
         SERVER);

    init("bundle-2-setting-2",
         &setting_2,
         "false",
         nullptr,
         "This is another test setting",
         SERVER);

    init("bundle-2-setting-3",
         &setting_3,
         "3",
         nullptr,
         "This is another test setting",
         SERVER);

    init("bundle-2-setting-4",
         &setting_4,
         "8",
         nullptr,
         "This is a test setting that can only be set via cli",
         SERVER | CLI_ONLY);

    init("bundle-2-setting-5",
         &setting_5,
         "10",
         nullptr,
         "This is a test setting that can only be set internally (by LD code)",
         INTERNAL_ONLY);
  }

 private:
  // Only UpdateableSettings can create this bundle.
  Bundle2() {}
  friend class UpdateableSettingsRaw<Bundle2>;
};

class test_parse_memory_budget
    : public setting_validators::parse_memory_budget {
  size_t getAvailableMemory() override {
    return 100'000'000;
  }
};

struct BundleMemoryBudget : public SettingsBundle {
 public:
  size_t my_memory_budget;

  const char* getName() const override {
    return "MemoryBudget";
  }

  void defineSettings(SettingEasyInit& init) override {
    using namespace SettingFlag;
    init("my-memory-budget",
         &my_memory_budget,
         "32M",
         test_parse_memory_budget(),
         "This is a test setting",
         SERVER);
  }

 private:
  // Only UpdateableSettings can create this bundle.
  BundleMemoryBudget() {}
  friend class UpdateableSettingsRaw<BundleMemoryBudget>;
};

class SettingsTest : public ::testing::Test {
 public:
  SettingsTest() {
    settings.registerSettings(bundle_1);
    settings.registerSettings(bundle_2);
  }

  void parseFromCLI(std::vector<std::string> args, bool server_only = true) {
    auto argc = args.size() + 1;
    const char* argv[argc];
    argv[0] = "SettingsTest";
    for (int i = 0; i < args.size(); ++i) {
      argv[i + 1] = args[i].data();
    }
    settings.parseFromCLI(
        argc,
        argv,
        server_only ? &SettingsUpdater::mustBeServerOption : nullptr);
  }

  UpdateableSettings<Bundle1> bundle_1;
  UpdateableSettings<Bundle2> bundle_2;
  SettingsUpdater settings;
};

TEST_F(SettingsTest, Simple) {
  // Check that the default values are correct.
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Change one setting.
  parseFromCLI({"--bundle-1-setting-1=43"});
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Change another setting.
  ServerConfig::SettingsConfig s;
  s["bundle-2-setting-3"] = "6";
  settings.setFromConfig(s);
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(6, bundle_2->setting_3);

  // Change another setting.
  settings.setFromAdminCmd("--bundle-1-setting-2", "60s..80s");
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::seconds{60}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{80}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(6, bundle_2->setting_3);

  // Change another setting.
  settings.setFromAdminCmd("bundle-2-setting-2", "true");
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::seconds{60}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{80}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_TRUE(bundle_2->setting_2);
  EXPECT_EQ(6, bundle_2->setting_3);

  // Change another setting.
  settings.setFromAdminCmd("bundle-2-setting-2", "false");
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::seconds{60}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{80}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(6, bundle_2->setting_3);

  // Change another setting.
  ServerConfig::SettingsConfig s2;
  s2["bundle-2-setting-3"] = "9";
  settings.setFromConfig(s2);
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::seconds{60}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{80}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(9, bundle_2->setting_3);

  // Change another setting.
  settings.setFromAdminCmd("bundle-2-setting-1", "4242");
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::seconds{60}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{80}, bundle_1->setting_2.hi);
  EXPECT_EQ(4242, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(9, bundle_2->setting_3);
}

// As settings are set/unset from different sources, verify that priority
// between sources is respected.
TEST_F(SettingsTest, Priority) {
  // Check that the default values are correct.
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Change one setting from the CLI
  parseFromCLI({"--bundle-1-setting-1=43"});
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Override some values from the config.
  ServerConfig::SettingsConfig s;
  s["bundle-1-setting-1"] = "52";
  s["bundle-1-setting-2"] = "333ms..444ms";
  settings.setFromConfig(s);
  EXPECT_EQ(52, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{333}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::milliseconds{444}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Override some values from the admin command.
  settings.setFromAdminCmd("bundle-1-setting-1", "64");
  settings.setFromAdminCmd("bundle-1-setting-2", "1ms..2ms");
  settings.setFromAdminCmd("bundle-2-setting-1", "888");
  EXPECT_EQ(64, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{1}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::milliseconds{2}, bundle_1->setting_2.hi);
  EXPECT_EQ(888, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Unset some values from the admin command.
  settings.unsetFromAdminCmd("--bundle-1-setting-2");
  EXPECT_EQ(64, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{333}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::milliseconds{444}, bundle_1->setting_2.hi);
  EXPECT_EQ(888, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Setting new values from config.
  // Note that bundle-1-setting-2 was set to 333ms..444ms from the config but it
  // does not appear anymore. It should be reset to the default value 100ms..1s.
  ServerConfig::SettingsConfig s2;
  s2["bundle-1-setting-1"] = "2048";
  s2["bundle-2-setting-3"] = "7";
  settings.setFromConfig(s2);
  EXPECT_EQ(64, bundle_1->setting_1); // Still overridden by admin command
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(888, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(7, bundle_2->setting_3);

  // Removing all values from the config.
  ServerConfig::SettingsConfig s3;
  settings.setFromConfig(s3);
  EXPECT_EQ(64, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(888, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Removing all admin command overrides.
  settings.unsetFromAdminCmd("bundle-1-setting-1");
  settings.unsetFromAdminCmd("bundle-2-setting-1");
  EXPECT_EQ(43, bundle_1->setting_1); // Back to value from CLI
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1); // Back to default value
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
}

// Verify that we cannot change bundle_1->setting_3 at run-time because it uses
// REQUIRES_RESTART.
// Verify that we cannot set bundle-2-setting-4 via the admin command or config
// because it uses CLI_ONLY.
TEST_F(SettingsTest, RequiresRestartAndCliOnly) {
  // Check that the default values are correct.
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(333, bundle_1->setting_3);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
  EXPECT_EQ(8, bundle_2->setting_4);

  // Set some settings from the CLI
  parseFromCLI({"--bundle-1-setting-1=43", "--bundle-1-setting-3=663"});
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(663, bundle_1->setting_3);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
  EXPECT_EQ(8, bundle_2->setting_4);

  // Set some settings from the config
  ServerConfig::SettingsConfig s;
  s["bundle-1-setting-3"] = "664";
  settings.setFromConfig(s);
  EXPECT_EQ(43, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(664, bundle_1->setting_3);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
  EXPECT_EQ(8, bundle_2->setting_4);

  // Set the setting from the admin command, which should fail because the
  // setting has the REQUIRES_RESTART flag.
  ASSERT_THROW(settings.setFromAdminCmd("bundle-2-setting-4", "12"),
               boost::program_options::error);

  // If the setting is changed again from the config, the change is ignored.
  ServerConfig::SettingsConfig s3;
  s3["bundle-1-setting-3"] = "668";
  s3["bundle-1-setting-1"] = "44";
  settings.setFromConfig(s3);
  EXPECT_EQ(44, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(664, bundle_1->setting_3);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
  EXPECT_EQ(8, bundle_2->setting_4);

  // Same, but this time the setting is removed from the config. If the setting
  // did not have the REQUIRES_RESTART flag, it would be expected it gets its
  // original value of 663 from the cli. However the setting should remain with
  // value 664.
  ServerConfig::SettingsConfig s4;
  s4["bundle-1-setting-1"] = "44";
  settings.setFromConfig(s4);
  EXPECT_EQ(44, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(664, bundle_1->setting_3);
  EXPECT_EQ(1337, bundle_2->setting_1);
  EXPECT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
  EXPECT_EQ(8, bundle_2->setting_4);
}

TEST_F(SettingsTest, InternalOnly) {
  // default value
  ASSERT_EQ(10, bundle_2->setting_5);
  settings.setInternalSetting("bundle-2-setting-5", "15");
  ASSERT_EQ(15, bundle_2->setting_5);

  // All other modifications should fail
  ASSERT_THROW(settings.setFromAdminCmd("bundle-2-setting-5", "20"),
               boost::program_options::error);
  ServerConfig::SettingsConfig from_conf;
  from_conf["bundle-2-setting-5"] = "25";
  settings.setFromConfig(from_conf);
  ASSERT_EQ(15, bundle_2->setting_5);
  ASSERT_THROW(parseFromCLI({"--bundle-2-setting-5", "30"}),
               boost::program_options::error);
  ASSERT_THROW(parseFromCLI({"--bundle-2-setting-5", "35"}, false),
               boost::program_options::error);
  ASSERT_EQ(15, bundle_2->setting_5);
}

TEST_F(SettingsTest, ConfigIgnoreUnknown) {
  // An unknown setting should not pass validation, but should work fine when
  // setting
  ServerConfig::SettingsConfig from_conf;
  from_conf["unrecognized-settings"] = "25";
  settings.setFromConfig(from_conf);
  ASSERT_THROW(
      settings.validateFromConfig(from_conf), boost::program_options::error);
}

TEST_F(SettingsTest, BoolImplicitValue) {
  // Check that the default values are correct.
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Change one setting.
  parseFromCLI({"--bundle-2-setting-2"});
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_TRUE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);

  // Change it back.
  settings.setFromAdminCmd("--bundle-2-setting-2", "false");
  EXPECT_EQ(42, bundle_1->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{100}, bundle_1->setting_2.lo);
  EXPECT_EQ(std::chrono::seconds{1}, bundle_1->setting_2.hi);
  EXPECT_EQ(1337, bundle_2->setting_1);
  ASSERT_FALSE(bundle_2->setting_2);
  EXPECT_EQ(3, bundle_2->setting_3);
}

TEST(SettingsTest_, DefaultsInConstructor) {
  Bundle1 b = create_default_settings<Bundle1>();
  b.setting_1 = 1337;
  b.setting_2.lo = std::chrono::milliseconds{3};
  b.setting_2.hi = std::chrono::milliseconds{8};
  UpdateableSettings<Bundle1> bundle(b);

  EXPECT_EQ(1337, bundle->setting_1);
  EXPECT_EQ(std::chrono::milliseconds{3}, bundle->setting_2.lo);
  EXPECT_EQ(std::chrono::milliseconds{8}, bundle->setting_2.hi);
}

TEST(SettingsTest_, ParseMemoryBudget) {
  SettingsUpdater settings;
  UpdateableSettings<BundleMemoryBudget> bundle;
  settings.registerSettings(bundle);

  // Check default value
  EXPECT_EQ(32000000, bundle->my_memory_budget);

  // Setting to 12K.
  ServerConfig::SettingsConfig from_conf;
  from_conf["my-memory-budget"] = "12K";
  settings.setFromConfig(from_conf);
  EXPECT_EQ(12000, bundle->my_memory_budget);

  // Setting to 5% of total ram.
  from_conf["my-memory-budget"] = "5%";
  settings.setFromConfig(from_conf);
  EXPECT_EQ(5000000, bundle->my_memory_budget);

  // Setting to 5.5% of total ram.
  from_conf["my-memory-budget"] = "5.73%";
  settings.setFromConfig(from_conf);
  EXPECT_EQ(5730000, bundle->my_memory_budget);

  // Back to default value.
  settings.setFromConfig(ServerConfig::SettingsConfig());
  EXPECT_EQ(32000000, bundle->my_memory_budget);

  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", ""),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "12.3"),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "0"),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "0%"),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "%"),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "101%"),
               boost::program_options::error);
  EXPECT_THROW(settings.setFromAdminCmd("my-memory-budget", "invalid value"),
               boost::program_options::error);
}

TEST(SettingsTest_, OverrideDefaults) {
  UpdateableSettings<Bundle1> s(
      {{"bundle-1-setting-1", "1337"}, {"bundle-1-setting-4", "value1"}});

  // Make sure the overrides applied.
  EXPECT_EQ(1337, s->setting_1);
  EXPECT_EQ(333, s->setting_3);
  EXPECT_EQ(TestEnum::VALUE1, s->setting_4);

  SettingsUpdater up;
  up.registerSettings(s);
  up.setFromCLI({{"bundle-1-setting-1", "13"}, {"bundle-1-setting-3", "31"}});

  // Make sure the overrides are still in effect after updating.
  EXPECT_EQ(13, s->setting_1);
  EXPECT_EQ(31, s->setting_3);
  EXPECT_EQ(TestEnum::VALUE1, s->setting_4);

  // Make sure overridden defaults show up in help message.
  std::string h = up.help();
  EXPECT_TRUE(h.find("1337") != std::string::npos);

  // Sanity check: make sure the overridden defaults don't show up in help
  // message if we don't do any overrides.
  {
    UpdateableSettings<Bundle1> s0;
    SettingsUpdater up0;
    up0.registerSettings(s0);
    std::string h0 = up0.help();
    EXPECT_EQ(std::string::npos, h0.find("1337"));
  }
}

}} // namespace facebook::logdevice
