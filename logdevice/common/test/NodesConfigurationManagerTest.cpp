/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

#include <chrono>

#include <folly/Conv.h>
#include <folly/json.h>
#include <folly/synchronization/Baton.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/FileBasedNodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/membership/utils.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/test/InMemNodesConfigurationStore.h"
#include "logdevice/common/test/MockNodesConfigurationStore.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/test/ZookeeperClientInMemory.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::configuration;
using namespace facebook::logdevice::configuration::nodes;
using namespace facebook::logdevice::configuration::nodes::ncm;
using namespace facebook::logdevice::membership;
using namespace std::chrono_literals;
using namespace facebook::logdevice::NodesConfigurationTestUtil;

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

struct TestDeps : public Dependencies {
  using Dependencies::Dependencies;
  ~TestDeps() override {}
};

namespace {
constexpr const MembershipVersion::Type kVersion{102};
constexpr const MembershipVersion::Type kNewVersion =
    MembershipVersion::Type{kVersion.val() + 1};
const std::string kConfigKey{"/foo"};

NodesConfiguration
makeDummyNodesConfiguration(MembershipVersion::Type version) {
  NodesConfiguration config{};
  config.setVersion(version);
  EXPECT_TRUE(config.validate());
  EXPECT_EQ(version, config.getVersion());
  return config;
}

bool waitTillNCMReceives(const NodesConfigurationManager& ncm,
                         MembershipVersion::Type new_version) {
  auto start_time = std::chrono::steady_clock::now();
  // TODO: better testing after offering a subscription API
  while ((ncm.getConfig() == nullptr ||
          ncm.getConfig()->getVersion() != new_version) &&
         // Wait for at most 10 seconds
         std::chrono::steady_clock::now() - start_time <=
             std::chrono::seconds(10)) {
    /* sleep override */ std::this_thread::sleep_for(200ms);
  }
  auto p = ncm.getConfig();
  EXPECT_EQ(new_version, p->getVersion());
  return new_version == p->getVersion();
}
} // namespace

class NodesConfigurationManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);
    NodesConfiguration initial_config{};
    initial_config.setVersion(MembershipVersion::EMPTY_VERSION);
    EXPECT_TRUE(initial_config.validate());
    auto z = std::make_unique<ZookeeperClientInMemory>(
        "unused quorum",
        ZookeeperClientInMemory::state_map_t{
            {kConfigKey,
             {NodesConfigurationCodec::serialize(initial_config),
              zk::Stat{.version_ = 4}}}});
    z_ = z.get();
    auto store = std::make_unique<ZookeeperNodesConfigurationStore>(
        kConfigKey,
        NodesConfigurationCodec::extractConfigVersion,
        std::move(z));

    Settings settings = create_default_settings<Settings>();
    settings.num_workers = 3;
    settings.enable_nodes_configuration_manager = true;
    settings.use_nodes_configuration_manager_nodes_configuration = true;
    settings.nodes_configuration_manager_store_polling_interval =
        std::chrono::seconds(1);
    settings.nodes_configuration_manager_intermediary_shard_state_timeout =
        std::chrono::seconds(2);

    auto stats_params = StatsParams().setIsServer(true);
    stats_ = std::make_unique<StatsHolder>(std::move(stats_params));
    processor_ = make_test_processor(settings, nullptr, stats_.get());

    auto deps = std::make_unique<TestDeps>(processor_.get(), std::move(store));
    ncm_ = NodesConfigurationManager::create(
        NodesConfigurationManager::OperationMode::forTooling(),
        std::move(deps));
    ASSERT_TRUE(ncm_->init(std::make_shared<const NodesConfiguration>()));
    ncm_->upgradeToProposer();
  }

  //////// Helper functions ////////
  void writeNewVersionToZK(MembershipVersion::Type new_version) {
    auto new_config = std::make_shared<const NodesConfiguration>(
        makeDummyNodesConfiguration(new_version));
    writeNewConfigToZK(std::move(new_config));
  }

  void
  writeNewConfigToZK(std::shared_ptr<const NodesConfiguration> new_config) {
    // fire and forget
    z_->setData(kConfigKey,
                NodesConfigurationCodec::serialize(*new_config),
                /* cb = */ {});
  }

  bool waitTillNCMReceives(MembershipVersion::Type new_version) const {
    return ::waitTillNCMReceives(*ncm_, new_version);
  }

  std::unique_ptr<StatsHolder> stats_;
  std::shared_ptr<Processor> processor_;
  ZookeeperClientBase* z_;
  std::shared_ptr<NodesConfigurationManager> ncm_;
};

TEST_F(NodesConfigurationManagerTest, basic) {
  writeNewVersionToZK(kNewVersion);
  waitTillNCMReceives(kNewVersion);

  // verify each worker has the up-to-date config
  auto verify_version = [](folly::Promise<folly::Unit> p) {
    auto nc = Worker::onThisThread()
                  ->getUpdateableConfig()
                  ->updateableNodesConfiguration();
    EXPECT_TRUE(nc);
    EXPECT_EQ(kNewVersion, nc->get()->getVersion());
    p.setValue();
  };
  auto futures =
      fulfill_on_all_workers<folly::Unit>(processor_.get(), verify_version);
  folly::collectAllSemiFuture(futures).get();
}

TEST_F(NodesConfigurationManagerTest, update) {
  waitTillNCMReceives(MembershipVersion::EMPTY_VERSION);
  {
    auto update = initialAddShardsUpdate();
    ncm_->update(std::move(update),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    waitTillNCMReceives(
        MembershipVersion::Type{MembershipVersion::EMPTY_VERSION.val() + 1});
  }
  auto provisioned_config = ncm_->getConfig();
  writeNewConfigToZK(provisioned_config->withVersion(kVersion));
  waitTillNCMReceives(kVersion);
  {
    // add new node
    NodesConfiguration::Update update = addNewNodeUpdate(*provisioned_config);
    ncm_->update(
        std::move(update),
        [](Status status,
           std::shared_ptr<const NodesConfiguration> new_config) mutable {
          EXPECT_EQ(Status::OK, status);
          EXPECT_EQ(kNewVersion, new_config->getVersion());
        });
    waitTillNCMReceives(kNewVersion);
  }
}

TEST_F(NodesConfigurationManagerTest, trackState) {
  {
    ncm_->update(initialAddShardsUpdate(),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    waitTillNCMReceives(MembershipVersion::Type{1});
    ncm_->update(addNewNodeUpdate(*ncm_->getConfig()),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    waitTillNCMReceives(MembershipVersion::Type{2});
    ncm_->update(markAllShardProvisionedUpdate(*ncm_->getConfig()),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    waitTillNCMReceives(MembershipVersion::Type{3});

    ncm_->update(
        enablingReadUpdate(
            ncm_->getConfig()->getStorageMembership()->getVersion()),
        [](Status status, std::shared_ptr<const NodesConfiguration> nc) {
          ASSERT_EQ(Status::OK, status);
          ASSERT_NE(nullptr, nc);
        });
    // Two version bumps to account for NONE -> NONE_TO_RO, and then
    // NONE_TO_RO to RO
    waitTillNCMReceives(MembershipVersion::Type{5});
  }
  {
    auto nc = ncm_->getConfig();
    auto p = nc->getStorageMembership()->getShardState(ShardID{17, 0});
    EXPECT_TRUE(p.hasValue());
    EXPECT_EQ(membership::StorageState::READ_ONLY, p->storage_state);
    EXPECT_EQ(membership::MetaDataStorageState::NONE, p->metadata_state);
  }
}

TEST_F(NodesConfigurationManagerTest, overwrite) {
  {
    std::vector<zk::Op> ops = {
        ZookeeperClientBase::makeDeleteOp(kConfigKey, /* version */ -1)};
    folly::Baton<> b;
    z_->multiOp(
        std::move(ops), [&b](int rc, std::vector<zk::OpResponse> responses) {
          EXPECT_EQ(ZOK, rc);
          EXPECT_EQ(1, responses.size());
          EXPECT_EQ(ZOK, responses.at(0).rc_);
          b.post();
        });
    b.wait();
  }
  {
    constexpr const MembershipVersion::Type kMidVersion{42};
    EXPECT_LT(kMidVersion, kVersion);
    // ensure we can overwrite even if the znode did not exist
    NodesConfiguration initial_config =
        makeDummyNodesConfiguration(kMidVersion);
    folly::Baton<> b;
    ncm_->overwrite(
        std::make_shared<const NodesConfiguration>(initial_config),
        [&b, &kMidVersion](
            Status status, std::shared_ptr<const NodesConfiguration> config) {
          ASSERT_EQ(E::OK, status);
          EXPECT_TRUE(config);
          EXPECT_EQ(kMidVersion, config->getVersion());
          b.post();
        });
    waitTillNCMReceives(kMidVersion);
    b.wait();
  }
  {
    // ensure we can overwrite the initial empty znode
    auto initial_config = makeDummyNodesConfiguration(kVersion);
    folly::Baton<> b;
    ncm_->overwrite(
        std::make_shared<const NodesConfiguration>(initial_config),
        [&b](Status status, std::shared_ptr<const NodesConfiguration> config) {
          ASSERT_EQ(E::OK, status);
          EXPECT_TRUE(config);
          EXPECT_EQ(kVersion, config->getVersion());
          b.post();
        });
    waitTillNCMReceives(kVersion);
    b.wait();
  }
  writeNewVersionToZK(kNewVersion);
  waitTillNCMReceives(kNewVersion);

  {
    // ensure that we cannot roll back version
    auto rollback_version = MembershipVersion::Type{kVersion.val() - 4};
    auto rollback_config = makeDummyNodesConfiguration(rollback_version);

    folly::Baton<> b;
    ncm_->overwrite(
        std::make_shared<const NodesConfiguration>(std::move(rollback_config)),
        [&b](Status status, std::shared_ptr<const NodesConfiguration> config) {
          EXPECT_EQ(E::VERSION_MISMATCH, status);
          EXPECT_TRUE(config);
          EXPECT_EQ(kNewVersion, config->getVersion());
          b.post();
        });
    b.wait();
    EXPECT_EQ(kNewVersion, ncm_->getConfig()->getVersion());
  }

  {
    // ensure that we cannot repeat the same version
    auto same_version = kVersion;
    auto diff_config = makeDummyNodesConfiguration(same_version);

    folly::Baton<> b;
    ncm_->overwrite(
        std::make_shared<const NodesConfiguration>(std::move(diff_config)),
        [&b](Status status, std::shared_ptr<const NodesConfiguration> config) {
          EXPECT_EQ(E::VERSION_MISMATCH, status);
          EXPECT_TRUE(config);
          EXPECT_EQ(kNewVersion, config->getVersion());
          b.post();
        });
    b.wait();
    EXPECT_EQ(kNewVersion, ncm_->getConfig()->getVersion());
  }

  {
    // ensure we could roll forward versions
    auto forward_version = MembershipVersion::Type{kVersion.val() + 9999};
    auto forward_config = makeDummyNodesConfiguration(forward_version);
    folly::Baton<> b;
    ncm_->overwrite(
        std::make_shared<const NodesConfiguration>(forward_config),
        [&b](Status status, std::shared_ptr<const NodesConfiguration>) {
          EXPECT_EQ(Status::OK, status);
          ld_info("Overwrite successful.");
          b.post();
        });
    waitTillNCMReceives(forward_version);
    b.wait();
  }
}

TEST_F(NodesConfigurationManagerTest, LinearizableReadOnStartup) {
  auto initial_config = makeDummyNodesConfiguration(kVersion);
  EXPECT_TRUE(initial_config.validate());
  std::string config = NodesConfigurationCodec::serialize(initial_config);

  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 3;

  {
    // This is a `forTooling` NCM. It doesn't need to do a linearizable read
    // at startup.
    auto processor = make_test_processor(settings);
    auto store = std::make_unique<MockNodesConfigurationStore>();
    EXPECT_CALL(*store, getConfig_(_, _))
        .Times(1)
        .WillOnce(Invoke([&](auto& cb,
                             folly::Optional<NodesConfigurationStore::version_t>
                                 base_version) { cb(Status::OK, config); }));
    EXPECT_CALL(*store, getLatestConfig_(testing::_)).Times(0);
    auto deps = std::make_unique<TestDeps>(processor.get(), std::move(store));
    auto m = NodesConfigurationManager::create(
        NodesConfigurationManager::OperationMode::forTooling(),
        std::move(deps));
    EXPECT_TRUE(m->init(std::make_shared<const NodesConfiguration>()));
    EXPECT_NE(nullptr, m->getConfig());
  }

  {
    // This is a storage node NCM. It must do a linearizable read on startup.
    auto processor = make_test_processor(settings);
    auto store = std::make_unique<MockNodesConfigurationStore>();
    EXPECT_CALL(*store, getConfig_(_, _)).Times(0);
    EXPECT_CALL(*store, getLatestConfig_(_))
        .Times(1)
        .WillOnce(Invoke([&](auto& cb) { cb(Status::OK, config); }));
    auto deps = std::make_unique<TestDeps>(processor.get(), std::move(store));

    NodeServiceDiscovery::RoleSet roles;
    roles.set(static_cast<size_t>(configuration::NodeRole::STORAGE));
    auto m = NodesConfigurationManager::create(
        NodesConfigurationManager::OperationMode::forNodeRoles(roles),
        std::move(deps));
    EXPECT_TRUE(m->init(std::make_shared<const NodesConfiguration>()));
    EXPECT_NE(nullptr, m->getConfig());
  }
}

TEST_F(NodesConfigurationManagerTest, DivergenceReporting) {
  auto expect_diverged = [&](bool diverged, bool same_version_stat_bumped) {
    const auto& ncm_nc = processor_->getNodesConfigurationFromNCMSource();
    const auto& server_nc =
        processor_->getNodesConfigurationFromServerConfigSource();

    EXPECT_EQ(
        !diverged, ncm_nc->equalWithTimestampAndVersionIgnored(*server_nc));
    auto wait_res = wait_until(
        "Diverged Stats are updated",
        [&]() {
          auto expected = (diverged ? 1 : 0);
          auto got = processor_->stats_->aggregate()
                         .nodes_configuration_server_config_diverged.load();
          ld_info("expected %d, got %ld", expected, got);
          return expected == got;
        },
        std::chrono::steady_clock::now() + std::chrono::seconds(10));
    EXPECT_EQ(0, wait_res);

    wait_res = wait_until(
        "Diverged with same version Stats are updated",
        [&]() {
          auto expected = (same_version_stat_bumped ? 1 : 0);
          auto got =
              processor_->stats_->aggregate()
                  .nodes_configuration_server_config_diverged_with_same_version
                  .load();
          ld_info("expected %d, got %ld", expected, got);
          return expected == got;
        },
        std::chrono::steady_clock::now() + std::chrono::seconds(10));
    EXPECT_EQ(0, wait_res);
  };

  {
    // Because we only log the divergence on servers, we need to set the server
    // setting to true.
    SettingsUpdater updater;
    updater.registerSettings(processor_->updateableSettings());
    updater.setInternalSetting("server", "true");
  }

  // Initially both the NCM NC and ServerConfig NC should be empty
  waitTillNCMReceives(
      MembershipVersion::Type{MembershipVersion::EMPTY_VERSION.val()});
  expect_diverged(false, false);

  // Do a ServerConfig update and expect the configs to diverge
  {
    auto new_server_config = processor_->config_->getServerConfig()
                                 ->withNodes(createSimpleNodesConfig(2))
                                 ->withIncrementedVersion();
    processor_->config_->updateableServerConfig()->update(
        std::move(new_server_config));
  }
  expect_diverged(true, false);

  // Re-sync the NC config and expect the divergence metrics to be 0
  {
    auto new_nc = processor_->getNodesConfigurationFromServerConfigSource();
    auto new_version = new_nc->getVersion();
    ncm_->overwrite(
        std::move(new_nc),
        [](Status status, std::shared_ptr<const NodesConfiguration>) {
          ASSERT_EQ(Status::OK, status);
        });
    waitTillNCMReceives(new_version);
  }
  expect_diverged(false, false);

  // Do a ServerConfig update without a version bump and expect the configs to
  // diverge and the with_version stat to get bumped
  {
    auto new_server_config = processor_->config_->getServerConfig()->withNodes(
        createSimpleNodesConfig(3));
    processor_->config_->updateableServerConfig()->update(
        std::move(new_server_config));
  }
  expect_diverged(true, true);

  // Re-sync the NC config and expect the divergence metrics to be 0
  {
    // We can't have a new NC with the same version, we will need to bump the
    // version of the new NC
    auto new_version = vcs_config_version_t(
        processor_->config_->getServerConfig()->getVersion().val() + 1);
    auto new_nc =
        processor_->getNodesConfigurationFromServerConfigSource()->withVersion(
            new_version);
    ncm_->overwrite(
        std::move(new_nc),
        [](Status status, std::shared_ptr<const NodesConfiguration>) {
          ASSERT_EQ(Status::OK, status);
        });
    waitTillNCMReceives(new_version);
  }
  expect_diverged(false, false);
}

// Second setup with multiple NCMs sharing the same underlying FileBasedNCS
class NodesConfigurationManagerTest2 : public ::testing::Test {
 public:
  void SetUp() override {
    dbg::currentLevel = getLogLevelFromEnv().value_or(dbg::Level::INFO);

    temp_dir_ = createTemporaryDir("NodesConfigurationManagerTest2");
    file_ncs_ = createNCS();

    // provision initial config in FileBasedNCS
    init_version_ = membership::MembershipVersion::Type{kVersion.val() - 2};
    auto nc = provisionNodes()->withVersion(init_version_);
    ASSERT_TRUE(nc->validate());
    auto serialized = NodesConfigurationCodec::serialize(*nc);
    ASSERT_EQ(Status::OK,
              file_ncs_->updateConfigSync(
                  serialized, /* base_version */ folly::none));

    Settings settings = create_default_settings<Settings>();
    settings.num_workers = 3;
    settings.enable_nodes_configuration_manager = true;
    settings.use_nodes_configuration_manager_nodes_configuration = true;
    settings.nodes_configuration_manager_store_polling_interval =
        std::chrono::seconds(1);
    settings.nodes_configuration_manager_intermediary_shard_state_timeout =
        std::chrono::seconds(2);

    processor1_ = make_test_processor(settings);

    auto ncs1 = createNCS();
    auto deps = std::make_unique<TestDeps>(processor1_.get(), std::move(ncs1));
    ncm1_ = NodesConfigurationManager::create(
        NodesConfigurationManager::OperationMode::forTooling(),
        std::move(deps));
    ASSERT_TRUE(ncm1_->init(std::make_shared<const NodesConfiguration>()));
    ncm1_->upgradeToProposer();

    waitTillNCMReceives(*ncm1_, init_version_);

    // create NCM #2
    settings.nodes_configuration_manager_store_polling_interval =
        std::chrono::seconds(3);
    processor2_ = make_test_processor(settings);

    auto ncs2 = createNCS();

    deps = std::make_unique<TestDeps>(processor2_.get(), std::move(ncs2));
    ncm2_ = NodesConfigurationManager::create(
        NodesConfigurationManager::OperationMode::forTooling(),
        std::move(deps));
    ASSERT_TRUE(ncm2_->init(std::make_shared<const NodesConfiguration>()));
    ncm2_->upgradeToProposer();
  }

  //////// Helper functions ////////
  void writeNewVersion(MembershipVersion::Type new_version) {
    auto new_config = std::make_shared<const NodesConfiguration>(
        makeDummyNodesConfiguration(new_version));
    writeNewConfig(std::move(new_config));
  }

  void writeNewConfig(std::shared_ptr<const NodesConfiguration> new_config) {
    // overwrite; fire and forget
    file_ncs_->updateConfig(NodesConfigurationCodec::serialize(*new_config),
                            /* base_version = */ folly::none,
                            /* cb = */ {});
  }

 private:
  // Returns an instance of a FileBasedNodesConfigurationStore with the _same_
  // state in temp_dir_; assumes temp_dir_ is initialized and valid.
  std::unique_ptr<FileBasedNodesConfigurationStore> createNCS() const {
    assert(temp_dir_);
    return std::make_unique<FileBasedNodesConfigurationStore>(
        kConfigKey,
        temp_dir_->path().string(),
        NodesConfigurationCodec::extractConfigVersion);
  }

 public:
  std::unique_ptr<folly::test::TemporaryDirectory> temp_dir_;
  std::unique_ptr<FileBasedNodesConfigurationStore> file_ncs_;
  membership::MembershipVersion::Type init_version_;

  std::shared_ptr<Processor> processor1_;
  std::shared_ptr<Processor> processor2_;

  std::shared_ptr<NodesConfigurationManager> ncm1_;
  std::shared_ptr<NodesConfigurationManager> ncm2_;
};

TEST_F(NodesConfigurationManagerTest2, race) {
  // init_version_ (empty)
  // |
  // | add N17
  // v
  // kVersion
  // |
  // | enable read on N17
  // v
  // kNewVersion
  // |
  // | N17 transitions to READ_ONLY (by NCM's ShardStateTracker)
  // v
  // final_version
  EXPECT_EQ(init_version_, ncm1_->getConfig()->getVersion());
  {
    // add N17
    ncm1_->update(
        addNewNodeUpdate(*ncm1_->getConfig()),
        [](Status status, std::shared_ptr<const NodesConfiguration> new_nc) {
          EXPECT_EQ(Status::OK, status);
          EXPECT_EQ(kVersion.val() - 1, new_nc->getVersion().val());
        });
    waitTillNCMReceives(
        *ncm1_, membership::MembershipVersion::Type{kVersion.val() - 1});

    // Mark as provisioned
    ncm1_->update(
        markAllShardProvisionedUpdate(*ncm1_->getConfig()),
        [](Status status, std::shared_ptr<const NodesConfiguration> new_nc) {
          EXPECT_EQ(Status::OK, status);
          EXPECT_EQ(kVersion, new_nc->getVersion());
        });
    waitTillNCMReceives(*ncm1_, kVersion);
    waitTillNCMReceives(*ncm2_, kVersion);
  }
  {
    // enable read on N17: this is an update that will be automatically advanced
    // to N17 being READ_ONLY.
    auto storage_membership_version =
        ncm1_->getConfig()->getStorageMembership()->getVersion();

    folly::Baton<> b1;
    folly::Baton<> b2;
    // results will be updated via different threads
    std::array<std::pair<Status, membership::MembershipVersion::Type>, 2>
        results{};

    // race the same update via 2 NCMs, one of them should fail with
    // VERSION_MISMATCH
    auto update = enablingReadUpdate(storage_membership_version);
    ncm1_->update(
        std::move(update),
        [&results, &b1](
            Status status, std::shared_ptr<const NodesConfiguration> new_nc) {
          EXPECT_TRUE(new_nc);
          results[0] = std::make_pair(status, new_nc->getVersion());
          b1.post();
        });

    update = enablingReadUpdate(storage_membership_version);
    ncm2_->update(
        std::move(update),
        [&results, &b2](Status status,
                        std::shared_ptr<const NodesConfiguration> stored_nc) {
          EXPECT_TRUE(stored_nc);
          results[1] = std::make_pair(status, stored_nc->getVersion());
          b2.post();
        });

    auto final_version =
        membership::MembershipVersion::Type{kNewVersion.val() + 1};
    waitTillNCMReceives(*ncm1_, final_version);
    b1.wait();
    waitTillNCMReceives(*ncm2_, final_version);
    b2.wait();

    // One should be (OK, kNewVersion) and the other should be
    // (VERSION_MISMATCH, _).
    EXPECT_TRUE((results[0] == std::make_pair(Status::OK, kNewVersion) &&
                 results[1].first == E::VERSION_MISMATCH &&
                 results[1].second > kVersion) ||
                (results[1] == std::make_pair(Status::OK, kNewVersion) &&
                 results[0].first == E::VERSION_MISMATCH &&
                 results[0].second > kVersion));
  }
}
