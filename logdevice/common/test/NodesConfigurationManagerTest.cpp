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
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/common/configuration/nodes/ZookeeperNodesConfigurationStore.h"
#include "logdevice/common/membership/utils.h"
#include "logdevice/common/request_util.h"
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
} // namespace

class NodesConfigurationManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
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
    processor_ = make_test_processor(settings);

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

  void waitTillNCMReceives(MembershipVersion::Type new_version) {
    // TODO: better testing after offering a subscription API
    while (ncm_->getConfig() == nullptr ||
           ncm_->getConfig()->getVersion() != new_version) {
      /* sleep override */ std::this_thread::sleep_for(200ms);
    }
    auto p = ncm_->getConfig();
    EXPECT_EQ(new_version, p->getVersion());
  }

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
    auto update = initialProvisionUpdate();
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
    NodesConfiguration::Update update = addNewNodeUpdate();
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
    auto update = initialProvisionUpdate();
    ncm_->update(std::move(update),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    waitTillNCMReceives(MembershipVersion::MIN_VERSION);
    ncm_->update(addNewNodeUpdate(),
                 [](Status status, std::shared_ptr<const NodesConfiguration>) {
                   ASSERT_EQ(Status::OK, status);
                 });
    auto enabling_read_base_version =
        MembershipVersion::Type{MembershipVersion::MIN_VERSION.val() + 1};
    waitTillNCMReceives(enabling_read_base_version);

    ncm_->update(
        enablingReadUpdate(enabling_read_base_version),
        [](Status status, std::shared_ptr<const NodesConfiguration> nc) {
          ASSERT_EQ(Status::OK, status);
          ASSERT_NE(nullptr, nc);
        });
    auto final_version =
        MembershipVersion::Type{enabling_read_base_version.val() + 2};
    waitTillNCMReceives(final_version);
  }
  {
    auto nc = ncm_->getConfig();
    auto p = nc->getStorageMembership()->getShardState(ShardID{17, 0});
    EXPECT_TRUE(p.first);
    EXPECT_EQ(membership::StorageState::READ_ONLY, p.second.storage_state);
    EXPECT_EQ(membership::MetaDataStorageState::NONE, p.second.metadata_state);
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
    EXPECT_CALL(*store, getConfig_(_)).Times(1).WillOnce(Invoke([&](auto& cb) {
      cb(Status::OK, config);
    }));
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
    EXPECT_CALL(*store, getConfig_(_)).Times(0);
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
