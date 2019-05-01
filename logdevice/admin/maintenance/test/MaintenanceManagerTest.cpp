/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManager.h"

#include <folly/executors/ManualExecutor.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"

using namespace ::testing;
using namespace facebook::logdevice;
using namespace facebook::logdevice::maintenance;
using facebook::logdevice::configuration::nodes::NodesConfiguration;

namespace facebook { namespace logdevice { namespace maintenance {

class MockMaintenanceManagerDependencies;
class MockMaintenanceManager;

class MaintenanceManagerTest : public ::testing::Test {
 public:
  explicit MaintenanceManagerTest() {}
  ~MaintenanceManagerTest();
  void init();
  void regenerateClusterMaintenanceWrapper();
  void overrideStorageState(
      std::unordered_map<ShardID, membership::StorageState> map);
  void addNewNode(node_index_t node);
  void runExecutor();
  void verifyShardOperationalState(
      ShardID shard,
      folly::Expected<ShardOperationalState, Status> state);

  std::unique_ptr<MockMaintenanceManager> maintenance_manager_;
  std::unique_ptr<MockMaintenanceManagerDependencies> deps_;
  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_config_;
  ClusterMaintenanceState cms_;
  EventLogRebuildingSet set_;
  UpdateableSettings<AdminServerSettings> settings_;
  std::unique_ptr<folly::ManualExecutor> executor_;
  bool start_subscription_called_{false};
  bool stop_subscription_called_{false};
};

class MockMaintenanceManagerDependencies
    : public MaintenanceManagerDependencies {
 public:
  explicit MockMaintenanceManagerDependencies(MaintenanceManagerTest* test)
      : MaintenanceManagerDependencies(nullptr, nullptr, nullptr),
        test_(test) {}

  ~MockMaintenanceManagerDependencies() override {}

  void startSubscription() override {
    test_->start_subscription_called_ = true;
  }

  void stopSubscription() override {
    test_->stop_subscription_called_ = true;
  }

  std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const override {
    return test_->nodes_config_;
  }
  MaintenanceManagerTest* test_;
};

class MockMaintenanceManager : public MaintenanceManager {
 public:
  explicit MockMaintenanceManager(MaintenanceManagerTest* test)
      : MaintenanceManager(test->executor_.get(), std::move(test->deps_)),
        test_(test) {}

  ~MockMaintenanceManager() {}

  MaintenanceManagerTest* test_;
};

MaintenanceManagerTest::~MaintenanceManagerTest() {}

void MaintenanceManagerTest::init() {
  nodes_config_ = NodesConfigurationTestUtil::provisionNodes();
  cms_.set_version(222);
  std::vector<MaintenanceDefinition> definitions;
  auto def1 = MaintenanceDefinition();
  def1.set_user("Automation");

  auto node1 = thrift::NodeID();
  node1.set_node_index(1);

  auto node2 = thrift::NodeID();
  node2.set_node_index(2);

  auto node3 = thrift::NodeID();
  auto node3_addr = thrift::SocketAddress();
  // this node will be matched by address.
  node3_addr.set_address("127.0.0.9");
  node3_addr.set_address_family(thrift::SocketAddressFamily::INET);
  node3.set_address(node3_addr);

  auto shard1 = thrift::ShardID();
  shard1.set_node(node1);
  shard1.set_shard_index(0);

  auto shard2 = thrift::ShardID();
  shard2.set_node(node2);
  shard2.set_shard_index(0);

  def1.set_shards({shard1, shard2});
  def1.set_shard_target_state(ShardOperationalState::MAY_DISAPPEAR);
  // A single group
  def1.set_group(true);
  // Group ID will be defined and set by the maintenance RSM.
  def1.set_group_id("911");
  definitions.push_back(def1);

  auto def2 = MaintenanceDefinition();
  def2.set_user("robots");
  def2.set_shards({shard2});
  def2.set_shard_target_state(ShardOperationalState::DRAINED);
  def2.set_group_id("122");
  def2.set_skip_safety_checks(true);
  def2.set_allow_passive_drains(true);
  definitions.push_back(def2);

  auto def3 = MaintenanceDefinition();
  auto shard3 = thrift::ShardID();
  shard3.set_node(node3);
  shard3.set_shard_index(0);
  def3.set_user("humans");
  def3.set_shards({shard3});
  def3.set_shard_target_state(ShardOperationalState::DRAINED);
  def3.set_group_id("520");
  def3.set_allow_passive_drains(true);
  // simulates an internal maintenance request where the shard is down.
  def3.set_force_restore_rebuilding(true);
  definitions.push_back(def3);

  // Nonexistent node that will be added later
  auto node4 = thrift::NodeID();
  node4.set_node_index(17);
  auto def4 = MaintenanceDefinition();
  auto shard4 = thrift::ShardID();
  shard4.set_node(node4);
  shard4.set_shard_index(0);
  def4.set_user("humans");
  def4.set_shards({shard4});
  def4.set_shard_target_state(ShardOperationalState::DRAINED);
  def4.set_group_id("620");
  def4.set_allow_passive_drains(true);
  definitions.push_back(def4);

  cms_.set_definitions(std::move(definitions));
  set_ = EventLogRebuildingSet();
  deps_ = std::make_unique<MockMaintenanceManagerDependencies>(this);

  AdminServerSettings settings = create_default_settings<AdminServerSettings>();
  settings.enable_maintenance_manager = true;

  executor_ = std::make_unique<folly::ManualExecutor>();

  maintenance_manager_ = std::make_unique<MockMaintenanceManager>(this);
  maintenance_manager_->start();
  runExecutor();
  ASSERT_TRUE(start_subscription_called_);
}

void MaintenanceManagerTest::runExecutor() {
  while (executor_->run()) {
  }
}

void MaintenanceManagerTest::verifyShardOperationalState(
    ShardID shard,
    folly::Expected<ShardOperationalState, Status> expectedResult) {
  auto f = maintenance_manager_->getShardOperationalState(shard);
  runExecutor();
  ASSERT_TRUE(f.hasValue());
  EXPECT_EQ(f.value(), expectedResult);
}

void MaintenanceManagerTest::regenerateClusterMaintenanceWrapper() {
  maintenance_manager_->cluster_maintenance_wrapper_ =
      std::make_unique<ClusterMaintenanceWrapper>(
          std::make_unique<ClusterMaintenanceState>(cms_), nodes_config_);
  maintenance_manager_->nodes_config_ = nodes_config_;
}

void MaintenanceManagerTest::addNewNode(node_index_t node) {
  NodesConfigurationTestUtil::NodeTemplate n;
  n.id = node;
  n.location = "aa.bb.cc.dd.ee";
  n.num_shards = 1;
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::addNewNodeUpdate(*nodes_config_, n));
  ld_check(nodes_config_);
}

void MaintenanceManagerTest::overrideStorageState(
    std::unordered_map<ShardID, membership::StorageState> map) {
  NodesConfiguration::Update update{};
  update.storage_config_update =
      std::make_unique<configuration::nodes::StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          nodes_config_->getVersion());

  for (const auto& it : map) {
    auto shard = it.first;
    auto [exists, shardState] =
        nodes_config_->getStorageMembership()->getShardState(shard);
    ld_check(exists);

    membership::ShardState::Update::StateOverride s;
    s.storage_state = it.second;
    s.flags = shardState.flags;
    s.metadata_state = shardState.metadata_state;

    membership::ShardState::Update u;
    u.transition = membership::StorageStateTransition::OVERRIDE_STATE;
    u.conditions = membership::Condition::FORCE;
    u.maintenance = membership::MaintenanceID::Type{123};
    u.state_override = s;

    update.storage_config_update->membership_update->addShard(shard, u);
  }

  nodes_config_ = nodes_config_->applyUpdate(std::move(update));
  ld_check(nodes_config_);
}

TEST_F(MaintenanceManagerTest, GetShardOperationalState) {
  init();
  regenerateClusterMaintenanceWrapper();
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  ShardID shard = ShardID(1, 0);
  // N1S0 Goes from RW -> RO -> DM -> NONE
  verifyShardOperationalState(
      shard, folly::makeExpected<Status>(ShardOperationalState::ENABLED));
  std::unordered_map<ShardID, membership::StorageState> map;
  map[shard] = membership::StorageState::READ_ONLY;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(
      shard, folly::makeExpected<Status>(ShardOperationalState::MAY_DISAPPEAR));
  map[shard] = membership::StorageState::DATA_MIGRATION;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(
      shard,
      folly::makeExpected<Status>(ShardOperationalState::MIGRATING_DATA));
  map[shard] = membership::StorageState::NONE;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(
      shard, folly::makeExpected<Status>(ShardOperationalState::DRAINED));

  // Nonexistent node
  verifyShardOperationalState(
      ShardID(111, 0), folly::makeUnexpected(E::NOTFOUND));
}
}}} // namespace facebook::logdevice::maintenance
