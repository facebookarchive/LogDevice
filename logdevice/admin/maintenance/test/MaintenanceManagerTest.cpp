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
  void createShardWorkflow(ShardID shard, MaintenanceStatus status);
  void addNewNode(node_index_t node);
  void runExecutor();
  void verifyShardOperationalState(
      ShardID shard,
      folly::Expected<ShardOperationalState, Status> state);
  void verifyStorageState(ShardID shard, membership::StorageState state);
  ShardWorkflow* getShardWorkflow(ShardID shard);
  SequencerWorkflow* getSequencerWorkflow(node_index_t node);
  void fulfillNCPromise(
      Status st,
      std::shared_ptr<const configuration::nodes::NodesConfiguration> nc);
  void fulfillSafetyCheckPromise();
  void verifyMMStatus(MaintenanceManager::MMStatus status);
  void verifyMaintenanceStatus(ShardID shard, MaintenanceStatus status);
  void applyNCUpdate();

  std::unique_ptr<MockMaintenanceManager> maintenance_manager_;
  std::unique_ptr<MockMaintenanceManagerDependencies> deps_;
  std::shared_ptr<const configuration::nodes::NodesConfiguration> nodes_config_;
  folly::Promise<NCUpdateResult> nc_update_promise_;
  folly::Promise<SafetyCheckResult> safety_check_promise_;
  using SeqWfResult =
      std::pair<std::vector<node_index_t>,
                std::vector<folly::SemiFuture<MaintenanceStatus>>>;
  SeqWfResult seq_wf_run_result_;
  using ShardWfResult =
      std::pair<std::vector<ShardID>,
                std::vector<folly::SemiFuture<MaintenanceStatus>>>;
  ShardWfResult shard_wf_run_result_;
  ShardWfResult getShardWorkflowResult();
  void setShardWorkflowResult(
      std::unordered_map<ShardID,
                         std::pair<MaintenanceStatus,
                                   membership::StorageStateTransition>> result);
  SeqWfResult getSequencerWorkflowResult();
  void setSequencerWorkflowResult(SeqWfResult result);
  std::unordered_map<ShardID, membership::StorageStateTransition>
      expected_storage_state_transition_;
  std::unique_ptr<configuration::nodes::StorageConfig::Update> shards_update_;
  std::unique_ptr<configuration::nodes::SequencerConfig::Update>
      sequencers_update_;
  std::vector<ShardID> safety_check_shards_;
  std::vector<node_index_t> safety_check_nodes_;
  folly::F14NodeMap<GroupID, Impact> unsafe_groups_;
  ClusterMaintenanceState cms_;
  EventLogRebuildingSet set_;
  UpdateableSettings<AdminServerSettings> settings_;
  std::unique_ptr<folly::ManualExecutor> executor_;
  bool start_subscription_called_{false};
  bool stop_subscription_called_{false};
  bool periodic_reeval_timer_active_{false};
};

class MockMaintenanceManagerDependencies
    : public MaintenanceManagerDependencies {
 public:
  explicit MockMaintenanceManagerDependencies(MaintenanceManagerTest* test)
      : MaintenanceManagerDependencies(nullptr,
                                       test->settings_,
                                       nullptr,
                                       nullptr,
                                       nullptr),
        test_(test) {}

  ~MockMaintenanceManagerDependencies() override {}

  void startSubscription() override {
    test_->start_subscription_called_ = true;
  }

  void stopSubscription() override {
    test_->stop_subscription_called_ = true;
  }

  folly::SemiFuture<NCUpdateResult> postNodesConfigurationUpdate(
      std::unique_ptr<configuration::nodes::StorageConfig::Update>
          shards_update,
      std::unique_ptr<configuration::nodes::SequencerConfig::Update>
          sequencers_update) override {
    test_->shards_update_ = std::move(shards_update);
    test_->sequencers_update_ = std::move(sequencers_update);
    ld_info("Requested NodesConfig update - StorageConfig::Update:%s, "
            "SequencerConfig::Update:%s",
            test_->shards_update_ ? test_->shards_update_->toString().c_str()
                                  : "null",
            test_->sequencers_update_
                ? test_->sequencers_update_->toString().c_str()
                : "null");
    ld_info("Version of nodes_config_ when reqeust was posted:%lu",
            test_->nodes_config_->getVersion().val_);
    auto pf_pair = folly::makePromiseContract<NCUpdateResult>();
    test_->nc_update_promise_ = std::move(pf_pair.first);
    return std::move(pf_pair.second);
  }

  folly::SemiFuture<SafetyCheckResult> postSafetyCheckRequest(
      const ClusterMaintenanceWrapper& maintenance_state,
      const ShardAuthoritativeStatusMap& status_map,
      const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
          nodes_config,
      const std::vector<const ShardWorkflow*>& shard_wf,
      const std::vector<const SequencerWorkflow*>& seq_wf) override {
    // Verify that Safety check is requested only for expected shards
    for (auto wf : shard_wf) {
      if (std::find(test_->safety_check_shards_.begin(),
                    test_->safety_check_shards_.end(),
                    wf->getShardID()) == test_->safety_check_shards_.end()) {
        ld_info("Safety check requested for unexpected Shard:%s",
                toString(wf->getShardID()).c_str());
        ld_check(false);
      }
    }
    for (auto wf : seq_wf) {
      if (std::find(test_->safety_check_nodes_.begin(),
                    test_->safety_check_nodes_.end(),
                    wf->getNodeIndex()) == test_->safety_check_nodes_.end()) {
        ld_info("Safety check requested for unexpected node:N%hd",
                wf->getNodeIndex());
        ld_check(false);
      }
    }
    auto pf_pair = folly::makePromiseContract<SafetyCheckResult>();
    test_->safety_check_promise_ = std::move(pf_pair.first);
    return std::move(pf_pair.second);
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

  MOCK_METHOD0(runSequencerWorkflows,
               std::pair<std::vector<node_index_t>,
                         std::vector<folly::SemiFuture<MaintenanceStatus>>>());

  MOCK_METHOD0(runShardWorkflows,
               std::pair<std::vector<ShardID>,
                         std::vector<folly::SemiFuture<MaintenanceStatus>>>());

  MOCK_METHOD1(getExpectedStorageStateTransition,
               membership::StorageStateTransition(ShardID));

  void activateReevaluationTimer() override {
    test_->periodic_reeval_timer_active_ = true;
  }

  void cancelReevaluationTimer() override {
    test_->periodic_reeval_timer_active_ = false;
  }

  MaintenanceManagerTest* test_;
};

MaintenanceManagerTest::~MaintenanceManagerTest() {
  auto f = maintenance_manager_->stop();
  if (nc_update_promise_.valid() && !nc_update_promise_.isFulfilled()) {
    nc_update_promise_.setValue(folly::makeUnexpected(E::SHUTDOWN));
  }
  if (safety_check_promise_.valid() && !safety_check_promise_.isFulfilled()) {
    safety_check_promise_.setValue(folly::makeUnexpected(E::SHUTDOWN));
  }
  // Drain any work before the executor is gone. Otherwise
  // we will crash when drain runs as part of executor destruction
  runExecutor();
  // Shtdown should have fulfilled the promise
  ld_check(f.hasValue());
  maintenance_manager_.reset();
  executor_.reset();
}

void MaintenanceManagerTest::init() {
  nodes_config_ = NodesConfigurationTestUtil::provisionNodes();
  cms_.set_version(1);
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
  def1.set_group_id("N1N2_MAYDISAPPEAR");
  definitions.push_back(def1);

  auto def2 = MaintenanceDefinition();
  def2.set_user("robots");
  def2.set_shards({shard2});
  def2.set_shard_target_state(ShardOperationalState::DRAINED);
  def2.set_group_id("N2_DRAINED");
  def2.set_skip_safety_checks(true);
  def2.set_allow_passive_drains(false);
  definitions.push_back(def2);

  auto def3 = MaintenanceDefinition();
  auto shard3 = thrift::ShardID();
  shard3.set_node(node3);
  shard3.set_shard_index(0);
  def3.set_user("humans");
  def3.set_shards({shard3});
  def3.set_shard_target_state(ShardOperationalState::DRAINED);
  def3.set_group_id("N9_DRAINED");
  def3.set_allow_passive_drains(false);
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
  def4.set_group_id("N17_DRAINED");
  def4.set_allow_passive_drains(true);
  definitions.push_back(def4);

  cms_.set_maintenances(std::move(definitions));
  set_ = EventLogRebuildingSet();
  deps_ = std::make_unique<MockMaintenanceManagerDependencies>(this);

  AdminServerSettings settings = create_default_settings<AdminServerSettings>();
  settings.enable_maintenance_manager = true;

  executor_ = std::make_unique<folly::ManualExecutor>();
  maintenance_manager_ = std::make_unique<MockMaintenanceManager>(this);
  verifyMMStatus(MaintenanceManager::MMStatus::NOT_STARTED);
  maintenance_manager_->start();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);
  ASSERT_TRUE(start_subscription_called_);
}

void MaintenanceManagerTest::verifyShardOperationalState(
    ShardID shard,
    folly::Expected<ShardOperationalState, Status> expectedResult) {
  auto f = maintenance_manager_->getShardOperationalState(shard);
  runExecutor();
  ASSERT_TRUE(f.hasValue());
  EXPECT_EQ(expectedResult, f.value());
}

void MaintenanceManagerTest::verifyStorageState(
    ShardID shard,
    membership::StorageState state) {
  auto f = maintenance_manager_->getStorageState(shard);
  runExecutor();
  ASSERT_TRUE(f.hasValue());
  ASSERT_TRUE(f.value().hasValue());
  EXPECT_EQ(state, f.value().value());
}

void MaintenanceManagerTest::verifyMMStatus(
    MaintenanceManager::MMStatus status) {
  ASSERT_EQ(maintenance_manager_->getStatusInternal(), status);
  if (status == MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE) {
    // reeval timer should be active
    EXPECT_TRUE(periodic_reeval_timer_active_);
  } else {
    EXPECT_FALSE(periodic_reeval_timer_active_);
  }
}

void MaintenanceManagerTest::verifyMaintenanceStatus(ShardID shard,
                                                     MaintenanceStatus status) {
  auto wf = getShardWorkflow(shard);
  ASSERT_NE(wf, nullptr);
  ASSERT_EQ(
      maintenance_manager_->active_shard_workflows_[shard].second, status);
}

void MaintenanceManagerTest::applyNCUpdate() {
  NodesConfiguration::Update update{};
  if (shards_update_) {
    ld_check(shards_update_->isValid());
    update.storage_config_update = std::move(shards_update_);
  }
  if (sequencers_update_) {
    update.sequencer_config_update = std::move(sequencers_update_);
  }
  nodes_config_ = nodes_config_->applyUpdate(std::move(update));
  // Verify update resulted in a valid config
  ld_check(nodes_config_);
}

void MaintenanceManagerTest::runExecutor() {
  while (executor_->run()) {
  }
}

void MaintenanceManagerTest::regenerateClusterMaintenanceWrapper() {
  maintenance_manager_->cluster_maintenance_wrapper_ =
      std::make_unique<ClusterMaintenanceWrapper>(
          std::make_unique<ClusterMaintenanceState>(cms_), nodes_config_);
  maintenance_manager_->nodes_config_ = nodes_config_;
}

void MaintenanceManagerTest::createShardWorkflow(ShardID shard,
                                                 MaintenanceStatus status) {
  maintenance_manager_->active_shard_workflows_[shard] =
      std::make_pair(std::make_unique<ShardWorkflow>(shard, nullptr), status);
}

ShardWorkflow* MaintenanceManagerTest::getShardWorkflow(ShardID shard) {
  return maintenance_manager_->active_shard_workflows_[shard].first.get();
}

SequencerWorkflow*
MaintenanceManagerTest::getSequencerWorkflow(node_index_t node) {
  return maintenance_manager_->active_sequencer_workflows_[node].first.get();
}

MaintenanceManagerTest::ShardWfResult
MaintenanceManagerTest::getShardWorkflowResult() {
  // Verify that target states are valid
  for (const auto& shard : shard_wf_run_result_.first) {
    auto wf = getShardWorkflow(shard);
    ld_check(wf && !wf->getTargetOpStates().empty());
  }
  return std::move(shard_wf_run_result_);
}

void MaintenanceManagerTest::setShardWorkflowResult(
    std::unordered_map<ShardID,
                       std::pair<MaintenanceStatus,
                                 membership::StorageStateTransition>> result) {
  shard_wf_run_result_.first.clear();
  shard_wf_run_result_.second.clear();
  expected_storage_state_transition_.clear();
  for (auto& kv : result) {
    shard_wf_run_result_.first.push_back(kv.first);
    shard_wf_run_result_.second.push_back(std::move(kv.second.first));
    expected_storage_state_transition_[kv.first] = kv.second.second;
  }
}

MaintenanceManagerTest::SeqWfResult
MaintenanceManagerTest::getSequencerWorkflowResult() {
  // Verify that target states are valid
  for (const auto& node : seq_wf_run_result_.first) {
    auto wf = getSequencerWorkflow(node);
    ld_check(wf->getTargetOpState() == SequencingState::UNKNOWN);
  }
  return std::move(seq_wf_run_result_);
}

void MaintenanceManagerTest::setSequencerWorkflowResult(SeqWfResult r) {
  seq_wf_run_result_ = std::move(r);
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

void MaintenanceManagerTest::fulfillNCPromise(
    Status st,
    std::shared_ptr<const configuration::nodes::NodesConfiguration> nc) {
  if (st == E::OK) {
    nc_update_promise_.setValue(nc);
  } else {
    nc_update_promise_.setValue(folly::makeUnexpected(st));
  }
}

void MaintenanceManagerTest::fulfillSafetyCheckPromise() {
  ld_check(safety_check_promise_.valid());
  SafetyCheckScheduler::Result result;
  result.unsafe_groups = std::move(unsafe_groups_);
  for (auto s : safety_check_shards_) {
    result.safe_shards.insert(s);
  }
  for (auto n : safety_check_nodes_) {
    result.safe_sequencers.insert(n);
  }
  safety_check_promise_.setValue(std::move(result));
}

void MaintenanceManagerTest::overrideStorageState(
    std::unordered_map<ShardID, membership::StorageState> map) {
  NodesConfiguration::Update update{};
  update.storage_config_update =
      std::make_unique<configuration::nodes::StorageConfig::Update>();
  update.storage_config_update->membership_update =
      std::make_unique<membership::StorageMembership::Update>(
          nodes_config_->getStorageMembership()->getVersion());

  for (const auto& it : map) {
    auto shard = it.first;
    auto result = nodes_config_->getStorageMembership()->getShardState(shard);
    ld_check(result.hasValue());

    auto shardState = result.value();
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
  verifyShardOperationalState(shard, ShardOperationalState::ENABLED);
  std::unordered_map<ShardID, membership::StorageState> map;
  map[shard] = membership::StorageState::READ_ONLY;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(shard, ShardOperationalState::MAY_DISAPPEAR);
  map[shard] = membership::StorageState::DATA_MIGRATION;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(shard, ShardOperationalState::MIGRATING_DATA);
  map[shard] = membership::StorageState::NONE;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState(shard, ShardOperationalState::DRAINED);

  // Nonexistent node
  verifyShardOperationalState(
      ShardID(111, 0), folly::makeUnexpected(E::NOTFOUND));
}

TEST_F(MaintenanceManagerTest, GetNodeStateTest) {
  init();
  node_index_t node = 17;
  ShardID shard = ShardID(node, 0);
  addNewNode(node);
  regenerateClusterMaintenanceWrapper();

  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  // Say we have active shard and sequencer workflows
  createShardWorkflow(shard, MaintenanceStatus::AWAITING_DATA_REBUILDING);
  getShardWorkflow(shard)->addTargetOpState({ShardOperationalState::DRAINED});
  // Update NC to reflect this state
  std::unordered_map<ShardID, membership::StorageState> map;
  map[shard] = membership::StorageState::DATA_MIGRATION;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();

  thrift::NodeState expected_node_state;
  expected_node_state.set_node_index(node);

  thrift::SequencerState expected_seq_state;
  expected_seq_state.set_state(SequencingState::ENABLED);
  expected_node_state.set_sequencer_state(expected_seq_state);

  thrift::ShardState expected_shard_state;
  expected_shard_state.set_data_health(ShardDataHealth::HEALTHY);
  expected_shard_state.set_current_operational_state(
      ShardOperationalState::MIGRATING_DATA);
  expected_shard_state.set_current_storage_state(
      thrift::ShardStorageState::DATA_MIGRATION);
  expected_shard_state.set_storage_state(
      membership::thrift::StorageState::DATA_MIGRATION);
  expected_shard_state.set_metadata_state(
      membership::thrift::MetaDataStorageState::NONE);

  thrift::ShardMaintenanceProgress expected_progress;
  expected_progress.set_status(MaintenanceStatus::AWAITING_DATA_REBUILDING);
  std::set<ShardOperationalState> state({ShardOperationalState::DRAINED});
  expected_progress.set_target_states(state);
  expected_progress.set_created_at(
      getShardWorkflow(shard)->getCreationTimestamp().toMilliseconds().count());
  expected_progress.set_last_updated_at(getShardWorkflow(shard)
                                            ->getLastUpdatedTimestamp()
                                            .toMilliseconds()
                                            .count());

  std::vector<thrift::MaintenanceGroupID> ids;
  ids.push_back("N17_DRAINED");
  expected_progress.set_associated_group_ids(std::move(ids));
  expected_shard_state.set_maintenance(expected_progress);

  std::vector<thrift::ShardState> vec;
  expected_node_state.set_shard_states(vec);
  expected_node_state.shard_states_ref()->push_back(expected_shard_state);

  auto result = maintenance_manager_->getNodeState(node);
  runExecutor();
  ASSERT_TRUE(result.hasValue());
  ASSERT_TRUE(result.value().hasValue());
  ASSERT_EQ(expected_node_state, result.value().value());
}

TEST_F(MaintenanceManagerTest, NodesConfigUpdateTest) {
  init();
  EXPECT_CALL(*maintenance_manager_, runShardWorkflows())
      .WillRepeatedly(Invoke([this]() { return getShardWorkflowResult(); }));
  EXPECT_CALL(*maintenance_manager_, runSequencerWorkflows())
      .WillRepeatedly(
          Invoke([this]() { return getSequencerWorkflowResult(); }));
  EXPECT_CALL(
      *maintenance_manager_, getExpectedStorageStateTransition(::testing::_))
      .WillRepeatedly(Invoke([this](ShardID shard) {
        return expected_storage_state_transition_[shard];
      }));

  // Setup result wf run
  auto N1S0 = ShardID(1, 0);
  auto N2S0 = ShardID(2, 0);
  auto N9S0 = ShardID(9, 0);

  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
  });

  setSequencerWorkflowResult(SeqWfResult());

  for (auto s : {N1S0, N2S0, N9S0}) {
    safety_check_shards_.push_back(s);
  }

  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);
  // deliver ClusterMaintenanceState Update
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  // deliver EventLogRebuildingSet Update
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();
  // There are no nodes to enable. So we should have requested safety check
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_SAFETY_CHECK_RESULTS);
  // None of wfs require NC update yet.
  ASSERT_TRUE(shards_update_ == nullptr);
  // Safety check request should have been posted
  ASSERT_TRUE(safety_check_promise_.valid());

  // Fulfill the promise as if SafetyCheck passes
  fulfillSafetyCheckPromise();
  runExecutor();

  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  // Apply the NC updtaed that is requested by MaintenanceManager. Should
  // result in a valid config
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  std::unordered_map<ShardID, membership::StorageState> map;
  for (auto s : {N1S0, N2S0, N9S0}) {
    verifyStorageState(s, membership::StorageState::RW_TO_RO);
    verifyShardOperationalState(s, ShardOperationalState::ENABLED);
    map[s] = membership::StorageState::READ_ONLY;
  }
  overrideStorageState(map);

  // Setup Result wf run
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver the nodes_config_ update from subscription
  // to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();

  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(N2S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N9S0, ShardOperationalState::MAY_DISAPPEAR);
  // update above should have triggered a call to evaluate, which eventually
  // calls runShardWorkflow and consequently nc update should be requested
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  verifyStorageState(N1S0, membership::StorageState::READ_ONLY);
  verifyStorageState(N2S0, membership::StorageState::DATA_MIGRATION);
  verifyStorageState(N9S0, membership::StorageState::DATA_MIGRATION);

  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(N2S0, ShardOperationalState::MIGRATING_DATA);
  verifyShardOperationalState(N9S0, ShardOperationalState::MIGRATING_DATA);

  // Setup Result wf run
  setShardWorkflowResult({
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyStorageState(N1S0, membership::StorageState::READ_ONLY);
  verifyStorageState(N2S0, membership::StorageState::NONE);
  verifyStorageState(N9S0, membership::StorageState::NONE);
  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N2S0, ShardOperationalState::DRAINED);
  verifyShardOperationalState(N9S0, ShardOperationalState::DRAINED);

  // Verify that the MaintenanceStatus is updated for maintenance that
  // have completed
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N2S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyMaintenanceStatus(N1S0, MaintenanceStatus::COMPLETED);
  verifyMaintenanceStatus(N2S0, MaintenanceStatus::COMPLETED);
  verifyMaintenanceStatus(N9S0, MaintenanceStatus::COMPLETED);
}

TEST_F(MaintenanceManagerTest, NodesConfigUpdateTestWithSafetyCheckFailures) {
  init();
  node_index_t node = 18;
  addNewNode(node);
  overrideStorageState({{ShardID(18, 0), membership::StorageState::NONE}});
  regenerateClusterMaintenanceWrapper();

  EXPECT_CALL(*maintenance_manager_, runShardWorkflows())
      .WillRepeatedly(Invoke([this]() { return getShardWorkflowResult(); }));
  EXPECT_CALL(*maintenance_manager_, runSequencerWorkflows())
      .WillRepeatedly(
          Invoke([this]() { return getSequencerWorkflowResult(); }));
  EXPECT_CALL(
      *maintenance_manager_, getExpectedStorageStateTransition(::testing::_))
      .WillRepeatedly(Invoke([this](ShardID shard) {
        return expected_storage_state_transition_[shard];
      }));

  // Setup result wf run
  auto N1S0 = ShardID(1, 0);
  auto N2S0 = ShardID(2, 0);
  auto N9S0 = ShardID(9, 0);
  auto N18S0 = ShardID(18, 0);

  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::ENABLING_READ}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);
  // deliver ClusterMaintenanceState Update
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  // deliver EventLogRebuildingSet Update
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();

  // There is a node to be enabled. We should have requested a NC update
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  // Apply the NC updtaed that is requested by MaintenanceManager. Should
  // result in a valid config
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  // Since there is a node in enabling, we should not run safety check
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  overrideStorageState({{N18S0, membership::StorageState::READ_ONLY}});

  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::ENABLE_WRITE}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();

  // There is a node to be enabled. We should have requested a NC update
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  // Apply the NC updtaed that is requested by MaintenanceManager. Should
  // result in a valid config
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  // Since there is a node in enabling, we should not run safety check
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  // Confirm that N18 is not enabled.
  verifyShardOperationalState(N18S0, ShardOperationalState::ENABLED);

  // Now add a maintenance for N18 which allows a passive drain
  auto existing_defs = std::move(cms_).get_maintenances();
  auto tnode = thrift::NodeID();
  tnode.set_node_index(18);
  auto def = MaintenanceDefinition();
  auto shard = thrift::ShardID();
  shard.set_node(tnode);
  shard.set_shard_index(0);
  def.set_user("humans");
  def.set_shards({shard});
  def.set_shard_target_state(ShardOperationalState::DRAINED);
  def.set_group_id("N18_DRAINED");
  def.set_allow_passive_drains(true);
  existing_defs.push_back(def);
  cms_.set_maintenances(std::move(existing_defs));
  cms_.set_version(2);
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, 2);

  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  for (auto s : {N1S0, N2S0, N9S0, N18S0}) {
    safety_check_shards_.push_back(s);
  }

  runExecutor();
  // There are no nodes to enable. So we should have requested safety check
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_SAFETY_CHECK_RESULTS);
  // None of wfs require NC update yet.
  ASSERT_TRUE(shards_update_ == nullptr);
  // Safety check request should have been posted
  ASSERT_TRUE(safety_check_promise_.valid());

  // Say all maintenances are determined to be unsafe
  Impact impact(Impact::ImpactResult::WRITE_AVAILABILITY_LOSS);
  for (auto group :
       {"N1N2_MAYDISAPPEAR", "N2_DRAINED", "N9_DRAINED", "N18_DRAINED"}) {
    unsafe_groups_[group] = impact;
  }
  safety_check_shards_.clear();

  fulfillSafetyCheckPromise();
  runExecutor();

  // Even though all maintenances are blocked by safety check, N18_DRAINED
  // allows passive drain. So we should request a config change
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  // Apply the NC updtaed that is requested by MaintenanceManager. Should
  // result in a valid config
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  // Set up shards expected to be in safety check
  for (auto s : {N1S0, N2S0, N9S0, N18S0}) {
    safety_check_shards_.push_back(s);
  }
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // deliver a dummy NC update to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();

  // There are no nodes to enable. So we should have requested safety check
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_SAFETY_CHECK_RESULTS);
  // None of wfs require NC update yet.
  ASSERT_TRUE(shards_update_ == nullptr);
  // Safety check request should have been posted
  ASSERT_TRUE(safety_check_promise_.valid());

  // Say Safety check passes for all shards this time
  unsafe_groups_.clear();
  fulfillSafetyCheckPromise();
  runExecutor();

  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  // Apply the NC updtaed that is requested by MaintenanceManager. Should
  // result in a valid config
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  std::unordered_map<ShardID, membership::StorageState> map;
  for (auto s : {N1S0, N2S0, N9S0, N18S0}) {
    verifyStorageState(s, membership::StorageState::RW_TO_RO);
    map[s] = membership::StorageState::READ_ONLY;
  }
  overrideStorageState(map);

  // Setup Result wf run
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver the nodes_config_ update from subscription
  // to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();

  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(N2S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N9S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N18S0, ShardOperationalState::MAY_DISAPPEAR);
  // update above should have triggered a call to evaluate, which eventually
  // calls runShardWorkflow and consequently nc update should be requested
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  verifyStorageState(N1S0, membership::StorageState::READ_ONLY);
  verifyStorageState(N2S0, membership::StorageState::DATA_MIGRATION);
  verifyStorageState(N9S0, membership::StorageState::DATA_MIGRATION);
  verifyStorageState(N18S0, membership::StorageState::DATA_MIGRATION);

  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N2S0, ShardOperationalState::MIGRATING_DATA);
  verifyShardOperationalState(N9S0, ShardOperationalState::MIGRATING_DATA);
  verifyShardOperationalState(N18S0, ShardOperationalState::MIGRATING_DATA);

  // Setup Result wf run
  setShardWorkflowResult({
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyStorageState(N1S0, membership::StorageState::READ_ONLY);
  verifyStorageState(N2S0, membership::StorageState::NONE);
  verifyStorageState(N9S0, membership::StorageState::NONE);
  verifyStorageState(N18S0, membership::StorageState::NONE);
  verifyShardOperationalState(N1S0, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(N2S0, ShardOperationalState::DRAINED);
  verifyShardOperationalState(N9S0, ShardOperationalState::DRAINED);
  verifyShardOperationalState(N18S0, ShardOperationalState::DRAINED);

  // Verify that the MaintenanceStatus is updated for maintenance that
  // have completed
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N2S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N18S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyMaintenanceStatus(N1S0, MaintenanceStatus::COMPLETED);
  verifyMaintenanceStatus(N2S0, MaintenanceStatus::COMPLETED);
  verifyMaintenanceStatus(N9S0, MaintenanceStatus::COMPLETED);
  verifyMaintenanceStatus(N18S0, MaintenanceStatus::COMPLETED);
}

TEST_F(MaintenanceManagerTest, TestProvisioningNode) {
  init();
  node_index_t node = 18;
  addNewNode(node);
  regenerateClusterMaintenanceWrapper();

  EXPECT_CALL(*maintenance_manager_, runShardWorkflows())
      .WillRepeatedly(Invoke([this]() { return getShardWorkflowResult(); }));
  EXPECT_CALL(*maintenance_manager_, runSequencerWorkflows())
      .WillRepeatedly(
          Invoke([this]() { return getSequencerWorkflowResult(); }));
  EXPECT_CALL(
      *maintenance_manager_, getExpectedStorageStateTransition(::testing::_))
      .WillRepeatedly(Invoke([this](ShardID shard) {
        return expected_storage_state_transition_[shard];
      }));

  auto N9S0 = ShardID(9, 0);
  auto N18S0 = ShardID(18, 0);

  safety_check_shards_.push_back(N9S0);
  setShardWorkflowResult({
      {N9S0,
       {MaintenanceStatus::AWAITING_SAFETY_CHECK,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODE_PROVISIONING,
        membership::StorageStateTransition::Count /* doesn't matter */}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);
  // deliver ClusterMaintenanceState Update
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  // deliver EventLogRebuildingSet Update
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();

  // Provisioning nodes shouldn't block safety checks
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_SAFETY_CHECK_RESULTS);
  verifyShardOperationalState(N18S0, ShardOperationalState::PROVISIONING);

  // Let's derive the N9S0 maintenance to completion
  setShardWorkflowResult({
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODE_PROVISIONING,
        membership::StorageStateTransition::Count /* doesn't matter */}},
  });
  fulfillSafetyCheckPromise();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();

  // N9S0 maintenance is complete, there's nothing MM can do about N18S0
  // other waiting for a state change.
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  // Simulate N18S0 marking itself as provisioned
  setShardWorkflowResult({
      {N9S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N18S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::ENABLING_READ}},
  });
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(
          *nodes_config_));
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);

  // The MM continues normally from here enabling the node.
}

TEST_F(MaintenanceManagerTest, TestBootstrappingFlag) {
  init();

  // Let's start with an empty provisioning NC.
  nodes_config_ = std::make_shared<const NodesConfiguration>();
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::initialAddShardsUpdate({18}));
  ld_check(nodes_config_);
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(
          *nodes_config_));
  ld_check(nodes_config_);

  ASSERT_TRUE(nodes_config_->getSequencerMembership()->isBootstrapping());
  ASSERT_TRUE(nodes_config_->getStorageMembership()->isBootstrapping());
  regenerateClusterMaintenanceWrapper();

  EXPECT_CALL(*maintenance_manager_, runShardWorkflows())
      .WillRepeatedly(Invoke([this]() { return getShardWorkflowResult(); }));
  EXPECT_CALL(*maintenance_manager_, runSequencerWorkflows())
      .WillRepeatedly(
          Invoke([this]() { return getSequencerWorkflowResult(); }));
  EXPECT_CALL(
      *maintenance_manager_, getExpectedStorageStateTransition(::testing::_))
      .WillRepeatedly(Invoke([this](ShardID shard) {
        return expected_storage_state_transition_[shard];
      }));

  auto N18S0 = ShardID(18, 0);
  setShardWorkflowResult(
      {{N18S0,
        {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
         membership::StorageStateTransition::ENABLING_READ}}});

  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);

  // deliver ClusterMaintenanceState Update
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  // deliver EventLogRebuildingSet Update
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();

  // Although we have a node that's ready to get enabled, MM should still wait
  // for the cluster to stop bootstrapping.
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::finalizeBootstrappingUpdate(*nodes_config_));
  ASSERT_NE(nullptr, nodes_config_);
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();

  // After removing the bootsrapping flag, the MM should start enabling
  // the node.
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);

  // The MM continues normally from here enabling the node.
}
}}} // namespace facebook::logdevice::maintenance
