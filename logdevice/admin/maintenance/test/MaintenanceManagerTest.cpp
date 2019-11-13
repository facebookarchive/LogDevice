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

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/common/settings/util.h"
#include "logdevice/common/test/MockTimer.h"
#include "logdevice/common/test/MockTraceLogger.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/test/TestUtil.h"

using namespace ::testing;

namespace facebook { namespace logdevice { namespace maintenance {

using ShardWorkflowMap = MaintenanceManager::ShardWorkflowMap;
using SequencerWorkflowMap = MaintenanceManager::SequencerWorkflowMap;

class MockMaintenanceManagerDependencies;
class MockMaintenanceManager;
class MockMaintenanceLogWriter;

std::string DEFAULT_LOC = "aa.bb.cc.dd.ee";

class MaintenanceManagerTest : public ::testing::Test {
 public:
  explicit MaintenanceManagerTest() : stats_(StatsParams().setIsServer(true)) {}

  ~MaintenanceManagerTest();
  void init();
  const ClusterMaintenanceWrapper& regenerateClusterMaintenanceWrapper();
  void overrideStorageState(
      std::unordered_map<ShardID, membership::StorageState> map);
  void createShardWorkflow(ShardID shard, MaintenanceStatus status);
  void addNewNode(node_index_t node, std::string location = DEFAULT_LOC);
  void runExecutor();
  ShardWorkflowMap
  createShardWorkflows(ShardWorkflowMap&& existing_shard_workflows,
                       const ClusterMaintenanceWrapper& maintenance_wrapper);
  SequencerWorkflowMap createSequencerWorkflows(
      SequencerWorkflowMap&& existing_sequencer_workflows,
      const ClusterMaintenanceWrapper& maintenance_wrapper);
  void verifyShardOperationalState(
      std::vector<ShardID> shards,
      folly::Expected<ShardOperationalState, Status> state);
  void verifyStorageState(std::vector<ShardID> shards,
                          membership::StorageState state);
  ShardWorkflow* getShardWorkflow(ShardID shard);
  SequencerWorkflow* getSequencerWorkflow(node_index_t node);
  void fulfillNCPromise(
      Status st,
      std::shared_ptr<const configuration::nodes::NodesConfiguration> nc);
  void fulfillSafetyCheckPromise();
  void verifyMMStatus(MaintenanceManager::MMStatus status);
  void verifyMaintenanceStatus(std::vector<ShardID> shards,
                               MaintenanceStatus status);
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
  void changeManualOverride(const std::unordered_set<ShardID>& shards,
                            const std::unordered_set<node_index_t>& seq,
                            bool manual_override);
  void addManualOverride(const std::unordered_set<ShardID>& shards,
                         const std::unordered_set<node_index_t>& seq);
  void removeManualOverride(const std::unordered_set<ShardID>& shards,
                            const std::unordered_set<node_index_t>& seq);
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
  UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  std::unique_ptr<folly::ManualExecutor> executor_;
  StatsHolder stats_;
  std::unique_ptr<MockMaintenanceLogWriter> mock_maintenance_log_writer_;
  bool start_subscription_called_{false};
  bool stop_subscription_called_{false};
  bool periodic_reeval_timer_active_{false};
  bool periodic_metadata_nodeset_timer_active_{false};
  std::shared_ptr<MockTraceLogger> logger_;
  std::unique_ptr<MaintenanceManagerTracer> tracer_;
};

class MockMaintenanceLogWriter : public MaintenanceLogWriter {
 public:
  MockMaintenanceLogWriter() : MaintenanceLogWriter(nullptr) {}

  MOCK_METHOD4(writeDelta,
               void(const MaintenanceDelta&,
                    std::function<void(Status st,
                                       lsn_t version,
                                       const std::string& failure_reason)>,
                    ClusterMaintenanceStateMachine::WriteMode mode,
                    folly::Optional<lsn_t> base_version));
};

class MockMaintenanceManagerDependencies
    : public MaintenanceManagerDependencies {
 public:
  explicit MockMaintenanceManagerDependencies(MaintenanceManagerTest* test)
      : MaintenanceManagerDependencies(nullptr,
                                       test->settings_,
                                       test->rebuilding_settings_,
                                       nullptr,
                                       nullptr,
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
    ld_info("Version of nodes_config_ when request was posted:%lu",
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
      const std::vector<const SequencerWorkflow*>& seq_wf,
      const ShardSet& /* unused */) override {
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

  StatsHolder* getStats() const override {
    return &test_->stats_;
  }

  MaintenanceLogWriter* getMaintenanceLogWriter() const override {
    return test_->mock_maintenance_log_writer_.get();
  }

  MaintenanceManagerTracer* getTracer() const override {
    return test_->tracer_.get();
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

  std::unique_ptr<Timer>
  createMetadataNodesetMonitorTimer(std::function<void()> cb) override {
    return std::make_unique<MockTimer>(cb);
  }

  void activateMetadataNodesetMonitorTimer() override {
    test_->periodic_metadata_nodeset_timer_active_ = true;
    dynamic_cast<MockTimer*>(metadata_nodeset_monitor_timer_.get())
        ->activate(std::chrono::microseconds(0));
  }

  void cancelMetadataNodesetMonitorTimer() override {
    test_->periodic_metadata_nodeset_timer_active_ = false;
    dynamic_cast<MockTimer*>(metadata_nodeset_monitor_timer_.get())->cancel();
  }

  void fireMetadataNodesetMonitorTimer() {
    dynamic_cast<MockTimer*>(metadata_nodeset_monitor_timer_.get())->trigger();
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
  node3.set_node_index(9);

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
  def4.set_sequencer_nodes({node4});
  def4.set_sequencer_target_state(SequencingState::DISABLED);
  def4.set_shards({shard4});
  def4.set_shard_target_state(ShardOperationalState::DRAINED);
  def4.set_group_id("N17_DRAINED");
  def4.set_allow_passive_drains(true);
  definitions.push_back(def4);

  cms_.set_maintenances(std::move(definitions));
  set_ = EventLogRebuildingSet();
  mock_maintenance_log_writer_ = std::make_unique<MockMaintenanceLogWriter>();

  deps_ = std::make_unique<MockMaintenanceManagerDependencies>(this);

  AdminServerSettings admin_settings =
      create_default_settings<AdminServerSettings>();
  admin_settings.enable_maintenance_manager = true;
  settings_ = UpdateableSettings<AdminServerSettings>(admin_settings);

  RebuildingSettings rebuilding_settings =
      create_default_settings<RebuildingSettings>();
  rebuilding_settings_ =
      UpdateableSettings<RebuildingSettings>(rebuilding_settings);

  executor_ = std::make_unique<folly::ManualExecutor>();
  maintenance_manager_ = std::make_unique<MockMaintenanceManager>(this);
  verifyMMStatus(MaintenanceManager::MMStatus::NOT_STARTED);
  maintenance_manager_->start();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::STARTING);
  maintenance_manager_->event_log_rebuilding_set_ =
      std::make_unique<EventLogRebuildingSet>(set_);
  // maintenance_manager_->last_ers_version_ = lsn_t(1);
  ASSERT_TRUE(start_subscription_called_);

  {
    // Dummy config for the tracer
    auto config =
        Configuration::fromJsonFile(TEST_CONFIG_FILE("sample_valid.conf"));
    auto updateable_config =
        std::make_shared<UpdateableConfig>(std::move(config));
    logger_ = std::make_shared<MockTraceLogger>(std::move(updateable_config));
    tracer_ = std::make_unique<MaintenanceManagerTracer>(logger_);
  }
}

void MaintenanceManagerTest::verifyShardOperationalState(
    std::vector<ShardID> shards,
    folly::Expected<ShardOperationalState, Status> expected_result) {
  for (auto shard : shards) {
    auto f = maintenance_manager_->getShardOperationalState(shard);
    runExecutor();
    ASSERT_TRUE(f.hasValue());
    EXPECT_EQ(expected_result, f.value());
  }
}

void MaintenanceManagerTest::verifyStorageState(
    std::vector<ShardID> shards,
    membership::StorageState state) {
  for (auto shard : shards) {
    auto f = maintenance_manager_->getStorageState(shard);
    runExecutor();
    ASSERT_TRUE(f.hasValue());
    ASSERT_TRUE(f.value().hasValue());
    EXPECT_EQ(state, f.value().value());
  }
}

void MaintenanceManagerTest::verifyMMStatus(
    MaintenanceManager::MMStatus status) {
  ASSERT_EQ(status, maintenance_manager_->getStatusInternal());
  if (status == MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE) {
    // reeval timer should be active
    EXPECT_TRUE(periodic_reeval_timer_active_);
  } else {
    EXPECT_FALSE(periodic_reeval_timer_active_);
  }
}

void MaintenanceManagerTest::verifyMaintenanceStatus(
    std::vector<ShardID> shards,
    MaintenanceStatus status) {
  for (auto shard : shards) {
    auto wf = getShardWorkflow(shard);
    ASSERT_NE(wf, nullptr);
    ASSERT_EQ(
        status, maintenance_manager_->active_shard_workflows_[shard].second);
  }
}

ShardWorkflowMap MaintenanceManagerTest::createShardWorkflows(
    ShardWorkflowMap&& existing_shard_workflows,
    const ClusterMaintenanceWrapper& maintenance_wrapper) {
  return maintenance_manager_->createShardWorkflows(
      std::move(existing_shard_workflows), maintenance_wrapper);
}

SequencerWorkflowMap MaintenanceManagerTest::createSequencerWorkflows(
    SequencerWorkflowMap&& existing_sequencer_workflows,
    const ClusterMaintenanceWrapper& maintenance_wrapper) {
  return maintenance_manager_->createSequencerWorkflows(
      std::move(existing_sequencer_workflows), maintenance_wrapper);
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

const ClusterMaintenanceWrapper&
MaintenanceManagerTest::regenerateClusterMaintenanceWrapper() {
  maintenance_manager_->cluster_maintenance_wrapper_ =
      std::make_unique<ClusterMaintenanceWrapper>(
          std::make_unique<ClusterMaintenanceState>(cms_), nodes_config_);
  maintenance_manager_->nodes_config_ = nodes_config_;
  return *maintenance_manager_->cluster_maintenance_wrapper_;
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

void MaintenanceManagerTest::addNewNode(node_index_t node,
                                        std::string location) {
  NodesConfigurationTestUtil::NodeTemplate n;
  n.id = node;
  n.location = location;
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

void MaintenanceManagerTest::changeManualOverride(
    const std::unordered_set<ShardID>& shards,
    const std::unordered_set<node_index_t>& seq,
    bool manual_override) {
  NodesConfiguration::Update update{};
  if (!shards.empty()) {
    update.storage_config_update =
        std::make_unique<configuration::nodes::StorageConfig::Update>();
    update.storage_config_update->membership_update =
        std::make_unique<membership::StorageMembership::Update>(
            nodes_config_->getStorageMembership()->getVersion());
  }
  if (!seq.empty()) {
    update.sequencer_config_update =
        std::make_unique<configuration::nodes::SequencerConfig::Update>();
    update.sequencer_config_update->membership_update =
        std::make_unique<membership::SequencerMembership::Update>(
            nodes_config_->getSequencerMembership()->getVersion());
  }

  for (const auto& shard : shards) {
    auto result = nodes_config_->getStorageMembership()->getShardState(shard);
    ld_check(result.hasValue());

    auto shardState = result.value();
    membership::ShardState::Update::StateOverride s;
    s.storage_state = shardState.storage_state;
    s.flags = shardState.flags;
    s.metadata_state = shardState.metadata_state;
    s.manual_override = manual_override;

    membership::ShardState::Update u;
    u.transition = membership::StorageStateTransition::OVERRIDE_STATE;
    u.conditions = membership::Condition::FORCE;
    u.state_override = s;
    update.storage_config_update->membership_update->addShard(shard, u);
  }

  for (auto nid : seq) {
    auto result = nodes_config_->getSequencerMembership()->getNodeState(nid);
    ld_check(result.hasValue());

    auto nodeState = result.value();
    membership::SequencerNodeState::Update seq_update;
    seq_update.sequencer_enabled = nodeState.sequencer_enabled;
    seq_update.manual_override = manual_override;
    seq_update.transition =
        membership::SequencerMembershipTransition::SET_ENABLED_FLAG;
    update.sequencer_config_update->membership_update->addNode(nid, seq_update);
  }

  nodes_config_ = nodes_config_->applyUpdate(std::move(update));
  ld_check(nodes_config_);
}

void MaintenanceManagerTest::addManualOverride(
    const std::unordered_set<ShardID>& shards,
    const std::unordered_set<node_index_t>& seq) {
  changeManualOverride(shards, seq, true);
}

void MaintenanceManagerTest::removeManualOverride(
    const std::unordered_set<ShardID>& shards,
    const std::unordered_set<node_index_t>& seq) {
  changeManualOverride(shards, seq, false);
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
  verifyShardOperationalState({shard}, ShardOperationalState::ENABLED);
  std::unordered_map<ShardID, membership::StorageState> map;
  map[shard] = membership::StorageState::READ_ONLY;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::MAY_DISAPPEAR);
  map[shard] = membership::StorageState::DATA_MIGRATION;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::MIGRATING_DATA);
  map[shard] = membership::StorageState::NONE;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::DRAINED);
  // N1S0 Goes from NONE -> RO -> RW
  map[shard] = membership::StorageState::NONE_TO_RO;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::ENABLING);
  map[shard] = membership::StorageState::READ_ONLY;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::MAY_DISAPPEAR);
  map[shard] = membership::StorageState::READ_WRITE;
  overrideStorageState(map);
  regenerateClusterMaintenanceWrapper();
  verifyShardOperationalState({shard}, ShardOperationalState::ENABLED);

  // Nonexistent node
  verifyShardOperationalState(
      {ShardID(111, 0)}, folly::makeUnexpected(E::NOTFOUND));
}

TEST_F(MaintenanceManagerTest, CreateWorkflows) {
  // Init generates a the following maintenances
  // - N1N2_MAYDISAPPEAR
  // - N2_DRAINED
  // - N9_DRAINED
  // - N17_DRAINED
  init();
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
      original_nodes_config =
          std::make_shared<const configuration::nodes::NodesConfiguration>(
              *nodes_config_);
  node_index_t node = 17;
  ShardID shard = ShardID(node, 0);
  addNewNode(node);
  const auto& wrapper = regenerateClusterMaintenanceWrapper();
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  // No workflows should have been created at this point.
  ShardWorkflowMap shard_workflows;
  shard_workflows = createShardWorkflows(std::move(shard_workflows), wrapper);
  ASSERT_EQ(4, shard_workflows.size());
  SequencerWorkflowMap sequencer_workflows;
  sequencer_workflows =
      createSequencerWorkflows(std::move(sequencer_workflows), wrapper);
  ASSERT_EQ(1, sequencer_workflows.size());
  // Let's remove a maintenance N9_DRAINED. That should reduce the number of
  // workflows by one.
  std::vector<MaintenanceDefinition> defs;
  defs = cms_.get_maintenances();
  for (auto it = defs.begin(); it != defs.end(); ++it) {
    if (it->group_id_ref().value() == "N9_DRAINED") {
      defs.erase(it);
      break;
    }
  }
  cms_.set_maintenances(defs);
  const auto& wrapper2 = regenerateClusterMaintenanceWrapper();
  shard_workflows = createShardWorkflows(std::move(shard_workflows), wrapper2);
  ASSERT_EQ(3, shard_workflows.size());
  sequencer_workflows =
      createSequencerWorkflows(std::move(sequencer_workflows), wrapper2);
  ASSERT_EQ(1, sequencer_workflows.size());
  // Let's revert to the original config (without node 17)
  nodes_config_ = original_nodes_config;
  const auto& wrapper3 = regenerateClusterMaintenanceWrapper();
  shard_workflows = createShardWorkflows(std::move(shard_workflows), wrapper3);
  ASSERT_EQ(2, shard_workflows.size());
  sequencer_workflows =
      createSequencerWorkflows(std::move(sequencer_workflows), wrapper3);
  ASSERT_EQ(0, sequencer_workflows.size());
}

TEST_F(MaintenanceManagerTest, GetNodeStateTest) {
  init();
  node_index_t node = 17;
  ShardID shard = ShardID(node, 0);
  addNewNode(node);
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(
          *nodes_config_));
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

  {
    thrift::NodeConfig nc;
    fillNodeConfig(nc, node, *nodes_config_);
    expected_node_state.set_config(nc);
  }

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
    verifyStorageState({s}, membership::StorageState::RW_TO_RO);
    verifyShardOperationalState({s}, ShardOperationalState::ENABLED);
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

  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(
      {N1S0, N2S0, N9S0}, ShardOperationalState::MAY_DISAPPEAR);
  // update above should have triggered a call to evaluate, which eventually
  // calls runShardWorkflow and consequently nc update should be requested
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  verifyStorageState({N1S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0, N9S0}, membership::StorageState::DATA_MIGRATION);

  verifyShardOperationalState({N1S0}, ShardOperationalState::MAY_DISAPPEAR);
  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(
      {N2S0, N9S0}, ShardOperationalState::MIGRATING_DATA);

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
  verifyStorageState({N1S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0, N9S0}, membership::StorageState::NONE);
  verifyShardOperationalState({N1S0}, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState({N2S0, N9S0}, ShardOperationalState::DRAINED);

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
  verifyMaintenanceStatus({N1S0, N2S0, N9S0}, MaintenanceStatus::COMPLETED);
}

TEST_F(MaintenanceManagerTest, MaintenanceBlockedByOverride) {
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
    verifyStorageState({s}, membership::StorageState::RW_TO_RO);
    verifyShardOperationalState({s}, ShardOperationalState::ENABLED);
    map[s] = membership::StorageState::READ_ONLY;
  }
  overrideStorageState(map);

  // Add a manual override for N9S0 such that it blocks further maintenance
  addManualOverride({N9S0}, {});
  ld_check(nodes_config_);

  // Setup Result wf run
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
      {N9S0,
       {MaintenanceStatus::BLOCKED_BY_ADMIN_OVERRIDE,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver the nodes_config_ update from subscription
  // to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(
      {N1S0, N2S0, N9S0}, ShardOperationalState::MAY_DISAPPEAR);
  // update above should have triggered a call to evaluate, which eventually
  // calls runShardWorkflow and consequently nc update should be requested
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  // N9S0 should not proceed to DATAMIGRATION and continue to be READ_ONLY
  // since it has a manual override
  verifyStorageState({N1S0, N9S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0}, membership::StorageState::DATA_MIGRATION);

  verifyShardOperationalState(
      {N1S0, N9S0}, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState({N2S0}, ShardOperationalState::MIGRATING_DATA);

  // Setup Result wf run
  setShardWorkflowResult({
      {N2S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::BLOCKED_BY_ADMIN_OVERRIDE,
        // This just for sake of satisfying the API. MM doesn't care about
        // expected transition when status is blocked
        membership::StorageStateTransition::START_DATA_MIGRATION}},
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
  verifyStorageState({N1S0, N9S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0}, membership::StorageState::NONE);
  verifyShardOperationalState(
      {N1S0, N9S0}, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState({N2S0}, ShardOperationalState::DRAINED);

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
       {MaintenanceStatus::BLOCKED_BY_ADMIN_OVERRIDE,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
  });
  setSequencerWorkflowResult(SeqWfResult());

  // Deliver a dummy update from subscription to trigger evaluation
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyMaintenanceStatus({N1S0, N2S0}, MaintenanceStatus::COMPLETED);

  // Now remove the manual override. Maintenance for N9S0 should proceed
  removeManualOverride({N9S0}, {});
  setShardWorkflowResult({
      {N1S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N2S0,
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {N9S0,
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::START_DATA_MIGRATION}},
  });
  maintenance_manager_->onNodesConfigurationUpdated();
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);
  verifyStorageState({N1S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0}, membership::StorageState::NONE);
  verifyStorageState({N9S0}, membership::StorageState::DATA_MIGRATION);
}

TEST_F(MaintenanceManagerTest, MetadataNodeset) {
  init();
  // Init adds only 2 nodes in two different racks during initial provision
  // The MetadataNodeSetSelector however will pick one more domain when
  // requested Add a new rack so that it has sufficient number of racks to pick
  auto old_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();
  ld_check(old_metadata_nodeset.size() == 2);
  // add a new rack
  addNewNode(node_index_t(18), "aa.bb.cc.dd.gg");
  nodes_config_ = nodes_config_->applyUpdate(
      NodesConfigurationTestUtil::markAllShardProvisionedUpdate(
          *nodes_config_));

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

  overrideStorageState({
      {N18S0, membership::StorageState::READ_ONLY},
  });

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

  // Now MM has the config with N18. And the Metadata nodeset monitor
  // timer should be active since first update has been delivered
  EXPECT_TRUE(periodic_metadata_nodeset_timer_active_);
  maintenance_manager_->fireMetadataNodesetMonitorTimer();

  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();

  auto new_metadata_nodeset =
      nodes_config_->getStorageMembership()->getMetaDataNodeSet();
  ld_info("Metadata nodeset after regeneration:%s",
          toString(new_metadata_nodeset).c_str());
  EXPECT_NE(old_metadata_nodeset, new_metadata_nodeset);
  EXPECT_TRUE(new_metadata_nodeset.count(node_index_t(18)));
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
  verifyShardOperationalState({N18S0}, ShardOperationalState::ENABLED);

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
    verifyStorageState({s}, membership::StorageState::RW_TO_RO);
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

  // 2 and 9 should be in MAY_DISAPPEAR even though the target state is DRAINED
  verifyShardOperationalState(
      {N1S0, N2S0, N9S0, N18S0}, ShardOperationalState::MAY_DISAPPEAR);
  // update above should have triggered a call to evaluate, which eventually
  // calls runShardWorkflow and consequently nc update should be requested
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_NODES_CONFIG_UPDATE);
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();
  verifyMMStatus(MaintenanceManager::MMStatus::AWAITING_STATE_CHANGE);

  verifyStorageState({N1S0}, membership::StorageState::READ_ONLY);
  verifyStorageState(
      {N2S0, N9S0, N18S0}, membership::StorageState::DATA_MIGRATION);

  verifyShardOperationalState({N1S0}, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(
      {N2S0, N9S0, N18S0}, ShardOperationalState::MIGRATING_DATA);

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
  verifyStorageState({N1S0}, membership::StorageState::READ_ONLY);
  verifyStorageState({N2S0, N9S0, N18S0}, membership::StorageState::NONE);
  verifyShardOperationalState({N1S0}, ShardOperationalState::MAY_DISAPPEAR);
  verifyShardOperationalState(
      {N2S0, N9S0, N18S0}, ShardOperationalState::DRAINED);

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
  verifyMaintenanceStatus(
      {N1S0, N2S0, N9S0, N18S0}, MaintenanceStatus::COMPLETED);
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
  verifyShardOperationalState({N18S0}, ShardOperationalState::PROVISIONING);

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

TEST_F(MaintenanceManagerTest, TestPurgeMaintenance) {
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

  // Prepare list of maintenances

  MaintenanceDefinition def1;
  def1.set_user("bunny");
  def1.set_shard_target_state(ShardOperationalState::DRAINED);
  def1.set_shards({mkShardID(1, 0), mkShardID(13, 0)});
  def1.set_sequencer_nodes({mkNodeID(1)});
  def1.set_sequencer_target_state(SequencingState::DISABLED);
  def1.set_group_id("empty_1");

  MaintenanceDefinition def2 = def1;
  def2.set_shards({mkShardID(1, 0), mkShardID(13, 0), mkShardID(2, 0)});
  def2.set_sequencer_nodes({});
  def2.set_group_id("empty_2");
  def2.set_expires_on(SystemTimestamp::now().toMilliseconds().count() +
                      1000 * 60 * 60 * 24); // Expires in 1 day

  cms_.set_maintenances({def1, def2});

  regenerateClusterMaintenanceWrapper();
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();

  // Simluate node removal by building a new nodes config without nodes 1 & 13
  nodes_config_ = NodesConfigurationTestUtil::provisionNodes({2, 9, 11})
                      ->withVersion(membership::MembershipVersion::Type(100));
  ASSERT_NE(nullptr, nodes_config_);
  maintenance_manager_->onNodesConfigurationUpdated();

  // Make sure that a delta will be written to remove empty_1 maintenance
  std::function<void(Status, lsn_t, std::string)> write_done_cb;
  EXPECT_CALL(*mock_maintenance_log_writer_, writeDelta(_, _, _, _))
      .Times(1)
      .WillOnce(Invoke([&](const MaintenanceDelta& delta, auto cb, auto, auto) {
        EXPECT_THAT(
            delta.get_remove_maintenances().get_filter().get_group_ids(),
            UnorderedElementsAre("empty_1"));
        // Capture the write callback to invoke it later.
        write_done_cb = std::move(cb);
      }));
  runExecutor();
  ASSERT_NE(nullptr, write_done_cb);

  // Simulate the remove of N2
  nodes_config_ =
      NodesConfigurationTestUtil::provisionNodes({9, 11})->withVersion(
          membership::MembershipVersion::Type(110));
  ASSERT_NE(nullptr, nodes_config_);
  maintenance_manager_->onNodesConfigurationUpdated();

  // Purging shouldn't happen because there's still a delta in flight.
  EXPECT_CALL(*mock_maintenance_log_writer_, writeDelta(_, _, _, _)).Times(0);
  runExecutor();

  // Now let's remove the maintenance and call the removal callback.
  write_done_cb(Status::OK, 1, "");

  // Add an expired maintenance which should be removed as well
  MaintenanceDefinition def3 = def1;
  def3.set_shards({mkShardID(9, 0)});
  def3.set_expires_on(SystemTimestamp::now().toMilliseconds().count() -
                      1000 * 60); // Expired a minute ago
  def3.set_group_id("expired_1");
  cms_.set_maintenances({def2, def3});

  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));

  EXPECT_CALL(*mock_maintenance_log_writer_, writeDelta(_, _, _, _))
      .Times(1)
      .WillOnce(Invoke([&](const MaintenanceDelta& delta, auto cb, auto, auto) {
        EXPECT_THAT(
            delta.get_remove_maintenances().get_filter().get_group_ids(),
            UnorderedElementsAre("empty_2", "expired_1"));
        cb(Status::OK, 2, "");
      }));
  runExecutor();
}

TEST_F(MaintenanceManagerTest, TestTracing) {
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

  // Define two maintenances, one of them in progress and the other completed.
  MaintenanceDefinition def1;
  def1.set_user("bunny2");
  def1.set_shard_target_state(ShardOperationalState::DRAINED);
  def1.set_shards({mkShardID(1, 0), mkShardID(13, 0)});
  def1.set_sequencer_nodes({mkNodeID(9)});
  def1.set_sequencer_target_state(SequencingState::DISABLED);
  def1.set_group_id("group1");

  MaintenanceDefinition def2;
  def2.set_user("bunny1");
  def2.set_shard_target_state(ShardOperationalState::DRAINED);
  def2.set_shards({mkShardID(11, 0)});
  def2.set_group_id("group2");

  cms_.set_maintenances({def1, def2});

  setShardWorkflowResult({
      {ShardID(11, 0),
       {MaintenanceStatus::COMPLETED,
        membership::StorageStateTransition::DATA_MIGRATION_COMPLETED}},
      {ShardID(1, 0),
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DISABLING_WRITE}},
      {ShardID(13, 0),
       {MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES,
        membership::StorageStateTransition::DISABLING_WRITE}},
  });

  setSequencerWorkflowResult(SeqWfResult());

  overrideStorageState({
      {ShardID(11, 0), membership::StorageState::NONE},
  });

  // Run a one complete evaluation loop
  regenerateClusterMaintenanceWrapper();
  maintenance_manager_->onClusterMaintenanceStateUpdate(cms_, lsn_t(1));
  maintenance_manager_->onEventLogRebuildingSetUpdate(set_, lsn_t(1));
  runExecutor();
  applyNCUpdate();
  fulfillNCPromise(E::OK, nodes_config_);
  runExecutor();

  // Expect to get one periodic evaluation sample describing the state of the
  // world at this point.
  EXPECT_EQ(1, logger_->pushed_samples.at("maintenance_manager").size());
  auto& sample = logger_->pushed_samples.at("maintenance_manager")[0];
  EXPECT_EQ("PERIODIC_EVALUATION_SAMPLE", sample->getNormalValue("event"));
  EXPECT_EQ(1, sample->getIntValue("maintenance_state_version"));
  EXPECT_EQ(nodes_config_->getLastChangeTimestamp().toMilliseconds().count(),
            sample->getIntValue("published_nc_ctime_ms"));
  EXPECT_EQ((std::set<std::string>{"group1", "group2"}),
            sample->getSetValue("maintenance_ids"));
  EXPECT_EQ((std::set<std::string>{"bunny1", "bunny2"}),
            sample->getSetValue("users"));
  EXPECT_EQ((std::set<std::string>{"N1:S0", "N11:S0", "N13:S0"}),
            sample->getSetValue("shards_affected"));
  EXPECT_EQ((std::set<std::string>{"1", "9", "11", "13"}),
            sample->getSetValue("node_ids_affected"));
  EXPECT_EQ(
      (std::set<std::string>{"server-1", "server-9", "server-11", "server-13"}),
      sample->getSetValue("node_names_affected"));
  EXPECT_EQ((std::set<std::string>{}),
            sample->getSetValue("maintenances_skipping_safety_checker"));
  EXPECT_EQ(
      (std::set<std::string>{}), sample->getSetValue("maintenances_unknown"));
  EXPECT_EQ((std::set<std::string>{}),
            sample->getSetValue("maintenances_blocked_until_safe"));
  EXPECT_EQ((std::set<std::string>{"group1"}),
            sample->getSetValue("maintenances_in_progress"));
  EXPECT_EQ((std::set<std::string>{"group2"}),
            sample->getSetValue("maintenances_in_completed"));
}

}}} // namespace facebook::logdevice::maintenance
