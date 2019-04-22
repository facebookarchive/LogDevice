/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManager.h"

#include "logdevice/admin/Conv.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"

namespace facebook { namespace logdevice { namespace maintenance {

void MaintenanceManagerDependencies::startSubscription() {
  ld_check(cluster_maintenance_state_machine_ != nullptr);
  ld_check(event_log_state_machine_ != nullptr);
  ld_check(processor_ != nullptr);

  // Register a callback for ClusterMaintenanceState update
  // This callback will be called on the thread running the
  // cluster maintenance state machine
  auto cms_callback = [this](const ClusterMaintenanceState& state,
                             const MaintenanceDelta* /*unused*/,
                             lsn_t version) {
    owner_->onClusterMaintenanceStateUpdate(state, version);
  };
  cms_update_handle_ =
      cluster_maintenance_state_machine_->subscribe(cms_callback);

  // Register a callback for EventLogRebuildingSet update
  // This callback will be called on the worker thread
  // running the event log state machine
  auto el_callback = [this](const EventLogRebuildingSet& state,
                            const EventLogRecord* /*unused*/,
                            lsn_t version) {
    owner_->onEventLogRebuildingSetUpdate(state, version);
  };
  el_update_handle_ = event_log_state_machine_->subscribe(el_callback);

  // Register a callback for the NodesConfiguration update
  // This callback will be called on a unspecified thread.
  auto nc_callback = [this]() { owner_->onNodesConfigurationUpdated(); };

  nodes_config_update_handle_ = std::make_unique<ConfigSubscriptionHandle>(
      processor_->config_->updateableNodesConfiguration()->subscribeToUpdates(
          nc_callback));
}

void MaintenanceManagerDependencies::stopSubscription() {
  cms_update_handle_.reset();
  el_update_handle_.reset();
  nodes_config_update_handle_.reset();
}

folly::SemiFuture<SafetyCheckResult>
MaintenanceManagerDependencies::postSafetyCheckRequest(
    const std::vector<const ShardWorkflow*>& shard_wf,
    const std::vector<const SequencerWorkflow*>& seq_wf) {
  // TODO: Integrate with SafetyCheckScheduler
  folly::Promise<SafetyCheckResult> p;
  return p.getSemiFuture();
}

folly::SemiFuture<NCUpdateResult>
MaintenanceManagerDependencies::postNodesConfigurationUpdate(
    std::unique_ptr<membership::StorageMembership::Update> shards_update,
    std::unique_ptr<membership::SequencerMembership::Update>
        sequencers_update) {
  // TODO: NodesConfig update path
  auto [p, f] = folly::makePromiseContract<NCUpdateResult>();
  return std::move(f);
}

void MaintenanceManagerDependencies::setOwner(MaintenanceManager* owner) {
  // Should only be called once
  ld_check(owner_ == nullptr);
  owner_ = owner;
}

EventLogStateMachine* MaintenanceManagerDependencies::getEventLog() {
  return event_log_state_machine_;
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
MaintenanceManagerDependencies::getNodesConfiguration() const {
  return processor_->getNodesConfiguration();
};

MaintenanceManager::MaintenanceManager(
    folly::Executor* executor,
    std::unique_ptr<MaintenanceManagerDependencies> deps)
    : SerialWorkContext(getKeepAliveToken(executor)), deps_(std::move(deps)) {
  ld_check(deps_);
  deps_->setOwner(this);
}

MaintenanceManager::~MaintenanceManager() {}

void MaintenanceManager::start() {
  add([this]() { startInternal(); });
}

void MaintenanceManager::startInternal() {
  if (status_ != MMStatus::NOT_STARTED && status_ != MMStatus::STOPPED) {
    ld_info("Maintenance Manager is already running");
    return;
  }
  ld_check(deps_ != nullptr);
  deps_->startSubscription();
  nodes_config_ = deps_->getNodesConfiguration();
  // Invalid promise
  shutdown_promise_ = folly::Promise<folly::Unit>::makeEmpty();
  status_ = MMStatus::STARTING;
}

folly::SemiFuture<folly::Unit> MaintenanceManager::stop() {
  auto [p, f] = folly::makePromiseContract<folly::Unit>();
  add([this, mpromise = std::move(p)]() mutable {
    shutdown_promise_ = std::move(mpromise);
    stopInternal();
  });
  return std::move(f);
}

void MaintenanceManager::stopInternal() {
  deps_->stopSubscription();
  if (status_ == MMStatus::STARTING ||
      status_ == MMStatus::AWAITING_STATE_CHANGE) {
    // We are waiting for a subscription callback to happen or
    // we are waiting for an initial state to be available
    // This means we are not running any workflows right now.
    // So its safe to shutdown right away
    finishShutdown();
    return;
  }
  status_ = MMStatus::STOPPING;
}

void MaintenanceManager::scheduleRun() {
  // We will call evaluate immediately if
  // 1/ This is the first run and we have an initial ClusterMaintenanceState
  // and EventLogRebuildingSet
  // 2/ MM is actually waiting for a state change
  // (i.e status_ == AWAITING_STATE_CHANGE)
  run_evaluate_.store(true);
  if (status_ == MMStatus::STARTING && last_cms_version_ != LSN_INVALID &&
      last_ers_version_ != LSN_INVALID) {
    ld_info("Initial state available: last_cms_version:%s, last_ers_version:%s",
            toString(last_cms_version_).c_str(),
            toString(last_ers_version_).c_str());
    evaluate();
  } else if (status_ == MMStatus::AWAITING_STATE_CHANGE) {
    evaluate();
  }
}

folly::SemiFuture<thrift::NodeState>
MaintenanceManager::getNodeState(node_index_t node) {
  auto [p, f] = folly::makePromiseContract<thrift::NodeState>();
  add([this, node, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getNodeStateInternal(node));
  });
  return std::move(f);
}

thrift::NodeState
MaintenanceManager::getNodeStateInternal(node_index_t node) const {
  thrift::NodeState state;
  return state;
}

folly::SemiFuture<thrift::SequencerState>
MaintenanceManager::getSequencerState(node_index_t node) {
  auto [p, f] = folly::makePromiseContract<thrift::SequencerState>();
  add([this, node, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getSequencerStateInternal(node));
  });
  return std::move(f);
}

thrift::SequencerState
MaintenanceManager::getSequencerStateInternal(node_index_t node) const {
  thrift::SequencerState state;
  return state;
}

folly::SemiFuture<thrift::ShardState>
MaintenanceManager::getShardState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<thrift::ShardState>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardStateInternal(shard));
  });
  return std::move(f);
}

thrift::ShardState
MaintenanceManager::getShardStateInternal(ShardID shard) const {
  thrift::ShardState state;
  return state;
}

folly::SemiFuture<ShardOperationalState>
MaintenanceManager::getShardOperationalState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<ShardOperationalState>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardOperationalStateInternal(shard));
  });

  return std::move(f);
}

ShardOperationalState
MaintenanceManager::getShardOperationalStateInternal(ShardID shard) const {
  return ShardOperationalState::INVALID;
}

folly::SemiFuture<ShardDataHealth>
MaintenanceManager::getShardDataHealth(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<ShardDataHealth>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardDataHealthInternal(shard));
  });

  return std::move(f);
}

ShardDataHealth
MaintenanceManager::getShardDataHealthInternal(ShardID shard) const {
  return ShardDataHealth::UNKNOWN;
}

folly::SemiFuture<SequencingState>
MaintenanceManager::getSequencingState(node_index_t node) {
  auto [p, f] = folly::makePromiseContract<SequencingState>();
  add([this, node, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getSequencingStateInternal(node));
  });

  return std::move(f);
}

SequencingState
MaintenanceManager::getSequencingStateInternal(node_index_t node) const {
  return SequencingState::ENABLED;
}

folly::SemiFuture<membership::StorageState>
MaintenanceManager::getStorageState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<membership::StorageState>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getStorageStateInternal(shard));
  });

  return std::move(f);
}

membership::StorageState
MaintenanceManager::getStorageStateInternal(ShardID shard) const {
  auto [exists, shardState] =
      nodes_config_->getStorageMembership()->getShardState(shard);
  return exists ? shardState.storage_state : membership::StorageState::INVALID;
}

folly::SemiFuture<membership::MetaDataStorageState>
MaintenanceManager::getMetaDataStorageState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<membership::MetaDataStorageState>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getMetaDataStorageStateInternal(shard));
  });

  return std::move(f);
}

membership::MetaDataStorageState
MaintenanceManager::getMetaDataStorageStateInternal(ShardID shard) const {
  auto [exists, shardState] =
      nodes_config_->getStorageMembership()->getShardState(shard);
  return exists ? shardState.metadata_state
                : membership::MetaDataStorageState::INVALID;
}

folly::SemiFuture<std::unordered_set<ShardOperationalState>>
MaintenanceManager::getShardTargetStates(ShardID shard) {
  auto [p, f] =
      folly::makePromiseContract<std::unordered_set<ShardOperationalState>>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardTargetStatesInternal(shard));
  });

  return std::move(f);
}

std::unordered_set<ShardOperationalState>
MaintenanceManager::getShardTargetStatesInternal(ShardID shard) const {
  if (!cluster_maintenance_wrapper_) {
    return {ShardOperationalState::UNKNOWN};
  }
  return cluster_maintenance_wrapper_->getShardTargetStates(shard);
}

folly::SemiFuture<SequencingState>
MaintenanceManager::getSequencerTargetState(node_index_t node) {
  auto [p, f] = folly::makePromiseContract<SequencingState>();
  add([this, node, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getSequencerTargetStateInternal(node));
  });

  return std::move(f);
}

SequencingState MaintenanceManager::getSequencerTargetStateInternal(
    node_index_t node_index) const {
  if (!cluster_maintenance_wrapper_) {
    return SequencingState::UNKNOWN;
  }
  return cluster_maintenance_wrapper_->getSequencerTargetState(node_index);
}

void MaintenanceManager::onNodesConfigurationUpdated() {
  add([this]() { scheduleRun(); });
}

void MaintenanceManager::onClusterMaintenanceStateUpdate(
    ClusterMaintenanceState state,
    lsn_t version) {
  add([s = std::move(state), v = version, this]() mutable {
    ld_debug("Received ClusterMaintenanceState update: version:%s",
             toString(v).c_str());
    cluster_maintenance_state_ =
        std::make_unique<ClusterMaintenanceState>(std::move(s));
    last_cms_version_ = v;
    scheduleRun();
  });
}

void MaintenanceManager::onEventLogRebuildingSetUpdate(
    EventLogRebuildingSet set,
    lsn_t version) {
  add([s = std::move(set), v = version, this]() mutable {
    ld_debug("Received EventLogRebuildingSet update: version:%s",
             toString(v).c_str());
    event_log_rebuilding_set_ =
        std::make_unique<EventLogRebuildingSet>(std::move(s));
    last_ers_version_ = v;
    scheduleRun();
  });
}

ShardWorkflow* FOLLY_NULLABLE
MaintenanceManager::getActiveShardWorkflow(ShardID shard) const {
  if (active_shard_workflow_.count(shard)) {
    return active_shard_workflow_.at(shard).first.get();
  }
  return nullptr;
}

SequencerWorkflow* FOLLY_NULLABLE
MaintenanceManager::getActiveSequencerWorkflow(node_index_t node) const {
  if (active_sequencer_workflow_.count(node)) {
    return active_sequencer_workflow_.at(node).first.get();
  }
  return nullptr;
}

void MaintenanceManager::evaluate() {}

void MaintenanceManager::finishShutdown() {
  // Stop was called. We should have a valid promise to fulfill
  ld_check(shutdown_promise_.valid());
  status_ = MMStatus::STOPPED;
  shutdown_promise_.setValue();
}

bool MaintenanceManager::shouldStopProcessing() {
  return status_ == MMStatus::STOPPING;
}

void MaintenanceManager::processShardWorkflowResult(
    const std::vector<ShardID>& shards,
    const std::vector<folly::Try<MaintenanceStatus>>& status) {}

void MaintenanceManager::processSequencerWorkflowResult(
    const std::vector<node_index_t>& nodes,
    const std::vector<folly::Try<MaintenanceStatus>>& status) {}

void MaintenanceManager::processSafetyCheckResult(
    SafetyCheckScheduler::Result result) {}

folly::SemiFuture<NCUpdateResult>
MaintenanceManager::scheduleNodesConfigUpdates() {
  return folly::SemiFuture<NCUpdateResult>::makeEmpty();
}

folly::SemiFuture<SafetyCheckResult> MaintenanceManager::scheduleSafetyCheck() {
  return folly::SemiFuture<SafetyCheckResult>::makeEmpty();
}

folly::SemiFuture<MaintenanceManager::MMStatus>
MaintenanceManager::getStatus() {
  auto [p, f] = folly::makePromiseContract<MaintenanceManager::MMStatus>();
  add([this, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getStatusInternal());
  });
  return std::move(f);
}

MaintenanceManager::MMStatus MaintenanceManager::getStatusInternal() const {
  return status_;
}

void MaintenanceManager::updateClientMaintenanceStateWrapper() {
  // Update the wrapper if we have a new version available
  if (cluster_maintenance_state_ != nullptr) {
    cluster_maintenance_wrapper_ = std::make_unique<ClusterMaintenanceWrapper>(
        std::move(cluster_maintenance_state_), nodes_config_);
  }

  // Regenerate definition indices if necessary
  cluster_maintenance_wrapper_->updateNodesConfiguration(nodes_config_);
}

std::pair<std::vector<ShardID>,
          std::vector<folly::SemiFuture<MaintenanceStatus>>>
MaintenanceManager::runShardWorkflows() {
  std::vector<ShardID> shards;
  std::vector<folly::SemiFuture<MaintenanceStatus>> futures;
  return std::make_pair(std::move(shards), std::move(futures));
}

void MaintenanceManager::createWorkflows() {}

void MaintenanceManager::removeShardWorkflow(ShardID shard) {
  if (active_shard_workflow_.count(shard)) {
    active_shard_workflow_.erase(shard);
  }
}

std::pair<std::vector<node_index_t>,
          std::vector<folly::SemiFuture<MaintenanceStatus>>>
MaintenanceManager::runSequencerWorkflows() {
  std::vector<node_index_t> nodes;
  std::vector<folly::SemiFuture<MaintenanceStatus>> futures;
  return std::make_pair(std::move(nodes), std::move(futures));
}

void MaintenanceManager::removeSequencerWorkflow(node_index_t node) {
  if (active_sequencer_workflow_.count(node)) {
    active_sequencer_workflow_.erase(node);
  }
}

EventLogWriter* MaintenanceManager::getEventLogWriter() {
  if (event_log_writer_ == nullptr) {
    event_log_writer_ = std::make_unique<EventLogWriter>(*deps_->getEventLog());
  }
  return event_log_writer_.get();
}

}}} // namespace facebook::logdevice::maintenance
