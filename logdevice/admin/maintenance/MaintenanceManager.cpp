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
  run_evaluate_ = true;
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

folly::SemiFuture<folly::Expected<ShardOperationalState, Status>>
MaintenanceManager::getShardOperationalState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<
      folly::Expected<ShardOperationalState, Status>>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardOperationalStateInternal(shard));
  });

  return std::move(f);
}

folly::Expected<ShardOperationalState, Status>
MaintenanceManager::getShardOperationalStateInternal(ShardID shard) const {
  auto storageState = getStorageStateInternal(shard);

  if (storageState.hasError()) {
    return folly::makeUnexpected(std::move(storageState.error()));
  }

  ld_check(storageState.hasValue());

  auto targetOpStates = getShardTargetStatesInternal(shard);

  if (targetOpStates.hasError()) {
    return folly::makeUnexpected(std::move(targetOpStates.error()));
  }

  ld_check(targetOpStates.hasValue());

  if (targetOpStates->count(ShardOperationalState::ENABLED)) {
    ld_check(targetOpStates->size() == 1);
    if (storageState.value() == membership::StorageState::READ_WRITE) {
      return ShardOperationalState::ENABLED;
    } else {
      // This does not necessarily mean we have an active workflow
      // right now but one will be created if this state holds
      return ShardOperationalState::ENABLING;
    }
  }

  ShardOperationalState result;
  ld_check(targetOpStates->count(ShardOperationalState::DRAINED) ||
           targetOpStates->count(ShardOperationalState::MAY_DISAPPEAR));

  switch (storageState.value()) {
    case membership::StorageState::NONE:
      result = ShardOperationalState::DRAINED;
      break;
    case membership::StorageState::NONE_TO_RO:
    case membership::StorageState::RW_TO_RO:
    case membership::StorageState::READ_ONLY:
      result = ShardOperationalState::MAY_DISAPPEAR;
      break;
    case membership::StorageState::DATA_MIGRATION:
      result = ShardOperationalState::MIGRATING_DATA;
      break;
    case membership::StorageState::READ_WRITE:
      result = ShardOperationalState::ENABLED;
      break;
    default:
      // This should never happen. All storage state
      // cases are handled above
      ld_assert(false);
      result = ShardOperationalState::UNKNOWN;
      break;
  }
  return std::move(result);
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

folly::SemiFuture<folly::Expected<membership::StorageState, Status>>
MaintenanceManager::getStorageState(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<
      folly::Expected<membership::StorageState, Status>>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getStorageStateInternal(shard));
  });

  return std::move(f);
}

folly::Expected<membership::StorageState, Status>
MaintenanceManager::getStorageStateInternal(ShardID shard) const {
  auto [exists, shardState] =
      nodes_config_->getStorageMembership()->getShardState(shard);
  return exists ? folly::makeExpected<Status>(shardState.storage_state)
                : folly::makeUnexpected(E::NOTFOUND);
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

folly::SemiFuture<
    folly::Expected<std::unordered_set<ShardOperationalState>, Status>>
MaintenanceManager::getShardTargetStates(ShardID shard) {
  auto [p, f] = folly::makePromiseContract<
      folly::Expected<std::unordered_set<ShardOperationalState>, Status>>();
  add([this, shard, mpromise = std::move(p)]() mutable {
    mpromise.setValue(getShardTargetStatesInternal(shard));
  });

  return std::move(f);
}

folly::Expected<std::unordered_set<ShardOperationalState>, Status>
MaintenanceManager::getShardTargetStatesInternal(ShardID shard) const {
  if (!cluster_maintenance_wrapper_) {
    return folly::makeUnexpected(E::NOTREADY);
  }
  return folly::makeExpected<Status>(
      cluster_maintenance_wrapper_->getShardTargetStates(shard));
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
  if (active_shard_workflows_.count(shard)) {
    return active_shard_workflows_.at(shard).first.get();
  }
  return nullptr;
}

SequencerWorkflow* FOLLY_NULLABLE
MaintenanceManager::getActiveSequencerWorkflow(node_index_t node) const {
  if (active_sequencer_workflows_.count(node)) {
    return active_sequencer_workflows_.at(node).first.get();
  }
  return nullptr;
}

void MaintenanceManager::evaluate() {
  if (!run_evaluate_) {
    // State has not changed as there are no updates.
    ld_info("No state change from previous evaluate run. Will run when next"
            "state change occurs");
    status_ = MMStatus::AWAITING_STATE_CHANGE;
    return;
  }

  run_evaluate_ = false;

  // This is required because it is possible that we are running evaluate
  // again before the NodesConfig update from previous iteration makes it
  // to processor.
  if (deps_->getNodesConfiguration()->getVersion() >
      nodes_config_->getVersion()) {
    nodes_config_ = deps_->getNodesConfiguration();
  }

  updateClientMaintenanceStateWrapper();

  // Create all required workflows
  createWorkflows();

  // Run Shard workflows
  status_ = MMStatus::RUNNING_WORKFLOWS;
  auto [shards, shard_futures] = runShardWorkflows();
  collectAllSemiFuture(shard_futures.begin(), shard_futures.end())
      .via(this)
      .thenValue([this, shards = std::move(shards)](
                     std::vector<folly::Try<MaintenanceStatus>> result) {
        processShardWorkflowResult(shards, result);
        auto [nodes, node_futures] = runSequencerWorkflows();
        return collectAllSemiFuture(node_futures.begin(), node_futures.end())
            .via(this)
            .thenValue([this, n = std::move(nodes)](
                           std::vector<folly::Try<MaintenanceStatus>> result) {
              processSequencerWorkflowResult(n, std::move(result));
              return folly::makeSemiFuture<
                  folly::Expected<folly::Unit, Status>>(
                  folly::makeExpected<Status>(folly::Unit()));
            });
      })
      .thenValue([this](folly::Expected<folly::Unit, Status> result) {
        if (shouldStopProcessing()) {
          auto e = folly::makeUnexpected<Status>(E::SHUTDOWN);
          return folly::makeSemiFuture<NCUpdateResult>(std::move(e));
        }
        return scheduleNodesConfigUpdates();
      })
      .thenValue([this](NCUpdateResult result) {
        if (result.hasError() && result.error() != Status::EMPTY) {
          auto e = folly::makeUnexpected<Status>(std::move(result.error()));
          return folly::makeSemiFuture<SafetyCheckResult>(std::move(e));
        }
        if (shouldStopProcessing()) {
          auto e = folly::makeUnexpected<Status>(E::SHUTDOWN);
          return folly::makeSemiFuture<SafetyCheckResult>(std::move(e));
        }
        ld_check(!result.hasError() || result.error() == Status::EMPTY);
        // Update local copy to the version in result
        if (result.hasValue()) {
          nodes_config_ = std::move(result.value());
        }
        // We have shards that need to be enabled which could potentially
        // imapct the outcome of safety checks. Hence we will run safety
        // check only if there are no more shards that need to be enabled.
        // Return E:RETRY so that we skip safety check and call evaluate
        // at the end of this chain
        if (has_shards_to_enable_) {
          ld_info("Received NCUpdateResult but we have shards that need "
                  "to be enabled. Returning E::RETRY so that we re-evaluate");
          return folly::makeSemiFuture<SafetyCheckResult>(
              folly::makeUnexpected<Status>(E::RETRY));
        }
        return scheduleSafetyCheck();
      })
      .thenValue([this](SafetyCheckResult result) {
        if (result.hasError()) {
          auto e = folly::makeUnexpected<Status>(std::move(result.error()));
          return folly::makeSemiFuture<NCUpdateResult>(std::move(e));
        }
        if (shouldStopProcessing()) {
          auto e = folly::makeUnexpected<Status>(E::SHUTDOWN);
          return folly::makeSemiFuture<NCUpdateResult>(std::move(e));
        }
        processSafetyCheckResult(std::move(result.value()));
        return scheduleNodesConfigUpdates();
      })
      .thenValue([this](NCUpdateResult result) {
        if (result.hasError() && result.error() == Status::SHUTDOWN) {
          ld_check(shouldStopProcessing());
          finishShutdown();
          return;
        }

        if (result.hasValue()) {
          nodes_config_ = std::move(result.value());
        }

        if (!shouldStopProcessing()) {
          evaluate();
        } else {
          finishShutdown();
        }
      });
}

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
    const std::vector<folly::Try<MaintenanceStatus>>& status) {
  ld_check(shards.size() == status.size());
  int i = 0;
  has_shards_to_enable_ = false;
  for (const auto& shard : shards) {
    ld_check(status[i].hasValue());
    ld_check(active_shard_workflows_.count(shard));
    auto s = status[i].value();
    switch (s) {
      case MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES:
      case MaintenanceStatus::AWAITING_SAFETY_CHECK:
      case MaintenanceStatus::AWAITING_DATA_REBUILDING:
      case MaintenanceStatus::RETRY:
        active_shard_workflows_[shard].second = s;
        if (active_shard_workflows_[shard].first->getTargetOpStates().count(
                ShardOperationalState::ENABLED)) {
          has_shards_to_enable_ = true;
        }
        break;
      case MaintenanceStatus::COMPLETED:
        if (active_shard_workflows_[shard].first->getTargetOpStates().count(
                ShardOperationalState::ENABLED)) {
          removeShardWorkflow(shard);
        }
        break;
      default:
        ld_critical("Unexpected Status set by workflow");
        active_shard_workflows_[shard].second = s;
        break;
    }
    i++;
  }
}

void MaintenanceManager::processSequencerWorkflowResult(
    const std::vector<node_index_t>& nodes,
    const std::vector<folly::Try<MaintenanceStatus>>& status) {
  ld_check(nodes.size() == status.size());
  int i = 0;
  for (node_index_t n : nodes) {
    ld_check(status[i].hasValue());
    auto s = status[i].value();
    switch (s) {
      case MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES:
      case MaintenanceStatus::AWAITING_SAFETY_CHECK:
        active_sequencer_workflows_[n].second = s;
        break;
      case MaintenanceStatus::COMPLETED:
        removeSequencerWorkflow(n);
        break;
      default:
        ld_critical("Unexpected Status set by workflow");
        active_sequencer_workflows_[n].second = s;
        break;
    }
    i++;
  }
}

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
  for (const auto& it : active_shard_workflows_) {
    auto shard_id = it.first;
    ShardWorkflow* wf = it.second.first.get();
    auto current_storage_state =
        nodes_config_->getStorageMembership()->getShardState(shard_id);
    // The shard should be in NodesConfig since workflow is created
    // only for shards in the config
    ld_check(current_storage_state.first);
    shards.push_back(shard_id);
    futures.push_back(wf->run(current_storage_state.second.storage_state,
                              getShardDataHealthInternal(shard_id),
                              getCurrentRebuildingMode(shard_id)));
  }
  return std::make_pair(std::move(shards), std::move(futures));
}

void MaintenanceManager::createWorkflows() {
  // Iterate over all the storage nodes in membership and create
  // workflows if required
  for (auto node : nodes_config_->getStorageNodes()) {
    auto num_shards = nodes_config_->getNumShards(node);
    for (shard_index_t i = 0; i < num_shards; i++) {
      auto shard_id = ShardID(node, i);
      const auto& targets =
          cluster_maintenance_wrapper_->getShardTargetStates(shard_id);
      if (targets.count(ShardOperationalState::ENABLED) &&
          isShardEnabled(shard_id)) {
        // Shard is already enabled, do not bother creating a workflow
        continue;
      }
      // Create a new workflow if one does not exist or if the target states
      // are different because some maintenance was removed or new maintenance
      // was added for this shard
      if (!active_shard_workflows_.count(shard_id) ||
          targets !=
              active_shard_workflows_[shard_id].first->getTargetOpStates()) {
        active_shard_workflows_[shard_id] = std::make_pair(
            std::make_unique<ShardWorkflow>(shard_id, getEventLogWriter()),
            MaintenanceStatus::STARTED);
      }
      ShardWorkflow* wf = active_shard_workflows_[shard_id].first.get();
      wf->isPassiveDrainAllowed(
          cluster_maintenance_wrapper_->isPassiveDrainAllowed(shard_id));
      wf->shouldSkipSafetyCheck(
          cluster_maintenance_wrapper_->shouldSkipSafetyCheck(shard_id));
      wf->rebuildInRestoreMode(
          cluster_maintenance_wrapper_->shouldForceRestoreRebuilding(shard_id));
    }
  }

  // Iterator over all the sequencer nodes in membership and create
  // workflows if required
  for (auto node : nodes_config_->getSequencerNodes()) {
    auto target = cluster_maintenance_wrapper_->getSequencerTargetState(node);
    if (target == SequencingState::ENABLED && isSequencingEnabled(node)) {
      // Sequencer is already enabled, do not bother creating a workflow
      continue;
    }

    if (!active_sequencer_workflows_.count(node) ||
        target != active_sequencer_workflows_[node].first->getTargetOpState()) {
      active_sequencer_workflows_[node] =
          std::make_pair(std::make_unique<SequencerWorkflow>(node),
                         MaintenanceStatus::STARTED);
    }
    SequencerWorkflow* wf = active_sequencer_workflows_[node].first.get();
    wf->shouldSkipSafetyCheck(
        cluster_maintenance_wrapper_->shouldSkipSafetyCheck(node));
  }
}

bool MaintenanceManager::isShardEnabled(const ShardID& shard) {
  // Shard is considered as enabled if its storage state is READ_WRITE
  // and there is no full rebuilding (mini rebuilding is fine)
  auto result = getStorageStateInternal(shard);
  return result.hasValue() &&
      result.value() == membership::StorageState::READ_WRITE &&
      !event_log_rebuilding_set_
           ->isRebuildingFullShard(shard.node(), shard.shard())
           .hasValue();
}

bool MaintenanceManager::isSequencingEnabled(node_index_t node) {
  return nodes_config_->getSequencerMembership()->isSequencingEnabled(node);
}

void MaintenanceManager::removeShardWorkflow(ShardID shard) {
  if (active_shard_workflows_.count(shard)) {
    active_shard_workflows_.erase(shard);
  }
}

std::pair<std::vector<node_index_t>,
          std::vector<folly::SemiFuture<MaintenanceStatus>>>
MaintenanceManager::runSequencerWorkflows() {
  std::vector<node_index_t> nodes;
  std::vector<folly::SemiFuture<MaintenanceStatus>> futures;
  for (const auto& it : active_sequencer_workflows_) {
    auto node = it.first;
    auto wf = it.second.first.get();
    nodes.push_back(node);
    futures.push_back(wf->run(isSequencingEnabled(node)));
  }
  return std::make_pair(std::move(nodes), std::move(futures));
}

void MaintenanceManager::removeSequencerWorkflow(node_index_t node) {
  if (active_sequencer_workflows_.count(node)) {
    active_sequencer_workflows_.erase(node);
  }
}

EventLogWriter* MaintenanceManager::getEventLogWriter() {
  if (event_log_writer_ == nullptr) {
    event_log_writer_ = std::make_unique<EventLogWriter>(*deps_->getEventLog());
  }
  return event_log_writer_.get();
}

RebuildingMode MaintenanceManager::getCurrentRebuildingMode(ShardID shard) {
  return RebuildingMode::INVALID;
}

}}} // namespace facebook::logdevice::maintenance
