/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/MaintenanceManager.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/Conv.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManager.h"
#include "logdevice/common/membership/utils.h"

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
      processor_->config_->updateableNCMNodesConfiguration()
          ->subscribeToUpdates(nc_callback));
}

void MaintenanceManagerDependencies::stopSubscription() {
  ld_info("Canceling subscription to ClustermaintenanceStateMachine");
  cms_update_handle_.reset();
  ld_info("Canceling subscription to EventLogStateMachine");
  el_update_handle_.reset();
  ld_info("Canceling subscription to NodesConfiguration");
  nodes_config_update_handle_.reset();
}

folly::SemiFuture<SafetyCheckResult>
MaintenanceManagerDependencies::postSafetyCheckRequest(
    const ClusterMaintenanceWrapper& maintenance_state,
    const ShardAuthoritativeStatusMap& status_map,
    const std::shared_ptr<const configuration::nodes::NodesConfiguration>&
        nodes_config,
    const std::vector<const ShardWorkflow*>& shard_wf,
    const std::vector<const SequencerWorkflow*>& seq_wf) {
  ld_check(safety_check_scheduler_);
  ld_debug("Posting Safety check request");
  return safety_check_scheduler_->schedule(
      maintenance_state, status_map, nodes_config, shard_wf, seq_wf);
}

folly::SemiFuture<NCUpdateResult>
MaintenanceManagerDependencies::postNodesConfigurationUpdate(
    std::unique_ptr<configuration::nodes::StorageConfig::Update> shards_update,
    std::unique_ptr<configuration::nodes::SequencerConfig::Update>
        sequencers_update) {
  auto pf = folly::makePromiseContract<NCUpdateResult>();

  NodesConfiguration::Update update{};

  if (shards_update) {
    update.storage_config_update = std::move(shards_update);
  }
  if (sequencers_update) {
    update.sequencer_config_update = std::move(sequencers_update);
  }

  auto cb = [promise = std::move(pf.first)](
                Status st,
                std::shared_ptr<const configuration::nodes::NodesConfiguration>
                    nc) mutable {
    if (st == E::OK) {
      ld_info("NodesConfig update succeeded. New version:%s",
              toString(nc->getVersion()).c_str());
      promise.setValue(nc);
    } else {
      // NodesConfig update failed. Return failure status
      // so that MaintenanceManager can retry all the
      // workflows again
      ld_info("NodesConfiguration update failed with status:%s",
              toString(st).c_str());
      promise.setValue(folly::makeUnexpected(st));
    }
  };

  ld_info("Posting NodesConfig update - StorageConfig::Update:%s, "
          "SequencerConfig::Update:%s",
          update.storage_config_update
              ? update.storage_config_update->toString().c_str()
              : "null",
          update.sequencer_config_update
              ? update.sequencer_config_update->toString().c_str()
              : "null");

  processor_->getNodesConfigurationManager()->update(
      std::move(update), std::move(cb));
  return std::move(pf.second);
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
  ld_info("Starting Maintenance Manager");
  status_ = MMStatus::STARTING;
  ld_debug("Updated MaintenanceManager status to STARTING");
}

folly::SemiFuture<folly::Unit> MaintenanceManager::stop() {
  auto pf = folly::makePromiseContract<folly::Unit>();
  add([this, mpromise = std::move(pf.first)]() mutable {
    shutdown_promise_ = std::move(mpromise);
    stopInternal();
  });
  return std::move(pf.second);
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
  ld_debug("Updated MaintenanceManager status to STOPPING");
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
            lsn_to_string(last_cms_version_).c_str(),
            lsn_to_string(last_ers_version_).c_str());
    evaluate();
  } else if (status_ == MMStatus::AWAITING_STATE_CHANGE) {
    evaluate();
  }
}

folly::SemiFuture<folly::Expected<thrift::NodeState, Status>>
MaintenanceManager::getNodeState(node_index_t node) {
  auto pf =
      folly::makePromiseContract<folly::Expected<thrift::NodeState, Status>>();
  add([this, node, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getNodeStateInternal(node));
  });
  return std::move(pf.second);
}

folly::Expected<thrift::NodeState, Status>
MaintenanceManager::getNodeStateInternal(node_index_t node) const {
  thrift::NodeState state;
  state.set_node_index(node);
  state.set_sequencer_state(getSequencerStateInternal(node));
  if (nodes_config_->getNumShards() > 0) {
    std::vector<thrift::ShardState> vec;
    for (shard_index_t i = 0; i < nodes_config_->getNumShards(); i++) {
      auto s = getShardStateInternal(ShardID(node, i));
      if (s.hasError()) {
        return folly::makeUnexpected(std::move(s.error()));
      }
      ld_check(s.hasValue());
      vec.push_back(std::move(s.value()));
    }
    state.set_shard_states(std::move(vec));
  }
  return std::move(state);
}

folly::SemiFuture<folly::Expected<thrift::SequencerState, Status>>
MaintenanceManager::getSequencerState(node_index_t node) {
  auto pf = folly::makePromiseContract<
      folly::Expected<thrift::SequencerState, Status>>();
  add([this, node, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(
        folly::makeExpected<Status>(getSequencerStateInternal(node)));
  });
  return std::move(pf.second);
}

thrift::SequencerState
MaintenanceManager::getSequencerStateInternal(node_index_t node) const {
  thrift::SequencerState state;
  state.set_state(getSequencingStateInternal(node));
  if (active_sequencer_workflows_.count(node)) {
    thrift::SequencerMaintenanceProgress progress;
    const auto& wf_status_pair = active_sequencer_workflows_.at(node);
    progress.set_status(wf_status_pair.second);
    progress.set_target_state(wf_status_pair.first->getTargetOpState());
    progress.set_created_at(
        wf_status_pair.first->getCreationTimestamp().toMilliseconds().count());
    progress.set_last_updated_at(wf_status_pair.first->getLastUpdatedTimestamp()
                                     .toMilliseconds()
                                     .count());
    state.set_maintenance(progress);
  }
  return state;
}

folly::SemiFuture<folly::Expected<thrift::ShardState, Status>>
MaintenanceManager::getShardState(ShardID shard) {
  auto pf =
      folly::makePromiseContract<folly::Expected<thrift::ShardState, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getShardStateInternal(shard));
  });
  return std::move(pf.second);
}

folly::Expected<thrift::ShardState, Status>
MaintenanceManager::getShardStateInternal(ShardID shard) const {
  thrift::ShardState state;

  auto dataHealth = getShardDataHealthInternal(shard);
  if (dataHealth.hasError()) {
    return folly::makeUnexpected(std::move(dataHealth.error()));
  }
  ld_check(dataHealth.hasValue());
  state.set_data_health(std::move(dataHealth.value()));

  auto opState = getShardOperationalStateInternal(shard);
  if (opState.hasError()) {
    return folly::makeUnexpected(std::move(opState.error()));
  }
  ld_check(opState.hasValue());
  state.set_current_operational_state(std::move(opState.value()));

  auto storageState = getStorageStateInternal(shard);
  if (storageState.hasError()) {
    return folly::makeUnexpected(std::move(storageState.error()));
  }
  ld_check(storageState.hasValue());
  state.set_storage_state(
      toThrift<membership::thrift::StorageState>(storageState.value()));

  auto metadataState = getMetaDataStorageStateInternal(shard);
  if (metadataState.hasError()) {
    return folly::makeUnexpected(std::move(metadataState.error()));
  }
  ld_check(metadataState.hasValue());
  state.set_metadata_state(toThrift<membership::thrift::MetaDataStorageState>(
      metadataState.value()));

  if (active_shard_workflows_.count(shard)) {
    thrift::ShardMaintenanceProgress progress;
    const auto& wf_status_pair = active_shard_workflows_.at(shard);
    progress.set_status(wf_status_pair.second);
    const auto& states = wf_status_pair.first->getTargetOpStates();
    std::set<ShardOperationalState> set;
    set.insert(states.begin(), states.end());
    progress.set_target_states(set);
    progress.set_created_at(
        wf_status_pair.first->getCreationTimestamp().toMilliseconds().count());
    progress.set_last_updated_at(wf_status_pair.first->getLastUpdatedTimestamp()
                                     .toMilliseconds()
                                     .count());
    ld_check(cluster_maintenance_wrapper_);
    std::vector<GroupID> ids;
    const auto& groups = cluster_maintenance_wrapper_->getGroupsForShard(shard);
    std::copy(groups.begin(), groups.end(), std::back_inserter(ids));
    progress.set_associated_group_ids(std::move(ids));
    state.set_maintenance(progress);
  }
  return std::move(state);
}

folly::SemiFuture<folly::Expected<ShardOperationalState, Status>>
MaintenanceManager::getShardOperationalState(ShardID shard) {
  auto pf = folly::makePromiseContract<
      folly::Expected<ShardOperationalState, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getShardOperationalStateInternal(shard));
  });

  return std::move(pf.second);
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

folly::SemiFuture<folly::Expected<ShardDataHealth, Status>>
MaintenanceManager::getShardDataHealth(ShardID shard) {
  auto pf =
      folly::makePromiseContract<folly::Expected<ShardDataHealth, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getShardDataHealthInternal(shard));
  });

  return std::move(pf.second);
}

folly::Expected<ShardDataHealth, Status>
MaintenanceManager::getShardDataHealthInternal(ShardID shard) const {
  if (!event_log_rebuilding_set_) {
    return folly::makeUnexpected(E::NOTREADY);
  }
  std::vector<node_index_t> donors_remaining;
  auto auth_status = event_log_rebuilding_set_->getShardAuthoritativeStatus(
      shard.node(), shard.shard(), donors_remaining);
  auto has_dirty_ranges = event_log_rebuilding_set_->shardIsTimeRangeRebuilding(
      shard.node(), shard.shard());
  return std::move(toShardDataHealth(auth_status, has_dirty_ranges));
}

folly::SemiFuture<folly::Expected<SequencingState, Status>>
MaintenanceManager::getSequencingState(node_index_t node) {
  auto pf =
      folly::makePromiseContract<folly::Expected<SequencingState, Status>>();
  add([this, node, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(
        folly::makeExpected<Status>(getSequencingStateInternal(node)));
  });

  return std::move(pf.second);
}

SequencingState
MaintenanceManager::getSequencingStateInternal(node_index_t node) const {
  return isSequencingEnabled(node) ? SequencingState::ENABLED
                                   : SequencingState::DISABLED;
}

folly::SemiFuture<folly::Expected<membership::StorageState, Status>>
MaintenanceManager::getStorageState(ShardID shard) {
  auto pf = folly::makePromiseContract<
      folly::Expected<membership::StorageState, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getStorageStateInternal(shard));
  });

  return std::move(pf.second);
}

folly::Expected<membership::StorageState, Status>
MaintenanceManager::getStorageStateInternal(ShardID shard) const {
  auto result = nodes_config_->getStorageMembership()->getShardState(shard);
  return result.first ? folly::makeExpected<Status>(result.second.storage_state)
                      : folly::makeUnexpected(E::NOTFOUND);
}

folly::SemiFuture<folly::Expected<membership::MetaDataStorageState, Status>>
MaintenanceManager::getMetaDataStorageState(ShardID shard) {
  auto pf = folly::makePromiseContract<
      folly::Expected<membership::MetaDataStorageState, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getMetaDataStorageStateInternal(shard));
  });

  return std::move(pf.second);
}

folly::Expected<membership::MetaDataStorageState, Status>
MaintenanceManager::getMetaDataStorageStateInternal(ShardID shard) const {
  auto result = nodes_config_->getStorageMembership()->getShardState(shard);
  return result.first
      ? folly::makeExpected<Status>(result.second.metadata_state)
      : folly::makeUnexpected(E::NOTFOUND);
}

folly::SemiFuture<
    folly::Expected<std::unordered_set<ShardOperationalState>, Status>>
MaintenanceManager::getShardTargetStates(ShardID shard) {
  auto pf = folly::makePromiseContract<
      folly::Expected<std::unordered_set<ShardOperationalState>, Status>>();
  add([this, shard, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getShardTargetStatesInternal(shard));
  });

  return std::move(pf.second);
}

folly::Expected<std::unordered_set<ShardOperationalState>, Status>
MaintenanceManager::getShardTargetStatesInternal(ShardID shard) const {
  if (!cluster_maintenance_wrapper_) {
    return folly::makeUnexpected(E::NOTREADY);
  }
  return cluster_maintenance_wrapper_->getShardTargetStates(shard);
}

folly::SemiFuture<folly::Expected<SequencingState, Status>>
MaintenanceManager::getSequencerTargetState(node_index_t node) {
  auto pf =
      folly::makePromiseContract<folly::Expected<SequencingState, Status>>();
  add([this, node, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getSequencerTargetStateInternal(node));
  });

  return std::move(pf.second);
}

folly::Expected<SequencingState, Status>
MaintenanceManager::getSequencerTargetStateInternal(
    node_index_t node_index) const {
  if (!cluster_maintenance_wrapper_) {
    return folly::makeUnexpected<Status>(E::NOTREADY);
  }
  return std::move(
      cluster_maintenance_wrapper_->getSequencerTargetState(node_index));
}

void MaintenanceManager::onNodesConfigurationUpdated() {
  add([this]() { scheduleRun(); });
}

void MaintenanceManager::onClusterMaintenanceStateUpdate(
    ClusterMaintenanceState state,
    lsn_t version) {
  add([s = std::move(state), v = version, this]() mutable {
    ld_debug("Received ClusterMaintenanceState update: version:%s",
             lsn_to_string(v).c_str());
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
    ld_info("Received EventLogRebuildingSet update: version:%s",
            lsn_to_string(v).c_str());
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
    ld_info("No state change from previous evaluate run. Will run when next "
            "state change occurs");
    status_ = MMStatus::AWAITING_STATE_CHANGE;
    return;
  }

  ld_info("Proceeding with evaluation of current state, "
          "Latest NodesConfig version:%s, "
          "Local NodesConfig version:%s, "
          "ClusterMaintenanceState version:%s, "
          "EventLogRebuildingSet version:%s",
          toString(deps_->getNodesConfiguration()->getVersion()).c_str(),
          toString(nodes_config_->getVersion()).c_str(),
          lsn_to_string(last_cms_version_).c_str(),
          lsn_to_string(last_ers_version_).c_str());

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
  ld_debug("Updated MaintenanceManager status to RUNNING_WORKFLOWS");
  auto shards_futures = runShardWorkflows();
  collectAllSemiFuture(
      shards_futures.second.begin(), shards_futures.second.end())
      .via(this)
      .thenValue([this, shards = std::move(shards_futures.first)](
                     std::vector<folly::Try<MaintenanceStatus>> result) {
        ld_debug("runShardWorkflows complete. processing results");
        processShardWorkflowResult(shards, result);
        auto nodes_futures = runSequencerWorkflows();
        return collectAllSemiFuture(
                   nodes_futures.second.begin(), nodes_futures.second.end())
            .via(this)
            .thenValue([this, n = std::move(nodes_futures.first)](
                           std::vector<folly::Try<MaintenanceStatus>>
                               sequencerResult) {
              ld_debug("runSequencerWorkflows complete. processing results");
              processSequencerWorkflowResult(n, std::move(sequencerResult));
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
          ld_debug("Updating local copy of NodesConfig to version in "
                   "NCUpdateResult:%s",
                   toString(nodes_config_->getVersion()).c_str());
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
          ld_debug("Updating local copy of NodesConfig to version in "
                   "NCUpdateResult:%s",
                   toString(nodes_config_->getVersion()).c_str());
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
  ld_debug("Updated MaintenanceManager status to STOPPED");
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
    ld_debug("MaintenanceStatus for Shard:%s set to %s",
             toString(shard).c_str(),
             apache::thrift::util::enumNameSafe(s).c_str());
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
        active_shard_workflows_[shard].second = s;
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

membership::StorageStateTransition
MaintenanceManager::getExpectedStorageStateTransition(ShardID shard) {
  ld_check(active_shard_workflows_.count(shard));
  return active_shard_workflows_.at(shard)
      .first->getExpectedStorageStateTransition();
}

void MaintenanceManager::processSequencerWorkflowResult(
    const std::vector<node_index_t>& nodes,
    const std::vector<folly::Try<MaintenanceStatus>>& status) {
  ld_check(nodes.size() == status.size());
  int i = 0;
  for (node_index_t n : nodes) {
    ld_check(status[i].hasValue());
    auto s = status[i].value();
    ld_debug("MaintenanceStatus for Sequencer Node:%s set to %s",
             toString(n).c_str(),
             apache::thrift::util::enumNameSafe(s).c_str());
    switch (s) {
      case MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES:
      case MaintenanceStatus::AWAITING_SAFETY_CHECK:
        active_sequencer_workflows_[n].second = s;
        break;
      case MaintenanceStatus::COMPLETED:
        active_sequencer_workflows_[n].second = s;
        if (active_sequencer_workflows_[n].first->getTargetOpState() ==
            SequencingState::ENABLED) {
          removeSequencerWorkflow(n);
        }
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
    SafetyCheckScheduler::Result result) {
  ld_debug("Processing Safety check results");
  for (auto shard : result.safe_shards) {
    ld_debug("Safety check passed for shard:%s", toString(shard).c_str());
    // We should have an active workflow for every shard in result
    ld_check(active_shard_workflows_.count(shard));
    // And its status should be waiting on safety check results
    ld_check(active_shard_workflows_.at(shard).second ==
             MaintenanceStatus::AWAITING_SAFETY_CHECK);
    active_shard_workflows_[shard].second =
        MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES;
    ld_debug("MaintenanceStatus for Shard:%s updated to %s",
             toString(shard).c_str(),
             apache::thrift::util::enumNameSafe(
                 active_shard_workflows_[shard].second)
                 .c_str());
  }

  for (auto node : result.safe_sequencers) {
    ld_debug("Safety check passed for node:%s", toString(node).c_str());
    // We should have an active workflow for every shard in result
    ld_check(active_sequencer_workflows_.count(node));
    // And its status should be waiting on safety check results
    ld_check(active_sequencer_workflows_.at(node).second ==
             MaintenanceStatus::AWAITING_SAFETY_CHECK);
    active_sequencer_workflows_[node].second =
        MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES;
    ld_debug("MaintenanceStatus for Sequencer Node:%s updated to %s",
             toString(node).c_str(),
             apache::thrift::util::enumNameSafe(
                 active_sequencer_workflows_[node].second)
                 .c_str());
  }

  for (const auto& it : result.unsafe_groups) {
    // TODO: update the impact result in MaintenanceDefinition
    // corresponding to the group

    // Iterate over shards and sequencers every unsafe
    // group and set status
    for (auto shard :
         cluster_maintenance_wrapper_->getShardsForGroup(it.first)) {
      if (active_shard_workflows_.count(shard) &&
          active_shard_workflows_.at(shard).second ==
              MaintenanceStatus::AWAITING_SAFETY_CHECK) {
        active_shard_workflows_[shard].second =
            MaintenanceStatus::BLOCKED_UNTIL_SAFE;
        ld_debug("MaintenanceStatus for Shard:%s updated to %s",
                 toString(shard).c_str(),
                 apache::thrift::util::enumNameSafe(
                     active_shard_workflows_[shard].second)
                     .c_str());
      }
    }

    for (auto node :
         cluster_maintenance_wrapper_->getSequencersForGroup(it.first)) {
      if (active_sequencer_workflows_.count(node) &&
          active_sequencer_workflows_.at(node).second ==
              MaintenanceStatus::AWAITING_SAFETY_CHECK) {
        ld_info("Maintenance for Sequencer:%s is blocked until safe",
                toString(node).c_str());
        active_sequencer_workflows_[node].second =
            MaintenanceStatus::BLOCKED_UNTIL_SAFE;
        ld_debug("MaintenanceStatus for Sequencer Node:%s updated to %s",
                 toString(node).c_str(),
                 apache::thrift::util::enumNameSafe(
                     active_sequencer_workflows_[node].second)
                     .c_str());
      }
    }
  }
}

folly::SemiFuture<NCUpdateResult>
MaintenanceManager::scheduleNodesConfigUpdates() {
  std::unique_ptr<membership::StorageMembership::Update>
      storage_membership_update;
  std::unique_ptr<configuration::nodes::StorageAttributeConfig::Update>
      storage_attributes_update;

  for (const auto& it : active_shard_workflows_) {
    if (it.second.second != MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES) {
      continue;
    }

    auto shard = it.first;
    ShardWorkflow* wf = it.second.first.get();

    // Membership update
    membership::ShardState::Update shard_state_update;
    shard_state_update.transition = getExpectedStorageStateTransition(shard);
    // TODO: Verify conditions are valid and met for each
    // requested transition
    shard_state_update.conditions =
        getCondition(shard, shard_state_update.transition);
    if (!storage_membership_update) {
      storage_membership_update =
          std::make_unique<membership::StorageMembership::Update>(
              nodes_config_->getVersion());
    }
    auto rv = storage_membership_update->addShard(shard, shard_state_update);
    ld_check(rv == 0);

    // Attribute update
    // From maintenance manager perspective, today we only care about the
    // exclude_from_nodesets storage config attribute. Check if current
    // value is different from what the workflow wants and update if required
    auto sa = nodes_config_->getNodeStorageAttribute(shard.node());
    if (wf->excludeFromNodeset() != sa->exclude_from_nodesets) {
      if (!storage_attributes_update) {
        storage_attributes_update = std::make_unique<
            configuration::nodes::StorageAttributeConfig::Update>();
      }
      configuration::nodes::StorageAttributeConfig::NodeUpdate node_update;
      node_update.transition =
          configuration::nodes::StorageAttributeConfig::UpdateType::RESET;
      node_update.attributes =
          std::make_unique<configuration::nodes::StorageNodeAttribute>(
              configuration::nodes::StorageNodeAttribute{
                  sa->capacity,
                  sa->num_shards,
                  sa->generation,
                  wf->excludeFromNodeset()});
      storage_attributes_update->addNode(shard.node(), std::move(node_update));
    }
  }

  std::unique_ptr<configuration::nodes::StorageConfig::Update>
      storage_config_update;
  if (storage_membership_update || storage_attributes_update) {
    storage_config_update =
        std::make_unique<configuration::nodes::StorageConfig::Update>();
    storage_config_update->membership_update =
        std::move(storage_membership_update);
    storage_config_update->attributes_update =
        std::move(storage_attributes_update);
  }

  std::unique_ptr<membership::SequencerMembership::Update>
      sequencer_membership_update;

  for (const auto& it : active_sequencer_workflows_) {
    if (it.second.second != MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES) {
      continue;
    }
    auto node = it.first;
    SequencerWorkflow* wf = it.second.first.get();
    membership::SequencerNodeState::Update seq_state_update;
    seq_state_update.transition =
        membership::SequencerMembershipTransition::SET_ENABLED_FLAG;
    seq_state_update.sequencer_enabled =
        (wf->getTargetOpState() == SequencingState::ENABLED);
    if (!sequencer_membership_update) {
      sequencer_membership_update =
          std::make_unique<membership::SequencerMembership::Update>(
              nodes_config_->getVersion());
    }
    auto rv = sequencer_membership_update->addNode(node, seq_state_update);
    ld_check(rv == 0);
  }

  std::unique_ptr<configuration::nodes::SequencerConfig::Update>
      sequencer_config_update;
  if (sequencer_membership_update) {
    sequencer_config_update =
        std::make_unique<configuration::nodes::SequencerConfig::Update>();
    sequencer_config_update->membership_update =
        std::move(sequencer_membership_update);
  }

  status_ = MMStatus::AWAITING_NODES_CONFIG_UPDATE;
  ld_debug("Updated MaintenanceManager status to AWAITING_NODES_CONFIG_UPDATE");
  return (!storage_config_update && !sequencer_config_update)
      ? folly::makeSemiFuture<NCUpdateResult>(
            folly::makeUnexpected(Status::EMPTY))
      : deps_->postNodesConfigurationUpdate(std::move(storage_config_update),
                                            std::move(sequencer_config_update));
}

membership::StateTransitionCondition MaintenanceManager::getCondition(
    ShardID shard,
    membership::StorageStateTransition transition) {
  membership::StateTransitionCondition c;
  c = membership::required_conditions(transition);
  auto result = nodes_config_->getStorageMembership()->getShardState(shard);
  ld_check(result.first);
  if (result.second.metadata_state ==
      membership::MetaDataStorageState::METADATA) {
    c |= membership::Condition::METADATA_CAPACITY_CHECK;
  }
  return c;
}

folly::SemiFuture<SafetyCheckResult> MaintenanceManager::scheduleSafetyCheck() {
  std::vector<const ShardWorkflow*> shard_wf;
  for (const auto& it : active_shard_workflows_) {
    if (it.second.second == MaintenanceStatus::AWAITING_SAFETY_CHECK) {
      shard_wf.push_back(it.second.first.get());
    }
  }
  std::vector<const SequencerWorkflow*> seq_wf;
  for (const auto& it : active_sequencer_workflows_) {
    if (it.second.second == MaintenanceStatus::AWAITING_SAFETY_CHECK) {
      seq_wf.push_back(it.second.first.get());
    }
  }
  status_ = MMStatus::AWAITING_SAFETY_CHECK_RESULTS;
  ld_debug(
      "Updated MaintenanceManager status to AWAITING_SAFETY_CHECK_RESULTS");
  return (shard_wf.empty() && seq_wf.empty())
      ? folly::makeSemiFuture<SafetyCheckResult>(
            folly::makeUnexpected(E::EMPTY))
      : deps_->postSafetyCheckRequest(
            *cluster_maintenance_wrapper_,
            event_log_rebuilding_set_->toShardStatusMap(*nodes_config_),
            nodes_config_,
            shard_wf,
            seq_wf);
}

folly::SemiFuture<MaintenanceManager::MMStatus>
MaintenanceManager::getStatus() {
  auto pf = folly::makePromiseContract<MaintenanceManager::MMStatus>();
  add([this, mpromise = std::move(pf.first)]() mutable {
    mpromise.setValue(getStatusInternal());
  });
  return std::move(pf.second);
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
  ld_check(event_log_rebuilding_set_);
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
                              getShardDataHealthInternal(shard_id).value(),
                              getCurrentRebuildingMode(shard_id)));
  }
  return std::make_pair(std::move(shards), std::move(futures));
}

void MaintenanceManager::createWorkflows() {
  ld_check(cluster_maintenance_wrapper_);
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
      wf->addTargetOpState(targets);
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
    wf->setTargetOpState(target);
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

bool MaintenanceManager::isSequencingEnabled(node_index_t node) const {
  ld_check(nodes_config_);
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
  ld_check(event_log_rebuilding_set_);
  return event_log_rebuilding_set_->getRebuildingMode(
      shard.node(), shard.shard());
}

}}} // namespace facebook::logdevice::maintenance
