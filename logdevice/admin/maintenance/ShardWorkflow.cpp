/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/

#include "logdevice/admin/maintenance/ShardWorkflow.h"

#include <folly/MoveWrapper.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/common/membership/utils.h"

namespace facebook { namespace logdevice { namespace maintenance {

using apache::thrift::util::enumName;

folly::SemiFuture<MaintenanceStatus>
ShardWorkflow::run(const membership::ShardState& shard_state,
                   ShardDataHealth data_health,
                   RebuildingMode rebuilding_mode,
                   ClusterStateNodeState node_gossip_state) {
  ld_spew("%s",
          folly::format(
              "State before update:"
              "current_storage_state_:{},"
              "expected_storage_state_transition_:{},"
              "current_rebuilding_mode_:{},"
              "gossip_state:{},"
              "current_data_health_:{},"
              "status_:{},"
              "event_type:{}",
              membership::toString(current_storage_state_).str(),
              membership::toString(expected_storage_state_transition_).str(),
              toString(current_rebuilding_mode_),
              ClusterState::getNodeStateString(node_gossip_state),
              apache::thrift::util::enumNameSafe(current_data_health_),
              apache::thrift::util::enumNameSafe(status_),
              (event_) ? toString(event_->getType()) : "nullptr")
              .str()
              .c_str());

  current_storage_state_ = shard_state.storage_state;
  current_data_health_ = data_health;
  gossip_state_ = node_gossip_state;
  current_rebuilding_mode_ = rebuilding_mode;
  event_.reset();
  if (shard_state.manual_override) {
    updateStatus(MaintenanceStatus::BLOCKED_BY_ADMIN_OVERRIDE);
  } else {
    computeMaintenanceStatus();
  }

  ld_spew("%s",
          folly::format(
              "State after update:"
              "current_storage_state_:{},"
              "expected_storage_state_transition_:{},"
              "current_rebuilding_mode_:{},"
              "current_data_health_:{},"
              "status_:{},"
              "event_type:{}",
              membership::toString(current_storage_state_).str(),
              membership::toString(expected_storage_state_transition_).str(),
              toString(current_rebuilding_mode_),
              apache::thrift::util::enumNameSafe(current_data_health_),
              apache::thrift::util::enumNameSafe(status_),
              (event_) ? toString(event_->getType()) : "nullptr")
              .str()
              .c_str());

  if (event_ != nullptr) {
    // We have a event that needs to be written to the event log.
    // Write the event first and fulfil the promise in callback
    auto promise_future = folly::makePromiseContract<MaintenanceStatus>();
    auto mpromise = folly::makeMoveWrapper(promise_future.first);
    writeToEventLog(
        std::move(event_),
        [mpromise, status = status_](Status st,
                                     lsn_t /*unused*/,
                                     const std::string& /*unused*/) mutable {
          auto result = st == E::OK ? status : MaintenanceStatus::RETRY;
          mpromise->setValue(result);
        });
    return std::move(promise_future.second);
  } else {
    auto promise_future = folly::makePromiseContract<MaintenanceStatus>();
    promise_future.first.setValue(status_);
    return std::move(promise_future.second);
  }
}

void ShardWorkflow::writeToEventLog(
    std::unique_ptr<EventLogRecord> event,
    std::function<void(Status st, lsn_t lsn, const std::string& str)> cb)
    const {
  ld_check(event_log_writer_);
  event_log_writer_->writeToEventLog(std::move(event), cb);
}

void ShardWorkflow::computeMaintenanceStatus() {
  if (target_op_state_.count(ShardOperationalState::DRAINED)) {
    computeMaintenanceStatusForDrain();
  } else if (target_op_state_.count(ShardOperationalState::MAY_DISAPPEAR)) {
    computeMaintenanceStatusForMayDisappear();
  } else if (target_op_state_.count(ShardOperationalState::ENABLED)) {
    computeMaintenanceStatusForEnable();
  } else {
    ld_info("Unknown ShardOperationalState requested as target for this(%s)"
            "shard workflow",
            toString(shard_).c_str());
    ld_assert(false);
  }
  return;
}

ShardID ShardWorkflow::getShardID() const {
  return shard_;
}

void ShardWorkflow::computeMaintenanceStatusForDrain() {
  ld_check(target_op_state_.count(ShardOperationalState::DRAINED));

  switch (current_storage_state_) {
    case membership::StorageState::NONE:
      // If the node is provisioning, we can consider it drained as well. Any
      // maintenance that needs this node to be drained will appear completed
      // immediately.
    case membership::StorageState::PROVISIONING:
      // We have reached the target already, there is no further transitions
      // needed to declare the shard as DRAINED.
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    case membership::StorageState::NONE_TO_RO:
      // In this case, we seem to have been trying to enable the shard but
      // decided to drain it again, let's cancel the enable.
      expected_storage_state_transition_ =
          membership::StorageStateTransition::ABORT_ENABLING_READ;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_WRITE:
      // The shard is fully enabled, we need to ensure that can disable writes
      // safely so we will schedule safety check and request to go to RO.
      expected_storage_state_transition_ =
          membership::StorageStateTransition::DISABLING_WRITE;
      updateStatus(skip_safety_check_
                       ? MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES
                       : MaintenanceStatus::AWAITING_SAFETY_CHECK);
      break;
    case membership::StorageState::RW_TO_RO:
      // if NC is stuck in transitioning state for too long
      // because too many nodes are down, we should start rebuilding
      // without waiting for transition to DATA_MIGRATION
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);
      if (restore_mode_rebuilding_ && isNcTransitionStuck()) {
        createRebuildEventIfRequired(RebuildingMode::RESTORE, true /*force*/);
      }
      break;
    case membership::StorageState::READ_ONLY:
      // Trigger rebuilding if one wasn't already triggered
      createRebuildEventIfRequired(
          restore_mode_rebuilding_ ? RebuildingMode::RESTORE
                                   : RebuildingMode::RELOCATE,
          status_ !=
              MaintenanceStatus::AWAITING_START_DATA_MIGRATION /*force*/);
      // If a new event was created, lets wait for this to event to be written
      // to event log
      if (event_) {
        updateStatus(MaintenanceStatus::AWAITING_START_DATA_MIGRATION);
      } else {
        // If no new event was triggered, the shard is already part of
        // the rebuilding set. Now make the nodes config transition to
        // DATA_MIGRATION
        expected_storage_state_transition_ =
            membership::StorageStateTransition::START_DATA_MIGRATION;
        updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      }
      break;
    case membership::StorageState::DATA_MIGRATION:
      createRebuildEventIfRequired(restore_mode_rebuilding_
                                       ? RebuildingMode::RESTORE
                                       : RebuildingMode::RELOCATE);
      // No new rebuild event was created. Check if the existing
      // rebuilding is complete.
      if (!event_ && current_data_health_ == ShardDataHealth::EMPTY) {
        expected_storage_state_transition_ =
            membership::StorageStateTransition::DATA_MIGRATION_COMPLETED;
        updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      } else {
        updateStatus(MaintenanceStatus::AWAITING_DATA_REBUILDING);
      }
      break;
    default:
      // Current StorageState is one of the transitional states.
      // Workflow cannot proceed until StorageState moves out of
      // the transitional state. NCM ensures that shard do not
      // stay in transitional state for long.
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);
      break;
  }
}

void ShardWorkflow::computeMaintenanceStatusForMayDisappear() {
  // This method is called only when target_op_state_ contains only
  // MAY_DISAPPEAR
  ld_check(target_op_state_.count(ShardOperationalState::MAY_DISAPPEAR));
  ld_check(!target_op_state_.count(ShardOperationalState::DRAINED));
  switch (current_storage_state_) {
    case membership::StorageState::NONE:
      createAbortEventIfRequired();
      expected_storage_state_transition_ =
          membership::StorageStateTransition::ENABLING_READ;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_ONLY:
      createAbortEventIfRequired();
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    case membership::StorageState::READ_WRITE:
      createAbortEventIfRequired();
      expected_storage_state_transition_ =
          membership::StorageStateTransition::DISABLING_WRITE;
      updateStatus(skip_safety_check_
                       ? MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES
                       : MaintenanceStatus::AWAITING_SAFETY_CHECK);
      break;
    case membership::StorageState::DATA_MIGRATION:
      createAbortEventIfRequired();
      expected_storage_state_transition_ =
          membership::StorageStateTransition::CANCEL_DATA_MIGRATION;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    default:
      // Current StorageState is one of the transitional states.
      // Workflow cannot proceed until StorageState moves out of
      // the transitional state. NCM ensures that shard do not
      // stay in transitional state for long
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);
      break;
  }
}

void ShardWorkflow::computeMaintenanceStatusForEnable() {
  ld_check(target_op_state_.count(ShardOperationalState::ENABLED));
  // We require that the node is in FULLY_STARTED|STARTING state before we
  // proceed with the enable workflow. This ensures that we are not setting the
  // shards or sequencers to READ_WRITE before the nodes are actually up and
  // running.
  if (gossip_state_ != ClusterStateNodeState::FULLY_STARTED &&
      gossip_state_ != ClusterStateNodeState::STARTING) {
    updateStatus(MaintenanceStatus::AWAITING_NODE_TO_BE_ALIVE);
    return;
  }

  switch (current_storage_state_) {
    case membership::StorageState::PROVISIONING:
      updateStatus(MaintenanceStatus::AWAITING_NODE_PROVISIONING);
      break;
    case membership::StorageState::NONE:
      expected_storage_state_transition_ =
          membership::StorageStateTransition::ENABLING_READ;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_ONLY:
      createAbortEventIfRequired();
      expected_storage_state_transition_ =
          membership::StorageStateTransition::ENABLE_WRITE;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::DATA_MIGRATION:
      createAbortEventIfRequired();
      expected_storage_state_transition_ =
          membership::StorageStateTransition::CANCEL_DATA_MIGRATION;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_WRITE:
      createAbortEventIfRequired();
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    case membership::StorageState::RW_TO_RO:
      // We can just abort the disable that is happening instead of waiting for
      // it.
      expected_storage_state_transition_ =
          membership::StorageStateTransition::ABORT_DISABLING_WRITE;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    default:
      // Current StorageState is one of the transitional states.
      // Workflow cannot proceed until StorageState moves out of
      // the transitional state. NCM ensures that shard do not
      // stay in transitional state for long
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_TRANSITION);
      break;
  }
}

void ShardWorkflow::createAbortEventIfRequired() {
  /* ShardDataHealth  RebuildingMode  Shard Rebuilding Type
   * Healthy          Relocate        Full (ABORT)
   * Lost_Regions     Restore         Mini (DO NOT ABORT)
   * Healthy          Invalid         NA   (DO NOT ABORT)
   * Unavilable       Restore         Full (ABORT)
   * Underreplication Restore         Full (ABORT)
   * Empty            Restore         Full (ABORT)
   * Empty            Relocate        Full (ABORT)
   */
  if (current_rebuilding_mode_ == RebuildingMode::RELOCATE ||
      (current_data_health_ != ShardDataHealth::LOST_REGIONS &&
       current_data_health_ != ShardDataHealth::HEALTHY)) {
    event_ = std::make_unique<SHARD_ABORT_REBUILD_Event>(
        shard_.node(), (uint32_t)shard_.shard(), LSN_INVALID);
  }
}

void ShardWorkflow::createRebuildEventIfRequired(RebuildingMode new_mode,
                                                 bool force) {
  if (force || current_rebuilding_mode_ != new_mode) {
    SHARD_NEEDS_REBUILD_flags_t flag{0};
    if (new_mode == RebuildingMode::RELOCATE) {
      flag = SHARD_NEEDS_REBUILD_Header::DRAIN;
    }
    event_ = std::make_unique<SHARD_NEEDS_REBUILD_Event>(
        SHARD_NEEDS_REBUILD_Header{shard_.node(),
                                   (uint32_t)shard_.shard(),
                                   "ShardWorkflow",
                                   "ShardWorkflow",
                                   flag});
  }
}

void ShardWorkflow::updateStatus(MaintenanceStatus status) {
  if (status != status_) {
    status_ = status;
    last_updated_at_ = SystemTimestamp::now();
  }
}

void ShardWorkflow::addTargetOpState(
    folly::F14FastSet<ShardOperationalState> state) {
  // TODO: Enable after implementing toString
  /*ld_check_in(state,
      ({ShardOperationalState::MAY_DISAPPEAR,
       ShardOperationalState::ENABLED,
       ShardOperationalState::DRAINED}));*/
  target_op_state_.insert(state.begin(), state.end());
}

void ShardWorkflow::isPassiveDrainAllowed(bool allow) {
  allow_passive_drain_ = allow;
}

void ShardWorkflow::shouldSkipSafetyCheck(bool skip) {
  skip_safety_check_ = skip;
}

void ShardWorkflow::rebuildInRestoreMode(bool is_restore) {
  restore_mode_rebuilding_ = is_restore;
}

folly::F14FastSet<ShardOperationalState>
ShardWorkflow::getTargetOpStates() const {
  return target_op_state_;
}

SystemTimestamp ShardWorkflow::getLastUpdatedTimestamp() const {
  return last_updated_at_;
}

SystemTimestamp ShardWorkflow::getCreationTimestamp() const {
  return created_at_;
}

membership::StorageStateTransition
ShardWorkflow::getExpectedStorageStateTransition() const {
  return expected_storage_state_transition_;
}

bool ShardWorkflow::allowPassiveDrain() const {
  return allow_passive_drain_;
}

bool ShardWorkflow::isNcTransitionStuck() const {
  return SystemTimestamp::now() >
      (last_updated_at_ +
       (2 *
        Worker::onThisThread()
            ->settings()
            .nodes_configuration_manager_intermediary_shard_state_timeout));
}

}}} // namespace facebook::logdevice::maintenance
