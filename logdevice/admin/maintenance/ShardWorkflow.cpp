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

namespace facebook { namespace logdevice { namespace maintenance {

using apache::thrift::util::enumName;

folly::SemiFuture<MaintenanceStatus>
ShardWorkflow::run(membership::StorageState storage_state,
                   ShardDataHealth data_health,
                   RebuildingMode rebuilding_mode) {
  // TODO: Enable once we have toString implemented
  /*ld_spew("old current_storage_state_:%s,"
       "expected_storage_state_:%s,"
       "current_rebuilding_mode_:%s,"
       "current_data_health_:%s,"
       "status_:%s,"
       "event_type:%s",
       toString(current_storage_state_).c_str(),
       toString(expected_storage_state_).c_str(),
       toString(current_rebuilding_mode_).c_str(),
       enumName(current_data_health_).c_str(),
       enumName(status_).c_str(),
       (event_)?toString(event_->getType()):"nullptr");*/

  current_storage_state_ = storage_state;
  current_data_health_ = data_health;
  current_rebuilding_mode_ = rebuilding_mode;
  event_.reset();
  computeMaintenanceStatus();
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

  /*ld_spew("new current_storage_state_:%s,"
       "expected_storage_state_:%s,"
       "current_rebuilding_mode_:%s,"
       "current_data_health_:%s,"
       "status_:%s,"
       "event_type:%s",
       toString(current_storage_state_).c_str(),
       toString(expected_storage_state_).c_str(),
       toString(current_rebuilding_mode_).c_str(),
       enumName(current_data_health_).c_str(),
       enumName(status_).c_str(),
       (event_)?toString(event_->getType()):"nullptr");*/
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
    exclude_from_nodesets_ = true;
    computeMaintenanceStatusForDrain();
  } else if (target_op_state_.count(ShardOperationalState::MAY_DISAPPEAR)) {
    exclude_from_nodesets_ = false;
    computeMaintenanceStatusForMayDisappear();
  } else if (target_op_state_.count(ShardOperationalState::ENABLED)) {
    exclude_from_nodesets_ = false;
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
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    case membership::StorageState::READ_WRITE:
      expected_storage_state_ = membership::StorageState::READ_ONLY;
      updateStatus(skip_safety_check_
                       ? MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES
                       : MaintenanceStatus::AWAITING_SAFETY_CHECK);
      break;
    case membership::StorageState::READ_ONLY:
      expected_storage_state_ = membership::StorageState::DATA_MIGRATION;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::DATA_MIGRATION:
      createRebuildEventIfRequired(restore_mode_rebuilding_
                                       ? RebuildingMode::RESTORE
                                       : RebuildingMode::RELOCATE);
      // No new rebuild event was created. Check if the existing
      // rebuilding is complete.
      if (!event_ && current_data_health_ == ShardDataHealth::EMPTY) {
        expected_storage_state_ = membership::StorageState::NONE;
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
      updateStatus(MaintenanceStatus::RETRY);
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
    case membership::StorageState::READ_ONLY:
      createAbortEventIfRequired();
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    case membership::StorageState::READ_WRITE:
      createAbortEventIfRequired();
      expected_storage_state_ = membership::StorageState::READ_ONLY;
      updateStatus(skip_safety_check_
                       ? MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES
                       : MaintenanceStatus::AWAITING_SAFETY_CHECK);
      break;
    case membership::StorageState::DATA_MIGRATION:
      createAbortEventIfRequired();
      expected_storage_state_ = membership::StorageState::READ_ONLY;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    default:
      // Current StorageState is one of the transitional states.
      // Workflow cannot proceed until StorageState moves out of
      // the transitional state. NCM ensures that shard do not
      // stay in transitional state for long
      updateStatus(MaintenanceStatus::RETRY);
      break;
  }
}

void ShardWorkflow::computeMaintenanceStatusForEnable() {
  ld_check(target_op_state_.count(ShardOperationalState::ENABLED));

  switch (current_storage_state_) {
    case membership::StorageState::NONE:
      expected_storage_state_ = membership::StorageState::READ_ONLY;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_ONLY:
      createAbortEventIfRequired();
      expected_storage_state_ = membership::StorageState::READ_WRITE;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::DATA_MIGRATION:
      createAbortEventIfRequired();
      expected_storage_state_ = membership::StorageState::READ_ONLY;
      updateStatus(MaintenanceStatus::AWAITING_NODES_CONFIG_CHANGES);
      break;
    case membership::StorageState::READ_WRITE:
      createAbortEventIfRequired();
      updateStatus(MaintenanceStatus::COMPLETED);
      break;
    default:
      // Current StorageState is one of the transitional states.
      // Workflow cannot proceed until StorageState moves out of
      // the transitional state. NCM ensures that shard do not
      // stay in transitional state for long
      updateStatus(MaintenanceStatus::RETRY);
      break;
  }
}

void ShardWorkflow::createAbortEventIfRequired() {
  /* ShardDataHealth  RebuildingMode  Shard Rebuilding Type
   * Healthy          Relocate        Full (ABORT)
   * Healthy          Restore         Mini (DO NOT ABORT)
   * Healthy          Invalid         NA   (DO NOT ABORT)
   * Unavilable       Restore         Full (ABORT)
   * Underreplication Restore         Full (ABORT)
   * Empty            Restore         Full (ABORT)
   * Empty            Relocate        Full (ABORT)
   */
  if (current_data_health_ != ShardDataHealth::HEALTHY ||
      current_rebuilding_mode_ == RebuildingMode::RELOCATE) {
    event_ = std::make_unique<SHARD_ABORT_REBUILD_Event>(
        shard_.node(), (uint32_t)shard_.shard(), LSN_INVALID);
  }
}

void ShardWorkflow::createRebuildEventIfRequired(RebuildingMode new_mode) {
  if (current_rebuilding_mode_ != new_mode) {
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
    std::unordered_set<ShardOperationalState> state) {
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

std::unordered_set<ShardOperationalState>
ShardWorkflow::getTargetOpStates() const {
  return target_op_state_;
}

membership::StorageState ShardWorkflow::getExpectedStorageState() const {
  return expected_storage_state_;
}

bool ShardWorkflow::excludeFromNodeset() const {
  return exclude_from_nodesets_;
}

}}} // namespace facebook::logdevice::maintenance
