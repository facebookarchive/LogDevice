/**
 *  Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 *  All rights reserved.
 *
 *  This source code is licensed under the BSD-style license found in the
 *  LICENSE file in the root directory of this source tree.
 **/
#pragma once

#include <folly/futures/Future.h>

#include "logdevice/admin/maintenance/EventLogWriter.h"
#include "logdevice/admin/maintenance/Workflow.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/membership/StorageState.h"

namespace facebook { namespace logdevice { namespace maintenance {
/**
 * A ShardMaintenanceworkflow is a state machine that tracks state
 * transitions of a shard.
 */
class ShardMaintenanceWorkflow : public Workflow {
 public:
  ShardMaintenanceWorkflow(ShardID shard,
                           const EventLogWriter* event_log_writer)
      : shard_(shard), event_log_writer_(event_log_writer) {}

  folly::SemiFuture<MaintenanceStatus>
  run(membership::StorageState storage_state,
      ShardDataHealth data_health,
      RebuildingMode rebuilding_mode);

  // Adds state to target_op_state_
  void addTargetOpState(ShardOperationalState state);

  // Called if this workflow should only execute active
  // drain. If safety check fails, we will not run passive
  // drain for this shard and the maintenance will be blocked
  void allowActiveDrainsOnly(bool allow);

  // Sets skip_safety_check_ to value of `skip`
  void shouldSkipSafetyCheck(bool skip);

  // Sets the RebuildingMode for the maintenance
  void rebuildInRestoreMode(bool is_restore);

  // Returns the target_op_state_
  std::set<ShardOperationalState> getTargetOpState() const;

  // Returns the EventLogRecord that needs to be written to
  // EventLog, if there is one. nullptr otherwise
  std::unique_ptr<EventLogRecord> getEventLogRecord();

  // Returns the StorageState that this workflow expects for this
  // shard in NodesConfiguration. This will be used
  // by the MaintenanceManager in NodesConfig update request
  membership::StorageState getExpectedStorageState() const;

 private:
  std::set<ShardOperationalState> target_op_state_;
  // The shard this workflow is for
  ShardID shard_;
  // Any even that needs to be written by this workflow
  // is written through this object
  const EventLogWriter* event_log_writer_;
  // Value of membership::StorageState to be updated
  // in the NodesConfiguration. Workflow will set this value
  // and MaintenanceManager will make use of it to request
  // the update in NodesConfiguration.
  membership::StorageState expected_storage_state_;
  // If safety checker determines that a drain is required, allow
  // active drains only
  bool active_only_{false};
  // If true, skip safety check for this workflow
  bool skip_safety_check_{false};
  // True if RebuildingMode requested by the maintenance is RESTORE.
  // Mainly set by internal maintenance request when a shard is down
  bool restore_mode_rebuilding_{false};
  // The EventLogRecord to write as determined by workflow.
  // nullptr if there isn't one to write
  std::unique_ptr<EventLogRecord> event_;
  // The last StorageState as informed by MM for this shard
  // Gets updated every time `run` is called
  membership::StorageState current_storage_state_;
  // The last ShardDataHealth as informed by the MM for this
  // shard.
  // Gets updated every time `run` is called
  ShardDataHealth current_data_health_;
  // The last RebuildingMode as informed by the MM for this
  // shard.
  // Gets updated every time `run` is called
  RebuildingMode current_rebuilding_mode_;
  // Determines the next MaintenanceStatus based on
  // current storage state, shard data health and rebuilding mode
  MaintenanceStatus computeMaintenanceStatus();
  // Method that is called when there is an event that needs to
  // be written to event log by this workflow
  virtual void writeToEventLog(
      std::unique_ptr<EventLogRecord> event,
      std::function<Status st, lsn_t lsn, const std::string & str> cb);
};

}}} // namespace facebook::logdevice::maintenance
