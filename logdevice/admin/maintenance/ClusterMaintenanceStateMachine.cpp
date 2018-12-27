/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"

#include <folly/dynamic.h>

#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/configuration/InternalLogs.h"

namespace facebook { namespace logdevice {

template class ReplicatedStateMachine<ClusterMaintenanceState,
                                      MaintenanceLogDeltaRecord>;

ClusterMaintenanceStateMachine::ClusterMaintenanceStateMachine(
    UpdateableSettings<Settings> settings)
    : Base(RSMType::MAINTENANCE_LOG_STATE_MACHINE,
           configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
           settings->maintenance_log_snapshotting
               ? configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS
               : LOGID_INVALID),
      settings_(settings) {
  setSnapshottingGracePeriod(settings_->maintenance_log_snapshotting_period);
}

void ClusterMaintenanceStateMachine::start() {
  Base::start();
}

std::unique_ptr<ClusterMaintenanceState>
ClusterMaintenanceStateMachine::makeDefaultState(lsn_t version) const {
  return std::make_unique<ClusterMaintenanceState>(version);
}

std::unique_ptr<ClusterMaintenanceState>
ClusterMaintenanceStateMachine::deserializeState(
    Payload payload,
    lsn_t version,
    std::chrono::milliseconds timestamp) const {
  // TODO: implmentation
  return nullptr;
}

void ClusterMaintenanceStateMachine::gotInitialState(
    const ClusterMaintenanceState& state) const {
  ld_info("Got initial ClusterMaintenanceState");
}

std::unique_ptr<MaintenanceLogDeltaRecord>
ClusterMaintenanceStateMachine::deserializeDelta(Payload payload) {
  // TODO:: implementation
  return nullptr;
}

int ClusterMaintenanceStateMachine::applyDelta(
    const MaintenanceLogDeltaRecord& delta,
    ClusterMaintenanceState& state,
    lsn_t version,
    std::chrono::milliseconds timestamp,
    std::string& failure_reason) {
  return state.update(delta, version, timestamp);
}

int ClusterMaintenanceStateMachine::serializeState(
    const ClusterMaintenanceState& state,
    void* buf,
    size_t buf_size) {
  // TODO: implemetation
  return 0;
}

int ClusterMaintenanceState::update(const MaintenanceLogDeltaRecord& delta,
                                    lsn_t version,
                                    std::chrono::milliseconds timestamp) {
  last_seen_lsn_ = version;
  auto type = delta.getType();
  int rv = 0;

  switch (type) {
    case MaintenanceRecordType::APPLY_MAINTENANCE: {
      const auto* record =
          dynamic_cast<const APPLY_MAINTENANCE_Record*>(&delta);
      for (auto& kv : record->shard_maintenance_map_) {
        auto& pendingMaintenances = storage_maintenance[kv.first];
        ShardMaintenance sm(kv.second);
        sm.user_id = record->header_.user_id_;
        sm.reason = record->header_.reason_;
        sm.created_on = RecordTimestamp(timestamp);
        sm.maintenance_id = record->header_.id_;
        sm.target_operational_state = kv.second;
        pendingMaintenances.insert(std::move(sm));
      }
      for (auto& kv : record->sequencer_maintenance_map_) {
        auto& pendingMaintenances = sequencer_maintenance[kv.first];
        SequencerMaintenance sm;
        sm.user_id = record->header_.user_id_;
        sm.reason = record->header_.reason_;
        sm.created_on = RecordTimestamp(timestamp);
        sm.maintenance_id = record->header_.id_;
        sm.target_sequencing_state = kv.second;
        pendingMaintenances.insert(std::move(sm));
      }
      break;
    }
    case MaintenanceRecordType::REMOVE_MAINTENANCE: {
      const auto* record =
          dynamic_cast<const REMOVE_MAINTENANCE_Record*>(&delta);
      // Remove shard maintenance
      for (auto shard : record->shard_maintenances_) {
        if (!storage_maintenance.count(shard)) {
          ld_warning("Trying to remove maintenance for a shard(%s) that does "
                     "not have pending maintenance",
                     toString(shard).c_str());
          continue;
        }
        auto& pendingShardMaintenanceSet = storage_maintenance[shard];
        auto it = std::find_if(
            std::begin(pendingShardMaintenanceSet),
            std::end(pendingShardMaintenanceSet),
            [user_id = record->header_.user_id_](const ShardMaintenance& sm) {
              return user_id == sm.user_id;
            });
        if (it != pendingShardMaintenanceSet.end()) {
          pendingShardMaintenanceSet.erase(it);
        }
      }
      // Remove sequencer maintenance
      for (auto node : record->sequencer_maintenances_) {
        if (!sequencer_maintenance.count(node)) {
          ld_warning("Trying to remove maintenance for a sequencer node(N%d) "
                     "that does not have pending maintenance",
                     node);
          continue;
        }
        auto& pendingSequencerMaintenanceSet = sequencer_maintenance[node];
        auto it = std::find_if(std::begin(pendingSequencerMaintenanceSet),
                               std::end(pendingSequencerMaintenanceSet),
                               [user_id = record->header_.user_id_](
                                   const SequencerMaintenance& sm) {
                                 return user_id == sm.user_id;
                               });
        if (it != pendingSequencerMaintenanceSet.end()) {
          pendingSequencerMaintenanceSet.erase(it);
        }
      }
      break;
    }
    default:
      ld_critical("Rejecting update of Unknown record type. version:%s",
                  lsn_to_string(version).c_str());
      rv = -1;
      break;
  }

  if (rv == 0) {
    last_applied_lsn_ = version;
  }

  return rv;
}

bool ClusterMaintenanceStateMachine::shouldCreateSnapshot() const {
  return false;
}

bool ClusterMaintenanceStateMachine::canSnapshot() const {
  return false;
}

bool ClusterMaintenanceStateMachine::shouldTrim() const {
  return false;
}

void ClusterMaintenanceStateMachine::onSnapshotCreated(Status st,
                                                       size_t snapshotSize) {
  return;
}

void ClusterMaintenanceStateMachine::snapshot(
    std::function<void(Status st)> cb) {
  return;
}

}} // namespace facebook::logdevice
