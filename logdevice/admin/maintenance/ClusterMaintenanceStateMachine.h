/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/ClusterMaintenanceLogRecord.h"
#include "logdevice/admin/maintenance/MaintenanceTransitionStatus.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"

/**
 * ClusterMaintenanceState is the state maintained by

 * ClusterMaintenanceStateMachine. ClusterMaintenanceState keeps track
 * of any pending Sequencer/Shard maintenances
 *
 * The state machine is backed by maintenance delta log.
 */

namespace facebook { namespace logdevice {

class ClusterMaintenanceState {
 public:
  explicit ClusterMaintenanceState(lsn_t version)
      : last_seen_lsn_(version), last_applied_lsn_(version) {}

  // Metadata we maintain for every shard/sequencer
  // maintenance.
  struct MaintenanceMetadata {
    std::string user_id;
    std::string reason;
    RecordTimestamp created_on;
    membership::MaintenanceID::Type maintenance_id;
  };

  struct SequencerMaintenance : MaintenanceMetadata {
    SequencingState target_sequencing_state;
    bool operator<(const SequencerMaintenance& rhs) const {
      if (target_sequencing_state == rhs.target_sequencing_state) {
        return maintenance_id < rhs.maintenance_id;
      }
      return target_sequencing_state > rhs.target_sequencing_state;
    }
  };

  struct ShardMaintenance : MaintenanceMetadata {
   public:
    explicit ShardMaintenance(ShardOperationalState target)
        : target_operational_state(target) {}
    ShardOperationalState target_operational_state;
    bool operator<(const ShardMaintenance& rhs) const {
      if (target_operational_state == rhs.target_operational_state) {
        return maintenance_id < rhs.maintenance_id;
      }
      return target_operational_state > rhs.target_operational_state;
    }
  };

  using PendingSequencerMaintenanceSet = std::set<SequencerMaintenance>;
  using PendingShardMaintenanceSet = std::set<ShardMaintenance>;

  int update(const MaintenanceLogDeltaRecord& delta,
             lsn_t version,
             std::chrono::milliseconds timestamp);

  std::unordered_map<ShardID, PendingShardMaintenanceSet, ShardID::Hash>
      storage_maintenance;
  std::unordered_map<node_index_t, PendingSequencerMaintenanceSet>
      sequencer_maintenance;

 private:
  // Useful for debugging purposes
  // The lsn of the delta log record that was last delivered from log
  lsn_t last_seen_lsn_;
  // The lsn of the delta log record that resulted in state being updated
  // successfully
  lsn_t last_applied_lsn_;
};

extern template class ReplicatedStateMachine<ClusterMaintenanceState,
                                             MaintenanceLogDeltaRecord>;

class ClusterMaintenanceStateMachine
    : public ReplicatedStateMachine<ClusterMaintenanceState,
                                    MaintenanceLogDeltaRecord> {
 public:
  using Base = ReplicatedStateMachine<ClusterMaintenanceState,
                                      MaintenanceLogDeltaRecord>;

  explicit ClusterMaintenanceStateMachine(
      UpdateableSettings<Settings> settings);

  /**
   * Start reading the maintenance log
   */
  void start();

  const ClusterMaintenanceState& getCurrentState() const {
    return getState();
  }

  static int serializeDelta(const MaintenanceLogDeltaRecord& delta,
                            void* buf,
                            size_t buf_size);

  void writeDelta(
      const MaintenanceLogDeltaRecord& delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  void snapshot(std::function<void(Status st)> cb);

 private:
  std::unique_ptr<ClusterMaintenanceState>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<ClusterMaintenanceState>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

  void gotInitialState(const ClusterMaintenanceState& state) const override;

  std::unique_ptr<MaintenanceLogDeltaRecord>
  deserializeDelta(Payload payload) override;

  int applyDelta(const MaintenanceLogDeltaRecord& delta,
                 ClusterMaintenanceState& state,
                 lsn_t version,
                 std::chrono::milliseconds timestamp,
                 std::string& failure_reason) override;

  int serializeState(const ClusterMaintenanceState& state,
                     void* buf,
                     size_t buf_size) override;

  // Returns true if now is the time to create a new snapshot.
  bool shouldCreateSnapshot() const;
  bool canSnapshot() const override;
  // Returns true if we can trim the RSM.
  bool shouldTrim() const;

  // Snapshot creation completion callback. On success, also issue a request to
  // trim the RSM if possible.
  void onSnapshotCreated(Status st, size_t snapshotSize) override;

  UpdateableSettings<Settings> settings_;
}; // ClusterMaintenanceStateMachine

}} // namespace facebook::logdevice
