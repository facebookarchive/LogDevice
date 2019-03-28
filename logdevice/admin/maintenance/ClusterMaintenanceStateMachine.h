/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/gen-cpp2/MaintenanceDelta_types.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"

/**
 * ClusterMaintenanceState is the state maintained by this replicated state
 * machine. ClusterMaintenanceState keeps track of any defined maintenance on
 * sequencers and shards.
 *
 * The state machine is backed by maintenance delta log.
 */

namespace facebook { namespace logdevice {
extern template class ReplicatedStateMachine<thrift::ClusterMaintenanceState,
                                             maintenance::MaintenanceDelta>;
}} // namespace facebook::logdevice

namespace facebook { namespace logdevice { namespace maintenance {

class ClusterMaintenanceStateMachine
    : public facebook::logdevice::ReplicatedStateMachine<
          thrift::ClusterMaintenanceState,
          MaintenanceDelta> {
 public:
  using ClusterMaintenanceState = thrift::ClusterMaintenanceState;
  using Base =
      facebook::logdevice::ReplicatedStateMachine<ClusterMaintenanceState,
                                                  MaintenanceDelta>;

  explicit ClusterMaintenanceStateMachine(
      UpdateableSettings<AdminServerSettings> settings);

  /**
   * Start reading the maintenance log
   */
  void start();

  const thrift::ClusterMaintenanceState& getCurrentState() const {
    return getState();
  }

  static int serializeDelta(const MaintenanceDelta& delta,
                            void* buf,
                            size_t buf_size);

  void writeDelta(
      const MaintenanceDelta& delta,
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

  std::unique_ptr<MaintenanceDelta> deserializeDelta(Payload payload) override;

  int applyDelta(const MaintenanceDelta& delta,
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

  UpdateableSettings<AdminServerSettings> settings_;
}; // ClusterMaintenanceStateMachine

}}} // namespace facebook::logdevice::maintenance
