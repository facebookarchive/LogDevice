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

  void writeMaintenanceDelta(
      std::unique_ptr<MaintenanceDelta> delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  void postWriteDeltaRequest(
      std::string delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  void snapshot(std::function<void(Status st)> cb);

  /**
   * Indicates whether this state machine has finished replaying or not.
   */
  bool isFullyLoaded() const {
    return is_fully_loaded_;
  }

  /**
   * Returns the WorkerType that this state machine should be running on
   */
  static WorkerType workerType(Processor* processor) {
    // This returns either WorkerType::BACKGROUND or WorkerType::GENERAL based
    // on whether we have background workers.
    if (processor->getWorkerCount(WorkerType::BACKGROUND) > 0) {
      return WorkerType::BACKGROUND;
    }
    return WorkerType::GENERAL;
  }

  static int getWorkerIndex(int nthreads) {
    return configuration::InternalLogs::MAINTENANCE_LOG_DELTAS.val_ % nthreads;
  }

 private:
  // Will be set to true when we finish replaying the state machine.
  bool is_fully_loaded_{false};
  std::unique_ptr<SubscriptionHandle> update_handle_;

  /**
   * The callback that gets called on every time we publish a new state
   */
  void onUpdate(const ClusterMaintenanceState& state,
                const MaintenanceDelta* /*unused*/,
                lsn_t version);

  std::unique_ptr<ClusterMaintenanceState>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<ClusterMaintenanceState>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

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

  friend class ClusterMaintenanceStateMachineTest;
}; // ClusterMaintenanceStateMachine

class StartClusterMaintenanceStateMachineRequest : public Request {
 public:
  explicit StartClusterMaintenanceStateMachineRequest(
      ClusterMaintenanceStateMachine* sm,
      WorkerType worker_type)
      : Request(RequestType::START_CLUSTER_MAINTENANCE_STATE_MACHINE),
        sm_(sm),
        worker_type_(worker_type) {}

  Execution execute() override;
  int getThreadAffinity(int nthreads) override {
    return ClusterMaintenanceStateMachine::getWorkerIndex(nthreads);
  }
  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  ClusterMaintenanceStateMachine* sm_;
  WorkerType worker_type_;
};

class MaintenanceLogWriteDeltaRequest : public Request {
 public:
  explicit MaintenanceLogWriteDeltaRequest(
      WorkerType worker_type,
      std::string delta,
      std::function<
          void(Status st, lsn_t version, const std::string& failure_reason)> cb,
      ClusterMaintenanceStateMachine::WriteMode mode =
          ClusterMaintenanceStateMachine::WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none)
      : Request(RequestType::MAINTENANCE_LOG_REQUEST),
        worker_type_(worker_type),
        delta_(std::move(delta)),
        cb_(std::move(cb)),
        mode_(std::move(mode)),
        base_version_(std::move(base_version)) {}

  ~MaintenanceLogWriteDeltaRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int nthreads) override {
    return ClusterMaintenanceStateMachine::getWorkerIndex(nthreads);
  }
  WorkerType getWorkerTypeAffinity() override {
    return worker_type_;
  }

 private:
  WorkerType worker_type_;
  std::string delta_;
  std::function<
      void(Status st, lsn_t version, const std::string& failure_reason)>
      cb_;
  ClusterMaintenanceStateMachine::WriteMode mode_;
  folly::Optional<lsn_t> base_version_;
};

}}} // namespace facebook::logdevice::maintenance
