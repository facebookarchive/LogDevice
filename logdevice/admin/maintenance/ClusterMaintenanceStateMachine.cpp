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
#include "logdevice/common/ThriftCodec.h"
#include "logdevice/common/configuration/InternalLogs.h"

namespace facebook { namespace logdevice {
template class ReplicatedStateMachine<thrift::ClusterMaintenanceState,
                                      maintenance::MaintenanceDelta>;
}} // namespace facebook::logdevice

namespace facebook { namespace logdevice { namespace maintenance {

using facebook::logdevice::thrift::ClusterMaintenanceState;

ClusterMaintenanceStateMachine::ClusterMaintenanceStateMachine(
    UpdateableSettings<AdminServerSettings> settings)
    : Base(RSMType::MAINTENANCE_LOG_STATE_MACHINE,
           configuration::InternalLogs::MAINTENANCE_LOG_DELTAS,
           settings->maintenance_log_snapshotting
               ? configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS
               : LOGID_INVALID),
      settings_(std::move(settings)) {
  setSnapshottingGracePeriod(settings_->maintenance_log_snapshotting_period);
}

void ClusterMaintenanceStateMachine::start() {
  Base::start();
}

std::unique_ptr<ClusterMaintenanceState>
ClusterMaintenanceStateMachine::makeDefaultState(lsn_t version) const {
  auto state = std::make_unique<ClusterMaintenanceState>();
  state->set_version(version);
  return state;
}

std::unique_ptr<ClusterMaintenanceState>
ClusterMaintenanceStateMachine::deserializeState(
    Payload payload,
    lsn_t version,
    std::chrono::milliseconds timestamp) const {
  // TODO: Implmentation
  return nullptr;
}

void ClusterMaintenanceStateMachine::gotInitialState(
    const ClusterMaintenanceState& state) const {
  ld_info("Got initial ClusterMaintenanceState");
}

std::unique_ptr<MaintenanceDelta>
ClusterMaintenanceStateMachine::deserializeDelta(Payload payload) {
  // TODO:: Implementation
  return nullptr;
}

int ClusterMaintenanceStateMachine::applyDelta(
    const MaintenanceDelta& delta,
    ClusterMaintenanceState& state,
    lsn_t version,
    std::chrono::milliseconds timestamp,
    std::string& failure_reason) {
  // TODO: Implementation
  return 0;
}

void ClusterMaintenanceStateMachine::writeMaintenanceDelta(
    std::unique_ptr<MaintenanceDelta> delta,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  std::string serializedDelta =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(*delta);
  postWriteDeltaRequest(
      std::move(serializedDelta), std::move(cb), mode, std::move(base_version));
}

void ClusterMaintenanceStateMachine::postWriteDeltaRequest(
    std::string delta,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  std::unique_ptr<Request> req =
      std::make_unique<MaintenanceLogWriteDeltaRequest>(
          maintenance::ClusterMaintenanceStateMachine::workerType(
              Worker::onThisThread()->processor_),
          std::move(delta),
          std::move(cb),
          std::move(mode),
          std::move(base_version));
  postRequestWithRetrying(req);
}

int ClusterMaintenanceStateMachine::serializeState(
    const ClusterMaintenanceState& state,
    void* buf,
    size_t buf_size) {
  // TODO: Implemetation
  return 0;
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

Request::Execution StartClusterMaintenanceStateMachineRequest::execute() {
  Worker* w = Worker::onThisThread();
  ld_check(sm_);
  w->setClusterMaintenanceStateMachine(sm_);
  sm_->start();
  return Request::Execution::COMPLETE;
}

Request::Execution MaintenanceLogWriteDeltaRequest::execute() {
  auto sm = Worker::onThisThread()->cluster_maintenance_state_machine_;
  if (sm) {
    sm->writeDelta(
        std::move(delta_), std::move(cb_), mode_, std::move(base_version_));
  } else {
    ld_warning("No ClusterMaintenanceStateMachine available on this worker. "
               "Returning E::NOTSUPPORTED.");
    cb_(E::NOTSUPPORTED, LSN_INVALID, "");
  }
  return Execution::COMPLETE;
}

}}} // namespace facebook::logdevice::maintenance
