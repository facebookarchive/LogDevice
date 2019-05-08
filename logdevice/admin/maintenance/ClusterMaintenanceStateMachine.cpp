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

}}} // namespace facebook::logdevice::maintenance
