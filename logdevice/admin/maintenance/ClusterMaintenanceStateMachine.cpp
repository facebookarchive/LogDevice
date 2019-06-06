/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"

#include <folly/dynamic.h>

#include "logdevice/admin/maintenance/MaintenanceDeltaTypes.h"
#include "logdevice/admin/maintenance/types.h"
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
  auto update_cb = [&](const ClusterMaintenanceState& state,
                       const MaintenanceDelta* delta,
                       lsn_t version) { onUpdate(state, delta, version); };
  update_handle_ = subscribe(update_cb);
  setSnapshottingGracePeriod(settings_->maintenance_log_snapshotting_period);
}

void ClusterMaintenanceStateMachine::start() {
  Base::start();
}

void ClusterMaintenanceStateMachine::onUpdate(
    const ClusterMaintenanceState& /* unused */,
    const MaintenanceDelta* /* unused */,
    lsn_t /* unused */) {
  // TODO: Implement snapshotting logic here
  is_fully_loaded_ = true;
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
  std::unique_ptr<ClusterMaintenanceState> state{nullptr};
  state = ThriftCodec::deserialize<apache::thrift::BinarySerializer,
                                   ClusterMaintenanceState>(Slice(payload));
  if (!state) {
    ld_warning("Failed to deserialize ClusterMaintenanceState with version:%s",
               toString(version).c_str());
  }
  return state;
}

std::unique_ptr<MaintenanceDelta>
ClusterMaintenanceStateMachine::deserializeDelta(Payload payload) {
  std::unique_ptr<MaintenanceDelta> delta{nullptr};
  delta = ThriftCodec::deserialize<apache::thrift::BinarySerializer,
                                   MaintenanceDelta>(Slice(payload));
  if (!delta) {
    ld_warning("Failed to deserialize the payload from maintenance log");
  }
  return delta;
}

int ClusterMaintenanceStateMachine::applyDelta(
    const MaintenanceDelta& delta,
    ClusterMaintenanceState& state,
    lsn_t version,
    std::chrono::milliseconds timestamp,
    std::string& failure_reason) {
  int rv = 0;
  auto type = delta.getType();
  switch (type) {
    case MaintenanceDelta::Type::apply_maintenances: {
      rv = MaintenanceDeltaTypes::applyMaintenances(
          delta.get_apply_maintenances(), state, failure_reason);
      break;
    }
    case MaintenanceDelta::Type::remove_maintenances: {
      rv = MaintenanceDeltaTypes::removeMaintenances(
          delta.get_remove_maintenances(), state, failure_reason);
      break;
    }
    default:
      ld_warning("Unknown type of MaintenanceDelta. Not applying the delta");
      failure_reason = "Unknown type";
      rv = -1;
      break;
  }
  if (rv == 0) {
    state.set_version(version);
  }
  return rv;
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
