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
#include "logdevice/common/TrimRequest.h"
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
    const ClusterMaintenanceState& state,
    const MaintenanceDelta* /* unused */,
    lsn_t version) {
  is_fully_loaded_ = true;
  if (snapshot_log_id_ == LOGID_INVALID) {
    // When not using snapshotting, we need to trim the delta log when there
    // are no pending maintenances
    if (state.get_maintenances().empty()) {
      trimNotSnapshotted(version);
    }
  } else {
    // When using snapshotting, well... we create a snapshot when the delta log
    // grows too big.
    if (shouldCreateSnapshot()) {
      snapshot(nullptr);
    }
  }
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
  auto serializedState =
      ThriftCodec::serialize<apache::thrift::BinarySerializer>(state);
  if (buf != nullptr) {
    ld_check(buf_size >= serializedState.size());
    memcpy(buf, serializedState.data(), serializedState.size());
  }
  return serializedState.size();
}

bool ClusterMaintenanceStateMachine::shouldCreateSnapshot() const {
  // Create a snapshot if:
  // 1. we are not already snapshotting;
  // 2. Maintenance log snapshotting is enabled in the settings;
  // 3. We reached the limits in delta log size as configured in settings.
  return canSnapshot() &&
      (numDeltaRecordsSinceLastSnapshot() >
           settings_->maintenance_log_max_delta_records ||
       numBytesSinceLastSnapshot() >
           settings_->maintenance_log_max_delta_bytes);
}

bool ClusterMaintenanceStateMachine::canSnapshot() const {
  return !snapshot_in_flight_ && settings_->maintenance_log_snapshotting &&
      snapshot_log_id_ != LOGID_INVALID;
}

bool ClusterMaintenanceStateMachine::shouldTrim() const {
  // Trim if:
  // 1. Maintenance log trimming is enabled in the settings;
  // 2. We are allowed to snapshot.
  return !settings_->disable_maintenance_log_trimming && canSnapshot();
}

void ClusterMaintenanceStateMachine::onSnapshotCreated(Status st,
                                                       size_t snapshotSize) {
  if (st == E::OK) {
    ld_info("Successfully created a snapshot");
    WORKER_STAT_SET(maintenance_log_snapshot_size, snapshotSize);
    if (shouldTrim()) {
      trim();
    }
  } else {
    ld_error("Could not create a snapshot: %s", error_name(st));
    // We'll try again next time we receive a delta.
    WORKER_STAT_INCR(maintenance_log_snapshotting_errors);
  }
  return;
}

void ClusterMaintenanceStateMachine::snapshot(
    std::function<void(Status st)> cb) {
  ld_info("Creating a snapshot of MaintenanceLog");
  Base::snapshot(cb);
  return;
}

void ClusterMaintenanceStateMachine::trim() {
  if (!trim_retry_handler_) {
    trim_retry_handler_ = std::make_unique<TrimRSMRetryHandler>(
        delta_log_id_, snapshot_log_id_, rsm_type_);
  }
  trim_retry_handler_->trim(settings_->maintenance_log_retention);
  return;
}

void ClusterMaintenanceStateMachine::trimNotSnapshotted(lsn_t lsn) {
  // This should not be called on a snapshotted maintenance log.
  ld_check(snapshot_log_id_ == LOGID_INVALID);

  if (settings_->disable_maintenance_log_trimming) {
    return;
  }

  ld_info("Trimming maintenance log up to lsn %s", lsn_to_string(lsn).c_str());

  auto on_trimmed = [lsn](Status status) {
    if (status != E::OK) {
      ld_error("Could not trim event log: %s (%s)",
               error_name(status),
               error_description(status));
    } else {
      ld_info("Successfully Trimmed maintenance log up to lsn %s",
              lsn_to_string(lsn).c_str());
    }
  };

  auto trimreq = std::make_unique<TrimRequest>(
      nullptr, delta_log_id_, lsn, std::chrono::seconds(10), on_trimmed);
  trimreq->bypassWriteTokenCheck();

  std::unique_ptr<Request> req(std::move(trimreq));
  Worker::onThisThread()->processor_->postWithRetrying(req);
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
