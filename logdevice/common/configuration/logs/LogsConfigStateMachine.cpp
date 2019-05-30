/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigStateMachine.h"

#include <cstring>
#include <functional>

#include "logdevice/common/PayloadHolder.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"
#include "logdevice/common/stats/ServerHistograms.h"

using facebook::logdevice::logsconfig::Delta;
using facebook::logdevice::logsconfig::DeltaOpType;
using facebook::logdevice::logsconfig::DirectoryNode;
using facebook::logdevice::logsconfig::FBuffersLogsConfigCodec;
using facebook::logdevice::logsconfig::LogGroupNode;
using facebook::logdevice::logsconfig::LogsConfigTree;

namespace facebook { namespace logdevice {

LogsConfigStateMachine::LogsConfigStateMachine(
    UpdateableSettings<Settings> settings,
    std::shared_ptr<UpdateableServerConfig> updateable_server_config,
    bool is_writable,
    bool allow_snapshotting)
    : Parent(RSMType::LOGS_CONFIG_STATE_MACHINE,
             configuration::InternalLogs::CONFIG_LOG_DELTAS,
             configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS),
      settings_(settings),
      updateable_server_config_(std::move(updateable_server_config)),
      is_writable_(is_writable),
      allow_snapshotting_(allow_snapshotting) {
  auto server_config = updateable_server_config_->get();
  delimiter_ = server_config->getNamespaceDelimiter();
  auto cb = [&]() {
    auto locked_ptr = delimiter_.wlock();
    *locked_ptr = updateable_server_config_->get()->getNamespaceDelimiter();
  };
  server_config_updates_handle_ =
      updateable_server_config_->subscribeToUpdates(cb);

  auto update_cb = [&](const LogsConfigTree& state,
                       const Delta* delta,
                       lsn_t version) { onUpdate(state, delta, version); };

  self_subscription_ = subscribe(update_cb);
  setSnapshottingGracePeriod(settings_->logsconfig_snapshotting_period);
}

bool LogsConfigStateMachine::canTrimAndSnapshot() const {
  // We can do neither trimming nor snapshotting if the RSM is non-writable.
  if (!is_writable_) {
    return false;
  }
  // Allow trimming and snapshotting if they are not to be done automatically.
  if (!allow_snapshotting_) {
    return true;
  }
  // Otherwise, check the node we are running on.

  auto w = Worker::onThisThread();
  NodeID my_node_id = w->processor_->getMyNodeID();
  ld_check(my_node_id.isNodeID());

  auto cs = w->getClusterState();
  ld_check(cs != nullptr);

  // The node responsible for trimming and snapshotting is the first node
  // that's alive according to the failure detector.
  return cs->getFirstNodeAlive() == my_node_id.index();
}

bool LogsConfigStateMachine::shouldTrim() const {
  // Trim if:
  // 1. LogsConfig trimming is enabled in the settings;
  // 2. This node is the first node alive according to the FD;
  // 3. We use a snapshot log.
  return !settings_->disable_logsconfig_trimming && canTrimAndSnapshot() &&
      snapshot_log_id_ != LOGID_INVALID;
}

bool LogsConfigStateMachine::canSnapshot() const {
  return allow_snapshotting_ && !snapshot_in_flight_ &&
      settings_->logsconfig_snapshotting && canTrimAndSnapshot() &&
      snapshot_log_id_ != LOGID_INVALID;
}

bool LogsConfigStateMachine::shouldCreateSnapshot() const {
  // Create a snapshot if:
  // 1. we are not already snapshotting;
  // 2. Event log snapshotting is enabled in the settings;
  // 3. This node is the first node alive according to the FD;
  // 4. We reached the limits in delta log size as configured in settings.
  return canSnapshot() &&
      (numDeltaRecordsSinceLastSnapshot() >
           settings_->logsconfig_max_delta_records ||
       numBytesSinceLastSnapshot() > settings_->logsconfig_max_delta_bytes);
}

void LogsConfigStateMachine::onUpdate(const LogsConfigTree& /* unused */,
                                      const Delta* /*unused */,
                                      lsn_t /* unused */) {
  if (is_writable_ && shouldCreateSnapshot()) {
    snapshot(nullptr);
  }
}

void LogsConfigStateMachine::onSnapshotCreated(Status st, size_t snapshotSize) {
  // Snapshot creation completion callback.
  // On success, issue a request to trim the RSM if possible.
  if (st == E::OK) {
    ld_info("LogsConfig snapshot was taken successfully!");
    STAT_INCR(getStats(), logsconfig_manager_snapshot_created);
    STAT_SET(getStats(), logsconfig_snapshot_size, snapshotSize);
    if (shouldTrim()) {
      STAT_INCR(getStats(), logsconfig_manager_trimming_requests);
      trim();
    }
  } else {
    ld_error("Could not create LogsConfig snapshot: %s", error_name(st));
    // We'll try again next time we receive a delta.
    STAT_INCR(getStats(), logsconfig_manager_snapshotting_errors);
  }
}

void LogsConfigStateMachine::trim() {
  if (!trim_retry_handler_) {
    trim_retry_handler_ = std::make_unique<TrimRSMRetryHandler>(
        delta_log_id_, snapshot_log_id_, rsm_type_);
  }
  // Set retention to 0 as we want to trim everything up to the last snapshot
  ld_info("Trimming LogsConfig Delta and Snapshot log...");
  trim_retry_handler_->trim(std::chrono::milliseconds::zero());
}

void LogsConfigStateMachine::writeDelta(
    std::string payload,
    std::function<
        void(Status st, lsn_t version, const std::string& failure_reason)> cb,
    WriteMode mode) {
  if (is_writable_) {
    STAT_INCR(getStats(), logsconfig_manager_delta_written);
    Parent::writeDelta(std::move(payload), std::move(cb), std::move(mode));
  } else {
    ld_error("Attempting to write to a read-only LogsConfigStateMachine, "
             "operation denied: E::ACCESS");
    cb(E::ACCESS, 0, "Access Denied!");
  }
}

std::unique_ptr<Delta>
LogsConfigStateMachine::deserializeDelta(Payload payload) {
  auto delta =
      FBuffersLogsConfigCodec::deserialize<Delta>(payload, *delimiter_.rlock());
  if (delta == nullptr) {
    // The returned tree can be nullptr if deserialization failed. In this case
    // we should ignore this tree when possible
    err = E::FAILED;
    return nullptr;
  }
  return delta;
}

int LogsConfigStateMachine::applyDelta(const logsconfig::Delta& delta,
                                       logsconfig::LogsConfigTree& tree,
                                       lsn_t version,
                                       std::chrono::milliseconds /* unused */,
                                       std::string& failure_reason) {
  auto apply_start_time = std::chrono::steady_clock::now();
  int rv = delta.apply(tree, failure_reason);
  const int64_t apply_latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - apply_start_time)
          .count();
  if (rv == 0) {
    tree.setVersion(version);
  }
  if (delta.type() == DeltaOpType::SET_TREE) {
    ld_info("LogsConfigTree has been reset to a new complete tree after "
            "applying a SetDeltaTree at version (%s)",
            lsn_to_string(version).c_str());
  }
  STAT_INCR(getStats(), logsconfig_manager_delta_applied);
  HISTOGRAM_ADD(Worker::stats(),
                logsconfig_manager_delta_apply_latency,
                apply_latency_us);
  return rv;
}

int LogsConfigStateMachine::serializeState(
    const logsconfig::LogsConfigTree& tree,
    void* buf,
    size_t size) {
  PayloadHolder holder = FBuffersLogsConfigCodec::serialize(tree, false);
  if (buf == nullptr) {
    return holder.size();
  }
  ld_check(size >= holder.size());
  memcpy(buf, holder.getPayload().data(), holder.size());
  return holder.size();
}

std::unique_ptr<logsconfig::LogsConfigTree>
LogsConfigStateMachine::deserializeState(
    Payload payload,
    lsn_t version,
    std::chrono::milliseconds /* unused */) const {
  auto tree = FBuffersLogsConfigCodec::deserialize<LogsConfigTree>(
      payload, *delimiter_.rlock());
  if (tree == nullptr) {
    // The returned tree can be nullptr if deserialization failed. In this case
    // we should ignore this tree when possible
    err = E::BADMSG;
    return nullptr;
  }
  tree->setVersion(version);
  return tree;
}

std::unique_ptr<LogsConfigTree>
LogsConfigStateMachine::makeDefaultState(lsn_t version) const {
  auto tree = LogsConfigTree::create(*delimiter_.rlock());
  tree->setVersion(version);
  return tree;
}

/**
 * Serialization and Deserialization Helpers
 */
std::string
LogsConfigStateMachine::serializeDirectory(const DirectoryNode& dir) {
  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize(dir, true /* flatten */);
  return payload.toString();
}

std::string
LogsConfigStateMachine::serializeLogGroup(const logsconfig::LogGroupNode& lg) {
  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize(lg, true /* flatten */);
  return payload.toString();
}

std::unique_ptr<logsconfig::LogGroupNode>
LogsConfigStateMachine::deserializeLogGroup(const std::string& payload,
                                            const std::string& delimiter) {
  return logsconfig::FBuffersLogsConfigCodec::deserialize<
      logsconfig::LogGroupNode>(
      Payload(payload.data(), payload.size()), delimiter);
}

std::unique_ptr<logsconfig::DirectoryNode>
LogsConfigStateMachine::deserializeDirectory(const std::string& payload,
                                             const std::string& delimiter) {
  return logsconfig::FBuffersLogsConfigCodec::deserialize<
      logsconfig::DirectoryNode>(
      Payload(payload.data(), payload.size()), delimiter);
}

std::unique_ptr<Delta>
LogsConfigStateMachine::deserializeDelta(std::string blob,
                                         std::string delimiter) {
  Payload payload{blob.data(), blob.size()};
  auto delta = FBuffersLogsConfigCodec::deserialize<Delta>(payload, delimiter);
  if (delta == nullptr) {
    // The returned tree can be nullptr if deserialization failed. In this case
    // we should ignore this tree when possible
    err = E::FAILED;
    return nullptr;
  }
  return delta;
}

StatsHolder* FOLLY_NULLABLE LogsConfigStateMachine::getStats() {
  if (Worker::onThisThread(false)) {
    return Worker::stats();
  } else {
    return nullptr;
  }
}

void LogsConfigStateMachine::snapshot(std::function<void(Status st)> cb) {
  STAT_INCR(getStats(), logsconfig_manager_snapshot_requested);
  Parent::snapshot(cb);
}

}} // namespace facebook::logdevice
