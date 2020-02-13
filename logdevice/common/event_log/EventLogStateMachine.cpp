/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogStateMachine.h"

#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/TrimRequest.h"

namespace facebook { namespace logdevice {

template class ReplicatedStateMachine<EventLogRebuildingSet, EventLogRecord>;

EventLogStateMachine::EventLogStateMachine(
    UpdateableSettings<Settings> settings)
    : Parent(RSMType::EVENT_LOG_STATE_MACHINE,
             configuration::InternalLogs::EVENT_LOG_DELTAS,
             settings->event_log_snapshotting
                 ? configuration::InternalLogs::EVENT_LOG_SNAPSHOTS
                 : LOGID_INVALID),
      settings_(settings) {
  auto cb = [&](const EventLogRebuildingSet& set,
                const EventLogRecord* delta,
                lsn_t version) { onUpdate(set, delta, version); };

  handle_ = subscribe(cb);
  setSnapshottingGracePeriod(settings_->eventlog_snapshotting_period);
}

bool EventLogStateMachine::thisNodeCanTrimAndSnapshot() const {
  if (!myNodeId_.hasValue()) {
    return false;
  }
  auto w = Worker::onThisThread();
  auto cs = w->getClusterState();
  ld_check(cs != nullptr);
  return cs->getFirstNodeAlive() == myNodeId_->index();
}

void EventLogStateMachine::start() {
  if (!write_delta_header_) {
    doNotWriteDeltaHeader();
  }

  // We don't support mocking that code, so we expect
  // updateWorkerShardStatusMap() to not be used in tests.
  Worker* w = Worker::onThisThread(false);
  if (update_workers_) {
    ld_check(w);
    gracePeriodTimer_.assign([this] { updateWorkerShardStatusMap(); });
  }

  Parent::start();
}

void EventLogStateMachine::stop() {
  ld_info("Stopping EventLogStateMachine");
  gracePeriodTimer_.cancel();
  handle_.reset();
  trim_retry_handler_.reset();
  Parent::stop();
}

void EventLogStateMachine::trim() {
  if (!trim_retry_handler_) {
    trim_retry_handler_ = std::make_unique<TrimRSMRetryHandler>(
        delta_log_id_, snapshot_log_id_, rsm_type_);
  }
  trim_retry_handler_->trim(settings_->event_log_retention);
}

void EventLogStateMachine::publishRebuildingSet() {
  Worker::onThisThread()->processor_->rebuilding_set_.update(
      std::make_shared<EventLogRebuildingSet>(getState()));
}

const std::shared_ptr<ServerConfig>
EventLogStateMachine::getServerConfig() const {
  Worker* w = Worker::onThisThread();
  ld_check(w); // Tests should override this method.
  return w->getServerConfig();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
EventLogStateMachine::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

std::unique_ptr<EventLogRebuildingSet>
EventLogStateMachine::makeDefaultState(lsn_t version) const {
  return std::make_unique<EventLogRebuildingSet>(version, myNodeId_);
}

std::unique_ptr<EventLogRebuildingSet> EventLogStateMachine::deserializeState(
    Payload payload,
    lsn_t version,
    std::chrono::milliseconds timestamp) const {
  auto verifier =
      flatbuffers::Verifier(static_cast<const uint8_t*>(payload.data()),
                            payload.size(),
                            128, /* max verification depth */
                            10000000 /* max number of tables to be verified */);

  if (!event_log_rebuilding_set::VerifySetBuffer(verifier)) {
    err = E::BADMSG;
    return nullptr;
  }

  auto res = EventLogRebuildingSetCodec::deserialize(
      event_log_rebuilding_set::GetSet(payload.data()), version, myNodeId_);

  for (const auto& it_shard : res->getRebuildingShards()) {
    res->recomputeAuthoritativeStatus(
        it_shard.first, timestamp, *getNodesConfiguration());
    res->recomputeShardRebuildTimeIntervals(it_shard.first);
  }

  return res;
}

void EventLogStateMachine::gotInitialState(
    const EventLogRebuildingSet& rebuilding_set) const {
  ld_info("Got base rebuilding set: %s", toString(rebuilding_set).c_str());
}

std::unique_ptr<EventLogRecord>
EventLogStateMachine::deserializeDelta(Payload payload) {
  std::unique_ptr<EventLogRecord> delta;
  const int rv = EventLogRecord::fromPayload(payload, delta);
  if (rv != 0) {
    WORKER_STAT_INCR(malformed_event_log_records_read);
    err = E::FAILED;
    return nullptr;
  }

  return delta;
}

int EventLogStateMachine::applyDelta(const EventLogRecord& delta,
                                     EventLogRebuildingSet& state,
                                     lsn_t version,
                                     std::chrono::milliseconds timestamp,
                                     std::string& /* unused */) {
  WORKER_STAT_INCR(num_event_log_records_read);
  ld_info("Applying delta ts=%s, %s: %s",
          format_time(timestamp).c_str(),
          lsn_to_string(version).c_str(),
          delta.describe().c_str());
  return state.update(version, timestamp, delta, *getNodesConfiguration());
}

int EventLogStateMachine::serializeDelta(const EventLogRecord& delta,
                                         void* buf,
                                         size_t buf_size) {
  return delta.toPayload(buf, buf_size);
}

int EventLogStateMachine::serializeState(const EventLogRebuildingSet& state,
                                         void* buf,
                                         size_t buf_size) {
  flatbuffers::FlatBufferBuilder builder;
  auto set = EventLogRebuildingSetCodec::serialize(builder, state);
  builder.Finish(set);
  Payload payload(builder.GetBufferPointer(), builder.GetSize());
  if (buf) {
    ld_check(buf_size >= builder.GetSize());
    memcpy(buf, builder.GetBufferPointer(), builder.GetSize());
  }
  return builder.GetSize();
}

void EventLogStateMachine::writeDelta(
    const EventLogRecord& delta,
    std::function<
        void(Status st, lsn_t version, const std::string& /* unused */)> cb,
    WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  const size_t size = serializeDelta(delta, nullptr, 0);
  std::string buf;
  buf.resize(size);
  ld_check(buf.size() > 0);
  const int rv = serializeDelta(delta, &buf[0], size);
  ld_check(rv == size);

  postWriteDeltaRequest(
      std::move(buf), std::move(cb), mode, std::move(base_version));
}

void EventLogStateMachine::postWriteDeltaRequest(
    std::string delta,
    std::function<
        void(Status st, lsn_t version, const std::string& /* unused */)> cb,
    WriteMode mode,
    folly::Optional<lsn_t> base_version) {
  std::unique_ptr<Request> req =
      std::make_unique<EventLogWriteDeltaRequest>(getWorkerId().val(),
                                                  std::move(delta),
                                                  std::move(cb),
                                                  mode,
                                                  std::move(base_version));
  postRequestWithRetrying(req);
}

void EventLogStateMachine::onUpdate(const EventLogRebuildingSet& set,
                                    const EventLogRecord* /*delta*/,
                                    lsn_t version) {
  if (update_workers_) {
    gracePeriodTimer_.activate(settings_->event_log_grace_period);
    publishRebuildingSet();
  }

  if (snapshot_log_id_ == LOGID_INVALID) {
    // When not using snapshotting, we need to trim the delta log when the
    // rebuilding set becomes empty otherwise it will grow indefinitely.
    if (getState().canTrimEventLog(*getNodesConfiguration())) {
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

void EventLogStateMachine::onSnapshotCreated(Status st, size_t snapshotSize) {
  if (st == E::OK) {
    ld_info("Successfully created a snapshot");
    WORKER_STAT_SET(eventlog_snapshot_size, snapshotSize);
    if (shouldTrim()) {
      trim();
    }
  } else {
    ld_error("Could not create a snapshot: %s", error_name(st));
    // We'll try again next time we receive a delta.
    WORKER_STAT_INCR(eventlog_snapshotting_errors);
  }
}

bool EventLogStateMachine::shouldTrim() const {
  // Trim if:
  // 1. Event log trimming is enabled in the settings;
  // 2. This node is the first node alive according to the FD.
  // 3. We use a snapshot log (otherwise trimming of delta log is done by
  //    noteConfigurationChanged());
  return !settings_->disable_event_log_trimming &&
      thisNodeCanTrimAndSnapshot() && snapshot_log_id_ != LOGID_INVALID;
}

bool EventLogStateMachine::canSnapshot() const {
  return !snapshot_in_flight_ && settings_->event_log_snapshotting &&
      thisNodeCanTrimAndSnapshot() && snapshot_log_id_ != LOGID_INVALID;
}

bool EventLogStateMachine::shouldCreateSnapshot() const {
  // Create a snapshot if:
  // 1. we are not already snapshotting;
  // 2. Event log snapshotting is enabled in the settings;
  // 3. This node is the first node alive according to the FD;
  // 4. We reached the limits in delta log size as configured in settings.
  return canSnapshot() &&
      (numDeltaRecordsSinceLastSnapshot() >
           settings_->event_log_max_delta_records ||
       numBytesSinceLastSnapshot() > settings_->event_log_max_delta_bytes);
}

Request::Execution StartEventLogStateMachineRequest::execute() {
  event_log_->start();
  Worker::onThisThread()->setEventLogStateMachine(event_log_);
  return Execution::COMPLETE;
}

Request::Execution EventLogWriteDeltaRequest::execute() {
  EventLogStateMachine::Parent* event_log =
      Worker::onThisThread()->getEventLogStateMachine();

  if (event_log) {
    event_log->writeDelta(
        std::move(delta_), std::move(cb_), mode_, std::move(base_version_));
  } else {
    cb_(E::FAILED, LSN_INVALID, "");
  }
  return Execution::COMPLETE;
}

void EventLogStateMachine::updateWorkerShardStatusMap() {
  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  auto map = getState().toShardStatusMap(*nodes_configuration);

  for (const auto& p : Worker::settings().authoritative_status_overrides) {
    map.setShardStatus(p.first.node(), p.first.shard(), p.second);
  }

  if (map != last_broadcast_map_) {
    UpdateShardAuthoritativeMapRequest::broadcastToAllWorkers(map);
    last_broadcast_map_ = std::move(map);
  }
}

void EventLogStateMachine::trimNotSnapshotted(lsn_t lsn) {
  // This should not be called on a snapshotted event log.
  ld_check(snapshot_log_id_ == LOGID_INVALID);

  if (!thisNodeCanTrimAndSnapshot() || settings_->disable_event_log_trimming) {
    return;
  }

  ld_info("Trimming event log up to lsn %s", lsn_to_string(lsn).c_str());

  // Function to trim the delta log when it is not snapshotted.
  auto on_trimmed = [](Status status) {
    if (status != E::OK) {
      ld_error("Could not trim event log: %s (%s)",
               error_name(status),
               error_description(status));
    }
  };

  auto trimreq = std::make_unique<TrimRequest>(
      nullptr, delta_log_id_, lsn, std::chrono::seconds(10), on_trimmed);
  trimreq->bypassWriteTokenCheck();

  std::unique_ptr<Request> req(std::move(trimreq));
  Worker::onThisThread()->processor_->postWithRetrying(req);
}

void EventLogStateMachine::noteConfigurationChanged() {
  // The purpose of this function is to eventually trim the event log when it is
  // not snapshotted but there are AUTHORITATIVE_EMPTY shards that were removed
  // from the config. As long as the shard is still in the config, we keep the
  // shard in the rebuilding set so it can later ack, however if the shard is
  // removed from the config we want to trim the event log so it does not grow
  // forever.
  //
  // Because we don't do this when the event log is snapshotted, this
  // effectively adds a small change in behavior. With snapshotting, a node that
  // was rebuilt and then removed from the config will be seen as
  // AUTHORITATIVE_EMPTY until it comes back,  and at this point it will have to
  // ack rebuilding before taking writes. Without snapshotting, we trim the
  // delta which resets the node's authoritative status to FULLY_AUTHORITATIVE.
  // This should not affect correctness as a node removed from the config is
  // discarded from nodesets in the read path anyway.
  //
  // NOTE: In the future, we could imagine making logdevice more robust against
  // human error where someone would remove some nodes from a config for
  // instance. Readers currently blindly remove the node from their gap
  // detection algorithm if it's removed from the config. We could instead look
  // at the node's authoritative status from the event log and expect it to be
  // AUTHORITATIVE_EMPTY if it's not in the config, otherwise the node may still
  // be accounted for gap detection algorithms even though it's not in the
  // config.

  if (snapshot_log_id_ != LOGID_INVALID) {
    // We don't manually trim the event log when it is snapshotted.
    return;
  }

  if (getState().empty()) {
    return;
  }

  if (getState().canTrimEventLog(*getNodesConfiguration())) {
    trimNotSnapshotted(getState().getLastSeenLSN());
  }
}

void EventLogStateMachine::onSettingsUpdated() {
  // In case shard status overrides have changed.
  updateWorkerShardStatusMap();
}

void EventLogStateMachine::snapshot(std::function<void(Status st)> cb) {
  ld_info("Creating a snapshot of EventLog...");
  enableSnapshotCompression(settings_->event_log_snapshot_compression);
  Parent::snapshot(cb);
}
}} // namespace facebook::logdevice
