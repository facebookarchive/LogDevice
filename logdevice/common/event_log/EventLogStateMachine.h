/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/event_log/EventLogRebuildingSetCodec.h"
#include "logdevice/common/event_log/EventLogRebuildingSet_generated.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/replicated_state_machine/ReplicatedStateMachine.h"
#include "logdevice/common/replicated_state_machine/TrimRSMRetryHandler.h"

/**
 * The Event Log is a replicated state machine that maintains the authoritative
 * status and the rebuilding state of all shards in the cluster.
 *
 * @see EventLogRebuildingSet.h for a more detailed explaination of the state
 * being maitained by this state machine.
 *
 * The state machine can be backed by only one log "the delta log" or by two
 * logs, a "delta log" and a "snapshot log". When backed by a single log, which
 * was mostly the case before we implemented snapshotting on top of it, we trim
 * it when the state becomes "empty". Indeed, we don't want the delta log to
 * grow too big as our tooling and servers (during restart) need to replay every
 * single update from the beginning of time in order to reconstruct the state.
 *
 * However, when using a snapshot log, we don't manually trim the delta log when
 * the state becomes empty, instead we periodically write a snapshot to the
 * snapshot log. On startup, logdeviced and our tooling will only need to read
 * the last snapshot and the deltas that come after it in order to reconstruct
 * the state. @see ReplicatedStateMachine.h for more details.
 *
 * When the state is updated, we notify each worker so they can update their
 * local map that contains the authoritative status of each shard in the
 * cluster. Workers use this information in various ways, for instance they can
 * use this to check whether or not a local shard is allowed to process STOREs,
 * they can forward that information to readers and recovery state machines so
 * they adjust their gap detection algorithms, etc. Notifications of state
 * changes to these workers is batched (@see gracePeriodTimer_) to limit the
 * amount of request we do when the rate of updates spikes.
 */

namespace facebook { namespace logdevice {

extern template class ReplicatedStateMachine<EventLogRebuildingSet,
                                             EventLogRecord>;

class EventLogStateMachine
    : public ReplicatedStateMachine<EventLogRebuildingSet, EventLogRecord> {
 public:
  using Parent = ReplicatedStateMachine<EventLogRebuildingSet, EventLogRecord>;

  explicit EventLogStateMachine(UpdateableSettings<Settings> settings);

  /**
   * Start reading the event log.
   */
  void start();

  /**
   * Stop reading the event log. Once called, the user should do nothing but
   * destroy this state machine.
   *
   * Note: This method hides the parent's method
   */
  void stop();

  /**
   * Must be called if this state machine is running on a server node.
   */
  void setMyNodeID(NodeID nid) {
    ld_assert(nid.isNodeID());
    myNodeId_ = nid;
  }

  void writeDeltaHeader() {
    write_delta_header_ = true;
  }

  void setWorkerId(worker_id_t worker) {
    worker_ = worker;
  }
  worker_id_t getWorkerId() {
    return worker_;
  }

  /**
   * EventLogStateMachine will update event log and shard status map in Workers
   * when rebuilding set changes.
   */
  void enableSendingUpdatesToWorkers() {
    update_workers_ = true;
  }

  const EventLogRebuildingSet& getCurrentRebuildingSet() const {
    return getState();
  }

  /**
   * Called when the server configuration has changed.
   */
  void noteConfigurationChanged();

  /**
   * Called when settings have changed.
   */
  void onSettingsUpdated();

  static int serializeDelta(const EventLogRecord& delta,
                            void* buf,
                            size_t buf_size);

  void writeDelta(
      const EventLogRecord& delta,
      std::function<
          void(Status st, lsn_t version, const std::string& /* unused */)> cb,
      WriteMode mode = WriteMode::CONFIRM_APPLIED,
      folly::Optional<lsn_t> base_version = folly::none);

  /**
   * Currently called by AdminCommand class
   */
  void snapshot(std::function<void(Status st)> cb);

  static int getWorkerIdx(int /*nthreads*/) {
    return 0;
  }

 protected:
  /**
   * @return True if this object is allowed to do snapshotting.
   */
  virtual bool canSnapshot() const override;

  /**
   * @return True if this object is responsible for trimming and snapshotting.
   */
  virtual bool thisNodeCanTrimAndSnapshot() const;

  virtual const std::shared_ptr<ServerConfig> getServerConfig() const;

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  /**
   * Posts to each Worker a request to update
   * Worker::shardStatusManager()::shard_status_ with the content of
   * getState().
   */
  virtual void updateWorkerShardStatusMap();

  /**
   * Update rebuilding set in Processor.
   */
  virtual void publishRebuildingSet();

  /**
   * Trim the RSM. Called after we successfully wrote a snapshot.
   */
  virtual void trim();

  /*
   * Trim the delta log when it is not paired with a snapshot log.
   *
   * @param lsn LSN up to which to trim.
   */
  virtual void trimNotSnapshotted(lsn_t lsn);

  /**
   * Posts an EventLogWriteDeltaRequest to the worker running this state machine
   */
  virtual void postWriteDeltaRequest(
      std::string delta,
      std::function<
          void(Status st, lsn_t version, const std::string& /* unused */)> cb,
      WriteMode mode,
      folly::Optional<lsn_t> base_version);

 private:
  std::unique_ptr<EventLogRebuildingSet>
  makeDefaultState(lsn_t version) const override;

  std::unique_ptr<EventLogRebuildingSet>
  deserializeState(Payload payload,
                   lsn_t version,
                   std::chrono::milliseconds timestamp) const override;

  void gotInitialState(const EventLogRebuildingSet& state) const override;

  std::unique_ptr<EventLogRecord> deserializeDelta(Payload payload) override;

  int applyDelta(const EventLogRecord& delta,
                 EventLogRebuildingSet& state,
                 lsn_t version,
                 std::chrono::milliseconds timestamp,
                 std::string& /* unused */) override;

  int serializeState(const EventLogRebuildingSet& state,
                     void* buf,
                     size_t buf_size) override;

  // Called when we get a new EventLogRebuildingSet state. delta is nullptr if
  // that new state comes from a new snapshot, otherwise delta is the delta that
  // was just applied to get to this new state.  Start the grace period so we
  // can eventually notify workers of the new rebuilding set.
  // Also check if now is the time to create a new snapshot.
  void onUpdate(const EventLogRebuildingSet& set,
                const EventLogRecord* delta,
                lsn_t version);

  // Returns true if now is the time to create a new snapshot.
  bool shouldCreateSnapshot() const;
  // Returns true if we can trim the RSM.
  bool shouldTrim() const;

  // Snapshot creation completion callback. On success, also issue a request to
  // trim the RSM if possible.
  void onSnapshotCreated(Status st, size_t snapshotSize) override;

  UpdateableSettings<Settings> settings_;

  bool update_workers_{false};
  bool write_delta_header_{true};

  // Used to publish rebuilding set changes after event_log_grace_period.
  Timer gracePeriodTimer_;

  // Last ShardAuthoritativeStatusMap that was broadcast.
  ShardAuthoritativeStatusMap last_broadcast_map_;

  std::unique_ptr<SubscriptionHandle> handle_;

  // folly::none if this object is not running on a server node.
  folly::Optional<NodeID> myNodeId_{folly::none};

  std::unique_ptr<TrimRSMRetryHandler> trim_retry_handler_;

  // worker running this state machine
  worker_id_t worker_{-1};
};

/**
 * A request for starting an event log state machine on a worker.
 */
class StartEventLogStateMachineRequest : public Request {
 public:
  StartEventLogStateMachineRequest(EventLogStateMachine* event_log, int worker)
      : Request(RequestType::START_EVENT_LOG_READER),
        event_log_(event_log),
        worker_(worker) {}
  ~StartEventLogStateMachineRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*nthreads*/) override {
    return worker_;
  }

 private:
  EventLogStateMachine* event_log_;
  int worker_;
};

/** A request to write a delta on the worker running the event
 * log state machine
 */
class EventLogWriteDeltaRequest : public Request {
 public:
  EventLogWriteDeltaRequest(
      int worker,
      std::string delta,
      std::function<void(Status st, lsn_t version, const std::string&)> cb,
      EventLogStateMachine::WriteMode mode,
      folly::Optional<lsn_t> base_version)
      : Request(RequestType::EVENT_LOG_WRITE_DELTA),
        worker_(worker),
        delta_(std::move(delta)),
        cb_(std::move(cb)),
        mode_(mode),
        base_version_(std::move(base_version)) {}
  ~EventLogWriteDeltaRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*nthreads*/) override {
    return worker_;
  }

 private:
  int worker_;
  std::string delta_;
  std::function<void(Status st, lsn_t version, const std::string& /* unused */)>
      cb_;
  EventLogStateMachine::WriteMode mode_;
  folly::Optional<lsn_t> base_version_;
};

}} // namespace facebook::logdevice
