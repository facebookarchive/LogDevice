/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/server/LogRebuilding.h"
#include "logdevice/server/RebuildingWakeupQueue.h"

namespace facebook { namespace logdevice {

/**
 * Object in charge of reading and re-replicating data during rebuilding.
 * Lives on the worker on which it's constructed. All methods, including
 * destructor, must be called from the same worker thread.
 */

class ShardRebuildingV1 : public ShardRebuildingInterface {
 public:
  using WeakRef = WorkerCallbackHelper<ShardRebuildingV1>::Ticket;

  // Global window is initially set to RecordTimestamp::min(), so
  // the ShardRebuildingV1 won't make progress until advanceGlobalWindow() is
  // called. Call it either before or after start().
  ShardRebuildingV1(shard_index_t shard,
                    lsn_t rebuilding_version,
                    lsn_t restart_version,
                    std::shared_ptr<const RebuildingSet> rebuilding_set,
                    LocalLogStore* store,
                    UpdateableSettings<RebuildingSettings> rebuilding_settings,
                    std::shared_ptr<UpdateableConfig> config,
                    ShardRebuildingInterface::Listener* listener);
  ~ShardRebuildingV1() override;

  void start(std::unordered_map<logid_t, std::unique_ptr<RebuildingPlan>> plan)
      override;

  void advanceGlobalWindow(RecordTimestamp new_window_end) override;

  /**
   * Abort LogRebuilding state machines for logs that were removed from config.
   */
  void noteConfigurationChanged() override;

  void noteRebuildingSettingsChanged() override {}

  /**
   * Called by a LogRebuilding state machine when it completed.
   */
  void onLogRebuildingComplete(logid_t logid);

  /**
   * Called by a LogRebuilding state machine when it reached the end of the
   * local window.
   *
   * @param logid          Log id of the state machine.
   * @param ts             Timestamp of the next record to be processed.
   * @param size           Total memory used by the LogRebuilding state machine
   */
  void onLogRebuildingWindowEnd(logid_t logid, RecordTimestamp ts, size_t size);

  /**
   * Called by a LogRebuilding state machine whenever the total memory used
   * changes
   *
   * @param logid          Log id of the state machine
   * @param size         Total memory used by LogRebuilding state machine
   *                       (See LogRebuilding::getTotalLogRebuildingSize for
   *                       how this size is defined)
   */
  void onLogRebuildingSizeUpdate(logid_t logid, size_t size);

  /**
   * Called by a LogRebuilding state machine when it has processed all
   * records till `until_lsn` but not all of them are known to be durable
   * yet
   */
  void onLogRebuildingReachedUntilLsn(logid_t logid, size_t size);

  /**
   * Fills the current row of @param table with debug information about the
   * state of rebuilding for this shard. Used by admin commands.
   */
  void getDebugInfo(InfoRebuildingShardsTable& table) const override;

  lsn_t getRestartVersion() const;
  RecordTimestamp getLocalWindowEnd() const;

  /**
   * Interface for sliding the shard's local window. Public for tests.
   */
  class LocalWindowSlider {
   public:
    explicit LocalWindowSlider(ShardRebuildingV1* owner) : owner_(owner) {}
    // This method can be used to fetch the new local window end. It doesn't
    // modify the ShardState directly. Returns false if this slider cannot move
    // the local window anymore, true otherwise.
    virtual bool getNewLocalWindowEnd(RecordTimestamp next_ts,
                                      RecordTimestamp* out) = 0;
    virtual ~LocalWindowSlider() {}

   protected:
    // Returns the end of the global window for a shard. Overridden in tests.
    virtual RecordTimestamp getGlobalWindowEnd();

    ShardRebuildingV1* owner_;
  };

  /**
   * Implementation that slides the window by a fixed size
   */
  class FixedSizeLocalWindowSlider : public LocalWindowSlider {
   public:
    FixedSizeLocalWindowSlider(ShardRebuildingV1* owner,
                               RecordTimestamp::duration slide_interval)
        : LocalWindowSlider(owner), slide_interval_(slide_interval) {}
    bool getNewLocalWindowEnd(RecordTimestamp next_ts,
                              RecordTimestamp* out) override;

   private:
    RecordTimestamp::duration slide_interval_;
  };

  /**
   * Implementation that slides the window partion-by-partition
   */
  class PartitionedLocalWindowSlider : public LocalWindowSlider {
   public:
    using LocalWindowSlider::LocalWindowSlider;
    bool getNewLocalWindowEnd(RecordTimestamp next_ts,
                              RecordTimestamp* out) override;

   protected:
    // populates partition_timestamps_. Overridden in tests.
    virtual void getPartitionTimestamps();

    // calls getPartitionTimestamps() and sets cur_partition_
    void resetTimestamps(RecordTimestamp next_ts);

    // max_timestamp of each partition in the store
    std::vector<RecordTimestamp> partition_timestamps_;

    // index of current partition
    int cur_partition_ = 0;
  };

 protected:
  // The following may be overridden by tests.

  virtual StatsHolder* getStats();

  virtual bool isPartitionedStore();

  virtual int getWorkerCount();

  /**
   * Create and send a LogRebuildingMoveWindowRequest. Mocked by tests.
   */
  virtual void sendLogRebuildingMoveWindowRequest(int worker_id,
                                                  logid_t logid,
                                                  RecordTimestamp end);

  /**
   * Create and send a StartLogRebuildingRequest. Mocked by tests.
   */
  virtual void sendStartLogRebuildingRequest(
      int worker_id,
      logid_t logid,
      std::shared_ptr<const RebuildingSet> rebuilding_set,
      RebuildingPlan rebuilding_plan,
      RecordTimestamp next_timestamp,
      lsn_t version);

  /**
   * Create and send a AbortLogRebuildingRequest. Mocked by tests.
   */
  virtual void sendAbortLogRebuildingRequest(int worker_id, logid_t logid);

  virtual void sendRestartLogRebuildingRequest(int worker_id, logid_t logid);

  /**
   * Called when LogRebuilding state machine informs that it reached the end
   * of the log by calling `onLogRebuildingReachedUntilLsn`
   */
  virtual void activateRestartTimerForLog(logid_t logid);
  virtual void onRestartTimerExpiredForLog(logid_t logid);
  virtual bool isRestartTimerActiveForLog(logid_t logid);
  virtual void cancelRestartTimerForLog(logid_t logid);

  /**
   * Called when the total size of all LogRebuilding state machines
   * exceeds `total_log_rebuilding_size_per_shard`
   */
  virtual void activateStallTimer();

  virtual void onStallTimerExpired();
  virtual void cancelStallTimer();
  virtual bool isStallTimerActive();

 protected:
  /**
   * State maintained per log for which there is an active LogRebuilding state
   * machine.
   */
  struct LogState {
    LogState(LogState&& other) = default;
    LogState& operator=(LogState&& other) = default;
    LogState(ShardRebuildingV1* owner,
             logid_t log_id,
             std::unique_ptr<RebuildingPlan> plan);
    ~LogState();

    ShardRebuildingV1* owner;

    logid_t logId;

    // Worker thread on which the LogRebuilding state machine is running.
    // When we create a LogRebuilding state machine, we decide on which worker
    // thread it will be running. We need to keep track of which worker was
    // selected so that subsequent Requests can be posted to the same worker.
    // Currently the Worker selection is deterministic and keping track of this
    // is actually useless. However it might not be in the future as we might
    // select workers based on their load.
    int workerId{-1};

    // Contains the set of epochs alongside their EpochMetaData. This instructs
    // the LogRebuilding of what and how to rebuilding.
    std::unique_ptr<RebuildingPlan> plan;

    bool hasLogRebuilding{false};

    // Used for asserting consistency.
    bool isLogRebuildingRunning{false};

    // Set to true when log is removed from config
    bool abort{false};

    // Total memory used by the corresponding LogRebuilding
    // state machine.
    size_t logRebuildingSize{0};

    // This timer is created lazily and started when the LogRebuilding State
    // machine has reached until_lsn but not all records are durable.
    std::unique_ptr<Timer> restartTimer;

    // When a LogRebuilding state machine reaches past the local timestamp
    // window, it notifies us through a call to onLogRebuildingWindowEnd() and
    // provides the timestamp of the next record to be rebuilt past the window.
    // We keep track of that timestamp here. As the local window slides, we
    // check it against this value to know if we need to wake up the
    // LogRebuilding state machine.
    RecordTimestamp nextTimestamp = RecordTimestamp::min();

    struct Comparer {
      bool operator()(const LogState* a, const LogState* b) {
        // Need min nextTimestamp at the top of priority_queue.
        // Comparing logId adds some determinism for tests' convenience.
        return std::tie(a->nextTimestamp, a->logId) >
            std::tie(b->nextTimestamp, b->logId);
      }
    };

   private:
    LogState() {}
  };

  /**
   * If less than `maxLogsInFlight_` LogRebuildings are running, restart logs in
   * restartLogRebuilding and/or  wake up/start some LogRebuilding state
   * machines that are waiting in the `wakeupQueue`.
   */
  void wakeUpLogs();

  /**
   * Called when an event might make it possible to slide the local window. Such
   * events are:
   * - a LogRebuilding state machine completed causing all remaining ones to be
   *   caught up;
   * - a LogRebuilding just caught up causing all LogRebuildings to be caught
   *   up;
   * - All LogRebuildings are caught up and the global timestamp window was
   *   slid.
   */
  void trySlideLocalWindow();

  /**
   * Called when some log rebuilding reaches end of log or the window or
   * completes durably or sends a size update which brings the total size down
   * from its current value. If the size of all LogRebuilding state machine for
   * this shard exceeds `total_log_rebuilding_size_per_shard_mb`, stall timer
   * is activated otherwise Wakes up another log, slides window or concludes
   * shard rebuilding.
   */
  void someLogMadeProgress();

  // Returns true if the logid is still present in activeLogs_
  // and hasn't been aborted. If the log has been aborted, removes
  // it from activeLogs_
  bool isLogRebuilding(logid_t logid);

  // Used in tests. Does what destructor does: aborts log rebuildings and clears
  // state. Doing it outside destructor allows virtual methods to dispatch to
  // the mocked implementations.
  void destroy();

  // LSN of the last SHARD_NEEDS_REBUILD we considered. This identifies the
  // current rebuilding state machine and changes each time we rewind it
  // because the rebuilding set is modified following receiving a
  // SHARD_NEEDS_REBUILD record.
  lsn_t rebuildingVersion_{LSN_INVALID};
  lsn_t restartVersion_{LSN_INVALID};

  uint32_t shard_;
  // Set of nodes that we are rebuilding this shard for.
  std::shared_ptr<const RebuildingSet> rebuildingSet_;
  LocalLogStore* store_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  std::shared_ptr<UpdateableConfig> config_;
  Listener* listener_;

  // Set of logs that are being rebuilt for this shard. Once this set becomes
  // empty, we conclude that the shard is rebuilt.
  std::unordered_map<logid_t, LogState, logid_t::Hash> activeLogs_;

  // Queue of active logs that don't have a running LogRebuilding.
  // Logs from this queue are woken up either when a
  // LogRebuilding reaches end of window, freeing up a slot in
  // `maxLogsInFlight_`, or when local window is slid, allowing logs to
  // proceed into higher timestamps.
  // Logs waiting for `untilLSN` or release are not here.
  // If `wakeupQueue_.nextTimestamp()` > `localWindowEnd_`, it's time to slide
  // the local window.
  RebuildingWakeupQueue wakeupQueue_;

  RecordTimestamp globalWindowEnd_{RecordTimestamp::min()};

  // If we are waiting for global window to slide, this is when we started
  // waiting.
  SteadyTimestamp waitingOnGlobalWindowSince_ = SteadyTimestamp::min();

  // true if all LogRebuilding have finished, and onComplete_ was called.
  bool completed_ = false;

  // Set of logs that are ready to be restarted as soon as a slot is
  // available. Logs are added to this set under two conditions:
  // 1. LogRebuilding reached until_lsn (i.e reached end of log but not all
  // records are known to be durable yet) and the restart timer for this log
  // expired before LogRebuilding completed durably (i.e
  // onLogRebuildingComplete is called)
  // 2. Stall timer expired and we have logs whose last known size is
  // non-zero
  std::unordered_set<logid_t, logid_t::Hash> restartLogRebuilding_;

  // How many LogRebuilding state machines are doing work now.
  // At most `maxLogsInFlight_`.
  int nRunningLogRebuildings_{0};

  // How many logs have reached the end of rebuilding but not durable
  // and hence have the restart timers active
  int nLogRebuildingRestartTimerActive_{0};

  // The total memory used by all LogRebuilding state machines
  // All state machines report their size as part of WindowEnd/
  // complete notification
  size_t logRebuildingTotalSize_{0};

  // Whenever the total amount of memory used by all LogRebuilding
  // state machines for a given shard exceeds
  // `total_log_rebuilding_size_per_shard_mb`, LogRebuilding is stalled
  // until memory usage comes below the limit. This timer is activated
  // when the limit is exceeded and on expiry, schedules the logs
  // with non-zero memory usage for restart
  std::unique_ptr<Timer> stallTimer_;

  // End of the local timestamp window for this shard. The local timestamp
  // window is smaller than the global timestamp window.  Each time a
  // LogRebuilding state machine reaches records past this value, it will
  // notify this object. Once this object realizes that all LogRebuilding
  // state machines for that shard reached the end of the window
  // (activeLogs_.size() == num_logs_window_end_), we slide this window
  // within the global window. If we can't slide the local window, we notify
  // other nodes about our progress by calling notifyShardDonorProgress().
  RecordTimestamp localWindowEnd_ = RecordTimestamp::min();

  // The object that slides the local window
  std::unique_ptr<LocalWindowSlider> localWindowSlider_;

  // A weak pointer to `this`. When ShardRebuildingV1 is destroyed, its
  // LogRebuildings are not destroyed synchronously (because they live on
  // different workers), so callbacks from `LogRebuilding`s may risk accessing
  // a deleted ShardRebuildingV1. To prevent this, the access happens through
  // this weak pointer that can tell whether the ShardRebuildingV1 is destroyed.
  WorkerCallbackHelper<ShardRebuildingV1> callbackHelper_;

  friend class RebuildingCoordinatorTest;
};

}} // namespace facebook::logdevice
