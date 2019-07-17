/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <unordered_map>

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/CircularBuffer.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/RebuildingEventsTracer.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/include/types.h"
#include "logdevice/server/RebuildingReadStorageTask.h"
#include "logdevice/server/RecordRebuildingAmend.h"
#include "logdevice/server/RecordRebuildingStore.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/IteratorCache.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/rebuilding/RebuildingFilter.h"
#include "logdevice/server/rebuilding/RebuildingPlan.h"

/**
 * @file LogRebuilding is the state machine for rebuilding a single log.
 *
 * The workflow varies subtly depending on whether rebuilding uses WAL or not.
 * The order in which methods are invoked for any record is the same.
 *  StartRecordRebuildingStores
 *  onAllStoresReceived
 *  onAllStoresDurable
 *  onAllAmendsReceived
 *  onAllAmendsDurable
 *
 * LogRebuilding starts processing a batch of records by starting
 * RecordRebuildingStore state machines for the records in `buffer_`. The
 * number of RecordRebuildingStore state machines started in parallel is defined
 * by `max-record-in-flight`.  Each RecordRebuildingStore state machine inturn
 * sends out stores and calls back into LogRebuilding once all the stores have
 * been received by recipients. Once all stores have been received, a new
 * RecordRebuildingStore state machine for next record from `buffer_` is
 * started. If any of the stores are not known to be fully durable,
 * LogRebuilding state machine will wait until it receives a flush confirmation
 * before starting the amends for that record. Once all amends have been
 * received and are known to be durable, the record is reaped.
 *
 * Amount of memory used by LogRebuilding (sum of memory used by
 * RecordRebuildingStore state machines in flight, memory records waiting for
 * stores to be durable, memory used by RecordRebuildingAmend state machines in
 * flight and memory used by records waiting for amends to be durable) is
 * limited by `max_log_rebuilding_size_mb`. When the sum exceeds this limit,
 * LogRebuilding will not process a new batch of records but will start a timer.
 * On expiration of the timer, LogRebuilding will be restarted from the last
 * lsn which was known to be durably stored and amended.
 *
 * A step by step workflow looks like this:
 *
 * 1/ Use MetaDataLogReader to figure out the set of epoch intervals to read
 *    from. We only want to read data in epochs whose nodeset contains at least
 *    one node from the rebuilding set.
 *
 * 2/ Read a batch from the local log store on a storage thread. The batch will
 *    contain at most `disk_read_batch_size` MB of data. The data read is placed
 *    in `buffer_`. If the total memory used by this state machine exceeds
 *    `max_log_rebuilding_size_mb`, do not read further and start a timer. On
 *    expiry of timer, restart the rebuilding.
 *
 * 3/ Start maximum of `max_records_in_flight` RecordRebuildingStore state
 *    machines.
 *
 * 4/ When all stores are received for an LSN, keep track of memtables to which
 *    stores were written to (FlushToken is available only if record was not
 *    durable immediately on recipient) and Start RecordRebuildingStore state
 *    machine for next record in `buffer_`
 *
 * 5/ When all stores are durable for an LSN, add the lsn to a list of lsns with
 *    durable stores and start RecordRebuildingAmend state machine. The number
 *    of RecordRebuildingAmend state machines that can run in parallel is
 *    governed by `max-amends-in-flight` setting. New amends are started as
 *    previously started amends complete
 *
 * 6/ When all amends are received for an LSN, keep track of memtables to which
 *    amends were written to. Start RecordRebuildingAmend for any LSN whose
 *    stores are durable.
 *
 * 7/ Once all the records in `buffer_` have been processed, go to step 2/ if
 *    the last batch had completed with status other than E::UNTIL_LSN_REACHED/
 *    E::WINDOW_END_REACHED.
 *
 * 7/ If the last batch status is E::UNTIL_LSN_REACHED, we have read and
 *    processed until the UNTIL_LSN provided for this Log. If all records
 *    have been stores durable, notify ShardRebuildingV1 that rebuilding
 *    of this log is complete. If not all records are known to
 *    be durable, send a notification about reaching UNTIL_LSN. If the last
 *    batch status is WINDOW_END, we have reached the end of the window. Send a
 *    Window end notification with size of the state machine. Note:
 *    Size will be 0 when rebuilding with WAL, since there will no outstanding
 *    state machines (RecordRebuildingStore/RecordRebuildingAmend)
 *
 * 8/ If a flush notification causes all outstanding records to be stored
 *    durably, based on lastBatchStatus_ (i.e if we reached end of window or
      end of log earlier) send notification to ShardRebuildingV1.
 *
 * This whole process includes a flow control mechanism based on time maintained
 * by ShardRebuildingV1. ShardRebuildingV1 maintains a local timestamp
 * window. Once we reach the end of that window we stop reading until
 * ShardRebuildingV1 wakes us up because it slid the window.
 */

namespace facebook { namespace logdevice {

// RecordDurabilityState holds data required to keep track of
// durability of stores/amends when rebuilding without WAL and for starting
// RecordRebuildingAmend state machine once stores are deemed to be durable.
// LogRebuilding state machine creates RDS for every record. The main motivation
// for using a separate struct (instead of just starting RecordRebuildingAmend
// the state machines) is to limit the amount of memory used while waiting for
// a store/amend to be durable and starting the RecordRebuildingAmend state
// machine only when we are ready to send amends.
struct RecordDurabilityState {
 public:
  explicit RecordDurabilityState(lsn_t l)
      : lsn(l), start_time_(SteadyTimestamp::now()) {}

  // Status defines the durability status of the stores/amends sent out
  // for a single record.
  enum class Status : uint8_t {
    // RecordRebuildingStore finished sending all stores
    // and received a successful response with FlushToken
    // the write was attributed to.
    STORES_NON_DURABLE = 0,
    // All the stores are now known to be durable based on
    // MEMTABLE_FLUSHED messages received.
    STORES_DURABLE,
    // RecordRebuildingAmend finished sending all amends
    // and received a successful response with FlushToken
    // the write was attributed to.
    AMENDS_NON_DURABLE,
    // All the amends are now known to be durable based on
    // MEMTABLE_FLUSHED messages received.
    AMENDS_DURABLE,
    UNKNOWN,
  };

  lsn_t lsn;
  size_t size{0};
  // Holds data required to start a RecordRebuildingAmend state machine
  // Populated by LogRebuilding on a RecordRebuildingStore reaching amend stage
  std::unique_ptr<RecordRebuildingAmendState> rras;
  std::unique_ptr<RecordRebuildingAmend> rra;
  std::unique_ptr<RecordRebuildingStore> rr;
  // When RecordRebuildingStore completes, this holds the memtable id to
  // which stores were attributed to. When RecordRebuildingAmend
  // completes, this holds the memtable id to which amends were
  // attributed to.
  std::unique_ptr<FlushTokenMap> flushTokenMap;
  folly::IntrusiveListHook hook;
  Status status{Status::UNKNOWN};
  // Used to track how long each stage record rebuilding took.
  SteadyTimestamp start_time_;
};

class LogRebuilding : public LogRebuildingInterface,
                      public RecordRebuildingOwner {
 public:
  using ReadPointer = LocalLogStoreReader::ReadPointer;

  /**
   * Create a LogRebuilding state machine for rebuilding a single log.
   *
   * @param owner                ShardRebuildingV1 object that owns this;
   * @param logid                Log id being rebuilt;
   * @param shard                Shard on which we are reading this log;
   * @param rebuilding_settings  Settings;
   * @param iterator_cache       IteratorCache to be used. Can be nullptr.
   */
  LogRebuilding(ShardRebuildingV1Ref owner,
                logid_t logid,
                shard_index_t shard,
                UpdateableSettings<RebuildingSettings> rebuilding_settings,
                std::shared_ptr<IteratorCache> iterator_cache);

  ~LogRebuilding() override {}

  /**
   * @return RebuildingSet version. Returns LSN_INVALID if start() has not been
   * called yet.
   */
  lsn_t getRebuildingVersion() const override {
    return version_;
  }

  /**  @return log_rebuilding_id_t Id that uniquely identifies the run of
   *                               LogRebuilding state machine
   */
  log_rebuilding_id_t getLogRebuildingId() const override {
    return id_;
  }

  /**
   * @return Restart version. Returns LSN_INVALID if start() has not been
   * called yet.
   */
  lsn_t getRestartVersion() const override {
    return restartVersion_;
  }

  ShardRebuildingV1Ref getOwner() const {
    return owner_;
  }

  /**
   * Start rebuilding for node `node_idx`. If this state machine is already
   * active because we are rebuilding for other nodes, abort and restart from
   * the beginning to include `node_idx`.
   *
   * @param rebuilding_set  Nodes to rebuild for.
   * @param until_lsn       Log needs rebuilding up to this LSN (exclusive).
   * @param window_end      High bound of the local timestamp window for the
   *                        log's shard. LogRebuilding must not rebuild records
   *                        with higher timestamps. Currently unused.
   * @pararm version        Rebuilding version. This is the LSN of the last
   *                        event log record that changed the rebuilding set. We
   *                        compare this to version included in the rebuilding
   *                        checkpoint.
   * @param restart_version Last seen record in the event log when we restarted
   *                        rebuilding. This version will be passed back to
   *                        RebuildingCoordinator when we notify it that we
   *                        completed or reached the end of the window.
   */
  void start(std::shared_ptr<const RebuildingSet> rebuilding_set,
             RebuildingPlan plan,
             RecordTimestamp window_end,
             lsn_t version,
             lsn_t restart_version);

  /**
   * Abort this LogRebuilding state machine.
   *
   * @param notify_complete If true, also notify ShardRebuildingV1 that this
   *                        state machine completed.
   */
  void abort(bool notify_complete = true) override;

  /**
   * Slide the window to have the new specified end.
   *
   * @param window_end New end of the window.
   */
  void window(RecordTimestamp window_end);

  /**
   * Update stats regarding skipped records.
   */
  void aggregateFilterStats();

  /**
   * @return Log id this LogRebuilding state machine is for.
   */
  logid_t getLogID() const override {
    return logid_;
  }

  /**
   * @return Set of nodes whose data this LogRebuilding is rebuilding.
   */
  const RebuildingSet& getRebuildingSet() const override {
    return *rebuildingSet_;
  }

  /**
   * Called when a RebuildingReadStorageTask was successfully processed.
   * Populate `buffer_` with records returned by the storage task. Expand this
   * state machine's sliding window within this buffer and start
   * RecordRebuildingStore state machines for each entry in the window.
   */
  void onReadTaskDone(RebuildingReadStorageTask& task) override;
  // Used by tests instead of the other overload.
  void onReadTaskDone(RebuildingReadStorageTask::RecordContainer& records,
                      Status status,
                      ReadPointer& read_ptr,
                      RecordTimestamp ts_window_high,
                      lsn_t restart_version);

  /**
   * Called when a RebuildingReadStorageTask was dropped. Retry reading a batch
   * of records later using a timer.
   */
  void onReadTaskDropped(RebuildingReadStorageTask& task) override;

  /**
   * Add some debugging information about this LogRebuilding state machine to
   * `table`.
   */
  void getDebugInfo(InfoRebuildingLogsTable& table) const;

  virtual ServerInstanceId getServerInstanceId() const override;

  /**
   * Looks up active RecordRebuildingStore object by record LSN.
   * If there's no RecordRebuildingStore for this LSN, returns nullptr.
   */
  RecordRebuildingInterface* findRecordRebuilding(lsn_t lsn) override;

  /**
   * Called when RecordRebuildingStore state machine has sent STORES and
   * received a successful STORED resposnes. Destroys the RecordRebuildingStore
   * state machine
   */
  void
  onAllStoresReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override;

  /**
   * Called when all stores for the given LSN are known to be durable
   */
  void onAllStoresDurable(lsn_t lsn);

  /**
   * Called when RecordRebuildingAmend state machine has send AMENDS and
   * received a successful STORED response. Destroys the RecordRebuildingAmend
   * state machine
   */
  void
  onAllAmendsReceived(lsn_t lsn,
                      std::unique_ptr<FlushTokenMap> flushTokenMap) override;

  /**
   * Called when all Amends for the given LSN are known to be durable
   */
  void onAllAmendsDurable(lsn_t lsn);

  /**
   * Called when RecordRebuildingAmend state machine finds out that
   * the copyset it was given is no longer valid. Copyset can become
   * invalid if a node in copyset is removed the config. Since the payload
   * has already been dropped, we will have to re-read the record to replicate
   * it again. However rewinding the state machine on a single occurrance of
   * such record would be costly. Hence the record is left in the
   * nonDurableRecordList which will ensure that the LogRebuilding state
   * machine is restarted eventually by ShardRebuildingV1 when
   * LogRebuilding reaches the end of the log, if not earlier. (LogRebuilding
   * could be restarted because of hitting memory limits as well)
   */
  void onCopysetInvalid(lsn_t lsn) override;

  /**
   * Starts new RecordRebuildingAmend state machines for LSNs whose
   * stores are known to be durable. Ensures that there is at most
   * `max-amends-in-flight` RecordRebuildingAmend state machines
   * running in parallel.
   */
  void startRecordRebuildingAmends();

  /*
   * Called when this node receives a MEMTABLE_FLUSHED message
   *
   * @param node_index          Index of the node that sent the flushed message
   * @param server_instance_id  Server instance id of the server that sent this
   *                            message
   * @param flushed_upto        FlushToken up to which this node has flushed to
   *                            disk
   */
  void onMemtableFlushed(node_index_t node_index,
                         ServerInstanceId server_instance_id,
                         FlushToken flushed_upto) override;

  /*
   * Called when a node sent a SHUTDOWN message
   *
   * @param node_index          Index of the node that sent the shutdown message
   * @param server_instance_id  Server instance id of the server that sent this
   *                            message
   */
  void onGracefulShutdown(node_index_t node_index,
                          ServerInstanceId server_instance_id) override;

  /**
   * Called when we retrieved the rebuilding checkpoint for log `logid_`.
   *
   * @param status         E::OK if the checkpoint was retrieved or was not
   *                       found (in which case rebuilt_upto=LSN_INVALID).
   *                       Otherwise:
   *                       - LOCAL_LOG_STORE_READ if there was an error reading
   *                         from the local log store;
   *                       - E::DROPPED if the storage task to read checkpoint
   *                       was dropped.
   * @param restart_version Version of this LogRebuilding at the time we issued
   *                        the read storage task to read that checkpoint. If it
   *                        does not match `restartVersion_`, we conclude that
   *                        this task was issued before we restarted the state
   *                        machine.
   * @param version         Rebuilding version this checkpoint is for. Equals
   *                        LSN_INVALID if status != E::OK or the checkpoint was
   *                        not found.
   * @param rebuilt_upto    LSN up to which the log has been rebuilt for
   *                        rebuilding version `version`. Equals LSN_INVALID if
   *                        status != E::OK or the checkpoint was not found.
   * @param trim_point      Trim point of the log. Equals LSN_INVALID if status
   *                        != E::OK, or the trim point could not be found, or
   *                        the LogRebuildingCheckpoint storage task was created
   *                        with read_trim_point=false.
   */
  void onCheckpointRead(Status status,
                        lsn_t restart_version,
                        lsn_t version,
                        lsn_t rebuilt_upto,
                        lsn_t trim_point);

  UpdateableSettings<RebuildingSettings>
  getRebuildingSettings() const override {
    return rebuildingSettings_;
  }

  virtual const Settings& getSettings() const;

  /**
   * Rewinds the state machine by resetting members and starts reading from
   * the lsn after the last known durable lsn (restartLsn)
   */
  void restart();

  ShardID getMyShardID() const;

 protected:
  // The following may be overridden by tests.

  virtual StatsHolder* getStats();

  virtual void putReadStorageTask(const LocalLogStoreReader::ReadContext& ctx);

  /**
   * Issue a storage task to read a checkpoint (if any).
   * When the storage task comes back, it must call onCheckpointRead().
   */
  virtual void readCheckpoint();

  /**
   * Issue a storage task to write a checkpoint.
   *
   * @param rebuilt_upto Last LSN rebuilt for that log.
   */
  virtual void writeCheckpoint(lsn_t rebuilt_upto);

  virtual NodeID getMyNodeID() const;

  /**
   * Notify ShardRebuildingV1 that we completed rebuilding of the log.
   */
  virtual void notifyComplete();

  /**
   * Notify ShardRebuildingV1 that we reached the end of the timestamp
   * window.
   */
  virtual void notifyWindowEnd(RecordTimestamp next);

  /**
   * Notify ShardRebuildingV1 that we processed all records till
   * `until_lsn`
   */
  virtual void notifyReachedUntilLsn();

  /**
   * Notify ShardRebuildingV1 about the updated size.
   */
  virtual void notifyLogRebuildingSize();

  /**
   * Remove ourself from the Worker's map of LogRebuilding state machines.
   */
  virtual void deleteThis();

  virtual std::shared_ptr<ReplicationScheme>
  createReplicationScheme(EpochMetaData metadata, NodeID sequencer_node_id);

  /**
   * Create a RecordRebuildingStore state machine.
   *
   * @param block_id            Current block id
   * @param record              Record to be rebuilt.
   * @param replication_scheme  How to select copysets.
   */
  virtual std::unique_ptr<RecordRebuildingStore> createRecordRebuildingStore(
      size_t block_id,
      RawRecord record,
      std::shared_ptr<ReplicationScheme> replication_scheme);

  /**
   * Create a RecordRebuildingAmend state machine.
   *
   * @param rras RecordRebuildingAmendState which hold the information
   *             necessary to send out amends
   */
  virtual std::unique_ptr<RecordRebuildingAmend>
  createRecordRebuildingAmend(RecordRebuildingAmendState rras);

  /**
   * Start reading a new batch of records, store the records in buffer_.
   * If rebuildingSettings_.use_rocksdb_cache is true, first try
   * reading records from rocksdb's cache on this worker thread.
   */
  virtual void readNewBatch();

  /**
   * Used to create `timer_`. May return nullptr in tests.
   */
  virtual std::unique_ptr<BackoffTimer>
  createTimer(std::function<void()> callback);

  /**
   * Creates a periodic timer to use for iterator invalidation.
   */
  virtual std::unique_ptr<Timer> createIteratorTimer();

  /**
   * Creates a timer to stall rebuilding on reaching the memory
   * threshold
   */
  virtual std::unique_ptr<Timer> createStallTimer();

  /**
   * Activates the stall timer
   */
  virtual void activateStallTimer();

  /**
   * Cancels the stall timer
   */
  virtual void cancelStallTimer();

  /**
   * Return true if stallTimer_ is active
   */
  virtual bool isStallTimerActive();

  /**
   * Creates a timer with zero-timeout to call `notifyRebuildingCoordinator`
   */
  virtual std::unique_ptr<Timer> createNotifyRebuildingCoordinatorTimer();

  /**
   * Activates notifyRebuildingCoordinatorTimer_
   */
  virtual void activateNotifyRebuildingCoordinatorTimer();

  /**
   * Creates a timer to call `readNewBatch`
   */
  virtual std::unique_ptr<Timer> createReadNewBatchTimer();

  /**
   * Activates `readNewBatchTimer_` with zero-timeout.
   * Used to call `readNewBatch` in the next iteration of event loop
   */
  virtual void activateReadNewBatchTimer();

  virtual std::shared_ptr<UpdateableConfig> getConfig() const;

  /**
   * Invalidate the iterators in `iteratorCache_` if they expired.
   */
  virtual void invalidateIterators();

  /**
   * Called once the outstanding records are deemed durable. This method
   * calls appropriate notification method based on the current state
   * of LogRebuilding state machine
   */
  void notifyRebuildingCoordinator();

 private:
  ShardRebuildingV1Ref owner_;

  logid_t logid_;
  shard_index_t shard_;

  // Set of nodes this state machine is rebuilding for.
  std::shared_ptr<const RebuildingSet> rebuildingSet_;

  // Set of epochs that need to be rebuilt.
  RebuildingPlan rebuildingPlan_;

  // Iterator to the epoch interval we are currently reading from.
  RebuildingPlan::epoch_ranges_t::const_iterator curEpochRange_;

  std::shared_ptr<ReplicationScheme> cur_replication_scheme_;

  // Current copyset and record filter. Updated by createReadContext().
  std::shared_ptr<RebuildingReadFilter> filter_;

  // The time span currently being processed.
  RecordTimeIntervals::iterator curDirtyTimeWindow_;

  // Optional timestamp to seek to before reading rebuilding records.
  folly::Optional<RecordTimestamp> seekToTimestamp_;

  // Timestamp of the end of the local timestamp window for the shard of this
  // log as maintained by by ShardRebuildingV1. When we reach records
  // greater to this timestamp, we notify ShardRebuildingV1 so that it knows
  // when to slide the shard's timestamp window.
  RecordTimestamp windowEnd_{RecordTimestamp::min()};

  // Pointer where we left off the last time we read a batch from the local log
  // store.
  ReadPointer readPointer_{LSN_OLDEST};

  lsn_t restartLsn_{LSN_INVALID};

  // Keep track of the state of reading the checkpoint.
  // Reset to NONE on failure (if the storage task is dropped) and timer_ is
  // activated.
  enum class ReadCheckpointState { NONE, READING, DONE };
  ReadCheckpointState readCheckpointState_{ReadCheckpointState::NONE};

  // Updated each time we write a checkpoint.
  lsn_t lastCheckpoint_{LSN_INVALID};

  size_t bytesRebuiltSinceCheckpoint_{0};

  size_t totalPendingAmendSize_{0};

  // When a storage task comes back with E::WINDOW_END_REACHED, we update this
  // value with the timestamp of the next record past the window.
  RecordTimestamp nextTimestamp_{RecordTimestamp::min()};

  // Last batch of records read from the local log store.
  std::vector<RawRecord> buffer_;

  // Index of the first record in buffer which is yet to be processed
  std::vector<RawRecord>::iterator curPosition_;

  // The copyset of the last record we've read.
  std::vector<ShardID> last_seen_copyset_;

  // A permanent buffer for parsing copysets into it and swapping with
  // last_seen_copyset_. Created as a member to avoid allocations on every
  // record
  std::vector<ShardID> last_seen_copyset_candidate_buf_;

  // Whenever the copyset of a record doesn't match the copyset of the previous
  // one, this counter is bumped. It is used to hash the copyset selector's RNG
  // to preserve sticky copyset blocks after rebuilding.
  size_t current_block_id_{0};

  // A counter we keep to split blocks that get too large. Without this blocks
  // that get accidentally "fused" together because they happen to have
  // identical copysets will never get unsplit. In a cluster with a small number
  // of nodes this could lead to very large block sizes after rebuilding a
  // significant part of the cluster, which will create an unbalanced
  // distribution of data.
  size_t bytes_in_current_block_{0};

  // If we encounter too many invalid records, stall rebuilding just in case.
  size_t num_malformed_records_seen_{0};

  // This uniquely identifies a run of this state machine. Updated every time
  // the state machine is restarted. This is used to discard stale STORED
  // responses
  log_rebuilding_id_t id_{LOG_REBUILDING_ID_INVALID};

  static std::atomic<log_rebuilding_id_t::raw_type> next_id;

  // Checks if the copyset has changed compared to the last seen record and
  // bumps current_block_id_ if it has.
  // If record is invalid, sets erro to E::MALFORMED_RECORD and returns -1.
  int checkRecordForBlockChange(const RawRecord& rec);

  // Returns Worker::settings().sticky_copysets_block_size.
  // Overridden in tests
  virtual size_t getMaxBlockSize();

  // Returns the total size of log rebuilding state machine
  virtual size_t getTotalLogRebuildingSize();

  // Returns the maximum amount of memory the LogRebuilding
  // state machine is allowed to use. Reading a new batch will
  // be stalled if the total size exceeds this threshold.
  virtual size_t getLogRebuildingSizeThreshold();

  // Returns true if the stall timer is active and there are no records
  // that aren't durable yet or there are some records that aren't durable
  // but we are below the size threshold and making progress.
  bool shouldCancelStallTimer();

  // Returns true if the rebuilding coordinator should be notified either becase
  // all records have been rebuilt or some records have been reaped causing the
  // memory usage to come down and a size update needs to be sent.
  bool shouldNotifyRebuildingCoordinator();

  // Returns true if all records that were being tracked are now durable
  // or the total size of the log rebuilding state machine is 1/2 of the
  // max allowed size and the head of non durable record list has moved.
  bool isLogRebuildingMakingProgress();

  UpdateableSettings<RebuildingSettings> rebuildingSettings_;

  // Snapshot of rebuildingSettings_->max_records_in_flight, also the size of
  // the `window_' buffer.  It's not safe to use the setting directly; if the
  // setting changes we only update this when we can resize the buffer.
  size_t max_records_in_flight_{0};

  using NodeFlushKey = std::tuple<node_index_t, ServerInstanceId, FlushToken>;

  struct NodeFlushKeyHasher {
    size_t operator()(const NodeFlushKey& key) const {
      return folly::hash::hash_combine(
          std::get<0>(key), std::get<1>(key), std::get<2>(key));
    }
  };

  // A list of LSNs that are waiting for a flush message per
  // {node,server instance,flushtoken} tuple
  std::unordered_map<NodeFlushKey, std::vector<lsn_t>, NodeFlushKeyHasher>
      LsnPendingFlush_;

  using NonDurableRecordList =
      folly::IntrusiveList<RecordDurabilityState, &RecordDurabilityState::hook>;

  // A map of LSN to RecordDurabilityState struct which holds all the
  // information necessary to keep track of record's durability status
  // and starting RecordRebuildingAmend state machines
  std::unordered_map<lsn_t, std::unique_ptr<RecordDurabilityState>>
      recordDurabilityState_;

  // An Intrusive list of RecordDurabilityStates to easily track
  // the first non durable record.
  NonDurableRecordList nonDurableRecordList_;

  struct NodeMemtableFlushStatus {
   public:
    NodeMemtableFlushStatus() {
      flushedUpto_ = FlushToken_INVALID;
      maxFlushToken_ = FlushToken_INVALID;
    }

    NodeMemtableFlushStatus(FlushToken flushedUpto, FlushToken maxFlushToken) {
      flushedUpto_ = flushedUpto;
      maxFlushToken_ = maxFlushToken;
    }
    // The last flushToken upto which the storage node has
    // already flushed. Updated everytime we receive a MEMTABLE_FLUSHED
    // message
    FlushToken flushedUpto_;
    // The max flushToken which this LogRebuilding is interested in
    // Updated based on known FlushTokens received as part of Stores/Amends
    FlushToken maxFlushToken_;
  };

  // A map to keep track of a {Node,ServerInstance} flush status
  std::unordered_map<NodeIndexServerInstancePair,
                     NodeMemtableFlushStatus,
                     NodeIndexServerInstancePairKeyHasher>
      nodeMemtableFlushStatus_;

  // Status of the last batch of records we read from the local log store. When
  // we finish processing the batch, we use this status to determine if we
  // should read another batch, complete the state machine, or wait for
  // ShardRebuildingV1 to slide the shard's timestamp window.
  Status lastBatchStatus_{E::UNKNOWN};

  // Uniquely identifies the global state machine for rebuilding the shard this
  // log belongs to. If the global state machine is rewound because a node is
  // added to the rebuilding set, this value will change.
  // Found checkpoints have a version compared with this member.
  lsn_t version_{LSN_INVALID};

  // This is bumped each time RebuildingCoordinator restarts LogRebuilding state
  // machines.
  // This helps ensure two things:
  // 1/ Any storage task that was in flight before a call to start() will be
  //    discarded when it comes back.
  // 2/ When we issue a request to the owning RebuildingCoordinator, we include
  //    this version number so that it can discard our request if in the mean
  //    time it bumped it.
  lsn_t restartVersion_{LSN_INVALID};

  // These are used for debugging and statistics.
  const SteadyTimestamp creation_time_;
  bool storageTaskInFlight_{false};
  size_t nRecordsSCDFiltered_{0};
  size_t nRecordsNotDirtyFiltered_{0};
  size_t nRecordsDrainedFiltered_{0};
  size_t nRecordsTimestampFiltered_{0};
  size_t numRecordRebuildingAmendsInFlight_{0};
  size_t numRecordRebuildingStoresInFlight_{0};
  size_t numRecordRebuildingPendingStoreDurable_{0};
  size_t numRecordRebuildingPendingAmend_{0};
  size_t numRecordRebuildingPendingAmendDurable_{0};
  size_t nRecordsReplicated_{0};
  size_t nRecordsLateFiltered_{0};
  size_t nBytesLateFiltered_{0};
  size_t nTimeBasedSeeks_{0};
  size_t nBytesReplicated_{0};
  SystemTimestamp startTimestamp_;
  SteadyTimestamp batch_read_start_time_;
  SteadyTimestamp batch_process_start_time_;
  size_t cur_batch_total_size_ = 0;
  size_t cur_window_total_size_ = 0;
  size_t cur_window_num_batches_ = 0;

  // Iterators used for this log rebuilding. Created once and reused.
  // Periodically invalidated (if unused) to avoid pinning resources.
  //
  // If this is set to nullptr, iterators are re-created for every batch.
  std::shared_ptr<IteratorCache> iteratorCache_;

  RebuildingEventsTracer rebuilding_events_tracer_;

  // Timer used to try issuing a read storage task again after some time
  // following a failure to do so because the storage task was dropped or a
  // storage task coming back with E::FAILED.
  std::unique_ptr<BackoffTimer> timer_;

  // Timer used to periodically invalidate the iterators inside
  // `iteratorCache_` so that they don't pin memtables for too long.
  // This timer is only initialized if iteratorCache_ is not null.
  std::unique_ptr<Timer> iteratorInvalidationTimer_;

  // Timer used to stall rebuilding when we are ready to read a new batch
  // but have hit the size limit
  std::unique_ptr<Timer> stallTimer_;

  // Timer used to call notifyRebuildingCoordinator in the next iteration of
  // event loop to prevent recursive calls
  std::unique_ptr<Timer> notifyRebuildingCoordinatorTimer_;

  // Timer used to call `readNewBatch` in the next iteration of the event loop
  std::unique_ptr<Timer> readNewBatchTimer_;

  // A queue of lsns for which stores are durable
  std::queue<lsn_t> lsnStoresDurable_;

  bool processingDurableStores_{false};

  void addCurWindowTotalSizeToHistogram();

  void addCurBatchTotalSizeToHistogram();

  void publishTrace(const std::string& status);

  /**
   * Return the max record timestamp that the current storage task can read
   * through.  This is the min of the local window as specified by the
   * ShardRebuildingV1 and the upper bound of the current dirty time
   * range we are processing.
   */
  RecordTimestamp getMaxStorageTaskTimestamp() const {
    return std::min(windowEnd_, curDirtyTimeWindow_->upper());
  }

  /**
   * Return the max record timestamp that the state machine can read through.
   * This is the min of the local window as specified by the
   * ShardRebuildingV1 and the upper bound of the last (most recent in
   * time) dirty time range.
   */
  RecordTimestamp getMaxTimestamp() const {
    auto last_dirty_range = --rebuildingSet_->all_dirty_time_intervals.end();
    return std::min(windowEnd_, last_dirty_range->upper());
  }

  /**
   * Called once we finished reading the checkpoint and the metadata.
   * Initialize curEpochRange_ and read the first batch.
   */
  void startReading();

  /**
   * Called when we finish processing all records in `buffer_`.
   * Decide what to do depending on the status returned when reading the last
   * batch: either read another batch immediately, complete the state machine,
   * or wait for ShardRebuildingV1 to slide the shard's timestamp window.
   */
  void onBatchEnd();

  /**
   * Start processing the records in the batch we just read.
   *
   * @param status         Status after reading the batch.
   * @param read_ptr       Read pointer after reading the batch.
   * @param ts_window_high if status == E::WINDOW_END_REACHED, timestamp of the
   *                       first record to be read in the next batch.
   */
  void startProcessingBatch(Status status,
                            ReadPointer& read_ptr,
                            RecordTimestamp ts_window_high);

  /**
   * Create and start `numRecordRebuildingToStart`  RecordRebuildingStore state
   * machines. This is called every time stores are received for a previous
   * LSN.
   */
  void startRecordRebuildingStores(int numRecordRebuildingToStart = 1);

  /**
   * Issue a storage task to write a checkpoint for the progress that has
   * currently been made.
   * LSN of the checkpoint is the LSN upto which all records have been rebuilt.
   * (i.e stores and amends are confirmed to be durable). `restartLsn_` keeps
   * track of the first non durable LSN. So the checkpoint is `restartLsn_`
   * minus one when it is valid.
   */
  void writeCheckpoint();

  /**
   * Helper function for creating a ReadContext for LocalLogStoreReader::read.
   */
  LocalLogStoreReader::ReadContext createReadContext();

  /**
   * Called once we determined we rebuilt all records for this log.
   * Notify ShardRebuildingV1 and remove ourself from the Worker's
   * `Worker::runningLogRebuildings()` map.
   */
  void onComplete(bool write_checkpoint = true);

  /**
   * `timer_`'s callback. May be called because we failed to read the metadata
   * log, read the checkpoint (if any) or issue a RebuildingReadStorageTask.
   */
  void timerCallback();

  /**
   * Mark all nodes in the rebuilding set as not available to receive copies.
   *
   * While a storage node is rebuilding, it replies to STORE messages with
   * STORED(status=E::DISABLED). We mark these recipients as unavailable so that
   * RecordRebuildingStore does not try to store copies on them.
   */
  void markNodesInRebuildingSetNotAvailable(ReplicationScheme& replication);

  /**
   * Helper function that either fetches the iterator from cache or creates
   * a new one.
   */
  std::shared_ptr<LocalLogStore::ReadIterator>
  getIterator(const LocalLogStore::ReadOptions& options);

  /**
   * Registers the LSN for memtable flush tracking and updates the
   * RecordDurabilityState's status based on the flush info.
   *
   */
  void registerAndUpdateFlushStatus(RecordDurabilityState* rds);

  /**
   * Transitions the RecordDurabilityState status based on
   * current flush info and status
   */
  void updateFlushStatus(RecordDurabilityState* rds);

  friend class LogRebuildingTest;
};

/**
 * Request that ShardRebuildingV1 uses to create and start a LogRebuilding
 * state machine on a worker.
 */
class StartLogRebuildingRequest : public Request {
 public:
  StartLogRebuildingRequest(
      ShardRebuildingV1Ref owner,
      int worker_id,
      logid_t logid,
      shard_index_t shard,
      UpdateableSettings<RebuildingSettings> rebuilding_settings,
      std::shared_ptr<const RebuildingSet> rebuilding_set,
      RebuildingPlan rebuilding_plan,
      RecordTimestamp max_timestamp,
      lsn_t version,
      lsn_t restart_version)
      : Request(RequestType::START_LOG_REBUILDING),
        owner_(owner),
        workerId_(worker_id),
        logid_(logid),
        shard_(shard),
        rebuildingSettings_(rebuilding_settings),
        rebuildingSet_(rebuilding_set),
        rebuildingPlan_(std::move(rebuilding_plan)),
        windowEnd_(max_timestamp),
        version_(version),
        restartVersion_(restart_version) {}
  ~StartLogRebuildingRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*nthreads*/) override {
    return workerId_;
  }

 private:
  ShardRebuildingV1Ref owner_;
  int workerId_;
  logid_t logid_;
  shard_index_t shard_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;
  std::shared_ptr<const RebuildingSet> rebuildingSet_;
  RebuildingPlan rebuildingPlan_;
  RecordTimestamp windowEnd_;
  lsn_t version_;
  lsn_t restartVersion_;
};

/**
 * Request that ShardRebuildingV1 uses to abort a LogRebuilding state
 * machine on a worker.
 */
class AbortLogRebuildingRequest : public Request {
 public:
  AbortLogRebuildingRequest(int worker_id,
                            logid_t logid,
                            shard_index_t shard,
                            lsn_t restart_version)
      : Request(RequestType::ABORT_LOG_REBUILDING),
        workerId_(worker_id),
        logid_(logid),
        shard_(shard),
        restartVersion_(restart_version) {}
  ~AbortLogRebuildingRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*nthreads*/) override {
    return workerId_;
  }

 private:
  int workerId_;
  logid_t logid_;
  shard_index_t shard_;
  lsn_t restartVersion_;
};

/**
 * Request that ShardRebuildingV1 uses to rewind a LogRebuilding state
 * machine on a worker.
 */
class RestartLogRebuildingRequest : public Request {
 public:
  RestartLogRebuildingRequest(int worker_id,
                              logid_t logid,
                              shard_index_t shard,
                              lsn_t restart_version)
      : Request(RequestType::RESTART_LOG_REBUILDING),
        workerId_(worker_id),
        logid_(logid),
        shard_(shard),
        restartVersion_(restart_version) {}
  ~RestartLogRebuildingRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*unused*/) override {
    return workerId_;
  }

 private:
  int workerId_;
  logid_t logid_;
  shard_index_t shard_;
  lsn_t restartVersion_;
};

class LogRebuildingMoveWindowRequest : public Request {
 public:
  LogRebuildingMoveWindowRequest(int worker_id,
                                 logid_t logid,
                                 shard_index_t shard,
                                 RecordTimestamp window_end,
                                 lsn_t restart_version)
      : Request(RequestType::LOG_REBUILDING_NEW_WINDOW),
        workerId_(worker_id),
        logid_(logid),
        shard_(shard),
        windowEnd_(window_end),
        restartVersion_(restart_version) {}
  ~LogRebuildingMoveWindowRequest() override {}
  Execution execute() override;
  int getThreadAffinity(int /*nthreads*/) override {
    return workerId_;
  }

 private:
  int workerId_;
  logid_t logid_;
  shard_index_t shard_;
  RecordTimestamp windowEnd_;
  lsn_t restartVersion_;
};

}} // namespace facebook::logdevice
