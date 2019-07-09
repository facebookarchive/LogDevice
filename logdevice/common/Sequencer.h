/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include <folly/SharedMutex.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/AtomicOptional.h"
#include "logdevice/common/EpochMetaDataMap.h"
#include "logdevice/common/RateEstimator.h"
#include "logdevice/common/RateLimiter.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/SequencerMetaDataLogManager.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/LogTailAttributes.h"

namespace facebook { namespace logdevice {

class AllSequencers;
class Configuration;
class CopySetManager;
class EpochSequencer;
struct EpochSequencerImmutableOptions;
struct ExponentialBackoffTimerNode;
class MetaDataLogWriter;
class PeriodicReleases;
class Processor;
enum class ReleaseType : uint8_t;
class StatsHolder;
struct NodeID;
class RecoveredLSNs;

/**
 * @file  a Sequencer is an object that manages writing, releasing records and
 *        failover recovery of a log in LogDevice.
 *
 *        TODO 7467469: more description
 *
 *        --- Note on graceful reactivation ---
 *
 *        Graceful Reactivation is a special case of sequencer activation in
 *        Logdevice. In graceful reactivation, a sequencer is reactivated
 *        while still being the active sequencer for the log and there is no
 *        failover or preemption scenario involved. Example of graceful
 *        sequencer reactivation include:
 *          (1) Reactivating sequencer to get new metadata when
 *              nodeset/replication_factor is changed.
 *          (2) Sequencer running out of ESNs needs to get a new epoch number
 *
 *       In these situations, if we perform full log recovery immediately after
 *       the sequencer activated to a new epoch, there is a risk of
 *       `self-preemption` for appenders still running on the previous epoch.
 *       The scenario is that these appends may be preempted by the log recovery
 *       of the same sequencer sealing storage nodes and get redirected by the
 *       client library and later successfully stored in the new epoch and
 *       acknowledges the client, while the partially stored records in the
 *       previous epoch may be re-replicated during recovery, making it possible
 *       to sliently duplicate a record that the client is unaware of.
 *
 *       Moreover, there is no need to perform fully recovery in graceful
 *       reactivation when there is no failover/preemption happened. As long
 *       as the remaining appenders in the previous epochs are fully stored,
 *       it is safe to consider the previous epoch is clean. We call the epoch
 *       `quiescent` as it has not yet marked as clean elsewhere. All we have to
 *       do is to mark the epoch clean in storage nodes and then in epoch store.
 *       It is still necessary to ensure the epoch is marked as clean for
 *       f-majority of storage nodes in the nodeset of previous epoch.
 *       Otherwise purging may delete legit records. As soon as the epoch is
 *       marked as clean on storage nodes and the epoch store, we can advance
 *       last release lsn to the next epoch just as the normal log recovery
 *       finishes.
 *
 *       With per-epoch sequencers, graceful reactivation involves starting to
 *       drain the then running epoch when reactivation happens, and waiting for
 *       the draining epoch to be quiescent before starting the graceful
 *       reactivation completion procedure stated above.
 *
 *       TODO: Currently the full log recovery is used as the graceful
 *       reactivation completion procedure, while recovery is still delayed
 *       after the last appender in the previous epoch is reaped to prevent
 *       self-preemption and hopefully making digest and mutation a no-op.
 *       Will implement the simplified completion if there are performance
 *       problems with graceful reactivation.
 */

// activation result when sequencer finishes its activation with metadata gotten
// from the epoch store. see Sequencer::completeActivationWithMetaData().
enum class ActivateResult {
  // activation success, log recovery should be started immediately
  RECOVERY,

  // activation success, and the sequencer also started draining the
  // previous epoch, log recovery should be deferred until the draining
  // is done
  GRACEFUL_DRAINING,

  // activation failed
  FAILED
};

class Sequencer {
 public:
  // A Sequencer object can be in one of the following states:
  enum class State : uint8_t {
    // Sequencer has not yet gotten a valid epoch metadata with an epoch number.
    // It is unable to accept appends and assign LSNs in this state. This is
    // also the state when Sequencer is first created.
    UNAVAILABLE,

    // Sequencer has initiated its activation procedure (i.e., by calling
    // startActivation()) and is in the process of getting epoch number and
    // metadata from the epoch store. Note that the sequencer can still accept
    // appends in this state _if_ it is running a valid epoch before activation.
    ACTIVATING,

    // Sequencer has been assigned an epoch number and has gotten valid epoch
    // metadata, which can be used for stamping and replicating records.
    ACTIVE,

    // Sequencer has detected the presence of another node running a sequencer
    // for the same log_id but with higher epoch. The sequencer is considered
    // as preempted. Note that in this state sequencer must have a valid
    // Seal record (NodeID, preemption_epoch pair) from the preemptor. Appenders
    // will either be redirected to the preemptor or trigger the reactivation
    // of the sequencer to be processed.
    PREEMPTED,

    // Sequencer must not be used because of a permanent process-wide error,
    // such as running out of ephemeral ports. The process should give up
    // sequencing for this log, and likely for all the other logs.
    PERMANENT_ERROR
  };

  /**
   * Create a Sequencer in the UNAVAILABLE state. The caller is
   * expected to insert the Sequencer into a Processor.allSequencers() right
   * after creation.
   *
   * @param log_id          log that this Sequencer will manage
   * @param settings        various settings that Sequencer will use throughout
   *                        its lifetime
   * @param stats           StatsHolder object for collecting stats, can be
   *                        nullptr
   */
  Sequencer(logid_t log_id,
            UpdateableSettings<Settings> settings,
            StatsHolder* stats,
            AllSequencers* parent = nullptr);

  virtual ~Sequencer();

  ///////////////////// Activation Workflow //////////////////

  // return 0 for success, -1 for failure
  using GetMetaDataFunc = std::function<int(logid_t)>;
  // return true if activation is allowed, false otherwise
  using ActivationPred = std::function<bool(const Sequencer&)>;

  /**
   * Initiate the activation procedure for the Sequencer, requesting epoch
   * number and metadata from the epoch store.
   *
   * @param metadata_func   A functor used to obtain epoch metadata
   *                        from the epoch store
   *
   * @param pred            predicate functor for determining whether
   *                        activation should proceed
   *
   * @return                0 for success, -1 for failure, and set err to:
   *                         INPROGRESS  Sequencer is already activating
   *                         SYSLIMIT    Sequencer is in permanent error state
   *                         TOOMANY     number of activations has exceeded
   *                                     the ratelimit
   *                         ABORTED     failed the activation predicate check
   *                         FAILED      metadata_func returns error
   */
  int startActivation(GetMetaDataFunc metadata_func,
                      ActivationPred pred = nullptr);

  /**
   * Called once the Sequencer has gotten the epoch metadata from epoch store
   * for activation, finish the activation process, advancing the Sequencer to
   * a new epoch with nodeset and replication properties provided by the given
   * EpochMetaData.
   *
   * @param epoch      the epoch number gotten from the epoch store
   * @param info       EpochMetaData gotten from the epoch store along with
   *                   epoch
   * @return           see ActivateResult
   */
  ActivateResult
  completeActivationWithMetaData(epoch_t epoch,
                                 const std::shared_ptr<Configuration>& cfg,
                                 std::unique_ptr<EpochMetaData> info);

  /**
   * Called when one of the epoch of the log has finished draining, either
   * successfully or not. Depending on the current epoch and the outcome
   * of the draining, the sequencer may perform _one_ of the following:
   * 1) start graceful reactivaton completion procedure; 2) start log recovery
   * procedure; 3) do nothing.
   *
   * @param epoch           epoch that finished draining attempt
   * @param drain_status    completion status of the drain, E::OK indicates
   *                        the epoch is successfuly drained, other status
   *                        code (e.g., E::TIMEDOUT, E::FAILED) indicates
   *                        unsuccessful.
   */
  void noteDrainingCompleted(epoch_t epoch, Status drain_status);

  /**
   * Put the Sequencer into PERMANENT_ERROR state. All epochs (current,
   * draining) will be evicted and the Sequencer can never be reactivated
   * again nor process any new appends.
   */
  void onPermanentError();

  /**
   * Called when the Sequencer activation workflow encountered a transient
   * error. Transition the Sequencer from ACTIVATING back to the original
   * state before activation was initiated.
   */
  void onActivationFailed();

  /**
   * Called when the result of a previous requested historical metadata
   * (i.e., getHistoricalMetaData) is available. Sequencer will update its
   * metadata map when the result is up-to-date.
   *
   * @param request_epoch    the `current' epoch of Sequencer when the request
   *                         was made
   *
   * @return                 true iff a retry for getting historical metadata
   *                         is needed. false if no retry is needed (e.g.,
   *                         successfully gotten metadata, or stale callback)
   */
  bool onHistoricalMetaData(
      Status status,
      epoch_t request_epoch,
      std::shared_ptr<const EpochMetaDataMap::Map> historical_metadata);

  /**
   * If epoch of the current sequencer is equal to the the given epoch,
   * updates nodeset_params in the sequencer's EpochMetadata
   * (EpochSequencer::setNodeSetParams()).
   * @return  true if the change was made, false if the epoch is not current or
   *          if params already had this value.
   */
  bool setNodeSetParamsInCurrentEpoch(epoch_t epoch,
                                      EpochMetaData::NodeSetParams params);

  ////////////////////// Appender //////////////////////////

  /**
   * Attempt to start an Appender in the current epoch. If the Appender is
   * accepted, it will be assinged a valid LSN in the current epoch and will
   * be started.
   *
   * See also EpochSequencer::runAppender().
   *
   * @return See RunAppenderStatus.  If ERROR_DELETE is returned, sets err to:
   *    NOSEQUENCER   Sequencer is not running a valid current epoch
   *    INPROGRESS    Sequencer is not running a valid current epoch but it is
   *                  in the process of activation
   *    INVALID_PARAM appender is nullptr or already running
   *                  (debug build asserts)
   *    NOBUFS        too many append requests are already in flight
   *    TEMPLIMIT     hard limit on the total size of appenders was reached
   *    TOOBIG        ESNs for the current epoch have been exhausted
   *    STALE         client expects a sequencer with a higher epoch
   *    SYSLIMIT      system-wide resource limit has been reached
   */
  RunAppenderStatus runAppender(Appender* appender);

  /**
   * Called when an Appender is reaped in its epoch and increases the last
   * known good esn of the epoch. Attempt to update last released LSN of the
   * Sequencer.
   *
   * @param reaped_lsn
   * @param last_released_epoch_out    See EpochSequencer::noteAppenderReaperd()
   * @return
   */
  bool noteAppenderReaped(lsn_t reaped_lsn, epoch_t* last_released_epoch_out);

  ///////////////////// Preemption And Redirect /////////////////////////

  /**
   * Called by Appender or LogRecoveryRequest when this sequencer has been
   * detected as preempted for `epoch'. Possibly updates preempted_epoch_ and
   * transition the Sequencer into PREEMPTED state.
   *
   * @param epoch         preempted epoch number
   * @param preempted_by  node that preempted the sequencer
   */
  virtual void notePreempted(epoch_t epoch, NodeID preempted_by);

  /**
   * Check if this Sequencer has been preempted for an epoch.
   *
   * @param epoch  an epoch number to check
   * @return       NodeID of the sequencer which preempted this one for `epoch',
   *               or an invalid NodeID if this sequencer hasn't been preempted
   */
  NodeID checkIfPreempted(epoch_t epoch) const;

  /**
   * @return  if this Sequencer has been preempted for the _current_ epoch
   */
  bool isPreempted() const;

  /**
   * Get Seal record for this sequencer.
   * Used for debugging purposes only.
   */
  Seal getSealRecord() const;

  /**
   * @return   true if Sequencer should handle Appends without redirecting.
   *           see no_redirect_until_.
   */
  bool checkNoRedirectUntil() const;

  /**
   * @return   set the duration for which Sequencer should not redirect when
   *           handling appends
   */
  void setNoRedirectUntil(std::chrono::steady_clock::duration duration);

  /**
   * Clear the currently set no redirect duration
   */
  void clearNoRedirectUntil();

  /////////////////////// LogRecovery ///////////////////////

  /**
   * Specify a particular worker thread to execute recovery.
   *
   * @return           index of the worker thread, or
   *                   -1 for not specifying the target worker. Noted that
   *                   in such case recovery thread affinity is still not
   *                   random, but a mapping from log_id_ to worker thread id
   */
  virtual worker_id_t recoveryWorkerThread() {
    return worker_id_t(-1);
  }

  /**
   * Initiate recovery for this log.
   *
   * @param delay          if non-zero, request execution will be deferred by
   *                       this much time
   * @return               0 on success (recovery request was posted), -1 if
   *                       Processor::postWithRetrying() failed. err is set to
   *                           SHUTDOWN  if the Processor is shutting down
   *                           INTERNAL  if Processor failed to post a
   *                                     request (debug build asserts)
   */
  virtual int startRecovery(
      std::chrono::milliseconds delay = std::chrono::milliseconds::zero());

  /**
   * Initiate get-trim-point request for this log.
   */
  virtual void startGetTrimPointRequest();

  /**
   * update the local copy of trim point if new value is larger.
   * this function is called in common/GetTrimRequest.cpp
   */
  void updateTrimPoint(Status status, lsn_t trim_point);

  /**
   * Check whether the log is empty, by comparing the currently known trim
   * point to the tail.
   * @return        the resulting status and whether the log was empty. Status
   *                is one of:
   *   OK         - Call succeeded
   *   AGAIN      - recovery is in progress, or trim point is not yet available
   */
  std::pair<Status, bool> isLogEmpty();

  /**
   * @return  if log recovery has completed for the current epoch
   */
  bool isRecoveryComplete() const;

  /**
   * Called when recovery procedure for this sequencer has completed. This
   * method updates last_released_ if recovery was successful. Otherwise,
   * it deactivates this sequencer. Called on a worker thread performing
   * recovery for logid_.
   *
   * @param status  status of the recovery operation. Must be one of:
   *   OK         - recovery completed successfully
   *   PREEMPTED  - recovery was aborted because a later Sequencer instance
   *                sealed a strand of the log through a higher epoch number
   *   NOTINCONFIG - we found out during recovery that this log is no longer
   *                 in the cluster config
   *   BADMSG     - recovery was aborted because we received a malformed
   *                SEALED message
   *
   * @param epoch  the current epoch of the sequencer when the log recovery
   *               was started. It is also the `next_epoch' of the
   *               LogRecoveryRequest
   *
   * @param previous_epoch_tail     tail record of next_epoch-1 computed by the
   *                                log recovery
   */
  virtual void
  onRecoveryCompleted(Status status,
                      epoch_t epoch,
                      TailRecord previous_epoch_tail,
                      std::unique_ptr<const RecoveredLSNs> recovered_lsns);

  /**
   * To avoid duplicate records, if we receive a redirected message during
   * recovery that has an LSN from that previous append attempt, we hold it
   * until recovery is complete.  This method then handles all such messages: if
   * we recovered the original message, we simply return the old LSN.  If not,
   * we create a new Appender for it and return the new LSN.
   */
  virtual void processRedirectedRecords();

  /**
   * Process just the records on a single worker.
   */
  void processRedirectedRecords(Status, logid_t logid);

  //////////////////// Releases and Periodical Releases //////////////////////

  using SendReleasesPred = std::function<bool(lsn_t, ReleaseType, ShardID)>;

  /**
   * Send a RELEASE message to every storage node that passes the predicate.
   * This function always tries to send to every destination node, even if some
   * sends fail.
   *
   * Will not check whether the lsn exceeds the last_released or LNG of the log.
   * This check is the responsibility of the caller.
   *
   * @param lsn           LSN of the release message. Forms the record id
   *                      together with the log id of this sequencer.
   * @param release_type  Type of release message to send.
   * @param pred          Optional predicate to pass. We only send a message
   *                      to storage nodes for which pred returns true. If
   *                      pred is an empty function, all storage shard pass.
   * @return              0 if all sends are successful. -1 if 1) at least one
   *                      send failed or 2) the epoch metadata for the given lsn
   *                      and release_type is not available or 3) historical
   *                      epoch metadata is not available. Will set err to
   *                      E::AGAIN in case 2) and 3).
   *
   * @seealso epochMetaDataAvailable
   */
  virtual int sendReleases(lsn_t lsn,
                           ReleaseType release_type,
                           const SendReleasesPred& pred = nullptr);
  /**
   * Attempts to send a RELEASE message (with last released LSN equal to
   * last_released_) to every storage node in the cluster. Called after
   * recovery is done.
   */
  void sendReleases();

  /**
   * Checks whether the EpochMetaData of the given epoch is known to be
   * available for reading. This determines whether it is safe to send a
   * per-epoch RELEASE message for the epoch.
   */
  bool epochMetaDataAvailable(epoch_t epoch) const;

  /**
   * Called whenever the recovery procedure for this sequencer has read some
   * EpochMetaData. Required by the sequencer so it can keep track of which
   * epochs have their metadata available for reads. (Readers cannot safely read
   * from an epoch before the EpochMetaData of their effective-since epoch has
   * been written, because nodesets may change between epochs.)
   *
   * @param effective_since  effective_since field of the EpochMetaData that
   *                         was read or is known to be readable.
   *
   * @seealso epochMetaDataAvailable
   */
  void onSequencerMetaDataRead(epoch_t effective_since);

  /**
   * Mark that a release message was successfully sent to the specified
   * shard.
   */
  virtual void noteReleaseSuccessful(ShardID shard,
                                     lsn_t released_lsn,
                                     ReleaseType release_type);

  /**
   * Ensure that RELEASE messages are periodically sent to all storage nodes
   * in the cluster.
   */
  virtual void schedulePeriodicReleases();

  virtual void startPeriodicReleasesBroadcast();

  void onStorageNodeDisconnect(node_index_t node_idx);

  /**
   * Notify periodic release about historical storage set changes.
   */
  void onMetaDataMapUpdate(
      const std::shared_ptr<const EpochMetaDataMap>& historical_metadata);

  ////////////////////////// Get Log Property ////////////////////////////

  folly::Optional<lsn_t> getTrimPoint() const {
    folly::Optional<lsn_t> result; // initially empty
    if (trim_point_.hasValue()) {
      result.assign(trim_point_.load());
    }
    return result;
  }

  logid_t getLogID() const {
    return log_id_;
  }

  /**
   * @return   the current epoch managed by the Sequencer. If the Sequencer has
   *           not yet gotten a valid epoch OR is in PERMANENT_ERROR state,
   *           returns EPOCH_INVALID.
   */
  epoch_t getCurrentEpoch() const {
    return epoch_t(current_epoch_.load());
  }

  State getState() const {
    return state_.load();
  }

  /**
   * @return sequencer number that will be assigned to the next record
   */
  lsn_t getNextLSN() const;

  /**
   * Checks if there are enough sequence numbers available in the current
   * epoch. If false, runAppend() will fail with TOOBIG and the sequencer
   * will have to be reactivated in order to accept more writes.
   */
  bool hasAvailableLsns() const;

  /**
   * @return   number of appends in flight (current size of window of
   *           appenders) in the current epoch. Note that this does not account
   *           for Appenders running in draining or other epochs.
   */
  size_t getNumAppendsInFlight() const;

  /**
   * @return    the sliding window capacity of the current epoch.
   *            0 if there is no active valid current epoch of the Sequencer.
   */
  size_t getMaxWindowSize() const;

  /**
   * Returns the ImmitableOptions of the current epoch sequencer.
   * If there's no epoch sequencer, returns false.
   */
  folly::Optional<EpochSequencerImmutableOptions>
  getEpochSequencerOptions() const;

  /**
   * @return  Last known good LSN of current epoch. LSN_INVALID if there is
   *          no valid current epoch. see EpochSequencer::getLastKnownGood().
   */
  size_t getLastKnownGood() const;

  /**
   * @return numerically highest LSN such that all records up to and including
   *         that LSN can be safely delivered to the readers of this log.
   */
  lsn_t getLastReleased() const {
    return last_released_.load();
  }

  /**
   * @return the number of milliseconds since the last append executed by this
   * sequencer, or std::chrono::milliseconds::max() if sequencer didn't execute
   * any appends yet.
   */
  std::chrono::milliseconds getTimeSinceLastAppend() const;

  /**
   * @return timestamp of the last state change.
   * Used for debugging purposes only.
   */
  std::chrono::steady_clock::duration getLastStateChangeTimestamp() const {
    return last_state_change_timestamp_;
  }

  /**
   * @return  current draining epoch. EPOCH_INVALID if no draining is going on.
   */
  epoch_t getDrainingEpoch() const;

  /**
   * @return a human-readable string with the name of state _st_
   */
  static const char* stateString(State st);

  /**
   * @return a human-readable string with the given ActivateResult
   */
  static const char* activateResultToString(ActivateResult result);

  bool active() const {
    return state_ == State::ACTIVE;
  }

  /**
   * @return   copyset mananger for the current epoch, nullptr if the sequencer
   *           does not have a valid current epoch
   */
  std::shared_ptr<CopySetManager> getCurrentCopySetManager() const;

  /**
   * @return   epoch metadata for the current epoch, nullptr if the sequencer
   *           does not have a valid current epoch
   */
  std::shared_ptr<const EpochMetaData> getCurrentMetaData() const;

  /**
   * @return  get the epoch metadata map which contains historical and
   *          current epoch metadata up to certain effective until epoch.
   *
   *          Note: currently it may return nullptr or stale metadata (i.e.,
   *          effective_until < current epoch) as historical metadata is
   *          not stored in epoch store.
   *
   *          TODO T23464964: address this problem
   */
  std::shared_ptr<const EpochMetaDataMap> getMetaDataMap() const;

  ///////////// offsets and tail attributes ///////////////////

  /**
   * Retrieve the tail of the log.
   *
   * @return    the tail of the log from the perspective of this Sequencer,
   *            in the form of TailRecord. Note that:
   *            1) only return a valid tail record if log recovery is completed;
   *               otherwise nullptr is returned;
   *            2) the return tail record should have accumulated offsets
   *               instead of the within epoch one.
   *            3) for tail optimized logs, the tail record is likely to include
   *               the payload but the payload could be unavailable for a while
   *               after a sequencer failover
   */
  std::shared_ptr<const TailRecord> getTailRecord() const;

  /**
   * @return    the accumulative, epoch-end OffsetMap of the previous epoch.
   *            it is invalid if the information is not available (e.g.,
   *            byte offsets not enabled, log recovery not completed). Counter
   *            BYTE_OFFSET is invalid if its value is BYTE_OFFSET_INVALID
   *            To have access to other counters, enable_offset_map should be
   *            set from settings.
   */
  OffsetMap getEpochOffsetMap() const;

  ///////////// Log Provision and MetaData Log /////////////////////

  virtual MetaDataLogWriter* getMetaDataLogWriter() const;

  ////////////////////// Others /////////////////////////////////

  virtual Processor* getProcessor() const;

  /**
   * Passes the config and the bool that says whether this node is a sequencer
   * node
   */
  void noteConfigurationChanged(
      std::shared_ptr<Configuration> cfg,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      bool is_sequencer_node);

  /**
   * Perform graceful shutdown by setting the sequencer to UNAVAILABLE state.
   * Called during Processor shutdown when Appenders are already drained. The
   * main purpose is to release any tail record that must be freed on worker
   * threads.
   */
  void shutdown();

  /**
   * Check the effective nodeset of the _current_ epoch for write availability.
   * See EpochSequencer::checkNodeSet() for details.
   */
  bool checkNodeSet() const;

  // expose settings
  const Settings& settings() const {
    return *settings_.get();
  }

  // Returns average append throughput in some window.
  // More precisely, returns pair p meaning that during the last p.second
  // milliseconds we've got p.first bytes of payloads.
  // Size of the time window is approximately Settings.nodeset_adjustment_period
  // but can be smaller or greater if the setting changed recently or if the
  // sequencer was only recently activated.
  std::pair<int64_t, std::chrono::milliseconds> appendRateEstimate() const;

  // timer callback for the epoch draining timer
  static void onDrainingTimerExpired(logid_t log_id,
                                     epoch_t draining,
                                     ExponentialBackoffTimerNode* node);

  void onNodeIsolated();

 public:
  // the following items should be private but made public for testing only

  // Defines actions to be performed when an epoch finished its draining.
  enum class DrainedAction {
    // nothing to do
    NONE,
    // start graceful reactivation completion
    GRACEFUL_COMPLETION,
    // start full-fledged log recovery
    RECOVERY
  };

  // Modes for getting historical metadata
  enum class GetHistoricalMetaDataMode { IMMEDIATE, PERIODIC };

  // exposes epoch sequencers for testing, pair.first is the current epoch
  // sequencer, while pair.second is the draining epoch sequencer
  std::pair<std::shared_ptr<EpochSequencer>, std::shared_ptr<EpochSequencer>>
  getEpochSequencers() const;

  // this is called from admin command to deactivate the sequencer
  void deactivateSequencer();

 protected:
  // create an EpochSequencer object for a given _epoch_ with _metadata_ for
  // replicaiton properties
  virtual std::shared_ptr<EpochSequencer>
  createEpochSequencer(epoch_t epoch, std::unique_ptr<EpochMetaData> metadata);

  virtual std::shared_ptr<Configuration> getClusterConfig() const;

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  // start a timer when Sequencer starts draining for an epoch
  virtual void startDrainingTimer(epoch_t draining);

  // perform the completion action when an epoch finishes draining
  virtual void finalizeDraining(DrainedAction action);

  // read historical metadata from the metadata log
  virtual void getHistoricalMetaData(GetHistoricalMetaDataMode mode);

 private:
  // EpochSequencers currently maintained by the Sequencer, one for the
  // `current' epoch, the other for `draining' epoch. Updated atomically.
  // Note that currently there are two cases where we could evict the `current'
  // epoch sequencer without replacing it with a valid one:
  // 1) logid is removed from the config file
  // 2) sequencer gets into permanent error state
  // Otherwise we do not evict the current epoch sequencer without
  // replacing it (i.e., reactivation)
  struct EpochSequencers {
    std::shared_ptr<EpochSequencer> current;
    std::shared_ptr<EpochSequencer> draining;
  };

  // id of log managed by this sequencer
  logid_t log_id_;

  // indicate if GetTrimPointRequest has been running or not
  std::atomic<bool> get_trim_point_running_{false};
  AtomicOptional<lsn_t> trim_point_{LSN_INVALID, EMPTY_OPTIONAL};

  UpdateableSettings<Settings> settings_;

  StatsHolder* const stats_;

  // nullptr for metadata logs and in unit tests.
  AllSequencers* const parent_;

  // current state of this Sequencer, initialized to UNAVAILABLE
  std::atomic<State> state_{State::UNAVAILABLE};

  // Timestamp of the last time the state was changed.
  // Used for debugging purposes.
  std::chrono::steady_clock::duration last_state_change_timestamp_;
  // serializes state_ changes
  mutable folly::SharedMutex state_mutex_;

  // epoch sequencers. initialized with {nullptr, nullptr};
  UpdateableSharedPtr<EpochSequencers> epoch_seqs_;

  // current epoch of Sequencer, maintained for faster access,
  // always in sync with the current epoch sequencer under the mutex
  std::atomic<epoch_t::raw_type> current_epoch_{EPOCH_INVALID.val_};

  // stores historical and current metadata up to the the current epoch.
  // get() can return nullptr if the metadata is not available yet.
  // similar to epoch_seqs_ and current_epoch_, updates are protected
  // by state_mutex_ to make sure they are in sync, while reading is lock-free
  UpdateableSharedPtr<const EpochMetaDataMap> metadata_map_;

  // limits the number of sequencer reactivations
  RateLimiter reactivation_limiter_;

  // Object responsible for writing metadata log records by the sequencer (i.e.
  // not those that were appended externally)
  SequencerMetaDataLogManager ml_manager_;

  // for data sequencers, this points to the proxy object that manages
  // the corresponding metadata log sequencer
  std::unique_ptr<MetaDataLogWriter> metadata_writer_;

  // periodically (re)sends release messages to storage nodes
  std::shared_ptr<PeriodicReleases> periodic_releases_;

  // Highest LSN such that all records up to and including that LSN are known to
  // be fully stored on enough storage nodes. Implies that
  // last_released_ <= lng of current epoch at all times. last_released_ may
  // temporarily lag behind lng during recovery.
  //
  // An Appender started by this Sequencer will send a global RELEASE message if
  // (and only if) the Appender's LSN is below or equal to this value. If not,
  // it will send a per-epoch RELEASE message if the Appender's LSN is below or
  // equal lng_ (and the per-epoch release feature is enabled).
  //
  // This field prevents releasing records in dirty epochs that have not yet
  // been fully recovered.
  std::atomic<lsn_t> last_released_{LSN_INVALID};

  // Time of the latest append executed by this sequencer. Represented as the
  // number of milliseconds since steady_clock's epoch (note: time_point can't
  // be stored directly as it isn't trivially copyable).
  std::atomic<std::chrono::milliseconds> last_append_{
      std::chrono::milliseconds(0)};

  // Estimates rate of appends (in bytes/s). Used for adjusting nodeset size.
  RateEstimator append_rate_estimator_;

  // tail record of the previous epoch, only populated when log recovery
  // initiated by the current epoch is completed
  UpdateableSharedPtr<const TailRecord> tail_record_previous_epoch_;
  UpdateableSharedPtr<const RecoveredLSNs> recovered_lsns_;

  // Preemption and redirects

  // Highest epoch number up to which this sequencer has been preempted. An
  // Appender will not send any RELEASE messages if Appender's LSN belongs to
  // one of the preempted epochs.
  // Also includes the NodeID of the sequencer node that preempted this one.
  // This allows runAppender() to error out early and reply with a redirect
  // without attempting to send STOREs.
  std::atomic<Seal> preempted_epoch_{Seal()};

  // How long should this sequencer be used to handle appends without
  // redirecting even if appends for this log should go to another node.
  // Updated when an APPEND with a NO_REDIRECT flag is received.
  std::atomic<std::chrono::steady_clock::duration> no_redirect_until_{
      std::chrono::steady_clock::duration::min()};

  // Maximum effective_since ever read by recovery.
  // @seealso onSequencerMetaDataRead
  std::atomic<epoch_t::raw_type> max_effective_since_read_{EPOCH_INVALID.val_};

  // get the current epoch sequencer currently managed
  std::shared_ptr<EpochSequencer> getCurrentEpochSequencer() const;
  // get the draining epoch sequencer currently managed
  std::shared_ptr<EpochSequencer> getDrainingEpochSequencer() const;

  // set the state of the Sequencer, updating last_state_change_timestamp_
  void setState(State state);

  // atomically replace currently managed epoch sequencers with the given
  // pair of (current, draining) sequencers
  void setEpochSequencers(std::shared_ptr<EpochSequencer> current,
                          std::shared_ptr<EpochSequencer> draining);

  enum class UnavailabilityReason {
    // The log was removed from the config
    LOG_REMOVED = 0,
    // This node is not a sequencer node anymore
    NOT_A_SEQUENCER_NODE = 1,
    // The sequencer node is shutting down
    SHUTDOWN = 2,
    // The sequencer is isolated.
    ISOLATED = 3,
    // Deactivated by the admin command
    DEACTIVATED_BY_ADMIN = 4
  };
  // Evict and destroy all managed epoch sequencers and put the sequencer into
  // UNAVAILABLE state
  void setUnavailable(UnavailabilityReason);

  // start the graceful reactivation completion procedure. Called when the
  // current `draining' epoch finishes draining
  void startGracefulReactivationCompletion();

  enum class UpdateMetaDataMapResult { UPDATED, READ_METADATA_LOG };

  // attempt to update the epoch metadata map with the new epoch metadata
  // gotten from epoch store. Called during activation with state_mutex_ held.
  //
  // @param epoch     new epoch which the sequencer is going to activate into
  // @param metadata  metadata for the new epoch
  //
  // @return   a pair of <UpdateMetDataMapResult, updated_metadata>, which can
  //           be
  //            UPDATED  metadata map is successfully updated and no further,
  //                     action needed; the newly updated metadata will be
  //                     returned as updated_metadata
  //
  //            READ_METADATA_LOG   need to read metadata logs to get historical
  //                                metadata; the updated_metadata will be
  //                                nullptr
  std::pair<UpdateMetaDataMapResult, std::shared_ptr<const EpochMetaDataMap>>
  updateMetaDataMap(epoch_t epoch, const EpochMetaData& metadata);

  // clear the historical and current epoch metadata map in sequencer. Called
  // with state_mutex_ held
  void clearMetaDataMapImpl();

  // A helper method for runAppender(), decides what to do with an Append that
  // was redirected/preempted here, and was given an LSN by the previous
  // sequencer.
  folly::Optional<RunAppenderStatus>
  handleAppendWithLSNBeforeRedirect(Appender*);

  // What window size to use when using append_rate_estimator_.
  std::chrono::milliseconds getRateEstimatorWindowSize() const;

  // locate the Sequencer object given a logid
  static std::shared_ptr<Sequencer> findSequencer(logid_t log_id);

  friend class SequencerMetaDataLogManager;
  friend class UnreleasedRecordDetectorTest;
};

}} // namespace facebook::logdevice
