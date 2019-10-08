/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <unordered_map>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/MetaDataLogReader.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/RetryHandler.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/protocol/SEALED_Message.h"
#include "logdevice/common/protocol/SEAL_Message.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file This request is a log recovery state machine. It's created and started
 *       by Sequencer::startRecovery(). Once it's done with recovery, it'll
 *       call Sequencer::onRecoveryCompleted().
 *
 *       == LogRecovery and NodeSet ==
 *
 *       With epoch metadata, each epoch of the data log may have different
 *       metadata (nodeset and replication factor). To properly recover these
 *       epochs, log recovery needs to first fetch their epoch metadata before
 *       attempting epoch recovery. Therefore, the entire recovery process has
 *       the following asynchronous stages:
 *
 *       1. get last clean epoch (lce) from the epoch store, the total epochs
 *          that needs recovery are within range of ]lce, next_epoch[
 *          (or [lce+1, next_epoch-1])
 *
 *       2. get epoch metadata for each recovering epoch by reading the metadata
 *          log, and use its nodeset and replication factor to initialize epoch
 *          recovery
 *
 *       3. send SEAL messages to all storage nodes in the union of nodesets
 *          for epochs in the recovery range. Notify and possibly kick off epoch
 *          recovery once they replied.
 *
 *      Besides these three asynchronous stages, there is another important
 *      condition to complete log recovery for data log sequencers:
 *      Log recovery for data log won't complete or release any lsn in
 *      next_epoch_ until it can read the same (with the same effective_since)
 *      epoch metadata which the sequencer used to activate next_epoch from
 *      metadata log. Instead, it will wait until the metadata (we call it
 *      `sequencer metadata') can be read. This property is crucial for
 *      guaranteeing the correctness of reading the metadata log. See the doc
 *      block in MetaDataLogReader.h for an explanation.
 *
 *      == LogRecovery and Rebuilding ==
 *
 *      If Rebuilding and Recovery were allowed to re-replicate
 *      the same record at the same time, this could produce hole-copy
 *      conflicts (if Recovery decides to replace record with a hole as
 *      Rebuilding re-replicates it). To avoid that, Rebuilding waits for
 *      records to be released before re-replicating them, effectively waiting
 *      for recovery and Appenders to finish. To avoid cyclic dependency (and a
 *      distributed deadlock), Recovery should be able to finish if any number
 *      of nodes are rebuilding. To do that, it pretends that
 *      nodeset-rebuilding_set is sort of an f-majority - we can proceed if we
 *      sealed/digested all nodes except the ones that are being rebuilt, even
 *      if it's not an f-majority (and even if it doesn't satisfy the
 *      synchronous replication requirement).
 *
 *      == Storage nodes losing data/seals during recovery ==
 *
 *      What happens if a storage shard loses all data after recovery has read
 *      a Digest from it but before the recovery finished cleaning?
 *      Does recovery need to detect it and re-seal/re-digest? No.
 *      The reasoning is:
 *      recovery managed to store a seal on shard N ->
 *      recovery started before N lost data ->
 *      recovery started before N's rebuilding started (*) ->
 *      N's rebuilding will wait for this recovery ->
 *      N won't accept CLEANs during the rest of this recovery ->
 *      this recovery will finish with an f-majority that doesn't contain N.
 *
 *      (*) Keep in mind that when a shard loses data, it doesn't accept SEALs
 *      and CLEANs until it's rebuilt.
 *
 */

class Worker;

// The set of LSNs recovered during recovery.
class RecoveredLSNs {
 public:
  enum class RecoveryStatus { REPLICATED, HOLE, UNKNOWN };

  RecoveredLSNs() {}

  RecoveredLSNs(const RecoveredLSNs&) = delete;
  const RecoveredLSNs& operator=(const RecoveredLSNs&) = delete;

  RecoveredLSNs(RecoveredLSNs&&) = delete;
  RecoveredLSNs& operator=(RecoveredLSNs&&) = delete;

  ~RecoveredLSNs();

  void noteMutationsCompleted(const EpochRecovery& epoch_recovery);

  RecoveryStatus recoveryStatus(lsn_t lsn) const;

 private:
  std::map<epoch_t, esn_t> last_esns_;
  std::unordered_set<lsn_t> hole_lsns_;
};

class LogRecoveryRequest : public Request,
                           public ShardAuthoritativeStatusSubscriber {
 public:
  enum class AuthoritativeSource {
    NODE,      // The authoritative status of the node is known from the
               // node's behavior. If the node responds to a SEAL or START
               // message with E::REBUILDING, its authoritative status is
               // changed to UNDERREPLICATION. If it responds with another
               // status, it is FULLY_AUTHORITATIVE.
    EVENT_LOG, // The authoritative status of the node is known from the event
               // log.
    OVERRIDE   // Status was overridden from an admin command. The authoritative
               // status will be changed once we can read a new record from the
               // event log or we can contact the node.
  };

  /**
   * @param log_id        log to perform recovery for
   * @param next_epoch    next epoch of the sequencer; perform recovery up to
   *                      (but NOT including) this epoch
   * @param seq_metadata  the epoch metadata which the sequencer used to
   *                      activate _next_epoch_
   * @param delay         if positive, start a timer for that amount of time,
   *                      and execute the request when the timer expires. Used
   *                      for retrying recovery after a soft error.
   */
  LogRecoveryRequest(
      logid_t log_id,
      epoch_t next_epoch,
      std::shared_ptr<const EpochMetaData> seq_metadata,
      std::chrono::milliseconds delay = std::chrono::milliseconds::zero());

  ~LogRecoveryRequest() override;

  // if called, run recovery on the Worker thread specified by worker_id
  // otherwise, use the default worker thread mapping based on log_id_
  void setTargetWorker(worker_id_t worker_id) {
    target_worker_.assign(worker_id);
  }

  int getThreadAffinity(int nthreads) override;

  Execution execute() override;

  void onSealMessageSent(ShardID to, epoch_t seal_epoch, Status status);
  void onSealReply(ShardID from, const SEALED_Message& reply);

  epoch_t getEpoch() const {
    return next_epoch_;
  }

  logid_t getLogID() const {
    return log_id_;
  }

  /**
   * @return the EpochRecovery machine for the oldest epoch whose
   *         recovery is still ongoing, nullptr if execute() has not yet
   *         been called on this Request, or the oldest epoch recovery is
   *         not activated yet.
   */
  EpochRecovery* getActiveEpochRecovery() {
    auto first = epoch_recovery_machines_.begin();
    if (first == epoch_recovery_machines_.end()) {
      return nullptr;
    }

    if (!first->isActive()) {
      // if we got all epoch metadata needed, the first epoch recovery
      // state machine should have been activated by LogRecoveryRequest
      ld_check(!epoch_metadata_finalized_);
      return nullptr;
    }

    return &*first;
  }

  /**
   * Returns the epoch number through which this recovery request will seal the
   * log.
   */
  epoch_t getSealEpoch() const {
    return epoch_t(next_epoch_.val_ - 1);
  }

  /**
   * Force the authoritative status of node `nid` to `status`.
   *
   * This is called from an admin command and is meant to be used for extreme
   * situations when a recovery state machine is stuck because too many machines
   * are unavailable and the event log is not available for reads.
   */
  void overrideShardStatus(ShardID shard, AuthoritativeStatus status);

  /**
   * Looks at Worker::shard_status_state::shard_status_map_ and changes node
   * states accordingly.  Called after `node_statuses_` is initialized and each
   * time Worker::shard_status_state::shard_status_map_ changes.
   * Note that if some node was marked as rebuilding because of E::REBUILDING
   * status in SEALED or STARTED message, this method will mark it as
   * non-rebuilding again if it's not in rebuilding set as seen in event log.
   */
  void applyShardStatus();

  void onShardStatusChanged() override {
    applyShardStatus();
  }

  /**
   * Called by EpochRecovery when it detects that the given shard was removed
   * from the config.
   *
   * TODO(T4408213): currently this function only logs an error as recovery has
   * not been modified to guarantee it does not stall if a shard is removed from
   * the config.
   *
   * @param shard  Shard that was removed.
   */
  void onShardRemovedFromConfig(ShardID shard);

  /**
   * Called by the EpochRecovery object at the front of epoch_recovery_machines_
   * list when the recovery of its epoch is done. The action is to destroy the
   * EpochRecovery object and to activate the next one, or if all epochs have
   * been recovered, to notify the controlling Sequencer.
   *
   * @param epoch         epoch number of the epoch that has been recovered
   * @param epoch_tail    tail record of recovered epoch
   * @param status  E::OK        if epoch recovery completed successfully,
   *                E::PREEMPTED if epoch recovery was aborted because this
   *                             Sequencer has been preempted
   * @param seal    Seal record of the sequencer that preempted this recovery
   *                sent by a storage node
   */
  void onEpochRecovered(epoch_t epoch,
                        TailRecord epoch_tail,
                        Status status,
                        Seal seal = Seal());

  /**
   * Used by the "info recoveries" admin command. For debugging.
   */
  void getDebugInfo(InfoRecoveriesTable& table) const;

  /**
   * Return timestamp when recovery request was created.
   */
  SteadyTimestamp getCreationTimestamp();

  /**
   * Change the authoritative status of a node.
   * Called when Worker::shard_status_state::shard_status_map_ is updated or a
   * node sent STARTED(E::REBUILDING) or SEALED(E::REBUILDING).
   *
   * This will call any EpochRecovery state machine in the mutation and cleaning
   * to restart.
   *
   * @param nid  Node for which to change the authoritative status.
   * @param st   New status for the node.
   * @param src  Whether the status is known from the event log or the node
   *             itself.
   */
  void setNodeAuthoritativeStatus(ShardID shard,
                                  AuthoritativeStatus st,
                                  AuthoritativeSource src);

  void noteMutationsCompleted(const EpochRecovery& er) {
    recovered_lsns_->noteMutationsCompleted(er);
  }

 private:
  class SocketClosedCallback : public SocketCallback {
   public:
    explicit SocketClosedCallback(LogRecoveryRequest* request, ShardID shard)
        : request_(request), shard_(shard) {}

    void operator()(Status st, const Address& address) override;

   private:
    LogRecoveryRequest* request_;
    ShardID shard_;
  };

  // the status of a node at a particular index in the cluster config
  // with respect to sealing
  struct NodeStatus {
    enum class State {
      NOT_SEALED,
      SEALED, // SEALED has been received from the shard
    };

    State state = State::NOT_SEALED;
    // Used to detect when a connection to a node has been closed and retry
    // sending a SEAL message.
    SocketClosedCallback socket_close_callback;

    // time of the last attempt to seal a node in recovery set by sending
    // a SEAL message
    std::chrono::steady_clock::time_point last_seal_time =
        std::chrono::steady_clock::time_point::min();

    // @see AuthoritativeStatus.h
    AuthoritativeStatus authoritative_status =
        AuthoritativeStatus::FULLY_AUTHORITATIVE;
    // We want to ensure that if the node says it's up and not rebuilding while
    // the event log says it is empty, what the node says takes precedence until
    // we lose a connection to it. Remember how we know the authoritative status
    // of the node.
    AuthoritativeSource authoritative_status_source =
        AuthoritativeSource::EVENT_LOG;

    explicit NodeStatus(LogRecoveryRequest* request, ShardID shard)
        : socket_close_callback(request, shard) {}

    void activateRebuildingCheckTimer();
  };

  // Set LCE to next_epoch-1 in epoch store and schedule a call to
  // complete(E::OK) on the next iteration of the event loop.
  void skipRecovery();

  void start();

  // First step of the recovery process. Obtains the value of last clean epoch
  // from EpochStore and passes it to sealLog().
  void getLastCleanEpoch();

  // Second step of the recovery process. Obtain epoch metadata for all epochs
  // that need to be recovered: [lce + 1, next_epoch - 1].
  void getEpochMetaData();

  /**
   * Create EpochRecovery state machines for recovering a range of epochs in
   * [start, until]. EpochRecovery state machines are created using the epoch
   * metadata (nodeset and replication factor) provided, and they are inserted
   * into epoch_recovery_machines_ list after creation. Caller of this function
   * must ensure that the function is called with epoch intervals which are
   * in order and not overlapping.
   */
  int createEpochRecoveryMachines(epoch_t start,
                                  epoch_t until,
                                  const EpochMetaData& metadata);

  // Callback function called when epoch metadata is fetched from the
  // metadata log
  void onEpochMetaData(Status st, MetaDataLogReader::Result result);

  // Called when epoch metadata for all epochs that need to be recovered
  // are fetched. Proceed to the next stage of sealLog().
  void finalizeEpochMetaData();

  // Start reading the metadata log until the epoch metadata used to activate
  // next_epoch of the sequencer (i.e., sequencer metadata) can be read
  void readSequencerMetaData();

  // Callback function called when epoch metadata is read from the epoch
  // store.
  void onSequencerMetaDataFromEpochStore(Status status,
                                         const EpochMetaData* metadata);

  // Call Sequencer::onSequencerMetaDataRead of our sequencer.
  void informSequencerOfMetaDataRead(epoch_t effective_since,
                                     Worker* worker = nullptr);

  // Called when all recovering epochs in [lce + 1, next_epoch - 1] have
  // finished recovery. For data logs, if the sequencer metadata has already
  // been read from the metadata log, complete the recovery process. Otherwise,
  // keep waiting for the metadata to appear in the metadata log.
  void allEpochsRecovered();

  // Sends SEAL messages to nodes in the cluster. Once some f-majority responds,
  // baton is handed off to EpochRecovery. A grace period is used to allow
  // additional nodes to also participate in recovery (as long as they reply
  // with a SEALED within that period).
  void sealLog();

  // Sends a single SEAL message to `shard'. Returns 0 on success, -1 if sending
  // failed. Called by seal_retry_handler_ periodically for each shard (until
  // a message is successfully sent to each shard or sealing completes).
  int sendSeal(ShardID shard);

  // scanning through nodes in the recovery set, for nodes that are not yet
  // sealed, scheduel a retry for sealing the node. This is used as a timer
  // callback for periodically checking unsealed nodes and making sure nodes
  // are eventually sealed.
  void checkNodesForSeal();

  void activateCheckSealTimer();

  /**
   * Called when recovery is complete. Deletes this request from Worker's
   * LogRecoveryRequest map, thus destroying this object.
   *
   * @param status   used to indicate success of this recovery; passed to
   *                 Sequencer::onRecoveryCompleted(). See that function for
   *                 the list of accepted values.
   */
  void complete(Status status);

  /**
   * Schedules a call to complete on the next iteration of the event loop.
   * Useful if we want to avoid completing from within the call stack of another
   * method.
   */
  void completeSoon(Status status);

  // Called when recovery was preempted by another sequencer.
  void onPreempted(epoch_t epoch, NodeID preempted_by);

  // Called from a SocketClosedCallback when a connection to `shard' has
  // been closed.
  void onConnectionClosed(ShardID shard, Status status);

  // Called from the completion function passed to EpochStore's
  // getLastCleanEpoch() method. Starts sealing the log on success, schedules
  // a retry on failure.
  void getLastCleanEpochCF(Status status,
                           epoch_t epoch,
                           TailRecord tail_record);

  void updateTrimPoint(Status status, lsn_t trim_point);

  const logid_t log_id_;
  epoch_t next_epoch_;

  folly::Optional<worker_id_t> target_worker_;

  // epoch metadata used by the sequencer to activate next_epoch_
  std::shared_ptr<const EpochMetaData> seq_metadata_;

  std::chrono::milliseconds delay_; // see constructor

  // Total number of nodes in the nodeset.
  size_t nodeset_size_;

  // Last clean epoch of the log. Gets assigned its value once lce is fetched
  // from the epoch store.
  folly::Optional<epoch_t> last_clean_epoch_;

  // Tail record of the log, initially fetched from epoch store along with lce.
  // Then updated after each epoch recovery up to end of last recovered epoch.
  folly::Optional<TailRecord> tail_record_;

  // Stores recovery results for the last epoch recovered.
  std::unique_ptr<RecoveredLSNs> recovered_lsns_;

  // total set of nodes that participate in this recovery, it is essentially
  // the union of all epoch nodesets that needs to be recovered
  std::set<ShardID> recovery_nodes_;

  // MetaDataLogReader used to fetch epoch metadata in epoch range of
  // [lce+1, next_epoch-1]. Unused (nullptr) if the log being recovered is
  // a metadata log.
  std::unique_ptr<MetaDataLogReader> meta_reader_;

  // indicates if all recovering epochs have finished recovery
  bool all_epochs_recovered_{false};
  // indicates if the sequencer metadata has appeared in the metadata log
  bool seq_metadata_read_{false};

  // bool flag indicating that we have collected all epoch metadata needed
  bool epoch_metadata_finalized_{false};

  // Header of the SEAL message sent to storage nodes
  std::unique_ptr<SEAL_Header> seal_header_;

  // A map that tracks the seal status of nodes participating recovery.
  // Key of the map are indexes of the union of  the nodesets of all epochs
  // participating in this log recovery. Values indicate whether we sent a SEAL
  // to and received a SEALED(OK) to the node at this offset in the cluster.
  std::unordered_map<ShardID, NodeStatus, ShardID::Hash> node_statuses_;

  // RetryHandler object responsible for resending SEAL messages when sending
  // fails.
  std::unique_ptr<RetryHandler> seal_retry_handler_;

  // timer used for periodically checking and scheduling retries on node that
  // has not yet been sealed
  std::unique_ptr<Timer> check_seal_timer_;

  // timer used for getLastCleanEpoch() retries
  std::unique_ptr<BackoffTimer> lce_backoff_timer_;

  // timer used for the initial delay (if requested)
  std::unique_ptr<Timer> start_delay_timer_;

  // timer used for reading the sequencer metadata
  std::unique_ptr<BackoffTimer> seq_metadata_timer_;

  std::unique_ptr<Timer> deferredCompleteTimer_;

  // a list of EpochRecovery machines for epochs in the ]lce, next_epoch[
  // range, sorted by epoch number ascending
  std::list<EpochRecovery> epoch_recovery_machines_;

  // Used for debugging.
  SteadyTimestamp creation_timestamp_;

  // min and max delay for retry sending SEAL messages in a backoff manner
  static const std::chrono::milliseconds SEAL_RETRY_INITIAL_DELAY;
  static const std::chrono::milliseconds SEAL_RETRY_MAX_DELAY;

  // interval for periodically checking nodes that are not yet SEALED and
  // scheduling retry
  static const std::chrono::milliseconds SEAL_CHECK_INTERVAL;
};

struct LogRecoveryRequestMap {
  std::
      unordered_map<logid_t, std::unique_ptr<LogRecoveryRequest>, logid_t::Hash>
          map;
};

}} // namespace facebook::logdevice
