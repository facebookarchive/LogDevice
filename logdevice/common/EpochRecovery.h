/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <set>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/AdminCommandTable-fwd.h"
#include "logdevice/common/Digest.h"
#include "logdevice/common/EpochStore.h"
#include "logdevice/common/Mutator.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/RecoverySet.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file EpochRecovery is a state machine that recovers a single epoch
 *       of a particular log after a sequencer failure. An EpochRecovery
 *       object is owned by a LogRecoveryRequest that orchestrates the
 *       recovery of that log.
 */

struct DataRecordOwnsPayload;
struct GAP_Header;
struct MUTATED_Header;
struct Settings;
class LogRecoveryRequest;
class RECORD_Message;
class SenderBase;

/**
 * External dependencies of EpochRecovery are isolated into this class for
 * the ease of unit testing.
 */
class EpochRecoveryDependencies {
 public:
  explicit EpochRecoveryDependencies(LogRecoveryRequest* driver);

  virtual epoch_t getSealEpoch() const;
  virtual epoch_t getLogRecoveryNextEpoch() const;

  // see LogRecoveryRequest::onEpochRecovered
  virtual void onEpochRecovered(epoch_t epoch,
                                TailRecord epoch_tail,
                                Status status,
                                Seal seal = Seal());

  virtual void onShardRemovedFromConfig(ShardID shard);

  /**
   * @return   if mutation can happen on _shard_; i.e., the shard is
   *           writable (not in rebuilding set).
   */
  virtual bool canMutateShard(ShardID shard) const;

  virtual NodeID getMyNodeID() const;

  virtual read_stream_id_t issueReadStreamID();

  virtual void noteMutationsCompleted(const EpochRecovery& erm);

  virtual std::unique_ptr<BackoffTimer> createBackoffTimer(
      const chrono_expbackoff_t<std::chrono::milliseconds>& backoff,
      std::function<void()> callback = nullptr);

  virtual std::unique_ptr<Timer>
  createTimer(std::function<void()> cb = nullptr);

  virtual int registerOnSocketClosed(const Address& addr, SocketCallback& cb);

  virtual int setLastCleanEpoch(logid_t logid,
                                epoch_t lce,
                                const TailRecord& tail_record,
                                EpochStore::CompletionLCE cf);

  virtual std::unique_ptr<Mutator>
  createMutator(const STORE_Header& header,
                const STORE_Extra& extra,
                Payload payload,
                StorageSet mutation_set,
                ReplicationProperty replication,
                std::set<ShardID> amend_metadata,
                std::set<ShardID> conflict_copies,
                EpochRecovery* epoch_recovery);

  virtual const Settings& getSettings() const;

  virtual StatsHolder* getStats() const;

  virtual logid_t getLogID() const;

  virtual ~EpochRecoveryDependencies();

 public:
  // for sending messages
  std::unique_ptr<SenderBase> sender_;

 private:
  // a backpointer to the LogRecoveryRequest that drives recovery for this log
  LogRecoveryRequest* const driver_;
};

class EpochRecovery {
 public:
  /**
   * General state of the epoch recovery state machine. Note that each shard
   * participating in recovery also has its own state (see RecoveryNode::State).
   * The per-node state does not always correspond to the state machine state,
   * e.g., the state machine can already start DIGEST while some of the shards
   * are still in SEALING phase.
   */
  enum class State : uint8_t {
    // EpochRecovery is inactive or waiting for more nodes to send their
    // SEALED replies
    SEAL_OR_INACTIVE = 0,

    // EpochRecovery has collected enough SEALED reply and started digest stream
    // for sealed nodes
    DIGEST = 1,

    // EpochRecovery has finished digesting and computed the final state of the
    // epoch. It now starts to mutate records stored in the set of nodes that
    // finished digest (i.e., mutation and cleaning set). The goal is to get
    // records stored in mutation and cleaning set in a consistent state.
    MUTATION = 2,

    // EpochRecovery has finished mutation, decided the tail of the epoch,
    // plugged the bridge record (if needed). It sends CLEAN messages to all
    // nodes in the mutation and cleaning set to finalize their local epoch and
    // advance their local lce.
    CLEAN = 3,

    // EpochRecovery has finished recovery of the epoch, and it attempts to
    // advance the global last clean epoch (lce) metadata in the epoch store.
    ADVANCE_LCE = 4,

    // Doesn't represent an actual state.  Used to count the number of elements.
    MAX
  };

  EpochRecovery(
      logid_t log_id,
      epoch_t epoch,
      const EpochMetaData& epoch_metadata,
      const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
      std::unique_ptr<EpochRecoveryDependencies> deps,
      bool tail_optimized);

  State getState() const {
    return state_;
  }

  esn_t getLastKnownGood() const {
    return lng_;
  }

  esn_t getDigestStart() const {
    return digest_start_esn_;
  }

  esn_t getBridgeEsn() const {
    return bridge_esn_;
  }

  const TailRecord& getEpochTailRecord() const {
    ld_check(final_tail_record_.isValid());
    return final_tail_record_;
  }

  const OffsetMap& getEpochSizeMap() const {
    return epoch_size_map_;
  }

  logid_t getLogID() const;

  EpochRecoveryDependencies& getDeps() {
    return *deps_;
  }

  bool isActive() const {
    return active_;
  }

  const Digest& getDigest() const {
    return digest_;
  }

  /**
   * @return true iff node @param dest is currently receiving digest records
   *         from read stream @param rsid
   */
  bool digestingReadStream(ShardID dest, read_stream_id_t rsid) {
    return recovery_set_.digestingReadStream(dest, rsid);
  }

  /**
   * Called by driver_ when a storage node in the recovery set replies with
   * a SEALED, indicating that the log was successfully sealed on that node and
   * the node will no longer accept STORE requests for epoch_.
   *
   * @param from   the id of node that replied with SEALED
   *
   * @param lng    the "last known good" ESN for epoch_ on the sealed node.
   *               The value may come from the lng field in record header, or
   *               from per-epoch and global releases. This may be ESN_INVALID
   *               if the node has no records for epoch_ of this log, or if its
   *               local lng has not been updated.
   *
   * @param max_seen_esn    maximum ESN value the node has ever stored in the
   *                        epoch_ for the log. note: ESN_MAX could mean that
   *                        reporting max_seen_esn is not supported on the
   *                        sealed node.
   *
   * @param epoch_size      OffsetMap containing counters that represent the
   *                        amount of data written in epoch_. May be invalid if:
   *                        1) node have never received STORE messages with
   *                           epoch_size_ info.
   *                        2) some failure of fetching offset happens on node
   *                           side.
   *                        3) LD version on node does not support counter
   *                           offsets.
   *
   * @param tail           the tail (i.e., last per-epoch released) record
   *                       reported on the sealed node. no value means that
   *                       no record was per-epoch released on the sealed node.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool onSealed(ShardID from,
                esn_t lng,
                esn_t max_seen_esn,
                const OffsetMap& epoch_size,
                folly::Optional<TailRecord> tail);

  /**
   * Change the authoritative status of a node.
   * Called when Worker::shard_status_state::shard_status_map_ is updated or a
   * node sent STARTED(E::REBUILDING) or SEALED(E::REBUILDING).
   *
   * If this epoch recovery state machine is in the mutation and cleaning phase,
   * restart the state machine.
   *
   * @param nid    Node for which to change the authoritative status.
   * @param status New status for the node.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool setNodeAuthoritativeStatus(ShardID shard, AuthoritativeStatus status);

  /**
   * Called by Message::onSent() when the send disposition of START or CLEAN
   * message sent by a RecoveryNode of this EpochRecovery machine becomes known.
   *
   * @param dest the node to which the message was addressed
   * @param type type of message sent. Must be START or CLEAN.
   * @param status  OK if the message was passed to TCP, otherwise one of the
   *                error codes documented for Message::onSent().
   * @param id   the stream id in the START header, not used for CLEAN
   */
  void onMessageSent(ShardID dest,
                     MessageType type,
                     Status status,
                     read_stream_id_t id = READ_STREAM_ID_INVALID) {
    recovery_set_.onMessageSent(dest, type, status, id);
  }

  /**
   * Called by STARTED_Message::onReceived() when the STARTED message is found
   * to belong to an active digest read stream of this EpochRecovery machine.
   *
   * @param from    node that sent STARTED
   * @param rsid    read stream id in STARTED, must match the read stream
   *                currently in use for that node by this recovery machine
   * @param lng     the last known good LSN on the digesting node when it
   *                recevies the START message, LSN_INVALID if not available
   *
   * @param status  one of status codes in STARTED_Header
   *                (see STARTED_Message.h)
   */
  void onDigestStreamStarted(ShardID from,
                             read_stream_id_t rsid,
                             lsn_t last_know_good_lsn,
                             Status status);

  /**
   * Called by RECORD_Message::onReceived() when a RECORD is received
   * from a storage node in the recovery set, to be included in the digest.
   *
   * @param from    id of the node that the record was received from
   * @param rsid    read stream that the record belongs to
   * @param record  the record message
   */
  void onDigestRecord(ShardID from,
                      read_stream_id_t rsid,
                      std::unique_ptr<DataRecordOwnsPayload> record);

  /**
   * Called by GAP_Message::onReceived() when a GAP message is received
   * from a storage node in the recovery set. If this is an epoch bump,
   * move the node into DIGESTED.
   *
   * TODO: figure out what to do for other gap types, such as trim gaps.
   *       Is this the only other gap type that a storage node may deliver?
   *
   * @param from    id of the node that the message was received from
   * @param gap     the header of the GAP message received
   */
  void onDigestGap(ShardID from, const GAP_Header& gap);

  /**
   * Called by MUTATED_Message::onReceived() when a MUTATED message is received
   * as a reply to a message sent by a Mutator.
   *
   * @param from    id of the node that the message was received from
   * @param header  the header of the MUTATED message
   *
   */
  void onMutated(ShardID from, const MUTATED_Header& header);

  /**
   * Called by STORE_Message::onSent() for STORE messages that are part of
   * recovery (RECOVERY flag set). This method forwards it to the corresponding
   * Mutator object.
   *
   * @param to      recipient of the message
   * @param header  the header of the STORE message
   * @param status  status of the send operation
   */
  void onStoreSent(ShardID to, const STORE_Header& header, Status status);

  /**
   * Called by a Mutator when it has successfully stored enough copies
   * of the record or plug, or when the Mutator determines that it can
   * make no further progress.
   *
   * This method removes the Mutator from mutators_ map and destroys it.
   *
   * @param esn   ESN of the mutator
   * @param st    mutation status:
   *                   E::OK           mutation succeeded
   *                   E::NOTINCONFIG  mutation was aborted because
   *                                   @param shard is no longer in config
   *                   E::PREEMPTED    mutation was aborted because this
   *                                   Sequencer has been preempted.
   *              if status is E::NOTINCONFIG, `shard` is the shard that was
   *              removed from the config.
   */
  void onMutationComplete(esn_t esn, Status st, ShardID shard = ShardID());

  /**
   * Called by CLEANED_Message::onReceived() when a CLEANED reply is
   * received from this log and epoch.
   *
   * @param from    id of the node that sent CLEANED
   * @param st      operation status from the CLEANED message header
   *
   * @param preempted_seal  if valid and st is E::PREEMPTED, contains the
   *                        seal record that preempted the CLEAN
   */
  void onCleaned(ShardID from, Status st, Seal preempted_seal);

  /**
   * Called by the EpochStore when a setLastCleanEpoch() operation
   * initiated by this EpochRecovery machine completes.
   *
   * @param st   Status::OK if LCE was successfully advanced, otherwise one of
   *             the status codes defined for EpochStore::Completion in
   *             EpochStore.h
   *
   * @param lce  If @param st is E::STALE, this is the (higher) last clean
   *             epoch value that the EpochStore currently has for this log.
   *             If @param st is Status::OK, this is the epoch number passed
   *             to setLastCleanEpoch(). Undefined for other status codes.
   *
   * @param tail_from_epoch_store
   *             tail record returned in the epoch store callback.
   *             If @param st is E::STALE, this is the latest log tail
   *             record stored in EpochStore.
   *             If @param st is Status::OK, this is the log tail
   *             record passed to setLastCleanEpoch().
   *             Undefined for other status codes.
   */
  void onLastCleanEpochUpdated(Status st,
                               epoch_t lce,
                               TailRecord tail_from_epoch_store);

  /**
   * Called when this EpochRecovery machine has received enough entire
   * digest streams to complete recovery.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool onDigestComplete();

  /**
   * Get the largest esn in the the digest collected from storage nodes. This is
   * essentially the right end of the epoch recovery window. If the digest is
   * empty, lng_ is returned.
   */
  esn_t getLastDigestEsn() const;

  /**
   * Get the set of nodes which did not participate or finish digesting
   * (i.e., not in the mutation and cleaning set).
   */
  const StorageSet& getAbsentNodes() const {
    return absent_nodes_;
  }

  /**
   * Called when mutation_and_cleaning_ timer expires.
   */
  void onTimeout();

  /**
   * Called when a storage node participated in epoch recovery has
   * actually transistioned to the state specified by @param to
   */
  void onNodeStateChanged(ShardID nid, RecoveryNode::State to);

  /**
   * Set active_ to true. If enough recovery nodes are in SEALED,
   * request digest records from them.
   *
   * @param tail_record     Tail record of the previous epoch that
   *                        has been successfully recovered. Contains
   *                        information about epoch_beginning_offset
   *                        (How much data were written to log before
   *                        epoch_) and last timestamp ever written to
   *                        a data record before epoch_, etc.
   */
  void activate(const TailRecord& prev_tail_record);

  /**
   * @return true iff this RecoveryEpoch is done building the digest and
   *         is now in the mutation and cleaning phase
   */
  bool digestComplete() const;

  /**
   * @return  a human-readable identifier of this epoch recovery object
   *          for use in error messages
   */
  std::string identify() const;

  /**
   * @return list of nodes in the recovery set and their state.
   * Used for debugging and logging.
   */
  std::string recoveryState() const;

  /**
   * Used by the "info recoveries" admin command. For debugging.
   */
  void getDebugInfo(InfoRecoveriesTable& table) const;

  ////////// expose some internal members for unit testing ////////////
  int mutationSetSize() const {
    return mutation_set_size_;
  }

  const RecoverySet& getRecoverySet() const {
    return recovery_set_;
  }

  const std::map<esn_t, std::unique_ptr<Mutator>>& getMutators() const {
    return mutators_;
  }

  Timer* getGracePeriodTimer() {
    return grace_period_.get();
  }

  Timer* getMutationCleaningTimer() {
    return mutation_and_cleaning_.get();
  }

  ///////// public members

  const logid_t log_id_; // log we are recovering
  const epoch_t epoch_;  // epoch we are recovering

  const std::unique_ptr<EpochRecoveryDependencies> deps_;

  ReplicationProperty replication_;

  // id that uniquely identifies this EpochRecovery instance within the
  // current process
  recovery_id_t id_;

  // Used for debugging.
  std::chrono::system_clock::duration creation_timestamp_;
  std::chrono::system_clock::duration last_restart_timestamp_;

  uint16_t num_restarts_{0};
  uint32_t num_holes_plugged_{0};
  uint32_t num_holes_re_replicate_{0};
  uint32_t num_holes_conflict_{0};
  uint32_t num_records_re_replicate_{0};

 private:
  /**
   * If we have sealed a certain minimum number of nodes, start requesting
   * digest records from nodes in SEALED state. Otherwise do nothing.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool onSealedOrActivated();

  /**
   * Determine the start esn of the digest based on the result of SEALED
   * replies collected. See digest_start_esn_ for details. Note that
   * digest_start_esn_ stays the same throughout epoch recovery until
   * epoch recovery got restarted.
   */
  esn_t computeDigestStart() const;

  /**
   * Compute the tail record and accumulative log attributes of the epoch based
   * on information from SEALED and digest replies.
   */
  void updateEpochTailRecord();

  /**
   * This function is called when this EpochRecovery machine
   * determines that it has received all digest records and gaps from
   * one of the nodes in the recovery set. The function transitions
   * the node into DIGESTED state. It then checks if the EpochRecovery
   * machine has enough nodes in DIGESTED to move on to the digest
   * processing and mutation stage of recovery, or at least to start
   * the grace period timer.
   *
   * @param from   the node that we reached the end of digest stream from
   */
  void onDigestStreamEndReached(ShardID from);

  /**
   * If !digestComplete(), but digest is authoritative or full,
   * calls onDigestComplete() or starts grace period timer.
   *
   * @param grace_period_expired if true, means the grace period already expired
   * and thus we should not try to activate it and wait if we get a non
   * authoritative majority.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool onDigestMayHaveBecomeComplete(bool grace_period_expired = false);

  /**
   * Called when grace_period_ timer expires.
   */
  void onGracePeriodExpired();

  /**
   * Kick off the mutation phase of epoch recovery, perform final processing
   * to the digest received. It then calls mutateEpoch() to perform mutations
   * for the epoch. Called from onDigestComplete().
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool startMutations();

  std::pair<STORE_Header, STORE_Extra>
  createMutationHeader(esn_t esn,
                       uint64_t timestamp,
                       STORE_flags_t flags,
                       DataRecordOwnsPayload* record) const;

  /**
   * Given the final digest, perform mutations for the epoch. Starting from
   * lng_, walks through entries in the digest, constructing Mutator objects
   * and start them.
   *
   * @return true if this EpochRecovery state machine was destroyed.
   */
  bool mutateEpoch(const std::set<ShardID>& mutation_set,
                   FmajorityResult fmajority_result);

  /**
   * Called from onMutationComplete() when all mutators are done. Moves on to
   * the cleaning phase.
   */
  void advanceToCleaning();

  /**
   * Restart epoch recovery from scratch.
   * @return true if this EpochRecovery was destroyed.
   */
  bool restart();

  /**
   * Make a setLastCleanEpoch() request to our EpochStore attempting
   * to advance the last clean epoch value for this log to epoch_.
   */
  void advanceLCE();

  /**
   * Starts mutation_and_cleaning_ timer with the timeout specified in the
   * --recovery-timeout command line option.
   */
  void startMutationAndCleaningTimer();

  // true if the log is tail optimized according to the logs config
  const bool tail_optimized_;

  // see doc for EpochRecovery::State
  State state_;

  // Determines whether this EpochRecovery machine is active. Inactive
  // EpochRecovery machines do not transition any nodes into DIGESTING
  // even if enough nodes are in SEALED. The LogRecoveryRequest driver
  // sets the lowest-numbered epoch active. Once that EpochRecovery machine
  // finishes, it activates the next epoch.
  bool active_ = false;

  // best estimate of last known good ESN for epoch_ so far
  esn_t lng_ = ESN_INVALID;

  // maximum stored esn for epoch reported by storage node so far
  esn_t max_seen_esn_ = ESN_INVALID;

  // highest (in terms of LSN) tail record reported by storage nodes in SEALED
  // replies
  TailRecord tail_record_from_sealed_;

  // the tail record before the epoch being recovered. set when this state
  // machine got activated by driver_
  TailRecord tail_record_before_this_epoch_;

  // if valid, this is the tail record of the log until the end of this epoch,
  // and it should have the accumulative byte offset (not the offset within
  // epoch). This record will be stored in the epoch store along with LCE.
  TailRecord final_tail_record_;

  // esn of start of the digest. Note:
  // 1) for each start of epoch recovery, all storage nodes in the
  // recovery set have the same digest start esn.
  // 2) determined by lng_, tail_record_from_sealed, and tail_optimized_
  //    property
  // 3) reset and got recomputed across epoch recovery restarts.
  esn_t digest_start_esn_ = ESN_INVALID;

  // esn of the tail record of the epoch, ESN_INVALID if the epoch is ended up
  // empty
  esn_t tail_esn_ = ESN_INVALID;

  // The ESN to insert the bridge record, effectively mark the end of the epoch.
  // The bridge esn is usually the next ESN following a normal (non-gap) record,
  // which is not necessarily always the last entry in the digest
  esn_t bridge_esn_ = ESN_INVALID;

  // a collection of RecoveryNode objects describing the storage nodes
  // participating in this recovery, and their current states, as well
  // as those nodes that were eligible to participate, but have been
  // excluded because they have not replied in time to SEALs sent to them
  RecoverySet recovery_set_;

  // nodes that did not finish digesting and are not in the mutation and
  // cleaning set, used by cleaning
  StorageSet absent_nodes_;

  // finds holes in the numbering sequence, determines which records are
  // under-replicated because of a failure
  Digest digest_;

  // true if we finished digesting and started mutation and cleaning phase
  bool finished_digesting_ = false;

  // when this EpochRecovery machine decides that it has a big enough
  // digest for recovery and does not need to accept any more digest
  // records, it sets this field to the number of nodes in DIGESTED
  // state. The set of those nodes becomes the mutation set. It remains
  // fixed until this epoch recovery attempt completes successfully.
  // The field is reset back to 0 when we start another recovery attempt,
  // e.g., after the recovery timeout expires.
  int mutation_set_size_ = 0;

  // active mutators. A mutator is active if it has sent one or more
  // STORE messages for the record or hole plug but has not yet
  // received enough successful STORED replies.
  std::map<esn_t, std::unique_ptr<Mutator>> mutators_;

  // This timer is started when we first get M=max(N-f, f+1) nodes in
  // DIGESTED state. The remaining N-M nodes in the epoch node set are
  // allowed to participate in recovery if we get SEALED, and later
  // digest RECORDs from them before this timer expires. The goal is to
  // maximize the number of nodes on which this epoch is marked clean
  // without delaying recovery too much. We move on to the mutation phase
  // once we (1) have received all recovery records from all N nodes in
  // the epoch node set, or (2) this timer expires.
  std::unique_ptr<Timer> grace_period_;

  // This timer is started when we start mutations. If it goes off
  // before we complete recovery of this epoch, all nodes in
  // recovery_set_ whose states are greater than SEALING are reset
  // into SEALED and recovery is restarted.
  std::unique_ptr<Timer> mutation_and_cleaning_;

  // increasing unique id that's assigned to each EpochRecovery object when
  // it's created or restarted
  static std::atomic<recovery_id_t::raw_type> next_id;

  // OffsetMap containing multiple counters on how much data is written
  // to current epoch. Refer to OffsetMap.h. Sealed nodes will send their view
  // of this info and epoch_size_ will be set as maximum value among them.
  OffsetMap epoch_size_map_;

  // Timestamp of the last written record before epoch_.
  uint64_t last_timestamp_ = 0;

  // Used for a paranoid consistency check.
  uint64_t last_timestamp_from_seals_ = 0;

  // used for latency histograms
  std::chrono::steady_clock::time_point activation_time_{
      std::chrono::steady_clock::time_point::min()};
  std::chrono::steady_clock::time_point last_digesting_start_{
      std::chrono::steady_clock::time_point::min()};
  std::chrono::steady_clock::time_point last_mutation_start_{
      std::chrono::steady_clock::time_point::min()};
  std::chrono::steady_clock::time_point last_cleaning_start_{
      std::chrono::steady_clock::time_point::min()};
};

std::ostream& operator<<(std::ostream&, EpochRecovery::State);

}} // namespace facebook::logdevice
