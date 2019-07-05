/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>

#include <folly/concurrency/AtomicSharedPtr.h>

#include "logdevice/common/Appender.h"
#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/SlidingWindowSingleEpoch.h"
#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file  EpochSequencer is responsible for managing sequencing and replication
 *        for a particular epoch of a log.
 *
 *        TODO 7467469: implement replication part and misc support such as byte
 *                      offset...
 *
 *        TODO 7467469: add description on Appender life cyle management and
 *        explain reference counting of EpochSequencer object.
 */

class Processor;
class Sequencer;
class TailRecord;

// Settings and log attributes affecting sequencer that EpochSequencer doesn't
// update on the fly. When these change, sequencer needs to be reactivated.
struct EpochSequencerImmutableOptions {
  copyset_size_t extra_copies = 0;
  copyset_size_t synced_copies = 0;
  int window_size = SLIDING_WINDOW_MIN_CAPACITY;
  esn_t esn_max = ESN_MAX;

  EpochSequencerImmutableOptions() = default;
  EpochSequencerImmutableOptions(const logsconfig::LogAttributes& log_attrs,
                                 const Settings& settings);

  bool operator==(const EpochSequencerImmutableOptions& rhs);
  bool operator!=(const EpochSequencerImmutableOptions& rhs);

  std::string toString() const;
};

class EpochSequencer : public std::enable_shared_from_this<EpochSequencer> {
 public:
  /**
   * An EpochSequencer object can be in one of the following four states.
   *
   * An important rule is that an EpochSequencer can only transition from a
   * state with lower value to a state with _higer_ value
   * (e.g., ACTIVE->DRAINING, DRAINGING->DYING, DRAINING->QUIESCENT), but never
   * in the opposite direction.
   */
  enum class State : uint8_t {
    // EpochSequencer has been initialized with an epoch number, along with the
    // epoch metadata used to replicating records in the epoch and can be used
    // for stamping records. This is the initial state when an EpochSequencer
    // object is created.
    ACTIVE = 0,

    // EpochSequencer stops accepting new appends but is not giving up on
    // existing appends in flight. In this state, the EpochSequencer gracefully
    // wait for all remaining Appenders in its sliding window to finish their
    // replication and retire. This can happen in situations like graceful
    // reactivation and graceful sequencer migration. After draining is
    // successfully completed, the EpochSequencer will transition into QUIESCENT
    // state.
    DRAINING = 1,

    // Similar to DRAINING, EpochSequencer stops accepting new appends in this
    // state. However, EpochSequencer does not gracefully wait for in-flight
    // Appenders to complete. Instead, it forcefully _abort_ them, having them
    // retire as soon as possible, regardless of their replication status. This
    // happends in situations such as the epoch is evicted by the Sequencer, and
    // discarding epochs that contains Appenders cannot finish replication or
    // long over-due. Once all existing Appenders are retired and reaped, the
    // EpochSequencer will transition into QUIESCENT state.
    DYING = 2,

    // All Appenders in the sliding window managed by the EpochSequencer have
    // retired and reaped after EpochSequencer transitioned into DRAINING or
    // DYING state. The EpochSequencer can be safely destroyed once its
    // reference count drops to zero.
    QUIESCENT = 3
  };

  /**
   * Creates an EpochSequencer in the ACTIVE state.
   *
   * @param logid      log that this EpochSequencer will manage
   * @param epoch      epoch in which this EpochSequencer will stamp records
   *
   * @param metadata   EpochMetaData that determines the node set,
   *                   replication property, etc of the epoch. Used by
   *                   Appenders managed by the EpochSequencer to replicate
   *                   copies of log records
   *
   * @param window_size  capacity of the sliding window used by EpochSequencer,
   *                     determines how many Appenders can be accepted in-flight
   *                     performing replication at the same time
   *
   * @param esn_max      maximum ESN allowed to be issued in this epoch. Once
   *                     this ESN is issued, the EpochSequencer won't be able
   *                     to accept new appends (i.e., they fail with E::TOOBIG)
   *
   * @param parent       parent sequencer object managing all epochs of the log
   */
  EpochSequencer(logid_t log_id,
                 epoch_t epoch,
                 std::unique_ptr<EpochMetaData> metadata,
                 const EpochSequencerImmutableOptions& immutable_options,
                 Sequencer* parent);

  virtual ~EpochSequencer() {}

  /**
   * Create the copyset manager for the epoch if there is none, or update the
   * copyset manager if it already exists.
   *
   * Note: users of EpochSequencer are expected to call this function
   *       immediately after creating the EpochSequencer object, as well
   *       as when configuration changes are detected and effective nodeset
   *       might be changed. Must be called before runAppender().
   */
  void createOrUpdateCopySetManager(
      const std::shared_ptr<Configuration>& cfg,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      const Settings& settings);

  /**
   * Attempt to transition the EpochSequencer from ACTIVE to the DRAINING state.
   * The EpochSequencer won't accept new appends after the call. If the
   * EpochSequencer is not in ACTIVE state (i.e., already in DRAINING, DYING,
   * QUIESCENT), the function has no effect.
   */
  void startDraining();

  /**
   * Attempt to transition the EpochSequencer to the DYING state.
   * The EpochSequencer won't accept new appends after the call. Acceptable
   * previous state is ACTIVE or DRAINING, otherwise the function has no effect.
   * If the EpochSequencer is in DRAINING state when the function is called,
   * draining will be aborted.
   */
  void startDestruction();

  /**
   * Attempt to start a new Appender in this epoch. Specifically, attempts to
   * grow the sliding window by inserting appender object _appender_
   * at the leading edge. _appender_ must not yet be running. If the insertion
   * is successful, starts the appender, passing to it the ESN assigned to the
   * record.
   *
   * @return See RunAppenderStatus.  If ERROR_DELETE is returned, sets err to:
   *    NOSEQUENCER   EpochSequencer is not in ACTIVE state
   *    INVALID_PARAM appender is nullptr or already running
   *                  (debug build asserts)
   *    NOBUFS        too many append requests are already in flight
   *    TOOBIG        ESNs for this epoch have been exhausted
   *    SYSLIMIT      system-wide resource limit has been reached
   */
  virtual RunAppenderStatus runAppender(Appender* appender);

  /**
   * Tells the EpochSequencer that the Appender whose record was assigned
   * @param lsn has fully stored the record, and the record may
   * now be released for delivery. This will do nothing if other
   * records ahead of this one are still in flight. Otherwise, this
   * may release record #lsn and possibly records following it in the
   * window if those got retired earlier. See Appender::releaseAllCopies()
   * for the full set of conditions under which a record gets released for
   * delivery, and for other actions taken when an Appender is reaped.
   *
   * @param st       Completion status of the appender
   * @param lsn      lsn of record to be retired
   * @param reaper   reaper object to which to pass all Appenders ready to
   *                 be reaped (and release their records)
   */
  virtual void retireAppender(Status /*st*/,
                              lsn_t lsn,
                              Appender::Reaper& reaper) {
    window_.retire(lsn, reaper);
  }

  /**
   * Called when an Appender managed by this EpochSequencer has been reaped
   * in the sliding window. Perform the following jobs:
   *
   * - Attempts to conditionally and atomically advance last known good ESN
   *   given the reaped Appender is fully replicated and lng_ is reaped_lsn-1.
   *
   * - If the EpochSequencer is in DRAINING or DYING state, check if all
   *   Appenders in the sliding window are reaped. If so, move the
   *   EpochSequencer into QUIESCENT state and notify parent sequencer
   *   if necessary.
   *
   * - Notify the parent Sequencer, attempting to update last released lsn
   *   for the log. The global last released is updated if LNG is updated
   *   and it is still within the same epoch as reaped_lsn.
   *
   * TODO 7467469: maybe improve the error report with per-epoch sequencers
   *
   * @param replicated               Indicates if the Appender is fully
   *                                 replicated when reaped
   *
   * @param reaped_lsn               the lsn of Appender whose Reaper is
   *                                 calling this function.
   *
   * @param tail_record              TailRecord that correspond to the reaped
   *                                 Appender
   *
   * @param last_released_epoch_out  If this function returns false and sets err
   *                                 to E::STALE, the epoch component of
   *                                 last_released_ is written here, useful for
   *                                 diagnostic messages.
   *
   * @param lng_changed_out          Set to true if lng_ was changed to
   *                                 reaped_lsn, set to false otherwise.
   *
   * @return true iff last_released_ was successfully updated. If false is
   *         returned, err is set to:
   *         - E::PREEMPTED   if epoch `lsn_to_epoch(reaped_lsn)` was preempted;
   *         - E::STALE       if this sequencer was re-activated with a
   *                          higher-numbered epoch. In this case, the new
   *                          epoch is written to @param last_released_epoch_out
   *         - E::ABORTED     if the appender is aborted without even advancing
   *                          LNG of the epoch
   */
  bool noteAppenderReaped(Appender::FullyReplicated replicated,
                          lsn_t reaped_lsn,
                          std::shared_ptr<TailRecord> tail_record,
                          epoch_t* last_released_epoch_out,
                          bool* lng_changed_out);

  /**
   * @return sequence number that will be assigned to the next record. If the
   *         EpochSequencer is not in ACTIVE state, return the LSN of the last
   *         accetped Appender plus 1.
   */
  lsn_t getNextLSN() const {
    return window_.next();
  }

  /**
   * @return see lng_ below. Returns lsn(epoch_, ESN_INVALID) if current epoch
   *         does not yet have any fully stored records.
   */
  lsn_t getLastKnownGood() const {
    return lng_.load();
  }

  /**
   * @return see tail_record_ below. Returns the tail record of this epoch.
   */
  std::shared_ptr<TailRecord> getTailRecord() const {
    return tail_record_.load();
  }

  /**
   * @return LSN of the last reaped Appender in the sliding window. Returns
   *         lsn(epoch_, ESN_INVALID) if current epoch does not yet have any
   *         Appenders reaped.
   */
  lsn_t getLastReaped() const {
    return last_reaped_.load();
  }

  /**
   * Checks if there are enough sequence numbers available in the current
   * epoch. If false, runAppend() will fail with TOOBIG and the sequencer
   * will have to be advanced to a new epoch in order to accept more writes.
   */
  bool hasAvailableLsns() const {
    return window_.can_grow();
  }

  /**
   * @return current number of appends in flight (current size of window of
   * appenders).
   */
  size_t getNumAppendsInFlight() const {
    return window_.size();
  }

  size_t getMaxWindowSize() const {
    return window_.capacity();
  }

  State getState() const {
    return state_;
  }

  logid_t getLogID() const {
    return log_id_;
  }

  epoch_t getEpoch() const {
    return epoch_;
  }

  const EpochSequencerImmutableOptions& getImmutableOptions() const {
    return immutable_options_;
  }

  /**
   * @return EpochMetaData associated with the epoch, used to create the
   *         EpochSequencer.
   */
  std::shared_ptr<const EpochMetaData> getMetaData() const;

  /**
   * @return copyset manager of the current epoch. Used for generating copysets
   *         for Appenders.
   */
  std::shared_ptr<CopySetManager> getCopySetManager() const {
    return copyset_manager_.get();
  }

  /**
   * Apply the WRITTEN_IN_METADATALOG flag to metadata stored.
   *
   * @return     true if the flag is set, false if the flag is already set
   */
  bool setMetaDataWritten();

  /**
   * Update nodeset_params in EpochMetaData to the given value. This happens if
   * a config change triggers a nodeset update, but the new nodeset
   * is identical to the current one, so instead of reactivating
   * sequencer we apply the non-significant EpochMetaData update in-place.
   *
   * Requires metadata written flag to be set.
   * Requires external synchronization: two calls to this method aren't allowed
   * to overlap.
   *
   * @return  true if the change was made, false if the params already has this
   *          value.
   */
  bool setNodeSetParams(const EpochMetaData::NodeSetParams& params);

  /**
   * Expose draining_target_ for unit tests, which is the LSN of last accepted
   * Appender when draining/destruction started.
   *
   * @return   LSN_MAX if draining/destruction is not yet started
   */
  lsn_t getDrainingTarget() const {
    return draining_target_.load();
  }

  /**
   * @return  a boolean value indicating if there are enough nodes in the node
   *          set to complete an APPEND request. If false is returned, sets
   *          err to:
   *            OVERLOADED  if too many storage nodes are overloaded
   *            NO_SPC      if too many storage nodes are out of disk space
   *            UNROUTABLE  if too many storage nodes have no route to them
   *            DISABLED    if too many storage nodes persistently don't accept
   *                        writes for this log
   */
  bool checkNodeSet() const;

  /**
   * Called when the cluster config has changed causing the effective nodeset
   * to change. Rebuild the copyset manager.
   */
  virtual void noteConfigurationChanged(
      const std::shared_ptr<Configuration>& cfg,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      const Settings& settings);

  /**
   * The following are proxy functions that access the parent Sequencer object,
   * ususally through the same function name.
   */

  virtual void noteAppenderPreempted(epoch_t epoch, NodeID preempted_by);

  virtual NodeID checkIfPreempted(epoch_t epoch) const;

  virtual bool epochMetaDataAvailable(epoch_t epoch) const;

  virtual void schedulePeriodicReleases();

 protected:
  // notify parent sequencer that draining for this epoch is completed and
  // all records in the epoch are fully stored
  virtual void noteDrainingCompleted(Status drain_status);

  // notify parent sequencer when an Appenders is reaped and changed LNG for the
  // epoch, possibly updating last released lsn of the log
  virtual bool updateLastReleased(lsn_t reaped_lsn,
                                  epoch_t* last_released_epoch_out);

  // send asynchronous requests to notify all Workers to abort all running
  // Appenders belonging to this epoch
  virtual void abortAppenders();

  virtual const Settings& getSettings() const;

  Sequencer* getParent() const {
    return parent_;
  }

  virtual Processor* getProcessor() const;

 private:
  // Sequencer object for the log, currently assumed to always outlive the
  // EpochSequencer object
  // TODO 7467469: if we ever going to destroy sequencers, changing this to a
  // weak reference
  Sequencer* const parent_;

  const logid_t log_id_;

  // epoch in which the sequencer is issuing LSNs
  const epoch_t epoch_;

  const EpochSequencerImmutableOptions immutable_options_;

  // EpochMetaData for epoch_, used for Appender replication.
  // Can be updated, but only in "non-substantial" and simple ways:
  //  1. setMetaDataWritten() sets WRITTEN_IN_METADATALOG flag. Idempotent.
  //  2. setNodeSetParams() updates nodeset_params. Done only
  //     when WRITTEN_IN_METADATALOG flag is set. External synchronization
  //     ensures that two calls can't overlap.
  // Updates are done by making a copy and atomically updating the pointer.
  UpdateableSharedPtr<const EpochMetaData> metadata_;

  // copyset manager for the epoch
  FastUpdateableSharedPtr<CopySetManager> copyset_manager_;

  // current state of the EpochSequencer
  std::atomic<State> state_;

  // sliding window of active Appender objects. window_.grow() returns
  // the next sequence number to use.
  SlidingWindowSingleEpoch<Appender, Appender::Reaper> window_;

  // for serializing state changes
  mutable std::mutex state_mutex_;

  ///// Accumulative state of the epoch ////////

  // "last known good" ESN in this epoch. This is
  // the highest ESN such that all records in this epoch
  // (epoch_) up to and including that ESN are known to be fully
  // stored on enough storage nodes.
  //
  // initialized to lsn(epoch, ESN_INVALID). The value is an atomic<lsn_t>
  // but the epoch part of lng_ always equals to this epoch (= window_.epoch()).
  std::atomic<lsn_t> lng_;

  // last reaped lsn of the epoch, updated whenever an Appender is reaped from
  // the sliding window. Similar to lng_, initialized to
  // lsn(epoch, ESN_INVALID). However, last_reaped_ might be ahead of lng_ in
  // case there are Appenders retired and reaped without being fully replicated
  // (e.g., preempted, aborted, ...)
  std::atomic<lsn_t> last_reaped_;

  // TODO 7467469: more accumulated states, byte offset, etc
  // Amount of data written to current epoch up to last appended record.
  AtomicOffsetMap offsets_within_epoch_;

  // Target lsn for the completion of draining/destruction (i.e.,
  // last appender lsn in the window when it was disabled). Initialized to
  // LSN_MAX. Should only be set once for the lifetime of the EpochSequencer.
  std::atomic<lsn_t> draining_target_{LSN_MAX};

  // tail record of the epoch, should be the record at LNG but it's not
  // guaranteed to be always in sync since tail_record_ and lng_ are not
  // updated atomically. nullptr if no records are fully replicated yet for
  // the epoch
  folly::atomic_shared_ptr<TailRecord> tail_record_;

  // return true if the epoch is quiescent, i.e., last_reaped_ advanced to
  // draining_target_
  bool epochQuiescent() const;
  // return true if the epoch is drained, i.e., LNG advanced to draining_target_
  bool epochDrained() const;

  // called when the epoch becomes quiescent. possible notify parent Sequencer
  // that the draining is completed
  void onEpochQuiescent();

  // helper function that attempts to transition the EpochSequencer to DRAINING
  // or DYING state. Implements startDraining() and startDestruction().
  // Note: @param next must be either State::DRAINING or State::DYING
  void transitionTo(State next);

  // Atomically increase current epoch byte size by number of bytes in current
  // appender.
  // If sufficient bytes have been appended to the epoch since the last
  // reporting of the epoch byte offset, offset information is attached to
  // appender so it can be transmitted in appender's STORE messages.
  void processNextBytes(Appender* appender);
};

}} // namespace facebook::logdevice
