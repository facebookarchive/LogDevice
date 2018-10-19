/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <mutex>

#include "logdevice/common/Appender.h"
#include "logdevice/common/CompletionRequest.h"
#include "logdevice/common/WriteMetaDataRecord.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Controller class responsible for managing writes to a metadata log.
 *       Owned by the sequencer for the corresponding data log. To perform
 *       the write operation, it spawns a WriteMetaDataRecord state machine,
 *       which essentially wraps a special sequencer for metadata logs, and
 *       passes the Appender object to the state machine. It orchestrates the
 *       WriteMetaDataRecord object for its entire life time, and enforces
 *       serialization for writes to metadata log by by only allowing one
 *       running WriteMetaDataRecord at a time.
 *
 *       After a write operation is done, MetdDataLogWriter needs to ensure that
 *       the parent sequencer is activated to a new epoch before processing the
 *       next write, for two reasons:
 *       (1) the metadata sequencer gets its epoch from the current epoch of the
 *           parent sequencer, so we need to make sure the next metadata
 *           sequencer will get a higher epoch
 *       (2) The write request was usually because of epoch metadata for the log
 *           was also updated in epoch store, so we would like the current
 *           data log sequencer to reactivate to get the latest epoch metadata
 *           from the epoch store.
 *
 *  Note that before starting a WriteMetaDataRecord state machine with the
 *  Appender, MetaDataLogWriter checks the payload of the Appender to see if it
 *  contains valid epoch metadata. Moreover, it checks the current epoch (_e_)
 *  of the data log sequencer against the `epoch' (_e0_) of the epoch metadata
 *  in the payload, and ensures it only starts the Appender iff _e_ >= _e0_ - 1.
 *  Otherwise, the append is rejected with E::STALE and data sequencer will be
 *  reactivated to a higher epoch.
 *
 *  The reason why we need such guarantee is to address a corner case of lsn
 *  reordering in the following scenario:
 *  1. sequencer on Node2 gets activated with epoch 5 while an existing
 *     sequencer on Node1 is running in epoch 4
 *  2. append A first gets routed to Node2 and successfully get fully stores,
 *     and it has a lsn in epoch 5.
 *  3. later, append B issued by the same client gets routed to Node1 because
 *     the state of cluster has changed (e.g., network event).
 *  4. If Node2 has not yet SEALed f-majority of the storage nodes during
 *     recovery, append B may get fully stored with a lsn of epoch 4 returned to
 *     the client. Otherwise, append B may get partially stored.
 *
 *  This may violate the invariant of monotonically increasing lsn in metadata
 *  logs. For correctness, we rely on the fact that record written later
 *  (with a higher metadata epoch) must appear after (with a higher lsn) than
 *  records written earlier. Moreover, even if append B gets partially stored
 *  and sends back E::PREEMPTED, the invariant is still violated as
 *  metadata log record can be read using the IGNORE_RELEASED_STATUS flag
 *  (See MetaDataLogReader.h).
 *
 *  With the guarantee that only accepting metadata append when epoch of data
 *  log sequencer is not stale the above corner case is impossible to happen.
 *  Below is a brief proof:
 *
 *  Invariants:
 *  I1: metadata log appends are always issused by a unique metadata utility
 *      after a sucessful update to the epoch store
 *  I2: payload of a metadata log append contains epoch metadata in the epoch
 *      store as the result of its last update. Specifically, the epoch field
 *      of the metadata is the `next epoch' at the time of update.
 *  I3: metadata utility only try to update epoch store and append a new
 *      metadata record iff the metadata log record for the previous update is
 *      fully stored
 *  I4: two metadata log appends processed by the same MetaDataLogWriter must be
 *      assigned strictly increasing epochs
 *  I5: MetaDataLogWriter only process metadata appends of epoch _e_ when the
 *      current epoch of the data sequencer >= _e_ - 1
 *
 *  Consider two metadata log record appends M1 and M2 issued by the utility.
 *  M1 is issued first and gets a success status, the epoch of its assigned lsn
 *  is _e1_ and its metadata epoch is _em1_. M2 is issued after M1 is stored,
 *  its lsn epoch is _e2_ and its metadata epoch is _em2_. We want to proof
 *  that _e2_ > _e1_:
 *
 *  1) from I5 we can get _e1_ >= _em1_ - 1 and _e2_ >= _em2_ -1;
 *  2) since _e1_ is an actual epoch used by a data sequencer, and _em2_ is the
 *     `next epoch' from the epoch store update AFTER _e1_ is assigned,
 *     according to I1 and I2 we have _em2_ > _e1_
 *  3) from 1) and 2) we know: _e2_ >= _em2_ - 1 > _e1_ - 1 which means
 *     _e2_ >= _e1_ given _e2_ and _e1_ are positive integers
 *  4) If _e2_ == _e1_, then M1 and M2 must be proccessed on the same node by
 *     the same data log sequencer and the same MetaDataLogWriter, according to
 *     I5, if M2 happen after M1, _e2_ must be larger than _e1_, a contradiction
 *  5) from (3) and (4) we get: _e2_ > _e1_
 *
 */

class Sequencer;
class Appender;
struct ExponentialBackoffTimerNode;

class MetaDataLogWriter {
 public:
  explicit MetaDataLogWriter(Sequencer* sequencer);

  ~MetaDataLogWriter();

  /**
   * Run an appender for the metadata log write request
   *
   * Can be called by multiple worker threads, but only one worker can proceed
   * to do the write at a time.
   *
   * @param appender   Appender object to run, if it is nullptr, a
   *                   MetaDataLogWriter will still be created just for
   *                   recovering the metadata log.
   *
   * @param bypass_recovery  Used for testing, skips recovery of the MetaData
   *                         log.
   *
   * @return  0 on success, -1 if operation failed, and err is set to:
   *   INPROGRESS   the appender cannot be started immediately because the
   *                metadata recovery is in progress. the appender should
   *                be buffered on the worker thread
   *   NOBUFS       a metadata log write operation is already in process
   *   NOSEQUENCER  metadata sequencer cannot be started or is in error state
   *   BADPAYLOAD   payload of the appender does not contain valid epoch
   *                metadata
   *   STALE        the epoch of the current data sequencer is stale
   *   SHUTDOWN     logdevice is shutting down
   *   INTERNAL     logdevice internal error
   */
  RunAppenderStatus runAppender(Appender* appender);

  /**
   * Called when the parent sequencer has been successfully activated.
   * Note: may be called from a different thread since we are not the only
   * one that activates the parent sequencer
   *
   * @param epoch    epoch number to which the parent sequencer activates to
   */
  void onDataSequencerReactivated(epoch_t epoch);

  /**
   * Called when the active WriteMetaDataRecord state machine is complete
   * @param st              Status of the finished state machine
   * @param last_released   updated last released lsn when the write finishes
   */
  void onWriteMetaDataRecordDone(Status st, lsn_t last_released);

  /**
   * Initiate recovery for the metadata log. The last released lsn will be
   * updated when recovery completes. Recovery is done by running a
   * WriteMetaDataRecord object without an appender, so during recovery
   * other metadata log appends will fail with E::NOBUFS.
   */
  void recoverMetaDataLog();

  /**
   * @return last released lsn for the metadata log. LSN_INVALID if the
   *         information is not available. In such case, and if recover=true, it
   *         calls recoverMetaDataLog() to recover the information.
   * @param recover If true, recover the metadata log to recover the last
   *                released lsn if it is missing.
   */
  lsn_t getLastReleased(bool recover = true);

  /**
   * Called by the Worker that is running a WriteMetaDataRecord object for
   * the controller. Abort the running write, switch the controller to
   * SHUTDOWN state and rejects all incoming writes.
   */
  void onWorkerShutdown();

  /**
   * Check if the payload of the appender to see if:
   * 1) it contains valid epoch metadata; and
   * 2) the epoch from the parent data sequencer is at least the epoch
   *    from metadata in payload - 1
   * return 0 iff both conditions are met, otherwise return -1 and set
   *        err to:
   *                BADPAYLOAD if given payload does not have valid metadata
   *                STALE      if epoch of data sequencer is stale
   */
  int checkAppenderPayload(Appender* appender, epoch_t epoch_from_parent);

  /**
   * Attempt to abort the running recovery-only WriteMetaDataRecord state
   * machine.
   */
  void abortRecovery();

  /**
   * Called when metadata sequencer was preempted, propagates the
   * information to data sequencer
   */
  void notePreempted(epoch_t epoch, NodeID preempted_by);

  /**
   * determine the worker thread index to run recovery-only WriteMetaDataRecord
   * state for a metadata log
   */
  static int recoveryTargetThread(logid_t meta_logid, int nthreads);

 private:
  enum class State : uint8_t {
    READY = 0,     // ready to accept a new metadata log appender
    WRITE_STARTED, // started a state machine to store the metadata log record
    WRITE_DONE,    // write state machine is done, needs to reactivate parent
    SHUTDOWN       // system is shutting down, stop accepting new writes
  };

  // indicate the current stage of the metadata writer
  std::atomic<State> state_;

  // epoch used by the metadata sequencer to write metadata records, atomic
  // since it is accessed in onDataSequencerReactivated(), which may be called
  // from multiple threads
  std::atomic<epoch_t::raw_type> current_epoch_{EPOCH_INVALID.val_};

  // epoch of the last finished WriteMetaDataRecord state machine
  // (recovery-only writers are excluded)
  std::atomic<epoch_t::raw_type> last_writer_epoch_{EPOCH_INVALID.val_};

  // indicate if the last created WriteMetaDataRecord state machine is
  // for recovery only
  std::atomic<bool> recovery_only_{false};

  // last released lsn for the metadata log
  std::atomic<lsn_t> last_released_{LSN_INVALID};

  // currently running WriteMetaDataRecord state machine, nullptr if no state
  // machine is running
  std::unique_ptr<WriteMetaDataRecord> running_write_;

  // The data log sequencer that owns the controller
  // guranteeed to outlive `this'
  Sequencer* const sequencer_;

  // identifies the Worker for the current running WriteMetaDataRecord state
  // machine
  worker_id_t current_worker_{-1};

  logid_t getDataLogID() const;
  logid_t getMetaDataLogID() const;

  // retry timer callback to ensure that the parent sequencer has successfully
  // reactivated to a new epoch
  static void checkActivation(logid_t log_id,
                              epoch_t epoch_before,
                              ExponentialBackoffTimerNode* node);

  // retry backoff timer delay interval
  static const std::chrono::milliseconds RETRY_ACTIVATION_INITIAL_DELAY;
  static const std::chrono::milliseconds RETRY_ACTIVATION_MAX_DELAY;
};

}} // namespace facebook::logdevice
