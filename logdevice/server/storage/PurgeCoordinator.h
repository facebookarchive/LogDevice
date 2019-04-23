/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include <folly/IntrusiveList.h>
#include <folly/Optional.h>

#include "logdevice/common/Address.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Seal.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LogStorageState_PurgeCoordinator_Bridge.h"

namespace facebook { namespace logdevice {

class CLEAN_Message;
class LogStorageState;
class PurgeUncleanEpochs;
class RELEASE_Message;
enum class ReleaseType : uint8_t;

/**
 * @file Coordinates RELEASE messages, CLEAN messages and PurgeUncleanEpochs
 * instances for a single log on a shard.
 *
 * In particular, when a RELEASE message comes in, this class decides if it
 * can immediately be processed.  If not (because there are some unclean
 * epochs before the one the RELEASE is for), the coordinator buffers the
 * RELEASE and kicks off a PurgeUncleanEpochs state machine.
 *
 * When a CLEAN message comes in, we similarly kick off a state machine and
 * only send a CLEANED response later.
 *
 * An additional wrinkle is that we may receive RELEASE and CLEAN messages for
 * newer epochs while a state machine is still running for an older epoch.  It
 * is this class's responsibility to sort that all out.
 */

class PurgeCoordinator : public LogStorageState_PurgeCoordinator_Bridge {
 public:
  PurgeCoordinator(logid_t log_id,
                   shard_index_t shard,
                   LogStorageState* parent);

  /**
   * Static handlers for incoming CLEAN and RELEASE messages; validates and
   * hands over to per-log PurgeCoordinator instance.
   */
  static Message::Disposition onReceived(CLEAN_Message* msg,
                                         const Address& from);
  static Message::Disposition onReceived(RELEASE_Message* msg,
                                         const Address& from);

  //
  // NOTE: all public methods expect the mutex *not* to be held
  //

  /**
   * Called when a RELEASE message is received from the sequencer.  If all
   * previous epochs are clean, the RELEASE is processed immediately.
   * Otherwise, we kick off a PurgeUncleanEpochs task to clean older epochs and
   * buffer the RELEASE internally.
   *
   * @param do_broadcast  Whether or not to call
   *                      ReleaseRequest::broadcastReleaseRequest(). May be
   *                      `false' in case there is already a pending
   *                      MergeMutablePerEpochLogMetadataTask that will do
   *                      the broadcast on termination. (The intention is to
   *                      broadcast the release request exactly once, after
   *                      the record cache or, if necessary, the metadata in
   *                      storage have been updated by that task.)
   */
  void onReleaseMessage(lsn_t lsn,
                        NodeID from,
                        ReleaseType release_type,
                        bool do_broadcast,
                        OffsetMap epoch_offsets = OffsetMap()) override;

  /**
   * Called when a CLEAN message is received from the sequencer.  Starts
   * PurgeUncleanEpochs to purge any unclean epochs and update the last clean
   * epoch entry in the local log store.
   *
   * @param clean_msg   CLEAN message received, will take its ownership
   * @param worker      ID of worker that originally received the CLEAN message
   *                    from the sequencer
   */
  void onCleanMessage(std::unique_ptr<CLEAN_Message> clean_msg,
                      NodeID from,
                      Address reply_to,
                      worker_id_t worker);

  /**
   * Attempt to start purging for buffered messages if any. Called by workers
   * when there are more purges for releases allowed to run.
   */
  void startBuffered();

  /**
   * Called by the state machine when the last clean epoch is updated or read
   * from the local log store.
   */
  void updateLastCleanInMemory(epoch_t epoch) override;

  /**
   * Called if this purge state machine enters a permanent error state because
   * a storage task encountered an un-recoverable failure.
   *
   * This function puts the local log store in fail-safe mode and notifies
   * LogStorageState that last_released_lsn will never be advanced.
   *
   * Note: purging will be stalled until the local log store is repaired and
   *       logdeviced is restarted.
   */
  void onPermanentError(const char* context, Status status);

  /**
   * Called when the currently running PurgeUncleanEpochs state machine has
   * completed.  We need to process any buffered RELEASEs and send any CLEANED
   * replies.
   */
  void onStateMachineDone();

  /**
   * Called by Worker when it is shutting down, to destroy the running
   * PurgeUncleanEpochs instance right away.
   */
  void shutdown();

  ~PurgeCoordinator() override;

 private:
  // LSN that we got a RELEASE for and NodeID of the sequencer we got it from.
  struct BufferedRelease {
    lsn_t lsn;
    NodeID from;
  };

  // If active_purge_ is not empty, this contains a set of CLEAN messages that
  // we have not replied to yet.  When a state machine completes, it sends any
  // replies and kicks off another state machine if needed (if we got more
  // CLEAN messages while the previous state machine was working).
  struct BufferedClean {
    std::unique_ptr<CLEAN_Message> message;
    NodeID from;
    Address reply_to;
    worker_id_t worker;
  };

  const logid_t log_id_;
  const shard_index_t shard_;
  LogStorageState* const parent_;

  // We'll be called from different worker threads and need non-trivial
  // synchronization for the remaining members.  They will only be used on
  // slow paths.
  std::mutex mutex_;

  // Currently running PurgeUncleanEpochs state machine, if any.  The state
  // machine is running on a worker and must be destroyed on the same worker.
  std::shared_ptr<PurgeUncleanEpochs> active_purge_;

  // this contains the highest LSN that we got a RELEASE for, as well as the
  // NodeID of the sequencer we got it from, but did not process yet.
  folly::Optional<BufferedRelease> buffered_release_;

  std::vector<BufferedClean> buffered_clean_;

  // Part of the lock-free path of processing RELEASE messages, called after
  // onReleaseMessage() has ascertained that all earlier epochs are clean and
  // we can process the RELEASE.  Updates the LogStorageStateMap and
  // broadcasts to workers.
  void doRelease(lsn_t lsn,
                 ReleaseType release_type,
                 bool do_broadcast,
                 OffsetMap epoch_offsets);

  // Check if the CLEAN message sent by a sequencer with @param sequencer_epoch
  // is preempted by existing Seals for the log on the storage node
  // @return      <Status::OK, Seal()>             if the CLEAN is not preempted
  //
  //              <Status::PREEMPTED, preempted_seal> if the CLEAN is preempted
  //                                                  by the preempted_seal
  std::pair<Status, Seal> checkPreemption(epoch_t sequencer_epoch);

  // Helper function for writing the EpochRecoveryMetadata and sending CLEANED
  // responses
  void sendCleanedResponse(Status status,
                           std::unique_ptr<CLEAN_Message> clean_msg,
                           Address reply_to,
                           worker_id_t worker_id,
                           Seal preempted_seal);

  // Returns true if the logsconfig is loaded and the log
  // does exist in logs config. Used to drop processing release
  // message if log does not exist as purging will fail
  virtual bool logExistsInConfig();

  // helper function for checking and updating last clean epoch in memory
  void updateLastCleanEpochInRecordCache(epoch_t lce);

  // helper method for starting purging with given buffered messages
  void startBufferedMessages(std::vector<BufferedClean> buffered_clean,
                             folly::Optional<BufferedRelease> buffered_release);

  // Start purge if not already running
  void startPurge(std::unique_lock<std::mutex> guard,
                  epoch_t purge_to,
                  epoch_t new_last_clean_epoch,
                  NodeID from,
                  ResourceBudget::Token token = ResourceBudget::Token());
};

}} // namespace facebook::logdevice
