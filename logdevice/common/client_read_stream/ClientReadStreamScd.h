/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/noncopyable.hpp>
#include <folly/FBVector.h>

#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/client_read_stream/ClientReadStreamFailureDetector.h"
#include "logdevice/common/client_read_stream/ClientReadStreamSenderState.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/START_Message.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

class Timer;

/**
 * @file Contains the logic of the Single Copy Delivery (SCD) optimization
 *       that makes it possible to receive only one copy of each record, saving
 *       network bandwidth and CPU.
 *
 *       When SCD is active, storage shards only send records for which they are
 *       the primary recipient in that record's copyset, causing each record to
 *       be sent once (assuming the record's copyset is equal in each copy).
 *
 *       @see doc/single-copy-delivery.md for more information about scd and
 *       how storage shards decide whether or not to send a copy.
 *
 *       This class contains the logic for failover mechanisms of the SCD
 *       optimization, which is always triggered by the client.
 *       We maintain a list of filtered out ShardIDs that we send to the storage
 *       shards via the START message. This list is used by the shards to know
 *       which shards are filtered out, and to send the records those shards
 *       were supposed to send if they are next in the copyset.
 *
 *       The filtered out list is formed of two sublists:
 *       - the shards down list contains shards that the client knows are down;
 *       - the shards slow list contains shards that are much slower than the
 *         others, and by filtering them out we can failover to a healthier
 *         shard;
 *
 *       There are different failover and recovery scenarios that we handle:
 *
 *       1. Immediate failover to ALL_SEND_ALL due to missing records.
 *          checkNeedsFailoverToAllSendAll() is the method that verifies if
 *          there is a reason to immediately switch to ALL_SEND_ALL mode. This
 *          function is called each time a gap or record is received.
 *          If at some point the number of shards that can't send
 *          next_lsn_to_deliver_ is measured to be equal to readSetSize(), we do
 *          the failover because this means that the storage shard that is
 *          supposed to send next_lsn_to_deliver_ does not have it or thinks
 *          it's not supposed to send it.
 *
 *      2. Rewinding the stream due to some connection failures.
 *         When ClientReadStream determines that a storage shard cannot send us
 *         its records, it calls addToShardsDownAndScheduleRewind(). This
 *         function ensures we soon rewind the stream with that shard in the
 *         filtered out list. ClientReadStream calls
 *         addToShardsDownAndScheduleRewind() when:
 *         - It could not send START because sendStartMessage() failed;
 *         - A shard sent STARTED with status=E::AGAIN or an unexpected status;
 *         - The socket to a storage shard was closed.
 *
 *      3. Immediate primary failover due to checksum fail.
 *         If we receive a record with a checksum fail, ClientReadStream calls
 *         addToShardsDownAndScheduleRewind() similarly.
 *
 *      4. Immediate primary failover due to a shard rebuilding.
 *         If we receive a STARTED(status=E::REBUILDING), ClientReadStream calls
 *         addToShardsDownAndScheduleRewind() similarly.
 *
 *      5. Optional primary failover due to a slow shard. If a shard (or set of
 *         shards) is slower at completing windows that all the other shards,
 *          we will add it to the shards slow list and rewind the stream.
 *          @see ClientReadStreamFailureDetector.
 *
 *      6. Failover to ALL_SEND_ALL because we are stuck:
 *         In rare cases where we can not make progress for some time and there
 *         are too many shards that are stuck, we failover to ALL_SEND_ALL mode.
 *         The `all_send_all_failover_timer_` timer takes care of detecting
 *         that.
 *
 *      7. Recovery to SCD:
 *         When in ALL_SEND_ALL mode, we switch back to SCD when the window is
 *         slid.
 *
 *      8. Removing shards from the shards down list.
 *         When a shard sends a record, ClientReadStream calls
 *         scheduleRewindIfShardBackUp() which checks if the shard is in the
 *         shards down list but has been sending records not in an
 *         under-replicated region. If that's the case, a rewind is scheduled to
 *         remove the shard from the shards down list.
 *
 *      9. Removing shards from the shards slow list.
 *         Shards will be removed from the slow shards list if we need to add
 *         another shard to the filtered out list, and we can't do that without
 *         the filtered out list satisfying the replication requirements. When
 *         this happens we will remove from the list the oldest slow shard
 *         We don't actively remove from the slow shards list whenever we detect
 *         that shards stop being slow because this can cause ping-ponging
 *         between adding/removing the shard from the list and thus doing
 *         unnecessary rewinds.
 */

class ClientReadStream;

class ClientReadStreamScd : public boost::noncopyable {
 public:
  enum class Mode { SCD, ALL_SEND_ALL, LOCAL_SCD };

  ClientReadStreamScd(ClientReadStream* owner, Mode mode);

  ~ClientReadStreamScd();

  /**
   * @return true if we are currently in SCD or LOCAL_SCD mode.
   */
  bool isActive() const {
    return mode_ == Mode::SCD || mode_ == Mode::LOCAL_SCD;
  }

  /**
   * @return true if we are currently in LOCAL_SCD mode.
   */
  bool localScdEnabled() const {
    return mode_ == Mode::LOCAL_SCD;
  }

  /**
   * Called when settings are updated.
   */
  void onSettingsUpdated();

  /**
   * Called when a storage shard's next lsn changed.
   */
  void onShardNextLsnChanged(ShardID shard, lsn_t next);

  /**
   * Called when a storage shard's authoritative status changed.
   */
  void setShardAuthoritativeStatus(ShardID shard, AuthoritativeStatus status);

  /**
   * Called by ClientReadStream::rewind(). Apply any scheduled changes to
   * transition to ALL_SEND_ALL or SCD if `scheduled_mode_transition_` is set.
   *
   * If SCD is currently active or if `scheduled_mode_transition_`==SCD, apply
   * any scheduled changes to the filtered out list.
   *
   * This function may decide that it's best to continue in ALL_SEND_ALL mode if
   * there have been no changes to the filtered out list.
   */
  void applyScheduledChanges();

  /**
   * Schedule a rewind to the given mode.
   * This function asserts that the current mode is not the requested mode.
   */
  void scheduleRewindToMode(Mode mode, std::string reason);

  /**
   * When in SCD mode, check how many shards do not have the next record to be
   * shipped. If this number is equal to the number of shards we are reading
   * from, failover to all send all mode.
   *
   * Called when:
   * - the cluster is shrunk (@see noteShardsRemovedFromConfig);
   * - a gap is received, or a record with lsn > next_lsn_to_deliver_ is
   *   received (@see updateGapState).
   *
   * @return true If we failed over to all send all mode, false otherwise.
   */
  bool checkNeedsFailoverToAllSendAll();

  /**
   * Check if the given shard was in the shards down list and has been sending
   * data. If that's the case, remove it from the shards down list and schedule
   * a rewind.
   */
  void scheduleRewindIfShardBackUp(ClientReadStreamSenderState& state);

  // Called whenever a window is slid.
  void onWindowSlid(lsn_t hi, filter_version_t filter_version);

  /**
   * @return List of shards to be filtered out, including both shards down and
   * shards slow.
   */
  const small_shardset_t& getFilteredOut() const {
    return filtered_out_.getAllShards();
  }

  /**
   * @return List of shards considered down.
   */
  const small_shardset_t& getShardsDown() const {
    return filtered_out_.getShardsDown();
  }
  /**
   * @return List of shards considered slow.
   */
  const small_shardset_t& getShardsSlow() const {
    return filtered_out_.getShardsSlow();
  }

  /**
   * Update the storage shard set that the filtered out list is based on.
   *
   * If any shards were removed from the filtered out list we will also
   * schedule a rewind.
   * Switch to all send all mode instead if the conditions are now held,
   * which can happen if all the remaining shards sent us a record with lsn >
   * next_lsn_to_deliver_.
   *
   * @return true if a rewind was scheduled
   */
  bool updateStorageShardsSet(const StorageSet& storage_set,
                              const ReplicationProperty& replication);

  /**
   * Called when we are unable to read from a storage shard because either:
   * - ClientReadStream::sendStartMessage() returned -1;
   * - ClientReadStream::onStartSent() is called with status != E::OK;
   * - ClientReadStream::onStarted() is called with msg.header_.status ==
   *   E::AGAIN or E::REBUILDING (or an unexpected error);
   * - ClientReadStream::onSocketClosed() is called;
   * - a storage shard sent a CHECKSUM_FAIL gap.
   * - a storage shard's authoritative status is changed to UNDERREPLICATION,
   *   AUTHORITATIVE_EMPTY, or UNAVAILABLE.
   *
   * Append the shard to `pending_immediate_scd_failover_` and schedule a
   * rewind. When the rewind happens in the next iteration of the event loop,
   * shards in `pending_immediate_scd_failover_` will be added to the shards
   * down list.
   *
   * Note: this function does not rewind the streams synchronously so that it
   * can be called multiple times in a row and the streams rewinded only once,
   * and also in order to not have sendStart() be called from within the call
   * stack of another call to sendStart().
   *
   * @return True if the rewind was scheduled (or if there is already a rewind
   * scheduled), or False if the given shard is already in the shards down list.
   */
  bool addToShardsDownAndScheduleRewind(const ShardID&, std::string reason);

  /**
   * Called when ClientReadStream changed the GapState of a sender.
   * Used to maintain a proper accounting of the number of shards in the
   * filtered out list that have gap state in (GapState::GAP,
   * GapState::UNDER_REPLICATED).
   */
  void
  onSenderGapStateChanged(ClientReadStreamSenderState& state,
                          ClientReadStreamSenderState::GapState prev_gap_state);

  /**
   * Returns the number of shards that are in filtered out list but sent a
   * record or gap with lsn > next_lsn_to_deliver_.
   */
  nodeset_size_t getGapShardsFilteredOut() const {
    return gap_shards_filtered_out_;
  }

  nodeset_size_t getUnderReplicatedShardsNotBlacklisted() const {
    return under_replicated_shards_not_blacklisted_;
  }

 private:
  // ClientReadStream that owns us.
  ClientReadStream* owner_;

  /**
   * Set to Mode::SCD when single copy delivery (scd) is active. When scd is
   * active we will receive only one copy of every record in steady state.
   */
  Mode mode_;

  /**
   * Number of shards for which the smallest LSN that might be delivered by that
   * shard is strictly greater than next_lsn_to_deliver_ and that shard is
   * currently in the filtered out list.
   *
   * Note: we do not bother adjusting this value each time we add or remove a
   * shard from the filtered out list because we immediately rewind the stream
   * when doing this, which blows away the gap state and resets this value to
   * zero.
   */
  nodeset_size_t gap_shards_filtered_out_ = 0;

  /**
   * Number of shards in GapState::UNDER_REPLICATED that are not in known down.
   */
  nodeset_size_t under_replicated_shards_not_blacklisted_ = 0;

  /**
   * There are two failover timers. One that rewinds the streams while adding
   * shards to the shards down list and one that rewinds the streams in all send
   * all mode.
   * This struct keeps data common to these two timers.
   */
  struct FailoverTimer {
    typedef std::function<bool(small_shardset_t new_nodes_down)> Callback;

    FailoverTimer(ClientReadStreamScd* scd,
                  std::chrono::milliseconds period,
                  Callback cb);

    void cancel();
    void activate();

    // The timer object.
    std::unique_ptr<Timer> timer_;
    // Period of the timer.
    std::chrono::milliseconds period_;
    // LSN of the next record we expected to deliver the last time the timer
    // expired. Each time the timer expires, we check next_lsn_to_deliver_
    // against this value. If the values are equal this means we could not make
    // progress during the timer's period.
    lsn_t next_lsn_to_deliver_at_last_tick_;
    // Callback called when the timer ticks and we were not able to make any
    // progress. new_nodes_down is the list of shards that should be added to
    // the known down list. The callback should return true if the timer should
    // be re-activated.
    Callback cb_;
    ClientReadStreamScd* scd_;
    void callback();

    friend class ClientReadStreamTest;
  };

  // Read the settings to configure the ClientReadStreamFailureDetector.
  // This may cause a rewind to be scheduled if the outliers change as a result
  // (because we disabled outlier detector or moved to observe only mode).
  void configureOutlierDetector();

  // Must be called when:
  // * the BlacklistState of a shard changed;
  // * the authoritative status of a shard changed;
  // * the read set changed.
  // If the outlier detector is enabled, notify it of the change.
  void updateFailureDetectorWorkingSet();

  // Activate the given timer.
  void activateTimer(FailoverTimer& timer);

  // When in single copy delivery mode, this timer will trigger at a regular
  // interval and check if we need to change the shards down list and rewind the
  // stream.
  FailoverTimer shards_down_failover_timer_;
  // The callback for this timer.
  bool shardsDownFailoverTimerCallback(small_shardset_t down);

  // When in single copy delivery mode, this timer will trigger at a regular
  // interval and check if we need to failover to all send all mode.
  FailoverTimer all_send_all_failover_timer_;
  // The callback for this timer.
  bool allSendAllFailoverTimerCallback(small_shardset_t down);

  /**
   * When in single copy delivery mode, maintains the list of filtered out
   * shards sent to storage shards.
   */
  class FilteredOut {
   public:
    enum class ShardState : bool { FILTERED = true, NOT_FILTERED = false };
    using FailureDomain =
        FailureDomainNodeSet<ShardState, HashEnum<ShardState>>;
    // Returns the shards down list.
    const small_shardset_t& getShardsDown() const {
      return shards_down_;
    }
    // Returns the shards slow list.
    const small_shardset_t& getShardsSlow() const {
      return shards_slow_;
    }
    // Returns all the filtered out shards.
    const small_shardset_t& getAllShards() const {
      return all_shards_;
    }
    // Returns the new shards slow list.
    const ShardSet& getNewShardsSlow() const {
      return new_shards_slow_;
    }
    // Returns the new shards down list.
    const ShardSet& getNewShardsDown() const {
      return new_shards_down_;
    }

    // Schedule a change to the list of slow shards.
    // @returns true if the change was scheduled or false if the scheduled list
    // of slow shards is already equal to `outliers`.
    bool deferredChangeShardsSlow(ShardSet outliers);

    // Schedule given shard to be added to the shards down/slow list the next
    // time applyDeferredChanges is called.
    // @returns whether the shard was added
    bool deferredAddShardDown(ShardID shard);

    // The given shard will be removed from the shards down list the next time
    // applyDeferredChanges is called.
    // @returns whether the shard was removed
    bool deferredRemoveShardDown(ShardID shard);

    // Applies all deferred changes to the shard down/slow list.
    // @returns whether there were any changes at all to be applied
    bool applyDeferredChanges();

    // Removes the shard from the filtered out list (including shards
    // down/slow list). Returns true if the shard was in the list.
    // If the shard was not in the list but was scheduled to be added,
    // cancel that.
    bool eraseShard(ShardID shard);

    // Clear all the lists and any scheduled change.
    void clear();

   private:
    // The current shards slow/down list
    small_shardset_t shards_slow_;
    small_shardset_t shards_down_;
    // The union of the shards down and shards slow list.
    small_shardset_t all_shards_;
    // Shards that are scheduled to replace the shards down/slow list.
    ShardSet new_shards_down_;
    // Shards that are scheduled to be added to the shards slow list, mapped
    // to the time this scheduling happened.
    ShardSet new_shards_slow_;
    friend class ClientReadStreamScd_FilteredOutTest;
  };

  std::unique_ptr<ClientReadStreamFailureDetector> outlier_detector_;

  FilteredOut filtered_out_;

  // Callback called by ClientReadStreamFailureDetector when it detects
  // outliers.  Will blacklist the outliers according to SCD rules if the
  // outlier detector is not configured in observe-only mode.
  void onOutliersChanged(ShardSet outliers, std::string reason);
  // Blacklist a set of outliers according to SCD rules. No-op if this read
  // stream is in ALL_SEND_ALL mode.
  void rewindWithOutliers(ShardSet outliers, std::string reason);

  // Utility method to get from client settings whether the detection of slow
  // shards is enabled.
  bool isSlowShardsDetectionEnabled();

  /**
   * Checks if we need to rewind after the storage shards set has been updated.
   *
   * @return true if the streams were rewound.
   */
  bool maybeRewindAfterShardsUpdated(bool shards_udpated);

  // When a rewind is scheduled, this is set to Mode::SCD or Mode::ALL_SEND_ALL
  // if this rewind should cause a transition to SCD or ALL_SEND_ALL mode
  // respectively.
  folly::Optional<Mode> scheduled_mode_transition_;

  // Returns true if a transition to the given mode is scheduled.
  bool scheduledTransitionTo(Mode mode) {
    return scheduled_mode_transition_.hasValue() &&
        scheduled_mode_transition_.value() == mode;
  }

  friend class ClientReadStreamTest;
  friend class ClientReadStreamScd_FilteredOutTest;
};

}} // namespace facebook::logdevice
