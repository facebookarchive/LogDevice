/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <vector>

#include <folly/Memory.h>
#include <folly/SharedMutex.h>
#include <folly/memory/EnableSharedFromThis.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sequencer.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file  A thread-safe class which, for each storage node, keeps track of the
 *        largest sequence number included in a RELEASE message successfully
 *        sent to that node. Its responsibility is to periodically
 *        (Settings::release_retry_interval) send releases to those nodes that
 *        don't have an up-to-date value.
 *
 *        PeriodicReleases internally keeps a timer (bound to some worker
 *        thread) which is created by schedule(). If a timer already exists,
 *        it will be reactivated. PeriodicReleases guarantees that a timer will
 *        only exist as long as there is activity on the log (new records are
 *        being released) or sending to some nodes keeps failing.
 *
 *        In addition to the retry timer which gets activated on a send failure,
 *        PeriodicReleases also have a broadcast timer which unconditionally
 *        broadcast releases to the union of all historical and current storage
 *        shards tracked by the sequencer. The broadcast timer fires at a much
 *        higher interval.
 */

class Sequencer;
struct ExponentialBackoffTimerNode;
enum class ReleaseType : uint8_t;

class PeriodicReleases
    : public folly::enable_shared_from_this<PeriodicReleases> {
 public:
  enum class Type { RETRY, BROADCAST };

  /**
   * @param sequencer  pointer to the owning Sequencer object
   */
  explicit PeriodicReleases(Sequencer* sequencer);
  virtual ~PeriodicReleases() {}

  /**
   * Mark that a release message was successfully sent to the specified storage
   * shard.
   */
  void noteReleaseSuccessful(ShardID shard,
                             lsn_t released_lsn,
                             ReleaseType release_type);

  /**
   * Ensure that a RELEASE message is eventually delivered to all shards in the
   * union of historical storage set. Called in the following situations:
   *    1) By an Appender after RELEASEs of the reaped record is send to the
   *       immediate copyset
   *    2) By sequencer when there is a RELEASE message failed to send after
   *       log recovery completion.
   *    3) whenever a RELEASE message wasn't sent successfully indicated by
   *       its onSent() callback
   *    Must be called from a worker thread.
   */
  void schedule();

  /**
   * Start broadcasting release messages to all storage shards (historical +
   * current) tracked by sequencer. No-op if broadcasting is already started.
   */
  void startBroadcasting();

  /**
   * Invalidate the per-shard release state for all storage shards for a
   * storage node when the connection with sequencer breaks. This is done
   * to account for the possibility of an undelivered RELEASE message.
   * Periodic Releases will send a RELEASE message to that node when the
   * connection is re-established.
   */
  void invalidateLastLsnsOfNode(node_index_t node_idx);

  /**
   * Called when sequencer updates its historical metadata (i.e., historical
   * storage sets). Add additional shards to the map for release tracking.
   */
  void onMetaDataMapUpdate(
      const std::shared_ptr<const EpochMetaDataMap>& historical_metadata);

  /**
   * Sends RELEASE messages and reactivates/deletes the timer if necessary
   */
  void timerCallback(ExponentialBackoffTimerNode* timer_node, Type type);

  /**
   * Used to start the broadcast timer on a random worker thread
   */
  class StartBroadcastingReleaseRequest : public Request {
   public:
    explicit StartBroadcastingReleaseRequest(
        std::weak_ptr<PeriodicReleases> periodic_releases);

    Execution execute() override;

   private:
    std::weak_ptr<PeriodicReleases> periodic_releases_;
  };

  bool isBroadcasting() const {
    return broadcasting_.load();
  }

 protected:
  virtual logid_t getLogID() const;

  virtual lsn_t getLastKnownGood() const;

  virtual lsn_t getLastReleased() const;

  virtual int sendReleaseToStorageSets(lsn_t lsn,
                                       ReleaseType release_type,
                                       const Sequencer::SendReleasesPred& pred);

  virtual void startPeriodicReleaseTimer(Type type);

  virtual void
  cancelPeriodicReleaseTimer(ExponentialBackoffTimerNode* timer_node,
                             Type type);

  virtual void
  activatePeriodicReleaseTimer(ExponentialBackoffTimerNode* timer_node,
                               Type type);

  // only send broadcast if the sequencer is in ACTIVE state
  virtual bool shouldSendBroadcast() const;

  virtual void postBroadcastingRequest(
      std::unique_ptr<StartBroadcastingReleaseRequest> request);

  virtual const Settings& getSettings() const;

  virtual bool isShuttingDown() const;

 private:
  // Describes all states PeriodicReleases can be in wrt the timer running
  // ReleaseTimerCallback. Graph of state transitions is a bidirectional chain:
  // schedule() moves to a higher state, while timerCallback() transitions
  // into a state with the lower number. The following actions are performed
  // on transitions:
  //
  // INACTIVE -(schedule)-> ACTIVE       a new timer is created and activated
  // NEEDS_REACTIVATION -(timer fire)-> ACTIVE   timer is reactivated
  // ACTIVE -(timer fire)-> INACTIVE             timer is destroyed
  // ACTIVE -(schedule)-> NEEDS_REACTIVATION     timer fired
  // NEEDS_REACTIVATION -(schedule)-> NEEDS_REACTIVATION   no-op
  //
  // NEEDS_ACTIVATION state is used to ensure that any schedule() calls that
  // happen concurrently with the timer firing are eventually handled, while
  // also ensuring that at most one timer is active at any moment in time.
  enum class State : uint8_t {
    // timer doesn't exist
    INACTIVE = 0,
    // timer is being created, already exists and is pending, or its callback
    // is currently executing
    ACTIVE = 1,
    // this state indicates that the timer needs to be reactivated once its
    // callback completes
    NEEDS_REACTIVATION = 2,
  };

  // A functor invoked after timer's timeout elapses that calls sendReleases();
  // has a weak_ptr to PeriodicReleases to detect the case in which the owning
  // Sequencer and PeriodicReleases objects are destroyed before the Worker
  // on which the timer was registered.
  class ReleaseTimerCallback {
   public:
    explicit ReleaseTimerCallback(std::weak_ptr<PeriodicReleases>,
                                  PeriodicReleases::Type);
    void operator()(ExponentialBackoffTimerNode*);

   private:
    std::weak_ptr<PeriodicReleases> periodic_releases_;
    const PeriodicReleases::Type type_;
  };

  // Keep track of the highest LSN(s) of both global and per-epoch RELEASEs
  // successfully sent to a storage shard in the current storage set.
  struct PerShardState {
    // global last released lsn
    std::atomic<lsn_t> last_released_lsn;
    // per-epoch released lsn
    std::atomic<lsn_t> lng;

    explicit PerShardState();
    PerShardState(const PerShardState& rhs);
    void operator=(const PerShardState& rhs);
  };

  // using a two layer map for efficiently accessing per-node states
  using ShardReleaseMap = std::unordered_map<shard_index_t, PerShardState>;

  using NodeReleaseMap = std::unordered_map<node_index_t, ShardReleaseMap>;

  // Called from a timerCallback() to (re)send RELEASE messages to those storage
  // nodes to which we either failed to send the original release, or which
  // weren't a part of any copyset in the first place (such as nodes that have
  // zero weight). Returns 0 on success, -1 if sendMessage() to any of the node
  // fails.
  int sendReleases(Type type);

  // parent Sequencer object
  Sequencer* const sequencer_;

  // current state
  std::atomic<State> state_{State::INACTIVE};

  // current state of release broadcasting
  std::atomic<bool> broadcasting_{false};

  // Keep track of PerShardState for each storage shard in the union of
  // all storage shards tracked. Atomically replaced when the union of
  // historical storage sets changes.
  UpdateableSharedPtr<NodeReleaseMap> node_release_map_;

  // mutex for protecting the shard_release_map_ updates. should be very rarely
  // contended as it only happens with sequencer activations
  mutable folly::SharedMutex map_mutex_;

  // convenient function for finding PerShardState in the two layer map,
  // return nullptr if the shard is not tracked in the map
  static PerShardState* findPerShardState(NodeReleaseMap* map, ShardID shard);
  static const PerShardState* findPerShardState(const NodeReleaseMap* map,
                                                ShardID shard);
};

}} // namespace facebook::logdevice
