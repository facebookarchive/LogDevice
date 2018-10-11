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
#include <unordered_set>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/event_log/EventLogWriter.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/WeakRefHolder.h"

/**
 * @file RebuildingSupervisor is in charge of triggering rebuilding (writing
 * SHARDS_NEEDS_REBUILD events to the event log):
 *  - when a node is reported to be dead by the failure detector, and
 *  - when some of our own shards appear to be broken.
 * The RebuildingSupervisor subscribes to node state changes from the
 * ClusterState object. Upon receiving an update, it either adds a trigger to
 * execute after a grace period if the node is dead, or removes current trigger
 * if the node is now alive. When the grace period expires, the
 * RebuildingSupervisor determines which node (among the healthy nodes) is the
 * rebuilding leader. The rebuilding leader then triggers rebuilding for the
 * dead node if the conditions are right. The other non-leader nodes may
 * trigger rebuilding later as well if they realize the leader didn't.
 * The conditions to trigger rebuilding are the following:
 * - the node must be dead
 * - there must not be too many nodes dead at the same time (in which case we
 * throttle triggering rebuilding to not make things worse)
 * - the total number of nodes being rebuilt at this time must not be greater
 * than max_node_rebuilding_percentage, a configurable percentage of the
 * cluster size
 * - its shards status must be FULLY_AUTHORITATIVE
 * If these conditions are not met, the RebuildingSupervisor may reschedule the
 * trigger to wait for another grace period until the node is back alive, or
 * all of its shards have been rebuilt.
 */

namespace facebook { namespace logdevice {

class RebuildingSupervisor {
 public:
  explicit RebuildingSupervisor(EventLogStateMachine* event_log,
                                Processor* processor,
                                UpdateableSettings<RebuildingSettings> settings)
      : eventLog_(event_log),
        processor_(processor),
        rebuildingSettings_(settings),
        thisRefHolder_(this),
        callbackHelper_(this) {}

  virtual ~RebuildingSupervisor() {}

  /**
   * Starts the RebuildingSupervisor on a random worker
   */
  void start();

  /**
   * Stops the RebuildingSupervisor
   * called during the shutdown procedure
   */
  void stop();

  /**
   * Request rebuilding of a shard of this node.
   */
  void myShardNeedsRebuilding(uint32_t shard_idx);

 protected:
  /** Structure to hold information about the trigger
   * A rebuilding trigger is created when a node transitions to being dead (per
   * the cluster state / failure detector) or when the node detects a
   * shard is broken. It is then inserted into triggers_ map. The state machine
   * executes them based on their expiration and removes them from the queue
   * when they are no longer needed.
   */
  struct RebuildingTrigger {
    // unique trigger id
    uint64_t id_;
    // node to be rebuilt (redundant with the key in triggers_ map)
    node_index_t node_id_;
    // list of shards of this node to be rebuilt. typically all shards are
    // added to the list when the trigger is created, but depending on whether
    // rebuilding could or could not be triggered for a particular shard, the
    // list is updated. shards are removed from the list once the rebuilding
    // has been triggered.
    std::unordered_set<uint32_t> shards_;
    // version of the shard authortiative status map that was used to determine
    // whether rebuilding can be triggered
    lsn_t base_version{LSN_INVALID};

    // For debugging:
    // Creation time of the trigger. This is to help debug in case the trigger
    // has been around for a long time and cannot initiate rebuilding for some
    // reason.
    SystemTimestamp creation_time_;
    // expected time at which the timer will fire.
    SystemTimestamp expiry_;
  };

  /**
   * Initializes the RebuildingSupervisor
   * Starts grace period timers for all the currently dead nodes.
   * Subscribes for node state updates from ClusterState
   *
   * must be called from the worker running the RebuildingSupervisor
   */
  void init();

  /**
   * Terminates the RebuildingSupervisor
   * Cancels any outstanding timer
   *
   * must be called from the worker running the RebuildingSupervisor
   */
  void shutdown();

  /**
   * Adds a rebuilding trigger to the queue.
   *
   * If node_id == myNodeId_.index(), shard_idx is the shard to rebuild,
   * or folly::none if it's a retry.
   * Otherwise rebuilding is requested for all shards.
   */
  void addForRebuilding(node_index_t node_id,
                        folly::Optional<uint32_t> shard_idx = folly::none);

  /**
   * If the rebuilding supervisor is not already executing, computes the time
   * of the next execution by looking at the first trigger in the queue and
   * activate the rebuilding timer.
   * a timeout may be specified to force delaying next execution.
   */
  void scheduleNextRun(
      folly::Optional<std::chrono::microseconds> timeout = folly::none);

  /**
   * Callback for cluster state changes. adds or removes triggers based on
   * state transitions
   */
  void onNodeStateChanged(node_index_t node_id, ClusterState::NodeState state);

  /**
   * Checks global conditions that may prevent us from triggering rebuilding at
   * the moment, such as too many triggers in the queue.
   * The rebuilding supervisor will retry later if that's the case.
   */
  bool shouldThrottleRebuilding(RebuildingTrigger& trigger);

  /**
   * Checks throttling condition and updates queue to keep track of throttling
   * mode.
   */
  void checkThrottlingMode();

  /**
   * Checks wether rebuilding can be triggered for that particular node, such
   * as whether it is back alive, no longer in the config or not a storage node.
   * If not, the trigger gets canceled for that node.
   */
  bool canTriggerNodeRebuilding(RebuildingTrigger& trigger);

  /**
   * Checks wether rebuilding can be triggered for that particular shard, such
   * as wether it is fully authoritative. If not, the shard is skipped.
   */
  bool canTriggerShardRebuilding(RebuildingTrigger& trigger);

  /**
   * Callback executed by the event log state machine after completion of the
   * append and confirmation and that its changes have been applied.
   * If the status is OK, it executes the next rebuilding. Otherwise, starts a
   * backoff timer to retry the operation later.
   */
  void onShardRebuildingTriggered(Status st,
                                  RebuildingTrigger trigger,
                                  uint32_t shard);

  /**
   * (debug) Prints contents of the trigger queue.
   */
  void dumpRebuildingTriggers();

  /**
   * Computes best threshold for the size of the trigger queue
   */
  size_t getRebuildingTriggerQueueThreshold() const;

  enum class Decision {
    // trigger should be executed immediately
    EXECUTE = 0,
    // trigger should be canceled and removed from the queue
    CANCEL = 1,
    // trigger should be postponed and retried at a later time
    POSTPONE = 2
  };

  /**
   * Evaluates whether the trigger can be executed at this time.
   *
   * Checks global conditions that may prevent us from triggering rebuilding,
   * such as if self-initiated-rebuilding is disabled, as well as wether
   * rebuilding can be triggered for that particular node, such
   * as whether it is back alive, no longer in the config or not a storage node.
   */
  Decision evaluateTrigger(RebuildingTrigger& trigger);

 private:
  /**
   * The trigger queue holds triggers sorted by expiry while allowing
   * search/access by node_id.
   */
  class RebuildingTriggerQueue {
   public:
    RebuildingTriggerQueue() {}
    ~RebuildingTriggerQueue() {}

    // adds a trigger
    bool push(RebuildingTrigger& trigger);
    // returns the trigger with the smallest expiry
    const RebuildingTrigger& top() const;
    // returns the trigger for the given node_id. asserts if not found.
    const RebuildingTrigger& getById(node_index_t node_id) const;
    // removes a trigger by node_id
    bool remove(node_index_t node_id);
    // checks if there is a trigger with given node_id
    bool exists(node_index_t node_id) const;
    // checks if queue is empty
    bool empty() const;
    // update existing trigger with new struct
    bool update(const RebuildingTrigger& trigger);
    // append a shard to the list of shards in the trigger of given node_id
    bool addShard(node_index_t node_id, uint32_t shard);
    // removes a shard from the list of shards in the trigger of given
    // node_id. removes trigger if shards list becomes empty.
    void removeShard(node_index_t node_id, uint32_t shard);
    // removes all triggers
    void clear();
    // prints representation of all triggers
    void dumpDebugInfo() const;
    // retrusn number of the triggers in the queue
    size_t size() const;

   private:
    // internal enum to explicitly name indexes of the multi_index_container
    enum { BY_NODE_ID = 0, BY_EXPIRY = 1 };

    // maintains one trigger per node in a map, indexed by:
    // - node_id (ordered and unique)
    // - expiration timestamp (ordered and non unique)
    boost::multi_index_container<
        RebuildingTrigger,
        boost::multi_index::indexed_by<
            boost::multi_index::ordered_unique<
                boost::multi_index::member<RebuildingTrigger,
                                           node_index_t,
                                           &RebuildingTrigger::node_id_>>,
            boost::multi_index::ordered_non_unique<
                boost::multi_index::member<RebuildingTrigger,
                                           SystemTimestamp,
                                           &RebuildingTrigger::expiry_>>>>
        queue_;
    uint64_t next_id_{0};
  };

  // trigger queue
  RebuildingTriggerQueue triggers_;
  ClusterState::SubscriptionHandle clusterStateSubscription_;
  EventLogStateMachine* eventLog_;
  NodeID myNodeId_;
  Processor* processor_;
  std::atomic<bool> shuttingDown_{false};
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;

  // states of the state machine:
  // - IDLE when there is no trigger in the queue
  // - PENDING when waiting for the next trigger to execute
  // - EXECUTING when wrtiing to the event log
  // technically, only that last state matters, in order to serialize writing
  // event log records one by one, and a boolean would have worked.
  enum State { IDLE = 0, PENDING, EXECUTING };

  // current state
  State state_{IDLE};

  // main timer. fires when the next rebuilding trigger is due for execution.
  Timer rebuilding_timer_;
  // retry timer. used when writing to the event log failed.
  ExponentialBackoffTimer retry_timer_;

  using ThisRef = WeakRefHolder<RebuildingSupervisor>::Ref;
  WeakRefHolder<RebuildingSupervisor> thisRefHolder_;

  bool throttling_{false};
  SystemTimestamp throttling_exit_time_;
  size_t throttling_threshold_{0};

  WorkerCallbackHelper<RebuildingSupervisor> callbackHelper_;

  // Posts a request to runs the function on the worker on which
  // RebuildingSupervisor lives. If RebuildingSupervisor was destroyed by the
  // time request runs, the function is not called; so it's safe for `cb' to
  // capture `this'.
  void runOnSupervisorWorker(std::function<void()> cb);

  // Get the position of this node in a list of nodes that can request
  // rebuilding of node node_id.
  int myIndexInLeaderChain(node_index_t node_id);

  // Picks the next trigger from the queue and executes it.
  // Performs all the checks to see if trigger is valid and can be executed.
  // And if the conditions are met, it initiates writing one SHARD_NEEDS_REBUILD
  // event for one shard of the node.
  // this method is called either from the rebuilding timer, the retry timer or
  // the onShardRebuildingTriggered completion handler to pick another shard to
  // rebuild.
  void triggerRebuilding();
};

}} // namespace facebook::logdevice
