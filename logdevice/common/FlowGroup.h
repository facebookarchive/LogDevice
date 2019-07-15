/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <thread>

#include <folly/ScopeGuard.h>

#include "logdevice/common/BWAvailableCallback.h"
#include "logdevice/common/Envelope.h"
#include "logdevice/common/FlowGroupDependencies.h"
#include "logdevice/common/FlowMeter.h"
#include "logdevice/common/PriorityQueue.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/FlowGroupPolicy.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

/**
 * @file  A FlowGroup meters bandwidth for connections that share a common
 *        resource constraint. For example, connections to nodes within
 *        the same rack will contend on the bandwidth capacity of their
 *        top of rack switch, connections between racks will contend on
 *        on a cluster switch, and connections between regions will be
 *        limited by inter-site bandwidth.
 *
 *        FlowGroups behavior is controlled by a FlowGroupPolicy. This
 *        dictates the maximum burst and guaranteed bandwidth allocation
 *        for messages of each priority level. The policy also allows the
 *        definition of a bandwidth pool which will be shared amongst
 *        priority levels. This additional bandwidth is distributed in
 *        priority order to priority levels that can still accept additional
 *        credit after receiving their guaranteed bandwidth allocation.  Access
 *        to the shared pool is limited by the maximum bandwidth limit policy
 *        setting for every priority level. This value, which must be greater
 *        than or equal to the guaranteed bandwidth setting, specifies the upper
 *        bound of bandwidth credit from any source (guaranteed bandwidth and
 *        transfers from the shared pool) that can be deposited into each
 *        priority level.
 *
 *        FlowGroups receive policy change notifications and their periodic
 *        bandwidth allotment via a FlowGroupsUpdate. FlowGroupsUpdates are
 *        distributed by the TrafficShaper
 *
 *        Initial configuration definitions and administrative updates of
 *        a FlowGroupPolicy are specified as global process limits. These
 *        are then normalized based on the number of workers and the
 *        bandwidth release frequency into per-worker and per-quantum
 *        values.
 *
 *        Traffic shaping is disabled by default. Like any other
 *        FlowGroupPolicy value, this can be changed dynamically and will
 *        take effect when the next FlowGroupsUpdate is released. See
 *        TrafficShaper for the default frequency of updates.
 *
 *        Credits into a FlowGroup are generated from the following sources:
 *        a) guaranteed_bw per quantum
 *        b) from Priority Queue bucket (which periodically collects overflow
 *           from other priority buckets)
 *        c) returned credit from the clients (this happens when the client
 *           requests more than it actually needed)
 */

class FlowGroupsUpdate {
 public:
  explicit FlowGroupsUpdate(std::set<NodeLocationScope> valid_scopes) {
    for (auto& s : valid_scopes) {
      GroupEntry g;
      group_entries[s] = std::move(g);
    }
  }

  struct GroupEntry {
    /**
     * OverflowEntries record the bandwidth that cannot be placed
     * within buckets of a given priority level across all Senders.
     * Since we don't know where in the update run the overages have
     * occurred, we take the overage from the last update run and try
     * to add it to low buckets during the subsequent run. Any of the
     * "last run" bandwidth that cannot be consumed is summed across
     * priorities and applied to the priority queue bucket.
     *
     * This scheme is designed to ensure that unused bandwidth in some
     * workers is made available to workers that can use it. A hot worker
     * may consume more bandwidth than the steady state allotment
     * provided during each FlowGroupUpdate, but at process global scope,
     * the configured restrictions are still honored.
     *
     * To ensure fairness, the "first fit" of last run's overflow is
     * applied in random order. See TrafficShaper::dispatchUpdate()'s
     * use of Processor::applyToWorkers(..., Order::RANDOM).
     */
    struct OverflowEntry {
      // Excess bandwidth from the last FlowGroupUpdate that augments
      // the allotment provided by this entry's policy.
      int64_t last_overflow = 0;
      // Excess bandwidth accrued during the current FlowGroupUpdate.
      // Note: Overflows from filling a FlowMeter entry using last_overflow
      //       bandwidth is returned to last_overflow, not added here.
      //       Otherwise, excess bandwidth that cannot fit given the
      //       configured maximum capacity (maximum burst) would accrue
      //       indefinitely, effectively overriding the maximum capacity
      //       setting.
      int64_t cur_overflow = 0;
    };

    /**
     * Convenience function for accessing the bandwidth update that
     * applies to the shared priority queue.
     */
    OverflowEntry& priorityQEntry() {
      return overflow_entries.back();
    }

    FlowGroupPolicy policy;
    std::array<OverflowEntry, asInt(Priority::NUM_PRIORITIES) + 1>
        overflow_entries;
  };

  std::unordered_map<NodeLocationScope, GroupEntry> group_entries;
};

class FlowGroup {
  friend class FlowGroupTest;

  std::shared_ptr<FlowGroupDependencies> deps_;

 public:
  explicit FlowGroup(std::shared_ptr<FlowGroupDependencies> deps)
      : deps_(std::move(deps)) {}

  bool configured() const {
    return configured_;
  }
  bool enabled() const {
    return enabled_;
  }
  bool empty() const {
    return priorityq_.empty();
  }
  NodeLocationScope scope() const {
    return scope_;
  }

  bool canDrainMeter(Priority p) const {
    return !enabled() || meter_.entries[asInt(p)].canDrain();
  }

  /**
   * Return true if sufficient bandwidth exists to transmit at least one
   * message at the given priority level.
   */
  bool canDrain(Priority p) const {
    bool res = !wouldCutInLine(p) && canDrainMeter(p);
    if (assert_can_drain_ && reordering_allowed_at_priority_ == p) {
      ld_check(res);
    }
    return res;
  }

  /**
   * Debit priority's meter unconditionally, e.g. if we took out
   * more credit than anticipated.
   **/
  void debitMeter(Priority p, size_t debit_amount) {
    meter_.entries[asInt(p)].drain(
        debit_amount, true /* drain_on_negative_level */);
  }

  bool isPriorityQueueBlocked(Priority p) {
    return !canDrainMeter(p) && !priorityq_.empty();
  }

  /**
   * See FlowMeter::Entry::returnCredits() docblock
   **/
  size_t returnCredits(Priority p, size_t amount);

  size_t debt(Priority p) const {
    return meter_.entries[asInt(p)].debt();
  }

  size_t depositBudget(Priority p) const {
    return meter_.entries[asInt(p)].depositBudget();
  }

  size_t level(Priority p) const {
    return meter_.entries[asInt(p)].level();
  }

  void setScope(Sender* sender, NodeLocationScope s) {
    // Should only be set once when Sender completes initialization
    // of its array of FlowGroups.
    ld_check(scope_ == NodeLocationScope::ROOT);
    ld_check(s <= NodeLocationScope::ROOT);
    scope_ = s;
    sender_ = sender;

    // The FlowGroups for NODE and ROOT scopes are automatically
    // configured. The configuration of ROOT guarantees that all Sockets
    // can be assigned to a configured FlowGroup even when no FlowGroups
    // are explicitly defined in the configuration. The configuration
    // of NODE is for convenience since very few configurations will need
    // to restrict traffic where source and destination are the same node.
    //
    // Note: configured_ and enabled_ mean different things. A
    //       configured_ FlowGroup accepts the Socket assignments. An
    //       enabled_ FlowGroup wll apply its configured traffic
    //       shaping restrictions.  Both configured_ and enabled_
    //       default to off. This means the actions here will result
    //       in a default configuration with Sockets assigned to either
    //       the NODE or ROOT FlowGroup, both of which will pass traffic
    //       unconditionally.
    if (scope_ == NodeLocationScope::NODE ||
        scope_ == NodeLocationScope::ROOT) {
      configured_ = true;
    }
  }

  void configure(bool configured) {
    configured_ = configured;
  }

  /**
   * If possible, consume cost bytes from the FlowMeter associated with
   * the priority.
   *
   * @return true if the FlowMeter had credit and the cost was decremented.
   */
  bool drain(const Envelope& e) {
    if (e.message().tc_ == TrafficClass::HANDSHAKE) {
      return true;
    }
    return drain(e.cost(), e.priority());
  }
  bool drain(size_t cost, Priority p);

  /**
   * @return  true iff bandwidth should be considered exhausted while
   *               processing the current message
   */
  bool injectShapingEvent(Priority);

  /**
   * Discard all accumulated capacity from a Meter.
   *
   * Currently only used during testing where injected traffic shaping
   * events force mesage deferrals by conuming all current capacity
   * for a given message priority.
   */
  void resetMeter(Priority p, int32_t level = 0) {
    meter_.entries[asInt(p)].reset(level);
  }

  /** Add a callback to the PriorityQueue for this FlowGroup. */
  void push(BWAvailableCallback& cb, Priority p) {
    ld_check(onMyWorker());
    ld_check(!cb.active());
    ld_check(p < Priority::NUM_PRIORITIES);
    cb.setAffiliation(this, p);
    priorityq_.push(cb);

    // As soon as a sender resorts to deferring a message, revert
    // wouldCutInLine() to its normal mode of operation. We can't
    // allow a future bandwidth delivery by the TrafficShaper to
    // cause the callback to inadvertently send a message out of order.
    if (p == reordering_allowed_at_priority_) {
      ld_check(!assert_can_drain_);
      reordering_allowed_at_priority_ = Priority::INVALID;
    }
  }

  /** Remove a callback from the PriorityQueue for this FlowGroup. */
  void erase(BWAvailableCallback& cb) {
    ld_check(onMyWorker());
    priorityq_.erase(cb);
    // Callbacks are removed from the queue prior to being executed.
    // Some depend on the priority being valid during the callback,
    // so invalidate the FlowGroup affiliation, but not the priority.
    cb.setAffiliation(nullptr, cb.priority());
  }

  /**
   * Add an Envelope,treated as a callback, to the PriorityQueue for
   * this FlowGroup.
   */
  void push(Envelope& e) {
    push(e, e.priority());
  }

  /**
   * Apply policy changes and bandwidth allotments to this FlowGroup. If
   * sufficient bandwidth has arrived to release queued messages, schedule
   * a queue run from Worker context.
   */
  bool applyUpdate(FlowGroupsUpdate::GroupEntry& update,
                   StatsHolder* stats = nullptr);

  /**
   * Release queued messages for which bandwidth is now available.
   *
   * @return  True iff run() returns early with outstanding work that
   *          could be processed but wasn't due to processing time
   *          exceeding run_deadline.
   */
  bool run(std::mutex& meter_mutex, SteadyTimestamp run_deadline);

  // Priority value which, when converted to an int, yields the index of
  // the FlowMeter::Entry for the priority queue.
  static constexpr Priority PRIORITYQ_PRIORITY = Priority::NUM_PRIORITIES;

 private:
  friend class RecordRebuildingMockSocket;
  friend class RecordRebuildingAmendMockSocket;

  bool onMyWorker() const {
    // sender_ is null in unit tests.
    return (sender_ == nullptr || &Worker::onThisThread()->sender() == sender_);
  }

  /**
   * Return true if allowing a new message to be transmitted at the given
   * priority level would violate the FIFO guarantee that we provide to
   * state machines.
   *
   * A thread may attempt to send a message during the window between
   * bandwidth becoming available (the meter is refilled or the policy for
   * this FlowGroup is disabled) and an explicit run down of callbacks that
   * are registered waiting for bandwidth. Return true if this thread is a
   * late comer and servicing it would cut the line of already queued waiters.
   *
   * NOTE: This check must be made regardless of the enabled status of the
   *       flow group so that message are not delivered out of order during
   *       a runtime transition (e.g. due to a config update) from the flow
   *       group from enabled to disabled.
   */
  bool wouldCutInLine(Priority p) const {
    return reordering_allowed_at_priority_ != p && !priorityq_.empty(p);
  }

  /**
   * Transfer the specified amount of credit from the 'source' to 'sink'
   * FlowMeter.
   */
  void transferCredit(Priority source, Priority sink, size_t amount) {
    auto& source_entry = meter_.entries[asInt(source)];
    auto& sink_entry = meter_.entries[asInt(sink)];
    auto initialSourceLevel = source_entry.level();
    source_entry.transferCredit(sink_entry, amount);
    deps_->statsAdd(&PerShapingPriorityStats::bwtransferred,
                    scope_,
                    sink,
                    initialSourceLevel - source_entry.level());

    // If the sink can no longer take additional credit, remove it from
    // contention for bandwidth from the priority queue.
    if (!sink_entry.canFill()) {
      priorityq_.enable(sink, false);
    }
  }

  /**
   * Dispatch a bandwidth available callback while asserting that
   * there should be sufficient bandwidth credit for the callback to
   * send at least one message.
   */
  void issueCallback(BWAvailableCallback& cb, std::mutex& mtx) {
    ld_check(reordering_allowed_at_priority_ == Priority::INVALID);
    ld_check(assert_can_drain_ == false);
    ld_check(cb.priority_ != Priority::INVALID);

    reordering_allowed_at_priority_ = cb.priority_;
    // A callback should only be invoked if it can issue at least one message
    // without hitting a traffic shaping limit.
    assert_can_drain_ = true;

    cb.deactivate();
    cb(*this, mtx);

    reordering_allowed_at_priority_ = Priority::INVALID;
    assert_can_drain_ = false;
  }

  PriorityQueue<BWAvailableCallback, &BWAvailableCallback::flow_group_links_>
      priorityq_;

  FlowMeter meter_;

  // The Sender that contains this FlowGroup.
  //
  // Used to catch unintended foreign thread manipulation of FlowGroups.
  // All operations on a FlowGroup, with the exception of bandwidth deposits
  // by the TrafficShaper, must be performed from the Sender's Worker.
  Sender* sender_ = nullptr;

  // The scope of connections being managed by this FlowGroup.
  NodeLocationScope scope_ = NodeLocationScope::ROOT;

  // If true, this flow group appears in the config and thus can be associated
  // with new connections.
  bool configured_ = false;

  // If true, the FlowMeters in this FlowGroup control the passage of
  // traffic. Otherwise, all packets are released immediately.
  bool enabled_ = false;

  // Usually if priorityq_ is not empty we don't allow sending messages, only
  // pushing more callbacks to priorityq_. But there's one exception: if we're
  // already inside a BWAvailableCallback, it's ok to send messages of the same
  // priority as the callback, as long as meter level is positive.
  // This field is the priority of currently running callback if we're inside
  // a callback and allowed to send messages. Otherwise INVALID.
  Priority reordering_allowed_at_priority_ = Priority::INVALID;

  // Used to validate the behavior of bandwidth available callbacks.
  // The first message sent from a bandwidth available callback at or above
  // the priority of the registered callback should always succeed. This
  // variable is set to true when we issue a callback and reset to false
  // either after the first message is sent or the callback completes.
  bool assert_can_drain_ = false;
};

}} // namespace facebook::logdevice
