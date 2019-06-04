/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/RebuildingSupervisor.h"

#include <chrono>
#include <cmath>
#include <functional>

#include "logdevice/admin/maintenance/types.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/server/FailureDetector.h"

namespace facebook { namespace logdevice {

static constexpr int THREAD_AFFINITY = 4; // run on thread 4

void RebuildingSupervisor::runOnSupervisorWorker(std::function<void()> cb) {
  class TheRequest : public Request {
   public:
    explicit TheRequest(ThisRef self, std::function<void()> cb)
        : Request(RequestType::REBUILDING_SUPERVISOR), self_(self), cb_(cb) {}

    int getThreadAffinity(int nworkers) override {
      return THREAD_AFFINITY % nworkers;
    }
    Execution execute() override {
      if (self_ && !self_->shuttingDown_) {
        cb_();
      }
      return Execution::COMPLETE;
    }
    ThisRef self_;
    std::function<void()> cb_;
  };

  std::unique_ptr<Request> req =
      std::make_unique<TheRequest>(thisRefHolder_.ref(), cb);
  processor_->postWithRetrying(req);
}

void RebuildingSupervisor::start() {
  runOnSupervisorWorker([this] { init(); });
}

void RebuildingSupervisor::init() {
  ld_info("Starting Rebuilding Supervisor");
  auto w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();

  myNodeId_ = w->processor_->getMyNodeID();

  auto cs = w->getClusterState();
  ld_check(cs != nullptr);

  maintenance_log_writer_ =
      std::make_unique<maintenance::MaintenanceLogWriter>(processor_);

  auto cb = [&](node_index_t node_id, ClusterState::NodeState state) {
    onNodeStateChanged(node_id, state);
  };
  clusterStateSubscription_ = cs->subscribeToUpdates(cb);

  for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
    auto nid = kv.first;
    if (!cs->isNodeAlive(nid) && nid != myNodeId_.index()) {
      addForRebuilding(nid);
    }
  }

  rebuilding_timer_.assign([this] { triggerRebuilding(); });
  retry_timer_.assign(
      [this]() {
        state_ = State::PENDING;
        triggerRebuilding();
      },
      std::chrono::milliseconds(500),
      rebuildingSettings_->self_initiated_rebuilding_grace_period);

  // activate timer if needed, based on the trigger expirations
  scheduleNextRun();
}

void RebuildingSupervisor::stop() {
  runOnSupervisorWorker([this] {
    shuttingDown_.store(true);
    shutdown();
  });
}

void RebuildingSupervisor::shutdown() {
  ld_info("Stopping Rebuilding Supervisor");
  auto cs = Worker::getClusterState();
  cs->unsubscribeFromUpdates(clusterStateSubscription_);
  triggers_.clear();
  if (rebuilding_timer_.isActive()) {
    rebuilding_timer_.cancel();
  }
  if (retry_timer_.isActive()) {
    retry_timer_.cancel();
  }
}

void RebuildingSupervisor::myShardNeedsRebuilding(uint32_t shard_idx) {
  runOnSupervisorWorker([this, shard_idx] {
    addForRebuilding(myNodeId_.index(), shard_idx);
    scheduleNextRun();
  });
}

void RebuildingSupervisor::addForRebuilding(
    node_index_t node_id,
    folly::Optional<uint32_t> shard_idx) {
  auto w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();

  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(node_id)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: not in the service "
                   "discovery config.",
                   node_id);
    WORKER_STAT_INCR(node_rebuilding_not_triggered_notinconfig);
    return;
  }

  if (!nodes_configuration->getStorageMembership()->hasShardShouldReadFrom(
          node_id)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: not a storage node or "
                   "all shards are disabled",
                   node_id);
    WORKER_STAT_INCR(node_rebuilding_not_triggered_notstorage);
    return;
  }

  if (!triggers_.exists(node_id)) {
    // add a new trigger

    auto now = SystemTimestamp::now();
    std::chrono::seconds timeout;
    RebuildingTrigger trigger;
    trigger.node_id_ = node_id;
    trigger.creation_time_ = now;

    if (node_id == myNodeId_.index()) {
      // this trigger is for our own shard. make sure the shard id was provided.
      ld_check(shard_idx.hasValue());
      // When starting rebuilding of our shard, we don't need a grace period.
      // Only wait for event_log_grace_period to make it likely that rebuilding
      // set is propagated to workers by the time we check
      // canTriggerShardRebuilding().
      // TODO (#12892014): If we get rid of event_log_grace_period, just use 0.
      timeout = std::chrono::duration_cast<std::chrono::seconds>(
          processor_->settings()->event_log_grace_period * 1.1);
      trigger.expiry_ = now + timeout;
      trigger.shards_.insert(shard_idx.value());
    } else {
      timeout = rebuildingSettings_->self_initiated_rebuilding_grace_period;
      trigger.expiry_ = now + timeout;
      for (int shard = 0; shard < nodes_configuration->getNumShards(node_id);
           ++shard) {
        trigger.shards_.insert(shard);
      }
    }

    triggers_.push(trigger);
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Scheduling rebuilding of N%d%s to execute in %s",
                   node_id,
                   shard_idx.hasValue()
                       ? (":S" + std::to_string(shard_idx.value())).c_str()
                       : "",
                   chrono_string(timeout).c_str());
    WORKER_STAT_ADD(shard_rebuilding_scheduled, trigger.shards_.size());
  } else {
    if (shard_idx.hasValue()) {
      ld_check(node_id == myNodeId_.index());
      if (triggers_.addShard(node_id, shard_idx.value())) {
        WORKER_STAT_INCR(shard_rebuilding_scheduled);
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "Scheduling rebuilding of N%d:S%d to execute now",
                       node_id,
                       shard_idx.value());
      }
    }
  }
}

void RebuildingSupervisor::onNodeStateChanged(node_index_t node_id,
                                              ClusterState::NodeState state) {
  if (node_id == myNodeId_.index()) {
    return;
  }

  if (!ClusterState::isAliveState(state)) {
    // here we also start the timer if the node if failing over (graceful
    // shutdown) because we don't know if the node is going to come back. but
    // if it does, then the timer will be cancelled.
    addForRebuilding(node_id);
    scheduleNextRun();
  } else {
    if (triggers_.exists(node_id)) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "Cancelling rebuilding trigger for node N%d",
                     node_id);
      auto& trigger = triggers_.getById(node_id);
      size_t shards_count = trigger.shards_.size();
      WORKER_STAT_ADD(shard_rebuilding_not_triggered_nodealive, shards_count);
      triggers_.remove(node_id);
      scheduleNextRun();
    }
  }
}

int RebuildingSupervisor::myIndexInLeaderChain(node_index_t node_id) {
  if (node_id == myNodeId_.index()) {
    return 0;
  }

  // Let's define leader chain as the list of all alive storage nodes, sorted
  // in order of increasing node index. My index in it is the number of alive
  // storage nodes with index less than myNodeId_.
  // Sequencer-only nodes don't run RebuildingSupervisor.
  auto w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();
  auto cs = Worker::getClusterState();
  ld_check(cs);
  int res = 0;

  for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
    auto node_index = kv.first;
    if (node_index < myNodeId_.index() &&
        nodes_configuration->getStorageMembership()->hasShardShouldReadFrom(
            node_index) &&
        cs->isNodeAlive(node_index) && node_index != node_id) {
      ++res;
    }
  }

  ld_debug("My index in leader chain is %d", res);

  return res;
}

void RebuildingSupervisor::scheduleNextRun(
    folly::Optional<std::chrono::microseconds> timeout) {
  // cancel current timer. it will be reactivated if needed.
  if (rebuilding_timer_.isActive()) {
    rebuilding_timer_.cancel();
  }

  // recalculate the throttling state
  adjustRebuildingThrottle();

  if (triggers_.empty()) {
    // queue is empty. no rebuilding to schedule
    if (retry_timer_.isActive()) {
      retry_timer_.cancel();
    }
    // restore initial delay so that next time we need to retry a write, we
    // starts from scratch and don't wait too long.
    retry_timer_.reset();
    state_ = State::IDLE;
    ld_debug("No rebuilding trigger to schedule");
    return;
  }

  if (!timeout.hasValue()) {
    // no timeout specified. let's compute the timeout based on expiration of
    // the next trigger.
    auto trigger = triggers_.top();
    auto now = SystemTimestamp::now();
    if (trigger.expiry_ > now) {
      timeout = std::chrono::duration_cast<std::chrono::microseconds>(
          trigger.expiry_ - now);
    } else {
      timeout = std::chrono::seconds(0);
    }
  }

  // if we are EXECUTING, we shouldn't activate the timer, as we are waiting
  // to complete writing an event log record and will pick up the next
  // available trigger automatically. This ensures that we write only one
  // event log record at a time.
  if (state_ != State::EXECUTING) {
    ld_debug("Scheduling next run to execute in approximately %.0fs",
             std::nearbyint(timeout.value().count() / 1000000.0));
    state_ = State::PENDING;
    rebuilding_timer_.activate(timeout.value());
  }
}

void RebuildingSupervisor::triggerRebuilding() {
  triggers_.dumpDebugInfo();

  // make sure we are not already executing a trigger.
  ld_check(state_ != State::EXECUTING);

  if (triggers_.empty()) {
    // the trigger got removed. update internal state and cancel timers.
    scheduleNextRun();
    return;
  }

  // pick the next rebuilding trigger
  auto trigger = triggers_.top();
  if (trigger.expiry_ > SystemTimestamp::now()) {
    ld_debug("first trigger expires later, scheduling next run.");
    // trigger is scheduled for later. wait until it's time.
    scheduleNextRun();
    return;
  }

  auto decision = evaluateTrigger(trigger);
  switch (decision) {
    case Decision::CANCEL:
      // rebuilding is not needed. remove trigger and wait until next run
      triggers_.remove(trigger.node_id_);
      scheduleNextRun();
      break;

    case Decision::POSTPONE: {
      // cannot trigger rebuilding now. retry later
      auto timeout =
          rebuildingSettings_->self_initiated_rebuilding_grace_period;
      trigger.expiry_ = SystemTimestamp::now() + timeout;
      triggers_.update(trigger);
      scheduleNextRun();
      break;
    }

    case Decision::EXECUTE: {
      // Transition to executing state
      state_ = State::EXECUTING;
      // Only write one event. The others will be written once this one
      // is confirmed.
      // pick first shard
      auto shard = *trigger.shards_.begin();
      requestRebuilding(trigger, shard);
      break;
    }
  }
}

void RebuildingSupervisor::requestRebuilding(RebuildingTrigger& trigger,
                                             shard_index_t shard) {
  auto ticket = callbackHelper_.ticket();
  auto cb = [trigger, shard, ticket](
                Status st, lsn_t version, const std::string& /* unused */) {
    if (st == E::TIMEDOUT && version != LSN_INVALID) {
      // Confirmation timed out but the append succeeded.
      // Consider it a success, as rebuilding event was written.
      st = E::OK;
    }
    ticket.postCallbackRequest([=](RebuildingSupervisor* p) {
      if (p) {
        p->onShardRebuildingTriggered(st, trigger, shard);
      }
    });
  };

  // ClusterMaintenanceStateMachine is enabled. So request rebuilding
  // by adding a new Maintenance
  if (adminSettings_->enable_cluster_maintenance_state_machine) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "Triggering rebuilding of N%d:S%d by writing to MaintenanceLog",
        trigger.node_id_,
        shard);
    auto delta = std::make_unique<maintenance::MaintenanceDelta>();
    delta->set_apply_maintenances(
        {maintenance::MaintenanceLogWriter::
             buildMaintenanceDefinitionForRebuilding(
                 ShardID(trigger.node_id_, shard),
                 "Triggered by RebuildingSupervisor")});
    maintenance_log_writer_->writeDelta(
        *delta,
        std::move(cb),
        maintenance::ClusterMaintenanceStateMachine::WriteMode::
            CONFIRM_APPLIED);
  } else {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Triggering rebuilding of N%d:S%d by writing to EventLog",
                   trigger.node_id_,
                   shard);
    SHARD_NEEDS_REBUILD_Event event(trigger.node_id_,
                                    (uint32_t)shard,
                                    myNodeId_.toString(),   // source
                                    "RebuildingSupervisor", // details
                                    SHARD_NEEDS_REBUILD_flags_t(0));
    auto mode = EventLogStateMachine::WriteMode::CONFIRM_APPLIED;
    eventLog_->writeDelta(event, cb, mode, trigger.base_version);
  }
}

void RebuildingSupervisor::onShardRebuildingTriggered(Status st,
                                                      RebuildingTrigger trigger,
                                                      uint32_t shard) {
  if (st == E::OK) {
    // Writing the event log records succedeed. Update stats and reset retry
    // timer in case it was used.
    WORKER_STAT_INCR(shard_rebuilding_triggered);
    retry_timer_.reset();
  }

  ld_check(!retry_timer_.isActive());

  if (!triggers_.exists(trigger.node_id_)) {
    // The entry may have been deleted if the node came back online
    // in that case, we are done here.
    state_ = State::PENDING;
    scheduleNextRun();
    return;
  }

  if (st == E::OK) {
    ld_debug("Rebuilding of N%d:S%d was successfully triggered",
             trigger.node_id_,
             shard);

    state_ = State::PENDING;
    trigger.shards_.erase(shard);
    if (trigger.shards_.empty()) {
      triggers_.remove(trigger.node_id_);
      scheduleNextRun();
    } else {
      triggers_.update(trigger);
      // trigger next rebuilding
      triggerRebuilding();
    }
  } else {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Rebuilding of N%d:S%d failed to be triggered: %s.",
                    trigger.node_id_,
                    shard,
                    error_description(st));
    // an error occurred while appending the rebuilding event
    // restart timer
    retry_timer_.activate();
  }
}

RebuildingSupervisor::Decision RebuildingSupervisor::evaluateTrigger(
    RebuildingSupervisor::RebuildingTrigger& trigger) {
  if (!rebuildingSettings_->enable_self_initiated_rebuilding) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: Rebuilding "
                   "Supervisor is disabled",
                   trigger.node_id_);
    return Decision::POSTPONE;
  }

  if (trigger.shards_.empty()) {
    // nothing to do
    return Decision::CANCEL;
  }

  if (!canTriggerShardRebuilding(trigger)) {
    return Decision::CANCEL;
  }

  if (trigger.node_id_ == myNodeId_.index()) {
    // always allow rebuilding for our own broken shard
    return Decision::EXECUTE;
  }

  if (!canTriggerNodeRebuilding(trigger)) {
    return Decision::CANCEL;
  }

  int leader_rank = myIndexInLeaderChain(trigger.node_id_);
  if (leader_rank != 0) {
    // This node is not the leader. Do not trigger rebuilding in that case.
    // In the unlikely situation where the leader is alive but for some
    // reason does not trigger rebuilding for a long period of time, the
    // oncall should be alerted to investigate or manually trigger
    // rebuilding.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Not triggering rebuilding of N%d: not the rebuilding "
                   "leader",
                   trigger.node_id_);
    // We want to keep the trigger in the queue however, in case this node
    // becomes the leader at a later time.
    return Decision::POSTPONE;
  }

  Decision decision = adjustRebuildingThrottle();
  return decision;
}

bool RebuildingSupervisor::canTriggerNodeRebuilding(
    RebuildingSupervisor::RebuildingTrigger& trigger) {
  auto w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();

  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(trigger.node_id_)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: not in the service "
                   "discovery config.",
                   trigger.node_id_);
    WORKER_STAT_INCR(node_rebuilding_not_triggered_notinconfig);
    return false;
  }

  if (!nodes_configuration->getStorageMembership()->hasShardShouldReadFrom(
          trigger.node_id_)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: not a storage node or "
                   "all shards are disabled",
                   trigger.node_id_);
    WORKER_STAT_INCR(node_rebuilding_not_triggered_notstorage);
    return false;
  }

  auto cs = Worker::getClusterState();
  if (cs->isNodeAlive(trigger.node_id_)) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Not triggering rebuilding of N%d: node is alive",
                   trigger.node_id_);
    WORKER_STAT_ADD(
        shard_rebuilding_not_triggered_nodealive, trigger.shards_.size());
    return false;
  }

  return true;
}

/*
 * Calculates whether we need to throttle. See comments in
 * RebuildingSupervisor.h.
 */
RebuildingSupervisor::Decision
RebuildingSupervisor::adjustRebuildingThrottle() {
  auto w = Worker::onThisThread();
  const auto& nodes_configuration = w->getNodesConfiguration();
  auto rebuilding_set = w->processor_->rebuilding_set_.get();
  auto threshold = rebuildingSettings_->max_node_rebuilding_percentage;
  size_t cluster_size = nodes_configuration->clusterSize();
  size_t trigger_count = triggers_.size();
  size_t rebuilding_nodes = 0;
  size_t draining_nodes = 0;

  struct NodeStatus {
    bool draining;
    bool mini_rebuilding;
  };

  std::unordered_map<node_index_t, NodeStatus> node_map;
  if (rebuilding_set != nullptr) {
    for (const auto& shard_info : rebuilding_set->getRebuildingShards()) {
      for (const auto& node_info : shard_info.second.nodes_) {
        if (!nodes_configuration->getStorageMembership()->hasNode(
                node_info.first)) {
          // ignore nodes that are not in the storage membership
          continue;
        }

        if (node_info.second.acked) {
          // ignore nodes that have already acked their rebuilding
          continue;
        }

        auto nodeStatus_it = node_map.find(node_info.first);
        if (nodeStatus_it != node_map.end()) {
          // A node is draining only if all rebuilding shards are draining
          if (nodeStatus_it->second.draining) {
            nodeStatus_it->second.draining = node_info.second.drain;
          }

          // A node is mini-rebuilding only if all rebuilding shards
          // are mini-rebuilding
          if (nodeStatus_it->second.mini_rebuilding) {
            nodeStatus_it->second.mini_rebuilding =
                !node_info.second.dc_dirty_ranges.empty();
          }
        } else {
          NodeStatus nodeStatus;
          nodeStatus.draining = node_info.second.drain;
          nodeStatus.mini_rebuilding =
              !node_info.second.dc_dirty_ranges.empty();
          node_map.insert(std::make_pair(node_info.first, nodeStatus));
        }
      }
    }

    // Capactity based throttling limits are against the non-drained
    // capacity of the cluster.
    draining_nodes =
        std::count_if(node_map.begin(), node_map.end(), [](const auto& node) {
          return node.second.draining;
        });
    cluster_size -= draining_nodes;

    // As for cluster_size, exclude drained/draining nodes. We also allow
    // the entire cluster to be mini-rebuilding, so exclude those nodes too.
    rebuilding_nodes =
        std::count_if(node_map.begin(), node_map.end(), [](const auto& node) {
          return !(node.second.draining || node.second.mini_rebuilding);
        });

    // Restarting an active rebuilding is free, so discount them. The only
    // exception is that we must account for the capacity loss when upgrading
    // a mini-rebuilding to a full rebuilding (the node is no longer
    // FULLY_AUTHORITATIVE and thus cannot take writes).
    // NOTE: Draining nodes can also crash and trigger mini-rebuilding.
    //       So we take care here to only count the promotion of nodes
    //       that weren't already draining.
    trigger_count -= std::count_if(
        node_map.begin(), node_map.end(), [self = this](const auto& node) {
          return self->triggers_.exists(node.first) &&
              (node.second.draining || !node.second.mini_rebuilding);
        });
  }

  size_t totalRebuildings = trigger_count + rebuilding_nodes;
  auto ratio = totalRebuildings * 100 / cluster_size;
  if (ratio > threshold) {
    // if there are too many triggers, it means there are too many dead nodes
    // in the cluster to safely request rebuilding. this may be caused by this
    // node being isolated, or any other reason. regardless, stop requesting
    // any rebuilding until the number of triggers goes back down. (which will
    // happen when nodes come back online, or rebuildings get manually
    // triggered).
    if (!throttling_) {
      ld_warning("Entering throttling mode: #draining=%lu, #rebuilding=%lu, "
                 "#triggers=%lu, "
                 "#adjusted cluster size=%lu, threshold=%lu%%, actual=%lu%%",
                 draining_nodes,
                 rebuilding_nodes,
                 trigger_count,
                 cluster_size,
                 threshold,
                 ratio);
      throttling_ = true;
    }
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Entering throttling mode: #draining=%lu, #rebuilding=%lu, "
                   "#triggers=%lu, "
                   "#adjusted cluster size=%lu, threshold=%lu%%, actual=%lu%%",
                   draining_nodes,
                   rebuilding_nodes,
                   trigger_count,
                   cluster_size,
                   threshold,
                   ratio);

    STAT_SET(
        Worker::stats(), rebuilding_supervisor_throttled, throttling_ ? 1 : 0);
    return Decision::POSTPONE;
  } else {
    if (throttling_) {
      // we were throttling but we are not anymore.
      ld_warning("Exiting throttling mode: #draining=%lu, #rebuilding=%lu, "
                 "#triggers=%lu, "
                 "#adjusted cluster size=%lu, threshold=%lu%%, actual=%lu%%",
                 draining_nodes,
                 rebuilding_nodes,
                 trigger_count,
                 cluster_size,
                 threshold,
                 ratio);
      // save the time and make sure we don't trigger any rebuilding within
      // one more grace period.
      throttling_exit_time_ = SystemTimestamp::now();
      throttling_ = false;
    }

    auto now = SystemTimestamp::now();
    auto deadline = throttling_exit_time_ +
        rebuildingSettings_->self_initiated_rebuilding_grace_period;
    if (now < deadline) {
      // make sure we wait at least one grace period after exiting throttling
      // before starting triggering rebuildings again.
      RATELIMIT_INFO(
          std::chrono::seconds(1),
          1,
          "Not triggering new rebuildings until %s: It's too soon after "
          "exiting throttling mode",
          deadline.toString().c_str());
      STAT_SET(Worker::stats(),
               rebuilding_supervisor_throttled,
               throttling_ ? 1 : 0);
      return Decision::POSTPONE;
    }
  }

  ld_debug("Not throttling triggers based on number of rebuildings: "
           "#draining=%lu, #rebuilding=%lu, "
           "#triggers=%lu, "
           "#adjusted cluster size=%lu, threshold=%lu%%, actual=%lu%%",
           draining_nodes,
           rebuilding_nodes,
           trigger_count,
           cluster_size,
           threshold,
           ratio);

  STAT_SET(
      Worker::stats(), rebuilding_supervisor_throttled, throttling_ ? 1 : 0);
  return Decision::EXECUTE;
}

bool RebuildingSupervisor::canTriggerShardRebuilding(
    RebuildingSupervisor::RebuildingTrigger& trigger) {
  auto set = Worker::onThisThread()->processor_->rebuilding_set_.get();
  if (set != nullptr) {
    trigger.base_version = set->getLastUpdate();
    for (auto it = trigger.shards_.begin(); it != trigger.shards_.end();) {
      // check whether rebuilding is running or has run, by looking at the
      // shard status. remove shards that are not fully authoritative, as we
      // won't trigger rebuilding for those.
      std::vector<node_index_t> unused;
      auto status =
          set->getShardAuthoritativeStatus(trigger.node_id_, *it, unused);
      if (status == AuthoritativeStatus::FULLY_AUTHORITATIVE) {
        ++it;
      } else {
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "Rebuilding of N%d:S%d already completed or in progress",
                       trigger.node_id_,
                       *it);
        WORKER_STAT_INCR(shard_rebuilding_not_triggered_started);
        it = trigger.shards_.erase(it);
      }
    }
  }

  return !trigger.shards_.empty();
}

void RebuildingSupervisor::RebuildingTriggerQueue::dumpDebugInfo() const {
  auto now = SystemTimestamp::now();
  for (const auto& trigger : queue_) {
    std::chrono::seconds since(0);
    if (now > trigger.creation_time_) {
      since = std::chrono::duration_cast<std::chrono::seconds>(
          now - trigger.creation_time_);
    }

    std::chrono::microseconds remaining(0);
    if (trigger.expiry_ > now) {
      remaining = std::chrono::duration_cast<std::chrono::microseconds>(
          trigger.expiry_ - now);
    }

    ld_debug("N%d: since=%s remaining=%s shards=[%s]",
             trigger.node_id_,
             chrono_string(since).c_str(),
             chrono_string(remaining).c_str(),
             folly::join(',', trigger.shards_).c_str());
  }
}

bool RebuildingSupervisor::RebuildingTriggerQueue::push(
    RebuildingTrigger& trigger) {
  trigger.id_ = next_id_++;
  auto result = queue_.insert(trigger);
  return result.second;
}

const RebuildingSupervisor::RebuildingTrigger&
RebuildingSupervisor::RebuildingTriggerQueue::top() const {
  ld_check(!queue_.empty());
  auto& index = queue_.get<BY_EXPIRY>();
  return *index.begin();
}

bool RebuildingSupervisor::RebuildingTriggerQueue::exists(
    node_index_t node_id) const {
  auto& index = queue_.get<BY_NODE_ID>();
  auto it = index.find(node_id);
  return it != index.end();
}

bool RebuildingSupervisor::RebuildingTriggerQueue::empty() const {
  return queue_.empty();
}

bool RebuildingSupervisor::RebuildingTriggerQueue::addShard(
    node_index_t node_id,
    uint32_t shard) {
  auto& index = queue_.get<BY_NODE_ID>();
  auto it = index.find(node_id);
  ld_check(it != index.end());
  RebuildingTrigger trigger = *it;
  auto result = trigger.shards_.insert(shard);
  if (result.second) {
    index.replace(it, trigger);
  }
  return result.second;
}

bool RebuildingSupervisor::RebuildingTriggerQueue::update(
    const RebuildingTrigger& trigger) {
  auto& index = queue_.get<BY_NODE_ID>();
  auto it = index.find(trigger.node_id_);
  if (it == index.end() || it->id_ != trigger.id_) {
    // original trigger doesn't exist or has been recreated
    // don't replace it in this case
    return false;
  }

  index.replace(it, trigger);
  return true;
}

void RebuildingSupervisor::RebuildingTriggerQueue::clear() {
  queue_.clear();
}

bool RebuildingSupervisor::RebuildingTriggerQueue::remove(
    node_index_t node_id) {
  auto& index = queue_.get<BY_NODE_ID>();
  auto it = index.find(node_id);
  if (it != index.end()) {
    index.erase(it);
    return true;
  }
  return false;
}

const RebuildingSupervisor::RebuildingTrigger&
RebuildingSupervisor::RebuildingTriggerQueue::getById(
    node_index_t node_id) const {
  auto& index = queue_.get<BY_NODE_ID>();
  auto it = index.find(node_id);
  ld_check(it != index.end());
  return *it;
}

size_t RebuildingSupervisor::RebuildingTriggerQueue::size() const {
  return queue_.size();
}
}} // namespace facebook::logdevice
