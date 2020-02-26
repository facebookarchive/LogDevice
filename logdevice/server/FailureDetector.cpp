/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/FailureDetector.h"

#include <unordered_set>

#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/small_vector.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Connection.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/stats/ServerHistograms.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/ServerProcessor.h"

namespace facebook { namespace logdevice {

class FailureDetector::RandomSelector : public FailureDetector::NodeSelector {
  NodeID getNode(FailureDetector* detector) override {
    const auto& nodes_configuration = detector->getNodesConfiguration();
    NodeID this_node = detector->getMyNodeID();
    // viable candidates are all sequencer/storage nodes we are able to talk to,
    // other than this one
    std::vector<NodeID> candidates;

    for (const auto& it : *nodes_configuration->getServiceDiscovery()) {
      node_index_t idx = it.first;
      if (idx != this_node.index() && detector->isValidDestination(idx)) {
        candidates.push_back(nodes_configuration->getNodeID(idx));
      }
    }

    if (candidates.size() == 0) {
      // no valid candidates
      return NodeID();
    }

    return candidates[folly::Random::rand32((uint32_t)candidates.size())];
  }
};

class FailureDetector::RoundRobinSelector
    : public FailureDetector::NodeSelector {
 public:
  NodeID getNode(FailureDetector* detector) override {
    const auto& nodes_configuration = detector->getNodesConfiguration();
    const auto& serv_disc = nodes_configuration->getServiceDiscovery();
    NodeID this_node = detector->getMyNodeID();

    if (next_node_idx_.hasValue() &&
        !serv_disc->hasNode(next_node_idx_.value())) {
      // The node was removed from config.
      next_node_idx_ = folly::none;
    }

    if (!next_node_idx_.hasValue() || iters_ >= serv_disc->numNodes()) {
      // Every `nodes.size()' iterations reset next_node_idx_ to this
      // node's neighbour. This is meant to avoid all nodes gossiping to the
      // same node in each round, even in the presence of sporadic down nodes.
      auto it = serv_disc->find(this_node.index());
      ld_check(it != serv_disc->end());
      ++it;
      if (it == serv_disc->end()) {
        it = serv_disc->begin();
      }
      next_node_idx_ = it->first;
      iters_ = 0;
    }

    NodeID target;
    for (int attempts = serv_disc->numNodes(); attempts > 0; --attempts) {
      ld_assert(next_node_idx_.hasValue());
      ld_assert(serv_disc->hasNode(next_node_idx_.value()));
      target = nodes_configuration->getNodeID(next_node_idx_.value());

      auto it = serv_disc->find(next_node_idx_.value());
      ld_check(it != serv_disc->end());
      ++it;
      if (it == serv_disc->end()) {
        it = serv_disc->begin();
      }
      next_node_idx_ = it->first;

      if (target != this_node && detector->isValidDestination(target.index())) {
        // target found
        break;
      }
    }

    ++iters_;

    target = (target != this_node) ? target : NodeID();
    return target;
  };

 private:
  // index of a next target within the list of all sequencer nodes
  folly::Optional<node_index_t> next_node_idx_;

  // number of calls to getNode()
  size_t iters_{0};
};

FailureDetector::FailureDetector(UpdateableSettings<GossipSettings> settings,
                                 ServerProcessor* processor,
                                 StatsHolder* stats)
    : settings_(std::move(settings)), stats_(stats), processor_(processor) {
  size_t max_nodes = processor->settings()->max_nodes;

  initial_time_ms_ = getCurrentTimeInMillis();
  for (size_t i = 0; i < max_nodes; ++i) {
    Node& node = nodes_[i];
    node.last_suspected_at_ = initial_time_ms_;
  }

  switch (settings_->mode) {
    case GossipSettings::SelectionMode::RANDOM:
      selector_.reset(new RandomSelector());
      break;
    case GossipSettings::SelectionMode::ROUND_ROBIN:
      selector_.reset(new RoundRobinSelector());
      break;
    default:
      ld_error("Invalid gossip mode(%d)", (int)settings_->mode);
      ld_check(false);
  }

  start_time_ = std::chrono::steady_clock::now();
  instance_id_ = std::chrono::milliseconds(processor->getServerInstanceId());
  ld_info(
      "Failure Detector created with instance id: %lu", instance_id_.count());

  auto cs = processor->cluster_state_.get();
  for (const auto& it : nodes_) {
    cs->setNodeState(it.first, ClusterState::NodeState::DEAD);
    cs->setNodeStatus(it.first, NodeHealthStatus::UNHEALTHY);
  }

  registered_rsms_.push_back(configuration::InternalLogs::EVENT_LOG_DELTAS);
  registered_rsms_.push_back(configuration::InternalLogs::CONFIG_LOG_DELTAS);
  /* skipping maintenance log as it doesn't run on cluster */
}

void FailureDetector::fetchVersions() {
  Worker* w = Worker::onThisThread(false);
  if (!w) {
    return;
  }

  /* Update RSM versions of this node */
  Processor* p = w->processor_;
  node_index_t my_idx = getMyNodeID().index();
  auto& this_node = nodes_[my_idx];
  auto rsm_versions = p->getAllRSMVersions();
  for (auto& v : rsm_versions) {
    this_node.rsm_versions_[v.first] = v.second;
  }

  /* Update NCM versions of this node */
  const auto& nodes_configuration = getNodesConfiguration();
  this_node.ncm_versions_[0] = nodes_configuration->getVersion();
  this_node.ncm_versions_[1] =
      nodes_configuration->getSequencerMembership()->getVersion();
  this_node.ncm_versions_[2] =
      nodes_configuration->getStorageMembership()->getVersion();
}

FailureDetector::Node&
FailureDetector::insertOrGetNode(size_t node_idx,
                                 folly::SharedMutex::ReadHolder& nodes_lock) {
  auto it = nodes_.find(node_idx);
  if (it != nodes_.end()) {
    return it->second;
  }

  nodes_lock.unlock();
  folly::SharedMutex::WriteHolder write_lock(nodes_mutex_);
  auto inserted = nodes_.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(node_idx),
                                 std::forward_as_tuple());
  it = inserted.first;
  if (inserted.second) {
    it->second.last_suspected_at_ = initial_time_ms_;
  }
  nodes_lock = folly::SharedMutex::ReadHolder(std::move(write_lock));
  return it->second;
}

void FailureDetector::start() {
  std::unique_ptr<Request> rq =
      std::make_unique<FuncRequest>(worker_id_t(0),
                                    WorkerType::FAILURE_DETECTOR,
                                    RequestType::FAILURE_DETECTOR_INIT,
                                    [this] {
                                      std::lock_guard lock(mutex_);
                                      startGetClusterState();
                                    });
  int rv = processor_->postImportant(rq);
  ld_check(rv == 0);
}

StatsHolder* FailureDetector::getStats() {
  return stats_;
}

void FailureDetector::startGetClusterState() {
  auto cs = Worker::getClusterState();
  if (!cs) {
    ld_info("Invalid get-cluster-state");
    buildInitialState();
  } else {
    ld_info("Sending GET_CLUSTER_STATE to build initial FD cluster view");

    Worker* w = Worker::onThisThread();
    auto settings = w->processor_->settings();

    auto cb = [&](Status status,
                  const std::vector<std::pair<node_index_t, uint16_t>>&
                      cs_update,
                  std::vector<node_index_t> boycotted_nodes,
                  std::vector<std::pair<node_index_t, uint16_t>>
                      cs_status_update) {
      std::lock_guard lock(mutex_);

      if (status != E::OK) {
        ld_error(
            "Unable to refresh cluster state: %s", error_description(status));
        buildInitialState();
        return;
      }

      std::vector<std::string> dead;
      for (auto& [node_idx, state] : cs_update) {
        if (state) {
          dead.push_back("N" + std::to_string(node_idx));
        }
      }

      std::vector<std::string> boycotted_tostring;
      boycotted_tostring.reserve(boycotted_nodes.size());
      for (auto index : boycotted_nodes) {
        boycotted_tostring.emplace_back("N" + std::to_string(index));
      }

      ld_info(
          "Cluster state received with %lu dead nodes (%s) and %lu boycotted "
          "nodes (%s)",
          dead.size(),
          folly::join(',', dead).c_str(),
          boycotted_tostring.size(),
          folly::join(',', boycotted_tostring).c_str());

      buildInitialState(
          cs_update, std::move(boycotted_nodes), std::move(cs_status_update));
    };

    std::unique_ptr<Request> req = std::make_unique<GetClusterStateRequest>(
        settings_->gcs_wait_duration,
        settings->get_cluster_state_wave_timeout,
        std::move(cb));
    auto result = req->execute();
    if (result == Request::Execution::CONTINUE) {
      req.release();
    }
  }
}

void FailureDetector::buildInitialState(
    const std::vector<std::pair<node_index_t, uint16_t>>& cs_update,
    std::vector<node_index_t> boycotted_nodes,
    std::vector<std::pair<node_index_t, uint16_t>> cs_status_update) {
  ld_info("Wait over%s", cs_update.size() ? " (cluster state received)" : "");

  ld_check(waiting_for_cluster_state_);
  waiting_for_cluster_state_ = false;

  const auto& nodes_configuration = getNodesConfiguration();
  node_index_t my_idx = getMyNodeID().index();
  auto cs = getClusterState();

  if (!cs_update.empty() || !cs_status_update.empty()) {
    folly::SharedMutex::ReadHolder nodes_lock(nodes_mutex_);

    if (!cs_update.empty()) {
      for (auto& [node_idx, new_state] : cs_update) {
        if (node_idx == my_idx ||
            node_idx > nodes_configuration->getMaxNodeIndex()) {
          continue;
        }
        Node& node = insertOrGetNode(node_idx, nodes_lock);
        cs->setNodeState(
            node_idx, static_cast<ClusterState::NodeState>(new_state));
        FailureDetector::NodeState state =
            cs->isNodeAlive(node_idx) ? NodeState::ALIVE : NodeState::DEAD;
        node.state_.store(state);
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "N%d transitioned to %s",
                       node_idx,
                       (state == NodeState::ALIVE) ? "ALIVE" : "DEAD");
      }
    }

    if (!cs_status_update.empty()) {
      for (auto& [node_idx, new_status] : cs_status_update) {
        if (node_idx == my_idx ||
            node_idx > nodes_configuration->getMaxNodeIndex()) {
          continue;
        }
        Node& node = insertOrGetNode(node_idx, nodes_lock);
        auto status = static_cast<NodeHealthStatus>(new_status);
        cs->setNodeStatus(node_idx, status);
        node.status_ = status;
        RATELIMIT_INFO(std::chrono::seconds(1),
                       10,
                       "N%d transitioned to %s (status)",
                       node_idx,
                       toString(status).c_str());
      }
    }

    if (!cs_update.empty()) {
      cs->setBoycottedNodes(std::move(boycotted_nodes));
    }
  }

  if (!isolation_checker_) {
    ld_info("Initializing DomainIsolationChecker");
    isolation_checker_ = std::make_unique<DomainIsolationChecker>();
    isolation_checker_->init();
  }

  startSuspectTimer();

  // Tell others that this node is alive, so that they can start sending
  // gossips.
  broadcastBringupUpdate(0);

  // Start gossiping after we have got a chance to build FD state.
  // If cluster-state reply doesn't come, it is fine, since we will
  // move every potentially ALIVE node from DEAD to SUSPECT on receiving
  // the first regular gossip message after the 'min_gossips_for_stable_state'
  // limit.
  startGossiping();
}

bool FailureDetector::checkSkew(const GOSSIP_Message& msg) {
  const auto millis_now = getCurrentTimeInMillis();
  long skew = labs(millis_now.count() - msg.sent_time_.count());
  HISTOGRAM_ADD(Worker::stats(), gossip_recv_latency, skew * 1000);

  auto threshold = settings_->gossip_time_skew_threshold;
  bool drop_gossip = (skew >= threshold.count());

  if (drop_gossip || skew >= 1000) {
    STAT_INCR(getStats(), gossips_delayed_total);
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "A delayed gossip received from %s, delay:%lums"
                      ", sender time:%lu, now:%lu,%s",
                      msg.gossip_node_.toString().c_str(),
                      skew,
                      msg.sent_time_.count(),
                      millis_now.count(),
                      drop_gossip ? " Dropping." : "");
  }

  if (drop_gossip) {
    STAT_INCR(getStats(), gossips_dropped_total);
  }

  return drop_gossip;
}

bool FailureDetector::isValidInstanceId(std::chrono::milliseconds id,
                                        node_index_t idx) {
  const auto now = getCurrentTimeInMillis();
  if (id > now + settings_->gossip_time_skew_threshold) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Rejecting the instance id:%lu for N%hu as its too far "
                      "in future, now:%lu",
                      id.count(),
                      idx,
                      now.count());
    return false;
  }
  return true;
}

void FailureDetector::noteConfigurationChanged() {
  std::lock_guard lock(mutex_);

  if (need_to_broadcast_starting_state_finished_ && isLogsConfigLoaded()) {
    broadcastBringupUpdate(0);
  }

  if (isolation_checker_) {
    isolation_checker_->noteConfigurationChanged();
  }
}

void FailureDetector::gossip() {
  const auto& nodes_configuration = getNodesConfiguration();
  const auto& serv_disc = nodes_configuration->getServiceDiscovery();

  folly::SharedMutex::ReadHolder nodes_lock(nodes_mutex_);

  NodeID dest = selector_->getNode(this);

  if (shouldDumpState()) {
    ld_info("FD state before constructing gossip message for %s",
            dest.isNodeID() ? dest.toString().c_str() : "none");
    dumpFDState(nodes_lock);
  }

  NodeID this_node = getMyNodeID();
  auto now = SteadyTimestamp::now();
  // bump other nodes' entry in gossip list if at least 1 gossip_interval
  // passed
  if (now >= last_gossip_tick_time_ + settings_->gossip_interval) {
    for (auto& it : nodes_) {
      if (it.first != this_node.index()) {
        // overflow handling
        uint32_t gl = it.second.gossip_;
        it.second.gossip_ = std::max(gl, gl + 1);
      }
    }
    last_gossip_tick_time_ = now;
  }

  // stayin' alive
  Node& node(nodes_.at(this_node.index()));
  node.gossip_ = 0;
  node.gossip_ts_ = instance_id_;
  node.failover_ =
      failover_.load() ? instance_id_ : std::chrono::milliseconds::zero();
  node.is_node_starting_ = !isLogsConfigLoaded();
  node.status_ = node_health_status_.load(std::memory_order_relaxed);
  // Don't trigger other nodes' state transition until we receive min number
  // of gossips. The GCS reply is not same as a regular gossip, and therefore
  // doesn't contain Node::gossip_ values. The default values of Node::gossip_
  // mean that this node has never heard from other cluster nodes, which
  // translates to DEAD state.
  // It is possible that a GCS reply can move a node to ALIVE
  // and immediately after that detectFailures() will detect the node as DEAD
  // based on gossip_list[], which will again move the node into
  // DEAD->SUSPECT->ALIVE state machine.
  if (num_gossips_received_ >= settings_->min_gossips_for_stable_state) {
    detectFailures(this_node.index(), nodes_lock);
  } else {
    // In normal scenario, 'this' node will move out of suspect state,
    // either upon expiration of suspect timer, or eventually because of
    // calling detectFailures() above(which calls updateNodeState())
    //
    // But in cases where
    // a) we hit the libevent timer bug, and
    // b) we can't pick a node to send a gossip to, or
    //    there's only 1 node in the cluster
    // we still want to transition out of suspect state when it expires,
    // and change cluster_state_ accordingly.
    updateNodeState(this_node.index(),
                    node,
                    false /*dead*/,
                    true /*self*/,
                    false /*failover*/);
    updateNodeStatus(this_node.index(), node, node.status_);
  }

  getClusterState()->setBoycottedNodes(
      getBoycottTracker().getBoycottedNodes(SystemTimestamp::now()));

  if (!dest.isNodeID()) {
    RATELIMIT_WARNING(std::chrono::minutes(1),
                      1,
                      "Unable to find a node to send a gossip message to");
    // For single node cases, update self's rsm version
    if (nodes_configuration->clusterSize() == 1) {
      fetchVersions();
    }
    return;
  }

  GOSSIP_Message::GOSSIP_flags_t flags = 0;

  // If at least one entry in the failover list is non-zero, include the
  // list in the message.
  // In GOSSIP protocol >= HASHMAP_SUPPORT_IN_GOSSIP, this wouldn't be
  // necessary because failover list is sent through a list of GOSSIP_Node.
  for (auto& it : nodes_) {
    if (it.second.failover_ > std::chrono::milliseconds::zero()) {
      flags |= GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
      break;
    }
  }

  // In GOSSIP protocol >= HASHMAP_SUPPORT_IN_GOSSIP, this wouldn't be
  // necessary because starting list is sent through a list of GOSSIP_Node.
  flags |= GOSSIP_Message::HAS_STARTING_LIST_FLAG;

  const auto boycott_map = getBoycottTracker().getBoycottsForGossip();
  std::vector<Boycott> boycotts;
  boycotts.reserve(boycott_map.size());

  std::transform(boycott_map.cbegin(),
                 boycott_map.cend(),
                 std::back_inserter(boycotts),
                 [](const auto& entry) { return entry.second; });

  const auto boycott_durations_map =
      getBoycottTracker().getBoycottDurationsForGossip();
  std::vector<BoycottAdaptiveDuration> boycott_durations;
  boycott_durations.reserve(boycott_durations_map.size());
  std::transform(boycott_durations_map.cbegin(),
                 boycott_durations_map.cend(),
                 std::back_inserter(boycott_durations),
                 [](const auto& entry) { return entry.second; });

  GOSSIP_Message::node_list_t gossip_node_list;
  GOSSIP_Message::versions_node_list_t versions_list;
  if (!skip_sending_versions_) {
    fetchVersions();
    flags |= GOSSIP_Message::HAS_VERSIONS;
  }
  for (auto serv_it = serv_disc->begin(); serv_it != serv_disc->end();
       ++serv_it) {
    if (nodes_.find(serv_it->first) == nodes_.end()) {
      continue;
    }
    auto& fdnode = nodes_[serv_it->first];
    // If at least one entry in the failover list is non-zero, include the
    // list in the message.
    // In GOSSIP protocol >= HASHMAP_SUPPORT_IN_GOSSIP, this wouldn't be
    // necessary because failover list is sent through a list of GOSSIP_Node.
    if (fdnode.failover_ > std::chrono::milliseconds::zero()) {
      flags |= GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
    }

    GOSSIP_Node gnode;
    gnode.node_id_ = serv_it->first;
    gnode.gossip_ = fdnode.gossip_;
    gnode.gossip_ts_ = fdnode.gossip_ts_;
    gnode.failover_ = fdnode.failover_;
    gnode.is_node_starting_ = fdnode.is_node_starting_;
    gnode.node_status_ = fdnode.status_;
    gossip_node_list.push_back(gnode);

    if (flags & GOSSIP_Message::HAS_VERSIONS) {
      Versions_Node rnode;
      rnode.node_id_ = serv_it->first;
      for (auto& rsm_type : registered_rsms_) {
        lsn_t rsm_version = LSN_INVALID;
        if (fdnode.rsm_versions_.find(rsm_type) != fdnode.rsm_versions_.end()) {
          rsm_version = fdnode.rsm_versions_[rsm_type];
        }
        rnode.rsm_versions_.push_back(rsm_version);
      }
      rnode.ncm_versions_ = fdnode.ncm_versions_;
      versions_list.push_back(rnode);
    }
  }
  skip_sending_versions_ = (skip_sending_versions_ + 1) %
      (settings_->gossip_include_rsm_versions_frequency);

  // bump the message sequence number
  ++current_msg_id_;
  int rv = sendGossipMessage(
      dest,
      std::make_unique<GOSSIP_Message>(this_node,
                                       std::move(gossip_node_list),
                                       instance_id_,
                                       getCurrentTimeInMillis(),
                                       std::move(boycotts),
                                       std::move(boycott_durations),
                                       flags,
                                       current_msg_id_,
                                       registered_rsms_,
                                       std::move(versions_list)));

  if (rv != 0) {
    RATELIMIT_DEBUG(std::chrono::seconds(1),
                    10,
                    "Failed to send GOSSIP to node %s: %s",
                    Sender::describeConnection(Address(dest)).c_str(),
                    error_description(err));
  }

  if (shouldDumpState()) {
    ld_info("FD state after constructing gossip message for %s",
            dest.toString().c_str());
    dumpFDState(nodes_lock);
  }
}

bool FailureDetector::processFlags(
    const GOSSIP_Message& msg,
    Node& sender_node,
    const folly::SharedMutex::ReadHolder& nodes_lock) {
  const bool is_node_bringup =
      bool(msg.flags_ & GOSSIP_Message::NODE_BRINGUP_FLAG);
  const bool is_suspect_state_finished =
      bool(msg.flags_ & GOSSIP_Message::SUSPECT_STATE_FINISHED);
  const bool is_start_state_finished =
      bool(msg.flags_ & GOSSIP_Message::STARTING_STATE_FINISHED);
  auto msg_type = flagsToString(msg.flags_);

  if (!is_node_bringup && !is_suspect_state_finished) {
    return false; /* not a special broadcast message */
  }

  RATELIMIT_INFO(std::chrono::seconds(1),
                 5,
                 "Received %s message from %s with instance id:%lu"
                 ", sent_time:%lums",
                 msg_type.c_str(),
                 msg.gossip_node_.toString().c_str(),
                 msg.instance_id_.count(),
                 msg.sent_time_.count());
  if (shouldDumpState()) {
    dumpFDState(nodes_lock);
  }

  node_index_t my_idx = getMyNodeID().index();
  node_index_t sender_idx = msg.gossip_node_.index();
  ld_check(my_idx != sender_idx);

  if (!isValidInstanceId(msg.instance_id_, sender_idx)) {
    return true;
  }

  // marking sender alive
  sender_node.gossip_ = 0;
  sender_node.gossip_ts_ = msg.instance_id_;
  sender_node.failover_ = std::chrono::milliseconds::zero();
  bool is_starting = sender_node.is_node_starting_ = !is_start_state_finished;

  if (is_suspect_state_finished) {
    if (sender_node.state_.load() != NodeState::ALIVE) {
      updateDependencies(sender_idx,
                         sender_node,
                         NodeState::ALIVE,
                         /*failover*/ false,
                         is_starting);
      auto cs = getClusterState();
      ld_info("N%hu transitioned to %s as a result of receiving "
              "suspect-state-finished message, FD State:"
              "(gossip: %u, instance-id: %lu, failover: %lu, starting: %u)",
              sender_idx,
              cs->getNodeStateAsStr(sender_idx),
              sender_node.gossip_,
              sender_node.gossip_ts_.count(),
              sender_node.failover_.count(),
              is_starting);
    }
  } else if (is_node_bringup) {
    updateNodeState(sender_idx, sender_node, false, false, false);
    // When new instance id comes up, reset all version information
    resetVersions(sender_idx);
  } else {
    ld_check(true);
    return true;
  }

  if (shouldDumpState()) {
    ld_info("FD state after receiving %s message from %s",
            msg_type.c_str(),
            msg.gossip_node_.toString().c_str());
    dumpFDState(nodes_lock);
  }

  return true;
}

std::string
FailureDetector::flagsToString(GOSSIP_Message::GOSSIP_flags_t flags) {
  std::string s;
  if (flags & GOSSIP_Message::SUSPECT_STATE_FINISHED) {
    s = "suspect-state-finished";
  }
  if (flags & GOSSIP_Message::STARTING_STATE_FINISHED) {
    if (!s.empty()) {
      s += "|";
    }
    s += "starting_state_finished";
  }

  if (!s.empty()) {
    return s;
  }

  if (flags & GOSSIP_Message::NODE_BRINGUP_FLAG) {
    return "bringup";
  }

  return "unknown";
}

void FailureDetector::onGossipReceived(const GOSSIP_Message& msg) {
  std::lock_guard<std::mutex> lock(mutex_);

  if (waiting_for_cluster_state_) {
    // Don't process gossip messages before we build initial state.
    RATELIMIT_DEBUG(std::chrono::seconds(1),
                    10,
                    "Ignoring GOSSIP message because we're still waiting for "
                    "get-cluster-state request.");
    return;
  }

  node_index_t this_index = getMyNodeID().index();

  if (shouldDumpState()) {
    ld_info("Gossip message received from node %s, sent_time:%lums",
            msg.gossip_node_.toString().c_str(),
            msg.sent_time_.count());
    dumpGossipMessage(msg);
  }

  if (checkSkew(msg)) {
    return;
  }

  node_index_t sender_idx = msg.gossip_node_.index();
  folly::SharedMutex::ReadHolder nodes_lock(nodes_mutex_);

  {
    Node& sender_node = insertOrGetNode(sender_idx, nodes_lock);

    if (sender_node.gossip_ts_ > msg.instance_id_) {
      RATELIMIT_WARNING(std::chrono::seconds(1),
                        5,
                        "Possible time-skew detected on %s, received a lower "
                        "instance id(%lu) from sender than already known(%lu)",
                        msg.gossip_node_.toString().c_str(),
                        msg.instance_id_.count(),
                        sender_node.gossip_ts_.count());
      STAT_INCR(getStats(), gossips_rejected_instance_id);
      return;
    }

    if (processFlags(msg, sender_node, nodes_lock)) {
      // It's a bringup message, not a real gossip message.
      return;
    }
  }

  // Merge the contents of gossip list with those from the message
  // by taking the minimum.
  const bool has_failover_list =
      msg.flags_ & GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
  const bool has_starting_list =
      msg.flags_ & GOSSIP_Message::HAS_STARTING_LIST_FLAG;

  bool update_statuses = senderUsingHealthMonitor(sender_idx, msg.node_list_);
  std::unordered_set<size_t> node_ids_to_skip;
  std::unordered_set<size_t> nodes_with_new_instances;
  for (auto node : msg.node_list_) {
    size_t id = node.node_id_;

    // Don't modify this node's state based on gossip message.
    if (id == this_index) {
      node_ids_to_skip.insert(id);
      continue;
    }

    if (has_failover_list && node.failover_ > node.gossip_ts_) {
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "Received invalid combination of Failover(%lu) and"
                         " Instance id(%lu) for N%zu from %s",
                         node.failover_.count(),
                         node.gossip_ts_.count(),
                         id,
                         msg.gossip_node_.toString().c_str());
      ld_check(false);
      continue;
    }

    Node& node_state = insertOrGetNode(id, nodes_lock);

    // If the incoming Gossip message knows about an older instance of the
    // process running on Node Ni, then ignore this update.
    if (node_state.gossip_ts_ > node.gossip_ts_) {
      ld_spew("Received a stale instance id from %s,"
              " for N%zu, our:%lu, received:%lu",
              msg.gossip_node_.toString().c_str(),
              id,
              node_state.gossip_ts_.count(),
              node.gossip_ts_.count());
      node_ids_to_skip.insert(id);
      continue;
    } else if (node_state.gossip_ts_ < node.gossip_ts_) {
      // If the incoming Gossip message knows about a valid
      // newer instance of Node Ni, then copy everything
      if (isValidInstanceId(node.gossip_ts_, id)) {
        node_state.gossip_ = node.gossip_;
        node_state.gossip_ts_ = node.gossip_ts_;
        node_state.failover_ = has_failover_list
            ? node.failover_
            : std::chrono::milliseconds::zero();
        if (has_starting_list) {
          node_state.is_node_starting_ = node.is_node_starting_;
        }
        if (update_statuses || id == sender_idx) {
          node_state.status_ = node.node_status_;
        }
        nodes_[id].status_ = node.node_status_;
        nodes_with_new_instances.insert(id);
      }
      continue;
    }

    if (node.gossip_ <= nodes_[id].gossip_) {
      nodes_[id].gossip_ = node.gossip_;
      if (update_statuses || id == sender_idx) {
        nodes_[id].status_ = node.node_status_;
      }
    }

    if (has_starting_list) {
      node_state.is_node_starting_ &= node.is_node_starting_;
    }
    if (has_failover_list) {
      node_state.failover_ = std::max(node_state.failover_, node.failover_);
    }
  }

  getBoycottTracker().updateReportedBoycotts(msg.boycott_list_);
  getBoycottTracker().updateReportedBoycottDurations(
      msg.boycott_durations_list_, std::chrono::system_clock::now());
  updateVersions(msg, node_ids_to_skip, nodes_with_new_instances);

  num_gossips_received_++;
  if (num_gossips_received_ <= settings_->min_gossips_for_stable_state) {
    ld_debug("Received gossip#%zu", num_gossips_received_);
  }
}

void FailureDetector::startSuspectTimer() {
  ld_info("Starting suspect state timer");
  node_index_t my_idx = getMyNodeID().index();

  folly::SharedMutex::ReadHolder nodes_lock(nodes_mutex_);
  Node& node = insertOrGetNode(my_idx, nodes_lock);
  node.gossip_ = 0;
  updateNodeState(my_idx, node, false, true, false);
  updateNodeStatus(my_idx, node, node.status_);

  suspect_timer_.assign([this, my_idx] {
    ld_info("Suspect timeout expired");
    std::lock_guard lock(mutex_);
    folly::SharedMutex::ReadHolder nodes_lock2(nodes_mutex_);
    Node& node = nodes_.at(my_idx);
    updateNodeState(my_idx, node, false, true, false);
    updateNodeStatus(my_idx, node, node.status_);
  });

  suspect_timer_.activate(settings_->suspect_duration);
}

void FailureDetector::startGossiping() {
  ld_info("Start Gossiping.");

  Worker* w = Worker::onThisThread();
  gossip_timer_node_ = w->registerTimer(
      [this](ExponentialBackoffTimerNode* node) {
        std::lock_guard lock(mutex_);
        gossip();
        node->timer->activate();
      },
      settings_->gossip_interval,
      settings_->gossip_interval);
  ld_check(gossip_timer_node_ != nullptr);
  gossip_timer_node_->timer->activate();
}

std::string FailureDetector::dumpGossipList(std::vector<uint32_t> list) {
  const auto& nodes_configuration = getNodesConfiguration();
  size_t n = nodes_configuration->getMaxNodeIndex() + 1;
  n = std::min(n, list.size());
  std::string res;

  for (size_t i = 0; i < n; ++i) {
    // if i doesn't exist, generation 1 will be used
    NodeID node_id = nodes_configuration->getNodeID(i);
    res += node_id.toString() + " = " + folly::to<std::string>(list[i]) +
        (i < n - 1 ? ", " : "");
  }

  return res;
}

std::string
FailureDetector::dumpInstanceList(std::vector<std::chrono::milliseconds> list) {
  const auto& nodes_configuration = getNodesConfiguration();
  size_t n = nodes_configuration->getMaxNodeIndex() + 1;
  n = std::min(n, list.size());
  std::string res;

  for (size_t i = 0; i < n; ++i) {
    // if i doesn't exist, generation 1 will be used
    NodeID node_id = nodes_configuration->getNodeID(i);
    res += node_id.toString() + " = " +
        folly::to<std::string>(list[i].count()) + (i < n - 1 ? ", " : "");
  }

  return res;
}

bool FailureDetector::shouldDumpState() {
  if (isTracingOn()) {
    return true;
  }
  if (facebook::logdevice::dbg::currentLevel <
      facebook::logdevice::dbg::Level::SPEW) {
    return false;
  }
  // Throttle to once every 0.5 seconds.
  const std::chrono::milliseconds state_dump_period{500};
  if (SteadyTimestamp::now() < last_state_dump_time_ + state_dump_period) {
    return false;
  }
  last_state_dump_time_ = SteadyTimestamp::now();
  return true;
}

void FailureDetector::dumpFDState(
    const folly::SharedMutex::ReadHolder& /* nodes_lock */) {
  const auto& nodes_configuration = getNodesConfiguration();
  size_t n = nodes_configuration->getMaxNodeIndex() + 1;
  std::string status_str;

  for (size_t i = 0; i < n; ++i) {
    if (!nodes_configuration->isNodeInServiceDiscoveryConfig(i) ||
        !nodes_.count(i)) {
      continue;
    }

    NodeID node_id = nodes_configuration->getNodeID(i);
    if (!status_str.empty()) {
      status_str += ", ";
    }
    status_str += node_id.toString() + ": " + nodes_.at(i).toString();
  }

  const dbg::Level level = isTracingOn() ? dbg::Level::INFO : dbg::Level::SPEW;
  ld_log(
      level, "Failure Detector status for all nodes: %s", status_str.c_str());
}

std::string FailureDetector::Node::toString() const {
  return folly::sformat(
      "({}, {}, bl: {}, gs: {}, ts: {}, fo: {}, starting: {}, health: {})",
      getNodeStateString(state_.load()),
      logdevice::toString(status_),
      blacklisted_.load(),
      gossip_,
      RecordTimestamp(gossip_ts_).toString(),
      RecordTimestamp(failover_).toString(),
      is_node_starting_,
      static_cast<int>(status_));
}

void FailureDetector::shutdown() {
  ld_info("Stopping failure detector");
  suspect_timer_.cancel();
  gossip_timer_node_ = nullptr;
}

void FailureDetector::dumpGossipMessage(const GOSSIP_Message& msg) {
  ld_info(
      "Flags from %s 0x%x", msg.gossip_node_.toString().c_str(), msg.flags_);
}

void FailureDetector::getClusterDeadNodeStats(size_t* effective_dead_cnt,
                                              size_t* effective_cluster_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (effective_dead_cnt != nullptr) {
    *effective_dead_cnt = effective_dead_cnt_;
  }
  if (effective_cluster_size != nullptr) {
    *effective_cluster_size = effective_cluster_size_;
  }
}

void FailureDetector::detectFailures(
    node_index_t self,
    const folly::SharedMutex::ReadHolder& /* nodes_lock */) {
  const int threshold = settings_->gossip_failure_threshold;
  const auto& nodes_configuration = getNodesConfiguration();
  const auto& serv_disc = nodes_configuration->getServiceDiscovery();

  size_t dead_cnt = 0;
  size_t effective_dead_cnt = 0;
  size_t overloaded_cnt = 0;
  size_t unhealthy_cnt = 0;
  size_t effective_unhealthy_cnt = 0;
  size_t cluster_size = nodes_configuration->clusterSize();
  size_t effective_cluster_size = cluster_size;

  // Finally, update all the states
  for (auto& it : nodes_) {
    if (it.first == self) {
      // don't transition yourself to DEAD
      updateNodeState(
          self, it.second, false /*dead*/, true /*self*/, false /*failover*/);
      updateNodeStatus(self, it.second, it.second.status_);
      switch (it.second.status_) {
        case NodeHealthStatus::UNHEALTHY:
          ++unhealthy_cnt;
          ++effective_unhealthy_cnt;
          break;
        case NodeHealthStatus::OVERLOADED:
          ++overloaded_cnt;
          break;
        default:
          break;
      };
      continue;
    }

    // Node 'it' is likely dead if this node and
    // other nodes haven't heard from it in a long time
    // OR
    // Node 'it' is performing a graceful shutdown.
    // Mark it DEAD so no work ends up sent its way.
    bool failover = (it.second.failover_ > std::chrono::milliseconds::zero());
    bool dead = (it.second.gossip_ > threshold);
    if (dead) {
      // if the node is actually dead, clear the failover boolean, to make
      // sure we don't mistakenly transition from DEAD to FAILING_OVER in the
      // Cluster State.
      failover = false;
    } else {
      // node is not dead but may be failing over. we don't have proper
      // FAILING OVER state in the FD, instead we use this boolean to carry
      // over the fact that the node is shutting down and consider it dead
      // in that case...
      // TODO: revisit Failure Detector to better handle failover
      dead = failover;
    }

    updateNodeState(it.first, it.second, dead, false, failover);
    if (serv_disc->hasNode(it.first)) {
      updateNodeStatus(it.first,
                       it.second,
                       dead ? NodeHealthStatus::UNHEALTHY : it.second.status_);
    }

    // re-check node's state as it may be suspect, in which case it is still
    // considered dead
    dead = (it.second.state_.load() != NodeState::ALIVE);

    if (nodes_configuration->isNodeInServiceDiscoveryConfig(it.first)) {
      bool node_disabled =
          configuration::nodes::isNodeDisabled(*nodes_configuration, it.first);
      if (node_disabled) {
        // It is disabled. Do not count it towards the effective cluster
        // size, as this node doesn't serve anything
        --effective_cluster_size;
      }
      if (dead) {
        ++dead_cnt;
        ++unhealthy_cnt;
        if (!node_disabled) {
          // only active nodes matter for isolation. see comment below.
          ++effective_dead_cnt;
          ++effective_unhealthy_cnt;
        }
      } else {
        switch (it.second.status_) {
          case NodeHealthStatus::UNHEALTHY:
            ++unhealthy_cnt;
            ++effective_unhealthy_cnt;
            break;
          case NodeHealthStatus::OVERLOADED:
            ++overloaded_cnt;
            break;
          default:
            break;
        };
      }
    }
  }

  effective_dead_cnt_ = effective_dead_cnt;
  effective_cluster_size_ = effective_cluster_size;
  STAT_SET(getStats(), num_nodes, cluster_size);
  STAT_SET(getStats(), num_dead_nodes, dead_cnt);
  STAT_SET(getStats(), effective_num_nodes, effective_cluster_size);
  STAT_SET(getStats(), effective_dead_nodes, effective_dead_cnt);
  // HealthMonitor stats
  STAT_SET(getStats(), num_overloaded_nodes, overloaded_cnt);
  STAT_SET(getStats(), num_unhealthy_nodes, unhealthy_cnt);
  STAT_SET(getStats(), effective_unhealthy_nodes, effective_unhealthy_cnt);

  // Check whether more than half of the nodes are dead. This may mean that
  // there is a network partitioning and we are in a minority. We record this
  // fact in the isolated_ boolean for sequencers to take action.
  // For the purpose of this check, we consider only the effective numbers. We
  // ignore nodes that are disabled, meaning that they do not participate in
  // cluster activities. This allows the cluster to keep functioning with a
  // subset of nodes, when the others are disabled. The reasoning is: if a
  // node dies while being disabled, it shouldn't affect the cluster, and so
  // shouldn't trigger isolation mode.
  isolated_.store(2 * effective_dead_cnt > effective_cluster_size);

  if (isolation_checker_ != nullptr) {
    isolation_checker_->processIsolationUpdates();
  }
}

void FailureDetector::updateDependencies(node_index_t idx,
                                         Node& node,
                                         FailureDetector::NodeState new_state,
                                         bool failover,
                                         bool starting) {
  node.state_.store(new_state);
  const bool is_dead = (new_state != NodeState::ALIVE);
  auto cs = getClusterState();

  if (isolation_checker_ != nullptr) {
    // may not set in tests
    if (is_dead && cs->isNodeAlive(idx)) {
      isolation_checker_->onNodeDead(idx);
    } else if (!is_dead && !cs->isNodeAlive(idx)) {
      isolation_checker_->onNodeAlive(idx);
    }
  }

  ClusterState::NodeState state;
  if (is_dead) {
    state = failover ? ClusterState::NodeState::FAILING_OVER
                     : ClusterState::NodeState::DEAD;
  } else {
    state = starting ? ClusterState::NodeState::STARTING
                     : ClusterState::NodeState::FULLY_STARTED;
  }

  cs->setNodeState(idx, state);
}

void FailureDetector::updateNodeState(node_index_t idx,
                                      Node& node,
                                      bool dead,
                                      bool self,
                                      bool failover) {
  bool starting;
  if (self) {
    starting = !isLogsConfigLoaded();
  } else {
    starting = node.is_node_starting_;
  }

  NodeState current = node.state_.load(), next = NodeState::DEAD;
  auto current_time_ms = getCurrentTimeInMillis();
  auto suspect_duration = settings_->suspect_duration;

  if (!dead) {
    next = current;
    switch (current) {
      case DEAD:
        // Transition from DEAD -> SUSPECT requires
        // last_suspect_time to be reset.
        if (settings_->suspect_duration.count() > 0) {
          next = NodeState::SUSPECT;
          node.last_suspected_at_.store(current_time_ms);
        } else {
          next = NodeState::ALIVE;
        }
        break;
      case SUSPECT:
        ld_check(suspect_duration.count() > 0);
        if (current_time_ms >
            node.last_suspected_at_.load() + suspect_duration) {
          next = NodeState::ALIVE;
        }
        break;
      case ALIVE:
        // Node stays ALIVE
        break;
    }
  }

  if (current != next) {
    if (next != NodeState::DEAD) {
      // Node's state is no longer DEAD. Reset connection throttling
      // on a server socket to that node to allow subsequent gossip
      // messages to be immediately sent to it.
      Connection* conn = getServerConnection(idx);
      if (conn) {
        conn->resetConnectThrottle();
      }
    } else {
      // This node should transition itself to DEAD.
      if (self) {
        ld_check(false);
      }
    }

    ld_info("N%hu transitioned from %s to %s, FD State:"
            "(gossip: %u, instance-id: %lu, failover: %lu, starting: %u)",
            idx,
            getNodeStateString(current),
            getNodeStateString(next),
            nodes_[idx].gossip_,
            nodes_[idx].gossip_ts_.count(),
            nodes_[idx].failover_.count(),
            starting);
  }

  updateDependencies(idx, node, next, failover, starting);
  if (self && current != next && next == NodeState::ALIVE) {
    broadcastBringupUpdate(GOSSIP_Message::SUSPECT_STATE_FINISHED);
  }
}

void FailureDetector::updateNodeStatus(node_index_t idx,
                                       Node& node,
                                       NodeHealthStatus status) {
  // This is called after locking nodes_mutex_ read lock
  NodeHealthStatus current = node.status_;
  if (current != status) {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "N%hu transitioned from %s to %s,(status) FD State:"
                   "(gossip: %u, instance-id: %lu)",
                   idx,
                   toString(current).c_str(),
                   toString(status).c_str(),
                   node.gossip_,
                   node.gossip_ts_.count());
  }
  node.status_ = status;
  getClusterState()->setNodeStatus(idx, status);
}

bool FailureDetector::isValidDestination(node_index_t node_idx) {
  {
    folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

    auto it = nodes_.find(node_idx);

    if (it == nodes_.end()) {
      return false;
    }

    if (it->second.blacklisted_.load()) {
      // exclude blacklisted nodes
      return false;
    }
  }

  Connection* conn = getServerConnection(node_idx);
  if (!conn) {
    // If a connection to the node doesn't exist yet, consider it as a valid
    // destination.
    return true;
  }

  int rv = conn->checkConnection(nullptr);
  if (rv != 0) {
    if (err == E::DISABLED || err == E::NOBUFS) {
      ld_spew("Can't gossip to N%u: %s", node_idx, error_description(err));
      return false;
    }
  }

  return true;
}

bool FailureDetector::isMyDomainIsolated(NodeLocationScope scope) const {
  if (isolation_checker_ == nullptr) {
    // not attached
    return false;
  }
  return isolation_checker_->isMyDomainIsolated(scope);
}

const char* FailureDetector::getNodeStateString(NodeState state) {
  switch (state) {
    case ALIVE:
      return "ALIVE";
    case SUSPECT:
      return "SUSPECT";
    case DEAD:
      return "DEAD";
  }
  return "UNKNOWN";
}

std::string FailureDetector::getStateString(node_index_t idx) const {
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  auto it = nodes_.find(idx);
  if (it == nodes_.end()) {
    return "invalid node index";
  }

  const Node& node = it->second;

  return folly::sformat(
      "(gossip: {}, instance-id: {}, failover: {}, starting: {}, "
      "status: {}, state: {})",
      node.gossip_,
      node.gossip_ts_.count(),
      node.failover_.count(),
      (int)node.is_node_starting_,
      toString(node.status_),
      getNodeStateString(node.state_.load()));
}

folly::dynamic FailureDetector::getStateJson(node_index_t idx) const {
  folly::dynamic obj = folly::dynamic::object;
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  auto it = nodes_.find(idx);
  if (it == nodes_.end()) {
    obj["error"] = "invalid node index";
    return obj;
  }
  const Node& node = it->second;

  obj["gossip"] = node.gossip_;
  obj["instance-id"] = node.gossip_ts_.count();
  obj["failover"] = node.failover_.count();
  obj["starting"] = (int)node.is_node_starting_;
  obj["state"] = getNodeStateString(node.state_.load());
  obj["status"] = toString(node.status_);

  return obj;
}

std::string FailureDetector::getDomainIsolationString() const {
  if (isolation_checker_) {
    return isolation_checker_->getDebugInfo();
  }
  return "";
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
FailureDetector::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

NodeID FailureDetector::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

ClusterState* FailureDetector::getClusterState() const {
  return Worker::getClusterState();
}

void FailureDetector::broadcastBringupUpdate(
    GOSSIP_Message::GOSSIP_flags_t additional_flags) {
  auto flags = additional_flags | GOSSIP_Message::NODE_BRINGUP_FLAG;

  bool config_loaded = isLogsConfigLoaded();
  if (config_loaded) {
    flags |= GOSSIP_Message::STARTING_STATE_FINISHED;
  }
  need_to_broadcast_starting_state_finished_ = !config_loaded;

  const auto& nodes_configuration = getNodesConfiguration();
  const auto& serv_disc = nodes_configuration->getServiceDiscovery();
  node_index_t my_idx = getMyNodeID().index();
  NodeID dest;

  std::string msg_type = flagsToString(flags);
  if (msg_type == "unknown") {
    RATELIMIT_ERROR(std::chrono::seconds(1), 1, "Invalid flags=%d", flags);
    ld_check(false);
  }

  ld_info("Broadcasting %s message.", msg_type.c_str());
  for (const auto& it : *serv_disc) {
    node_index_t idx = it.first;
    if (idx == my_idx) {
      continue;
    }

    auto gossip_msg = std::make_unique<GOSSIP_Message>();
    gossip_msg->node_list_.clear();
    gossip_msg->gossip_node_ = getMyNodeID();
    gossip_msg->flags_ |= flags;
    gossip_msg->instance_id_ = instance_id_;
    gossip_msg->sent_time_ = getCurrentTimeInMillis();

    dest = nodes_configuration->getNodeID(idx);
    RATELIMIT_INFO(std::chrono::seconds(1),
                   2,
                   "Sending %s message with instance id:%lu to node %s",
                   msg_type.c_str(),
                   instance_id_.count(),
                   Sender::describeConnection(Address(dest)).c_str());
    int rv = sendGossipMessage(dest, std::move(gossip_msg));
    if (rv != 0) {
      ld_info("Failed to send %s message to node %s: %s",
              msg_type.c_str(),
              Sender::describeConnection(Address(dest)).c_str(),
              error_description(err));
    }
  }
}

int FailureDetector::sendGossipMessage(NodeID node,
                                       std::unique_ptr<GOSSIP_Message> gossip) {
  ld_spew("Sending Gossip message to %s", node.toString().c_str());

  return Worker::onThisThread()->sender().sendMessage(std::move(gossip), node);
}

void FailureDetector::onGossipMessageSent(Status st,
                                          const Address& to,
                                          uint64_t msg_id) {
  ld_check(to.isNodeAddress());

  if (st != E::OK) {
    // keep track of the number of errors
    STAT_INCR(getStats(), gossips_failed_to_send);

    auto nidx = to.asNodeID().index();
    if (isAlive(nidx)) {
      STAT_INCR(getStats(), gossips_failed_to_send_to_alive_nodes);
    }
  }

  if (current_msg_id_ != msg_id || msg_id == 0) {
    // ignore this callback as it was for an older message
    return;
  }

  // set the limit of retries to the number of nodes in the cluster.
  // this is arbitrary to try best to send a message to any node.
  const size_t max_attempts = getNodesConfiguration()->clusterSize();
  if (st == E::OK) {
    // message was sent successfully, reset the counter.
    num_gossip_attempts_failed_ = 0;
  } else if (num_gossip_attempts_failed_ == max_attempts) {
    // failed to send message, but we reached the maximum retries.
    // reset the counter and log a message
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Could not send gossip to %s: %s. "
                      "Consecutively failed to send %lu gossips.",
                      Sender::describeConnection(Address(to)).c_str(),
                      error_description(st),
                      num_gossip_attempts_failed_);
    num_gossip_attempts_failed_ = 0;
  } else {
    // failed to send message, let's try another node.
    // bump the counter and retry
    ++num_gossip_attempts_failed_;
    if (gossip_timer_node_) {
      RATELIMIT_INFO(std::chrono::seconds(1),
                     1,
                     "Could not send gossip to %s: %s. "
                     "Trying another node.",
                     Sender::describeConnection(Address(to)).c_str(),
                     error_description(st));
      // here we do not directly send another gossip but rather schedule the
      // gossip timer for immediate execution.
      gossip_timer_node_->timer->fire();
    }
  }
}

bool FailureDetector::senderUsingHealthMonitor(
    node_index_t sender_idx,
    GOSSIP_Message::node_list_t node_list) {
  bool using_health_monitor{false};
  for (auto& node : node_list) {
    if (node.node_id_ != sender_idx) {
      continue;
    }
    using_health_monitor = node.node_status_ != NodeHealthStatus::UNDEFINED;
  }
  return using_health_monitor;
}

void FailureDetector::setOutliers(std::vector<NodeID> outliers) {
  getBoycottTracker().setLocalOutliers(std::move(outliers));
}

void FailureDetector::resetBoycottedNode(node_index_t node_index) {
  getBoycottTracker().resetBoycott(node_index);
}

bool FailureDetector::isBoycotted(node_index_t node_index) {
  return getBoycottTracker().isBoycotted(node_index);
}

folly::Optional<Boycott>
FailureDetector::getNodeBoycottObject(node_index_t node_index) {
  if (!getBoycottTracker().isBoycotted(node_index)) {
    return folly::none;
  }
  const auto& boycotts = getBoycottTracker().getBoycottsForGossip();
  auto it = boycotts.find(node_index);
  if (it == boycotts.end()) {
    return folly::none;
  }
  return it->second;
}

Connection* FailureDetector::getServerConnection(node_index_t idx) {
  return Worker::onThisThread()->sender().findServerConnection(idx);
}

void FailureDetector::setBlacklisted(node_index_t idx, bool blacklisted) {
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_.count(idx)) {
    nodes_.at(idx).blacklisted_.store(blacklisted);
  }
}

bool FailureDetector::isBlacklisted(node_index_t idx) const {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  auto it = nodes_.find(idx);
  return it != nodes_.end() && it->second.blacklisted_.load();
}

bool FailureDetector::isAlive(node_index_t idx) const {
  /* common case */
  bool alive = getClusterState()->isNodeAlive(idx);
  if (alive) {
    return true;
  }

  /* We'll check whether suspect duration has already passed
   * or not, only for this node.
   */
  node_index_t my_idx = getMyNodeID().index();

  if (my_idx != idx) {
    return false;
  }

  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_.find(idx) == nodes_.end()) {
    // TODO: how to handle this exception?
    return false;
  }

  /* If the current node's SUSPECT state has elapsed,
   * return ALIVE instead of DEAD.
   * FD Thread will soon transition us(this node) to ALIVE.
   */
  auto current_time_ms = getCurrentTimeInMillis();
  auto suspect_duration = settings_->suspect_duration;
  if (suspect_duration.count() > 0) {
    if (current_time_ms >
        nodes_.at(idx).last_suspected_at_.load() + suspect_duration) {
      RATELIMIT_INFO(
          std::chrono::seconds(1),
          1,
          "Suspect duration for this node(N%hu) expired, "
          "but FD hasn't yet made the transition, treating the node as ALIVE",
          idx);
      return true;
    }
  }

  return false;
}

bool FailureDetector::isIsolated() const {
  if (settings_->ignore_isolation) {
    return false;
  }
  return isolated_.load();
}

bool FailureDetector::isStableState() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return num_gossips_received_ >= settings_->min_gossips_for_stable_state;
}

bool FailureDetector::isLogsConfigLoaded() {
  ld_check(Worker::onThisThread(false));
  return processor_->isLogsConfigLoaded();
}

void FailureDetector::updateVersions(
    const GOSSIP_Message& msg,
    std::unordered_set<size_t> node_ids_to_skip,
    std::unordered_set<size_t> nodes_with_new_instances) {
  for (const auto& node : msg.versions_) {
    size_t id = node.node_id_;
    if (node_ids_to_skip.find(id) != node_ids_to_skip.end()) {
      continue;
    }
    if (nodes_.find(id) == nodes_.end()) {
      continue;
    }

    bool new_instance_id =
        nodes_with_new_instances.find(id) != nodes_with_new_instances.end();
    auto& fdnode = nodes_[id];
    for (size_t i = 0; i < msg.rsm_types_.size(); ++i) {
      if (std::find(registered_rsms_.begin(),
                    registered_rsms_.end(),
                    msg.rsm_types_[i]) == registered_rsms_.end()) {
        ld_info("Adding RSM type:%lu to this node's FD state, because %s "
                "included it in the Gossip message",
                msg.rsm_types_[i].val_,
                msg.gossip_node_.toString().c_str());
        // To handle cases where different nodes are gossiping different rsms at
        // the same time, e.g. during rolling-restart where old server and
        // new server may have different rsms registered.
        registered_rsms_.push_back(msg.rsm_types_[i]);
        fdnode.rsm_versions_[msg.rsm_types_[i]] = LSN_INVALID;
      }
      lsn_t existing = fdnode.rsm_versions_[msg.rsm_types_[i]];
      lsn_t recvd = node.rsm_versions_[i];
      if ((recvd > existing) || new_instance_id) {
        ld_debug("Updating RSM versions for N%zu, GOSSIP received from:%s, "
                 "new_instance_id:%s, rsm_type:%lu, recvd:%s, existing:%s",
                 id,
                 msg.gossip_node_.toString().c_str(),
                 new_instance_id ? "yes" : "no",
                 msg.rsm_types_[i].val_,
                 lsn_to_string(recvd).c_str(),
                 lsn_to_string(existing).c_str());
        fdnode.rsm_versions_[msg.rsm_types_[i]] = node.rsm_versions_[i];
      }
    }

    // Update NCM versions
    for (size_t i = 0; i < fdnode.ncm_versions_.size(); ++i) {
      auto& existing = fdnode.ncm_versions_[i];
      auto& recvd = node.ncm_versions_[i];
      if (recvd > existing || new_instance_id) {
        ld_debug("Updating NCM versions for N%zu, GOSSIP received from:%s, "
                 "new_instance_id:%s, recvd:%lu, existing:%lu",
                 id,
                 msg.gossip_node_.toString().c_str(),
                 new_instance_id ? "yes" : "no",
                 recvd.val_,
                 existing.val_);
        existing = recvd;
      }
    }
  }
}

Status FailureDetector::getRSMVersion(node_index_t idx,
                                      logid_t rsm_type,
                                      lsn_t& result_out) {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (std::find(registered_rsms_.begin(), registered_rsms_.end(), rsm_type) ==
      registered_rsms_.end()) {
    return E::NOTSUPPORTED;
  }

  if (nodes_[idx].state_.load() == NodeState::DEAD) {
    return E::STALE;
  }

  result_out = nodes_[idx].rsm_versions_[rsm_type];
  return E::OK;
}

Status FailureDetector::getAllRSMVersionsInCluster(
    logid_t rsm_type,
    std::map<lsn_t, node_index_t, std::greater<lsn_t>>& result_out) {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (std::find(registered_rsms_.begin(), registered_rsms_.end(), rsm_type) ==
      registered_rsms_.end()) {
    return E::NOTSUPPORTED;
  }

  for (auto& node : nodes_) {
    if (node.second.state_.load() != NodeState::DEAD) {
      result_out.insert(
          std::make_pair(node.second.rsm_versions_[rsm_type], node.first));
    }
  }
  return E::OK;
}

Status
FailureDetector::getRSMVersionsForNode(node_index_t idx,
                                       std::map<logid_t, lsn_t>& result_out) {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_[idx].state_.load() == NodeState::DEAD) {
    return E::STALE;
  }

  result_out = nodes_[idx].rsm_versions_;
  for (auto rsm : registered_rsms_) {
    if (result_out.find(rsm) == result_out.end()) {
      result_out[rsm] = LSN_INVALID;
    }
  }
  return E::OK;
}

Status FailureDetector::getNCMVersionsForNode(
    node_index_t idx,
    std::array<membership::MembershipVersion::Type, 3>& result_out) {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_[idx].state_.load() == NodeState::DEAD) {
    return E::STALE;
  }

  result_out = nodes_[idx].ncm_versions_;
  return E::OK;
}

void FailureDetector::resetVersions(node_index_t idx) {
  ld_info("Resetting RSM and NCM Versions for N%hu", idx);
  auto& fdnode = nodes_[idx];
  for (auto& rsm : fdnode.rsm_versions_) {
    rsm.second = LSN_INVALID;
  }

  fdnode.ncm_versions_[0] = membership::MembershipVersion::Type(0);
  fdnode.ncm_versions_[1] = membership::MembershipVersion::Type(0);
  fdnode.ncm_versions_[2] = membership::MembershipVersion::Type(0);
}

}} // namespace facebook::logdevice
