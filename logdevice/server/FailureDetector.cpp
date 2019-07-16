/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/FailureDetector.h"

#include <folly/Memory.h>
#include <folly/Random.h>
#include <folly/small_vector.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/GetClusterStateRequest.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Socket.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/nodes/utils.h"
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

// Request used to complete initialization that needs to
// happen on the thread only
class FailureDetector::InitRequest : public Request {
 public:
  explicit InitRequest(FailureDetector* parent)
      : Request(RequestType::FAILURE_DETECTOR_INIT), parent_(parent) {}

  Execution execute() override {
    parent_->startClusterStateTimer();
    return Execution::COMPLETE;
  }

  WorkerType getWorkerTypeAffinity() override {
    return WorkerType::FAILURE_DETECTOR;
  }

 private:
  FailureDetector* parent_;
};

FailureDetector::FailureDetector(UpdateableSettings<GossipSettings> settings,
                                 ServerProcessor* processor,
                                 StatsHolder* stats,
                                 bool attach)
    : settings_(std::move(settings)), stats_(stats), processor_(processor) {
  size_t max_nodes = processor->settings()->max_nodes;

  // Preallocating makes it easier to handle cluster expansion and shrinking.
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
      "Failure Detector starting with instance id: %lu", instance_id_.count());

  auto cs = processor->cluster_state_.get();
  for (const auto& it : nodes_) {
    cs->setNodeState(it.first, ClusterState::NodeState::DEAD);
  }

  if (attach) {
    std::unique_ptr<Request> rq = std::make_unique<InitRequest>(this);
    int rv = processor->postRequest(rq);
    if (rv) {
      ld_warning("Unable to post InitRequest, err=%d", rv);
    }
  }
}

FailureDetector::FailureDetector(UpdateableSettings<GossipSettings> settings,
                                 ServerProcessor* processor,
                                 bool attach)
    : FailureDetector(std::move(settings), processor, nullptr, attach) {}

StatsHolder* FailureDetector::getStats() {
  return stats_;
}

void FailureDetector::startClusterStateTimer() {
  auto cs = Worker::getClusterState();
  if (!cs) {
    ld_info("Invalid get-cluster-state");
    buildInitialState();
  } else {
    cs_timer_.assign([=] {
      if (waiting_for_cluster_state_) {
        ld_info("Timed out waiting for cluster state reply.");
        buildInitialState();
      }
    });
    cs_timer_.activate(settings_->gcs_wait_duration);

    ld_info("Sending GET_CLUSTER_STATE to build initial FD cluster view");
    sendGetClusterState();
  }
}

void FailureDetector::sendGetClusterState() {
  Worker* w = Worker::onThisThread();
  auto settings = w->processor_->settings();

  auto cb = [&](Status status,
                const std::vector<uint8_t>& cs_update,
                std::vector<node_index_t> boycotted_nodes) {
    if (status != E::OK) {
      ld_error(
          "Unable to refresh cluster state: %s", error_description(status));
      return;
    }

    std::vector<std::string> dead;
    for (int i = 0; i < cs_update.size(); i++) {
      if (cs_update[i]) {
        dead.push_back("N" + std::to_string(i));
      }
    }

    std::vector<std::string> boycotted_tostring;
    boycotted_tostring.reserve(boycotted_nodes.size());
    for (auto index : boycotted_nodes) {
      boycotted_tostring.emplace_back("N" + std::to_string(index));
    }

    ld_info("Cluster state received with %lu dead nodes (%s) and %lu boycotted "
            "nodes (%s)",
            dead.size(),
            folly::join(',', dead).c_str(),
            boycotted_tostring.size(),
            folly::join(',', boycotted_tostring).c_str());

    buildInitialState(cs_update, std::move(boycotted_nodes));
  };

  std::unique_ptr<Request> req = std::make_unique<GetClusterStateRequest>(
      settings->get_cluster_state_timeout,
      settings->get_cluster_state_wave_timeout,
      std::move(cb));
  auto result = req->execute();
  if (result == Request::Execution::CONTINUE) {
    req.release();
  }
}

void FailureDetector::buildInitialState(
    const std::vector<uint8_t>& cs_update,
    std::vector<node_index_t> boycotted_nodes) {
  if (waiting_for_cluster_state_ == false) {
    return;
  }

  cs_timer_.cancel();
  waiting_for_cluster_state_ = false;
  ld_info("Wait over%s", cs_update.size() ? " (cluster state received)" : "");

  if (cs_update.size()) {
    const auto& nodes_configuration = getNodesConfiguration();
    node_index_t my_idx = getMyNodeID().index();

    // Set the correct state of nodes instead of DEAD
    FailureDetector::NodeState state;

    auto cs = getClusterState();
    for (size_t i = 0; i <= nodes_configuration->getMaxNodeIndex(); ++i) {
      if (i == my_idx)
        continue;

      cs->setNodeState(i, static_cast<ClusterState::NodeState>(cs_update[i]));
      state = cs->isNodeAlive(i) ? NodeState::ALIVE : NodeState::DEAD;
      RATELIMIT_INFO(std::chrono::seconds(1),
                     10,
                     "N%zu transitioned to %s",
                     i,
                     (state == NodeState::ALIVE) ? "ALIVE" : "DEAD");
      nodes_[i].state = state;
    }

    cs->setBoycottedNodes(std::move(boycotted_nodes));
  }

  if (!isolation_checker_) {
    ld_info("Initializing DomainIsolationChecker");
    isolation_checker_ = std::make_unique<DomainIsolationChecker>();
    isolation_checker_->init();
  }

  // Tell others that this node is alive, so that they can
  // start sending gossips.
  broadcastBringup();

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
  if (skew >= std::min(std::chrono::milliseconds(1000), threshold).count()) {
    STAT_INCR(getStats(), gossips_delayed_total);
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "A delayed gossip received from %s, delay:%lums"
                      ", sender time:%lu, now:%lu",
                      msg.gossip_node_.toString().c_str(),
                      skew,
                      msg.sent_time_.count(),
                      millis_now.count());
  }

  bool drop_gossip = (skew >= threshold.count());

  if (drop_gossip) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Dropping delayed gossip received from %s, delay:%lums"
                      ", sender time:%lu, now:%lu",
                      msg.gossip_node_.toString().c_str(),
                      skew,
                      msg.sent_time_.count(),
                      millis_now.count());
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
  if (broadcasted_i_am_starting && isLogsConfigLoaded()) {
    broadcastBringup();
  }

  if (isolation_checker_) {
    isolation_checker_->noteConfigurationChanged();
  }
}

void FailureDetector::gossip() {
  const auto& nodes_configuration = getNodesConfiguration();
  size_t size = nodes_configuration->getMaxNodeIndex() + 1;

  std::lock_guard<std::mutex> lock(mutex_);

  NodeID dest = selector_->getNode(this);

  if (shouldDumpState()) {
    ld_info("FD state before constructing gossip message for %s",
            dest.isNodeID() ? dest.toString().c_str() : "none");
    dumpFDState();
  }

  NodeID this_node = getMyNodeID();
  auto now = SteadyTimestamp::now();
  // bump other nodes' entry in gossip list if at least 1 gossip_interval passed
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

  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  // stayin' alive
  Node& node(nodes_[this_node.index()]);
  node.gossip_ = 0;
  node.gossip_ts_ = instance_id_;
  node.failover_ =
      failover_.load() ? instance_id_ : std::chrono::milliseconds::zero();
  node.is_node_starting_ = !isLogsConfigLoaded();
  // Don't trigger other nodes' state transition until we receive min number
  // of gossips. The GCS reply is not same as a regular gossip, and therefore
  // doesn't contain gossip_list_ values. The default values of gossip_list_
  // mean that this node has never heard from other cluster nodes, which
  // translates to DEAD state.
  // It is possible that a GCS reply can move a node to ALIVE
  // and immediately after that detectFailures() will detect the node as DEAD
  // based on gossip_list[], which will again move the node into
  // DEAD->SUSPECT->ALIVE state machine.
  if (num_gossips_received_ >= settings_->min_gossips_for_stable_state) {
    detectFailures(this_node.index(), size);
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
    updateNodeState(
        this_node.index(), false /*dead*/, true /*self*/, false /*failover*/);
  }

  updateBoycottedNodes();

  if (!dest.isNodeID()) {
    RATELIMIT_WARNING(std::chrono::minutes(1),
                      1,
                      "Unable to find a node to send a gossip message to");
    return;
  }

  GOSSIP_Message::GOSSIP_flags_t flags = 0;

  // If at least one entry in the failover list is non-zero, include the
  // list in the message.
  // In GOSSIP protocol >= HASHMAP_SUPPORT_IN_GOSSIP, this wouldn't be necessary
  // because failover list is sent through a list of GOSSIP_Node.
  for (auto& it : nodes_) {
    if (it.second.failover_ > std::chrono::milliseconds::zero()) {
      flags |= GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
      break;
    }
  }

  // In GOSSIP protocol >= HASHMAP_SUPPORT_IN_GOSSIP, this wouldn't be necessary
  // because starting list is sent through a list of GOSSIP_Node.
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

  GOSSIP_Message::node_list_t node_list;
  for (auto& it : nodes_) {
    GOSSIP_Node gnode;
    gnode.node_id_ = it.first;
    gnode.gossip_ = it.second.gossip_;
    gnode.gossip_ts_ = it.second.gossip_ts_;
    gnode.failover_ = it.second.failover_;
    gnode.is_node_starting_ = it.second.is_node_starting_;
    node_list.push_back(gnode);
  }

  // bump the message sequence number
  ++current_msg_id_;
  int rv = sendGossipMessage(
      dest,
      std::make_unique<GOSSIP_Message>(this_node,
                                       std::move(node_list),
                                       instance_id_,
                                       getCurrentTimeInMillis(),
                                       std::move(boycotts),
                                       std::move(boycott_durations),
                                       flags,
                                       current_msg_id_));

  if (rv != 0) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      10,
                      "Failed to send GOSSIP to node %s: %s",
                      Sender::describeConnection(Address(dest)).c_str(),
                      error_description(err));
  }

  if (shouldDumpState()) {
    ld_info("FD state after constructing gossip message for %s",
            dest.toString().c_str());
    dumpFDState();
  }
}

namespace {
template <typename T>
bool update_min(T& x, const T val) {
  if (val <= x) {
    x = val;
    return true;
  }
  return false;
}
} // namespace

bool FailureDetector::processFlags(const GOSSIP_Message& msg) {
  // This is called after locking nodes_mutex_ read lock

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
    dumpFDState();
  }

  node_index_t my_idx = getMyNodeID().index();
  node_index_t sender_idx = msg.gossip_node_.index();
  ld_check(my_idx != sender_idx);

  if (nodes_.find(sender_idx) == nodes_.end()) {
    // This does not occur because this is checked before calling the function
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    1,
                    "Sender (%s) is not present in our config",
                    msg.gossip_node_.toString().c_str());
    return true;
  }

  if (!isValidInstanceId(msg.instance_id_, sender_idx)) {
    return true;
  }

  // marking sender alive
  Node& sender_node(nodes_[sender_idx]);
  sender_node.gossip_ = 0;
  sender_node.gossip_ts_ = msg.instance_id_;
  sender_node.failover_ = std::chrono::milliseconds::zero();
  bool is_starting = sender_node.is_node_starting_ = !is_start_state_finished;

  if (is_suspect_state_finished) {
    if (sender_node.state != NodeState::ALIVE) {
      updateDependencies(sender_idx,
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
    updateNodeState(sender_idx, false, false, false);
  } else {
    ld_check(true);
    return true;
  }

  if (shouldDumpState()) {
    ld_info("FD state after receiving %s message from %s",
            msg_type.c_str(),
            msg.gossip_node_.toString().c_str());
    dumpFDState();
  }

  return true;
}

std::string
FailureDetector::flagsToString(GOSSIP_Message::GOSSIP_flags_t flags) {
  if (flags & GOSSIP_Message::SUSPECT_STATE_FINISHED) {
    return "suspect-state-finished";
  } else if (flags & GOSSIP_Message::NODE_BRINGUP_FLAG) {
    return "bringup";
  } else if (flags & GOSSIP_Message::STARTING_STATE_FINISHED) {
    return "starting_state_finished";
  }

  return "unknown";
}

void FailureDetector::onGossipReceived(const GOSSIP_Message& msg) {
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
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  if (nodes_.find(sender_idx) == nodes_.end()) {
    read_lock.unlock();
    folly::SharedMutex::WriteHolder write_lock(nodes_mutex_);
    if (nodes_.find(sender_idx) == nodes_.end()) {
      Node& new_node = nodes_[sender_idx];
      new_node.last_suspected_at_ = initial_time_ms_;
    }
    read_lock = folly::SharedMutex::ReadHolder(std::move(write_lock));
  }

  if (nodes_[sender_idx].gossip_ts_ > msg.instance_id_) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "Possible time-skew detected on %s, received a lower "
                      "instance id(%lu) from sender than already known(%lu)",
                      msg.gossip_node_.toString().c_str(),
                      msg.instance_id_.count(),
                      nodes_[sender_idx].gossip_ts_.count());
    STAT_INCR(getStats(), gossips_rejected_instance_id);
    return;
  }

  if (processFlags(msg)) {
    return;
  }

  // Merge the contents of gossip list with those from the message
  // by taking the minimum.
  const bool has_failover_list =
      msg.flags_ & GOSSIP_Message::HAS_FAILOVER_LIST_FLAG;
  folly::small_vector<size_t, 64> to_update;

  const bool has_starting_list =
      msg.flags_ & GOSSIP_Message::HAS_STARTING_LIST_FLAG;

  for (auto node : msg.node_list_) {
    size_t id = node.node_id_;

    if (nodes_.find(id) == nodes_.end()) {
      read_lock.unlock();
      folly::SharedMutex::WriteHolder write_lock(nodes_mutex_);
      if (nodes_.find(id) == nodes_.end()) {
        Node& new_node = nodes_[sender_idx];
        new_node.last_suspected_at_ = initial_time_ms_;
      }
      read_lock = folly::SharedMutex::ReadHolder(std::move(write_lock));
    }

    // Don't modify this node's state based on gossip message.
    if (id == this_index) {
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

    // If the incoming Gossip message knows about an older instance of the
    // process running on Node Ni, then ignore this update.
    if (nodes_[id].gossip_ts_ > node.gossip_ts_) {
      ld_spew("Received a stale instance id from %s,"
              " for N%zu, our:%lu, received:%lu",
              msg.gossip_node_.toString().c_str(),
              id,
              nodes_[id].gossip_ts_.count(),
              node.gossip_ts_.count());
      continue;
    } else if (nodes_[id].gossip_ts_ < node.gossip_ts_) {
      // If the incoming Gossip message knows about a valid
      // newer instance of Node Ni, then copy everything
      if (isValidInstanceId(node.gossip_ts_, id)) {
        nodes_[id].gossip_ = node.gossip_;
        nodes_[id].gossip_ts_ = node.gossip_ts_;
        nodes_[id].failover_ = has_failover_list
            ? node.failover_
            : std::chrono::milliseconds::zero();
        if (has_starting_list) {
          // TODO: figure out what to do for compat
          nodes_[id].is_node_starting_ = node.is_node_starting_;
        }
        to_update.push_back(id);
      }
      continue;
    }

    if (update_min(nodes_[id].gossip_, node.gossip_) ||
        id == msg.gossip_node_.index()) {
      to_update.push_back(id);
    }
    if (has_starting_list) {
      nodes_[id].is_node_starting_ =
          nodes_[id].is_node_starting_ && node.is_node_starting_;
    }
    if (has_failover_list) {
      nodes_[id].failover_ = std::max(nodes_[id].failover_, node.failover_);
    }
  }

  getBoycottTracker().updateReportedBoycotts(msg.boycott_list_);
  getBoycottTracker().updateReportedBoycottDurations(
      msg.boycott_durations_list_, std::chrono::system_clock::now());

  num_gossips_received_++;
  if (num_gossips_received_ <= settings_->min_gossips_for_stable_state) {
    ld_debug("Received gossip#%zu", num_gossips_received_);
  }
}

void FailureDetector::startSuspectTimer() {
  ld_info("Starting suspect state timer");
  node_index_t my_idx = getMyNodeID().index();

  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  nodes_[my_idx].gossip_ = 0;
  updateNodeState(my_idx, false, true, false);

  suspect_timer_.assign([=] {
    ld_info("Suspect timeout expired");
    updateNodeState(my_idx, false, true, false);
  });

  suspect_timer_.activate(settings_->suspect_duration);
}

void FailureDetector::startGossiping() {
  ld_info("Start Gossiping.");

  Worker* w = Worker::onThisThread();
  gossip_timer_node_ = w->registerTimer(
      [this](ExponentialBackoffTimerNode* node) {
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

void FailureDetector::dumpFDState() {
  const auto& nodes_configuration = getNodesConfiguration();
  size_t n = nodes_configuration->getMaxNodeIndex() + 1;
  std::string status_str;

  for (size_t i = 0; i < n; ++i) {
    // if i doesn't exist, generation 1 will be used
    NodeID node_id = nodes_configuration->getNodeID(i);
    status_str +=
        node_id.toString() + "(" + getNodeStateString(nodes_[i].state) + "), ";
  }

  const dbg::Level level = isTracingOn() ? dbg::Level::INFO : dbg::Level::SPEW;
  ld_log(
      level, "Failure Detector status for all nodes: %s", status_str.c_str());
  // TODO: add dump of gossip_list_, gossip_ts_, and failover_list_
}

void FailureDetector::cancelTimers() {
  cs_timer_.cancel();
  suspect_timer_.cancel();
  gossip_timer_node_ = nullptr;
}

void FailureDetector::shutdown() {
  ld_info("Cancelling timers");
  cancelTimers();
}

void FailureDetector::dumpGossipMessage(const GOSSIP_Message& msg) {
  ld_info("Gossip List from %s [%s]",
          msg.gossip_node_.toString().c_str(),
          dumpGossipList(msg.gossip_list_).c_str());
  ld_info("Instance id list from %s [%s]",
          msg.gossip_node_.toString().c_str(),
          dumpInstanceList(msg.gossip_ts_).c_str());
  ld_info("Failover List from %s [%s]",
          msg.gossip_node_.toString().c_str(),
          dumpInstanceList(msg.failover_list_).c_str());
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

void FailureDetector::detectFailures(node_index_t self, size_t n) {
  // This is called after locking nodes_mutex_ read lock

  const int threshold = settings_->gossip_failure_threshold;
  const auto& nodes_configuration = getNodesConfiguration();

  size_t dead_cnt = 0;
  size_t effective_dead_cnt = 0;
  size_t cluster_size = nodes_configuration->clusterSize();
  size_t effective_cluster_size = cluster_size;

  // Finally, update all the states
  for (auto& it : nodes_) {
    if (it.first == self) {
      // don't transition yourself to DEAD
      updateNodeState(self, false /*dead*/, true /*self*/, false /*failover*/);
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
      // if the node is actually dead, clear the failover boolean, to make sure
      // we don't mistakenly transition from DEAD to FAILING_OVER in the
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

    updateNodeState(it.first, dead, false, failover);

    // re-check node's state as it may be suspect, in which case it is still
    // considered dead
    dead = (it.second.state != NodeState::ALIVE);

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
        if (!node_disabled) {
          // only active nodes matter for isolation. see comment below.
          ++effective_dead_cnt;
        }
      }
    }
  }

  effective_dead_cnt_ = effective_dead_cnt;
  effective_cluster_size_ = effective_cluster_size;
  STAT_SET(getStats(), num_nodes, cluster_size);
  STAT_SET(getStats(), num_dead_nodes, dead_cnt);
  STAT_SET(getStats(), effective_num_nodes, effective_cluster_size);
  STAT_SET(getStats(), effective_dead_nodes, effective_dead_cnt);

  // Check whether more than half of the nodes are dead. This may mean that
  // there is a network partitioning and we are in a minority. We record this
  // fact in the isolated_ boolean for sequencers to take action.
  // For the purpose of this check, we consider only the effective numbers. We
  // ignore nodes that are disabled, meaning that they do not participate in
  // cluster activities. This allows the cluster to keep functioning with a
  // subset of nodes, when the others are disabled. The reasoning is: if a node
  // dies while being disabled, it shouldn't affect the cluster, and so
  // shouldn't trigger isolation mode.
  isolated_.store(2 * effective_dead_cnt > effective_cluster_size);

  if (isolation_checker_ != nullptr) {
    isolation_checker_->processIsolationUpdates();
  }
}

void FailureDetector::updateDependencies(node_index_t idx,
                                         FailureDetector::NodeState new_state,
                                         bool failover,
                                         bool starting) {
  // This is called after locking nodes_mutex_ read lock
  if (nodes_.find(idx) == nodes_.end()) {
    return;
  }
  nodes_[idx].state = new_state;
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
                                      bool dead,
                                      bool self,
                                      bool failover) {
  // This is called after locking nodes_mutex_ read lock
  if (nodes_.find(idx) == nodes_.end()) {
    return;
  }

  bool starting;
  if (self) {
    starting = !isLogsConfigLoaded();
  } else {
    starting = nodes_[idx].is_node_starting_;
  }

  NodeState current = nodes_[idx].state, next = NodeState::DEAD;
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
          nodes_[idx].last_suspected_at_.store(current_time_ms);
        } else {
          next = NodeState::ALIVE;
        }
        break;
      case SUSPECT:
        ld_check(suspect_duration.count() > 0);
        if (current_time_ms >
            nodes_[idx].last_suspected_at_.load() + suspect_duration) {
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
      Socket* socket = getServerSocket(idx);
      if (socket) {
        socket->resetConnectThrottle();
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

  updateDependencies(idx, next, failover, starting);
  if (self && current != next && next == NodeState::ALIVE) {
    broadcastSuspectDurationFinished();
  }
}

bool FailureDetector::isValidDestination(node_index_t node_idx) {
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  if (nodes_.find(node_idx) == nodes_.end()) {
    return false;
  }

  if (nodes_[node_idx].blacklisted) {
    // exclude blacklisted nodes
    return false;
  }

  Socket* socket = getServerSocket(node_idx);
  if (!socket) {
    // If a connection to the node doesn't exist yet, consider it as a valid
    // destination.
    return true;
  }

  int rv = socket->checkConnection(nullptr);
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

const char* FailureDetector::getNodeStateString(NodeState state) const {
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

  if (nodes_.find(idx) == nodes_.end()) {
    return "invalid node index";
  }

  char buf[1024];
  {
    snprintf(buf,
             sizeof(buf),
             "(gossip: %u, instance-id: %lu, failover: %lu, starting: %d, "
             "state: %s)",
             nodes_.at(idx).gossip_,
             nodes_.at(idx).gossip_ts_.count(),
             nodes_.at(idx).failover_.count(),
             (int)nodes_.at(idx).is_node_starting_,
             getNodeStateString(nodes_.at(idx).state));
  }
  return std::string(buf);
}

folly::dynamic FailureDetector::getStateJson(node_index_t idx) const {
  folly::dynamic obj = folly::dynamic::object;
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);

  if (nodes_.find(idx) == nodes_.end()) {
    obj["error"] = "invalid node index";
    return obj;
  }

  {
    obj["gossip"] = nodes_.at(idx).gossip_;
    obj["instance-id"] = nodes_.at(idx).gossip_ts_.count();
    obj["failover"] = nodes_.at(idx).failover_.count();
    obj["starting"] = (int)nodes_.at(idx).is_node_starting_;
    obj["state"] = getNodeStateString(nodes_.at(idx).state);
  }
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

void FailureDetector::broadcastWrapper(GOSSIP_Message::GOSSIP_flags_t flags) {
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
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  nodes_[my_idx].gossip_ = 0;
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

void FailureDetector::updateBoycottedNodes() {
  getClusterState()->setBoycottedNodes(
      getBoycottTracker().getBoycottedNodes(SystemTimestamp::now()));
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

Socket* FailureDetector::getServerSocket(node_index_t idx) {
  return Worker::onThisThread()->sender().findServerSocket(idx);
}

void FailureDetector::setBlacklisted(node_index_t idx, bool blacklisted) {
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_.find(idx) != nodes_.end()) {
    nodes_[idx].blacklisted = blacklisted;
  }
}

bool FailureDetector::isBlacklisted(node_index_t idx) const {
  bool blacklisted = false;
  std::lock_guard<std::mutex> lock(mutex_);
  folly::SharedMutex::ReadHolder read_lock(nodes_mutex_);
  if (nodes_.find(idx) != nodes_.end()) {
    blacklisted = nodes_.at(idx).blacklisted;
  }
  return blacklisted;
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

bool FailureDetector::isLogsConfigLoaded() {
  if (!Worker::onThisThread(false)) {
    // we are here because we are in a test and FailureDetector is being
    // constructed.
    return true;
  }
  return processor_->isLogsConfigLoaded();
}

}} // namespace facebook::logdevice
