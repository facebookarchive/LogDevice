/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetClusterStateRequest.h"

#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/protocol/GET_CLUSTER_STATE_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

void GetClusterStateRequest::attachToWorker() {
  // attach to current worker
  auto insert_result =
      Worker::onThisThread()->runningGetClusterState().map.insert(
          std::make_pair(id_, std::unique_ptr<GetClusterStateRequest>(this)));
  ld_check(insert_result.second);
}

void GetClusterStateRequest::destroyRequest() {
  // delete this
  auto& rqmap = Worker::onThisThread()->runningGetClusterState().map;
  auto it = rqmap.find(id_);
  ld_check(it != rqmap.end());
  rqmap.erase(it); // destroys unique_ptr which owns this
}

Request::Execution GetClusterStateRequest::execute() {
  ld_spew("Executing cluster state refresh");
  WORKER_STAT_INCR(client.get_cluster_state_started);

  attachToWorker();
  initTimers();
  initNodes();
  start();
  return Execution::CONTINUE;
}

bool GetClusterStateRequest::start() {
  ld_spew("Starting cluster state refresh");

  activateWaveTimer();
  const auto& nodes_configuration = getNodesConfiguration();

  // send messages to the first nodes up to wave_size_
  auto cur = next_node_pos_;
  // computes the end of the range of node IDs we want to iterate through.
  // we do not want to directly use next_node_pos_ in the loop,
  // because it may be modified by a recursive call, via onError callback.
  auto end = std::min(next_node_pos_ + wave_size_, nodes_.size());
  next_node_pos_ = end;
  for (; cur < end; cur++) {
    node_index_t node = nodes_[cur];
    if (nodes_configuration->isNodeInServiceDiscoveryConfig(node) &&
        sendTo(nodes_configuration->getNodeID(node))) {
      return true;
    }
    // Node may have been removed from config.
  }
  return false;
}

void GetClusterStateRequest::initNodes() {
  const auto& nodes_configuration = getNodesConfiguration();

  // the following settings is only used in tests to force selection of nodes
  // to be recipients of GET_CLUSTER_STATE messages
  std::vector<node_index_t> test_recipients =
      getSettings().test_get_cluster_state_recipients_;

  node_index_t my_idx;
  bool server_side = getSettings().server;
  if (server_side) {
    my_idx = getMyNodeID().index();
  }

  nodes_.clear();
  next_node_pos_ = 0;

  if (dest_.hasValue()) {
    // An explicit recipient was passed in constructor. Put it by itself in the
    // nodes list and let the request execute immediately.
    nodes_.push_back(dest_.value().index());
    ld_spew("Adding dest node %s", dest_.value().toString().c_str());
    return;
  }

  if (test_recipients.empty()) {
    for (const auto& kv : *nodes_configuration->getServiceDiscovery()) {
      ld_spew("Adding node N%d", kv.first);
      if (server_side && kv.first == my_idx) {
        // Not adding self as a possible destination for
        // sending GET-CLUSTER-STATE
        continue;
      }

      nodes_.push_back(kv.first);
    }
  } else {
    nodes_ = test_recipients;
  }

  auto cs = getClusterState();
  ld_check(cs);

  // randomize the list of nodes
  std::shuffle(nodes_.begin(), nodes_.end(), folly::ThreadLocalPRNG());
  // partition the list such that nodes marked alive appear first
  // (note: the relative (random) order is preserved)
  std::stable_partition(
      nodes_.begin(), nodes_.end(), [cs](node_index_t node_id) {
        return cs->isNodeAlive(node_id);
      });
}

void GetClusterStateRequest::initTimers() {
  timer_ = std::make_unique<Timer>([&] { onTimeout(); });
  // Immediately activate request timer
  timer_->activate(timeout_);

  wave_timer_ = std::make_unique<Timer>([&] { onWaveTimeout(); });
}

void GetClusterStateRequest::activateWaveTimer() {
  if (wave_timer_) {
    if (wave_timer_->isActive()) {
      wave_timer_->cancel();
    }
    wave_timer_->activate(wave_timeout_);
  }
}

bool GetClusterStateRequest::done(Status status,
                                  std::vector<uint8_t> nodes_state,
                                  std::vector<node_index_t> boycotted_nodes) {
  ld_debug("Done getting cluster state from %s with status %s",
           dest_.hasValue() ? dest_.value().toString().c_str() : "<unknown>",
           error_description(status));
  if (timer_) {
    timer_->cancel();
  }
  if (wave_timer_) {
    wave_timer_->cancel();
  }

  ld_check(!callback_called_);
  callback_called_ = true;
  callback_(status, std::move(nodes_state), std::move(boycotted_nodes));

  destroyRequest();
  return true;
}

NodeID GetClusterStateRequest::getMyNodeID() const {
  return Worker::onThisThread()->processor_->getMyNodeID();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetClusterStateRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

ClusterState* GetClusterStateRequest::getClusterState() const {
  return Worker::getClusterState();
}

bool GetClusterStateRequest::sendTo(NodeID to) {
  ld_debug("Sending GET_CLUSTER_STATE to Node %s",
           Sender::describeConnection(to).c_str());

  GET_CLUSTER_STATE_Header header = {id_};
  auto msg = std::make_unique<GET_CLUSTER_STATE_Message>(header);
  int rv = Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
  if (rv != 0) {
    RATELIMIT_LEVEL(dest_.hasValue() ? facebook::logdevice::dbg::Level::SPEW
                                     : facebook::logdevice::dbg::Level::ERROR,
                    std::chrono::seconds(1),
                    5,
                    "Failed to queue a GET_CLUSTER_STATE message "
                    "for sending to %s: %s",
                    Sender::describeConnection(to).c_str(),
                    error_description(err));
    return onError(err);
  }
  return false;
}

bool GetClusterStateRequest::onError(Status status) {
  // increase the error count
  ++errors_;
  WORKER_STAT_INCR(client.get_cluster_state_errors);

  switch (status) {
    case E::PROTONOSUPPORT:
    case E::NOTSUPPORTED:
      // special case for PROTONOSUPPORT and NOTSUPPORTED
      // we don't want to send a storm of messages that will end up
      // being rejected, se we give up right away when we get such
      // errors, considered unrecoverable.
      // Note that the client may explicitly retry later, so if the servers
      // restarted and the problem went away, we will eventually pick it up.
      RATELIMIT_ERROR(std::chrono::seconds(1),
                      1,
                      "Retrieving the state of the cluster failed due to "
                      "feature not being supported on one server. giving up.");
      return done(status, std::vector<uint8_t>(), std::vector<node_index_t>());
      break;
    default: {
      // trigger the next wave if all the nodes
      // replied with error
      if (errors_ == next_node_pos_) {
        // if we exhausted all nodes of the cluster, we give up
        if (next_node_pos_ == nodes_.size()) {
          RATELIMIT_LEVEL(dest_.hasValue()
                              ? facebook::logdevice::dbg::Level::SPEW
                              : facebook::logdevice::dbg::Level::ERROR,
                          std::chrono::seconds(1),
                          10,
                          "Retrieving the state of the cluster failed. "
                          "giving up.");
          return done(
              E::FAILED, std::vector<uint8_t>(), std::vector<node_index_t>());
        } else {
          RATELIMIT_INFO(std::chrono::seconds(1),
                         10,
                         "Retrieving the state of the cluster failed. "
                         "sending another wave.");
          wave_size_ *= kWaveScaleFactor;
          // In that situation, we are starting a new wave. start() may return
          // true to let the caller know that it should stop and this object
          // may get destroyed in the new wave.
          return start();
        }
      }
    }
  }

  return false;
}

bool GetClusterStateRequest::onReply(
    const Address& from,
    Status status,
    std::vector<uint8_t> nodes_state,
    std::vector<node_index_t> boycotted_nodes) {
  if (status != E::OK) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      5,
                      "Could not retrieve the state of the cluster from %s: %s",
                      Sender::describeConnection(from).c_str(),
                      error_description(status));
    return onError(status);
  }

  ld_debug("Received GET_CLUSTER_STATE_REPLY message from Node %s",
           Sender::describeConnection(from).c_str());

  // finish and destroy request
  return done(E::OK, std::move(nodes_state), std::move(boycotted_nodes));
}

bool GetClusterStateRequest::onWaveTimeout() {
  // retry if possible
  WORKER_STAT_INCR(client.get_cluster_state_wave_timeout);
  if (next_node_pos_ >= nodes_.size()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "All attempts to retrieve the cluster state from nodes "
                    "of the cluster failed.");
    // the request has been sent to every node in the cluster and we still
    // time out... giving up.
    WORKER_STAT_INCR(client.get_cluster_state_failed);
    return done(
        E::TIMEDOUT, std::vector<uint8_t>(), std::vector<node_index_t>());
  } else {
    // send another wave.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Cluster state refresh timed out. sending another wave.");

    // calculate number of recipients for this wave
    // this state machine sends requests by wave, increasing the size of the
    // wave by a factor at every round if there is a timeout, until the message
    // has been sent to the whole cluster.
    wave_size_ *= kWaveScaleFactor;
    return start();
  }
  return false;
}

bool GetClusterStateRequest::onTimeout() {
  RATELIMIT_LEVEL(dest_.hasValue() ? facebook::logdevice::dbg::Level::SPEW
                                   : facebook::logdevice::dbg::Level::ERROR,
                  std::chrono::seconds(1),
                  10,
                  "Retrieving the state of the cluster timed out.");
  WORKER_STAT_INCR(client.get_cluster_state_timeout);
  WORKER_STAT_INCR(client.get_cluster_state_failed);
  return done(E::TIMEDOUT, std::vector<uint8_t>(), std::vector<node_index_t>());
}

const Settings& GetClusterStateRequest::getSettings() const {
  return Worker::settings();
}

}} // namespace facebook::logdevice
