/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/LogsConfigApiRequest.h"

#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/hash.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

constexpr size_t LogsConfigApiRequest::MAX_ERRORS;

Request::Execution LogsConfigApiRequest::execute() {
  ld_debug("Executing LogManagement request");
  WORKER_STAT_INCR(client.logsconfig_api_request_started);

  auto insert_result =
      Worker::onThisThread()->runningLogsConfigApiRequests().map.insert(
          std::make_pair(id_, std::unique_ptr<LogsConfigApiRequest>(this)));
  ld_check(insert_result.second);
  start();
  return Execution::CONTINUE;
}

void LogsConfigApiRequest::start() {
  ld_debug("Starting LogsConfigApiRequest");
  // Configuring timers
  timeout_timer_ = std::make_unique<Timer>(

      std::bind(&LogsConfigApiRequest::onTimeout, this));

  retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

      std::bind(&LogsConfigApiRequest::onRetry, this),
      std::chrono::milliseconds(250),
      timeout_);

  // Sends the request.
  onRetry();
}

void LogsConfigApiRequest::onRetry() {
  if (retry_timer_->isActive()) {
    retry_timer_->cancel();
  }
  NodeID target_node = pickNode();
  if (!target_node.isNodeID()) {
    // We cannot find any nodes anymore
    ld_error("Cannot find any available nodes to connect to, giving up!");
    onError(E::FAILED, "Cannot find any available nodes to connect to");
    return;
  }
  activateTimeoutTimer();
  sendRequestTo(target_node);
}

NodeID LogsConfigApiRequest::pickNode() {
  if (last_selected_node_.isNodeID()) {
    // return cached node
    return last_selected_node_;
  }

  const auto& nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  const auto& sd_config = nodes_configuration->getServiceDiscovery();

  ClusterState* cs = Worker::getClusterState();
  ld_check(cs);

  std::vector<NodeID> nodes;
  std::vector<double> weights;

  // considering only the nodes the failure detector thinks are alive and
  // exclude nodes that we have blacklisted.
  for (const auto& kv : *sd_config) {
    const node_index_t nid = kv.first;
    NodeID node_id(nid, nodes_configuration->getNodeGeneration(nid));
    if (blacklisted_nodes_.find(node_id) == blacklisted_nodes_.end()) {
      nodes.push_back(node_id);
      weights.push_back(cs->isNodeAlive(nid) ? 1 : 0);
    }
  }

  if (nodes.size() == 0) {
    // all nodes are filtered out, they are either not alive or blacklisted!
    errors_ = max_errors_;
    return NodeID();
  }
  // Use weighted consistent hashing to pick a server to talk to based on a
  // client-generated-once seed..
  uint64_t idx = hashing::weighted_ch(host_selection_seed_, weights);
  ld_check(idx < nodes.size());
  last_selected_node_ = nodes[idx];
  return last_selected_node_;
}

bool LogsConfigApiRequest::blacklistSelectedNode() {
  if (last_selected_node_.isNodeID()) {
    blacklisted_nodes_.insert(last_selected_node_);
    last_selected_node_ = NodeID();
  }
  const auto nodes_configuration =
      Worker::onThisThread()->getNodesConfiguration();
  ld_check(nodes_configuration);
  return blacklisted_nodes_.size() < nodes_configuration->clusterSize();
}

void LogsConfigApiRequest::activateTimeoutTimer() {
  ld_check(timeout_timer_);
  timeout_timer_->activate(timeout_);
}

void LogsConfigApiRequest::activateRetryTimer() {
  ld_check(retry_timer_);
  if (retry_timer_->isActive()) {
    retry_timer_->cancel();
  }
  retry_timer_->activate();
}

void LogsConfigApiRequest::cancelTimeoutTimer() {
  if (timeout_timer_) {
    timeout_timer_->cancel();
  }
}

void LogsConfigApiRequest::cancelRetryTimer() {
  if (retry_timer_) {
    retry_timer_->cancel();
  }
}

void LogsConfigApiRequest::onTimeout() {
  RATELIMIT_ERROR(std::chrono::seconds(1),
                  10,
                  "Execution of LogsConfigApiRequest timed out.");
  WORKER_STAT_INCR(client.logsconfig_api_request_timeout);
  WORKER_STAT_INCR(client.logsconfig_api_request_failed);
  done(E::TIMEDOUT, 0, std::string());
}

namespace {
class LogsConfigSocketClosedCallback : public SocketCallback {
 public:
  explicit LogsConfigSocketClosedCallback(request_id_t rqid) : rqid_(rqid) {}

  void operator()(Status st, const Address& /* unused */) override {
    Worker* worker = Worker::onThisThread();
    ld_check(worker);
    if (worker->shuttingDown()) {
      // if we're shutting down, let's not do anything
      return;
    }
    auto& rqmap = Worker::onThisThread()->runningLogsConfigApiRequests().map;
    auto it = rqmap.find(rqid_);
    // Whenever the request gets destroyed, it should destroy the callback.
    // Thus, if the callback fires, the request should be alive as well
    ld_check(it != rqmap.end());
    it->second->onError(st);
  }

 private:
  request_id_t rqid_;
};
} // namespace

void LogsConfigApiRequest::sendRequestTo(NodeID target) {
  ld_debug(
      "Sending LOGS_CONFIG_API_REQUEST to Node %s", target.toString().c_str());
  LOGS_CONFIG_API_Header header = {id_, request_type_};
  auto msg = std::make_unique<LOGS_CONFIG_API_Message>(header, payload_);
  onclose_callback_ = std::make_unique<LogsConfigSocketClosedCallback>(id_);
  int rv = Worker::onThisThread()->sender().sendMessage(
      std::move(msg), target, onclose_callback_.get());
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    5,
                    "Failed to queue a LOGS_CONFIG_API_REQUEST message for "
                    "sending to %s: %s",
                    target.toString().c_str(),
                    error_description(err));
    onError(err);
  }
}

void LogsConfigApiRequest::onReply(NodeID from,
                                   Status status,
                                   uint64_t config_version,
                                   std::string payload,
                                   size_t total_payload_size) {
  if (status != E::OK) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        5,
        "Could not execute the LogManagement operation on node %s: %s",
        from.toString().c_str(),
        error_description(status));
    // The payload may contain the failure reason
    onError(status, payload);
    return;
  }
  ld_debug("Received a LOGS_CONFIG_API_REPLY message from Node %s",
           from.toString().c_str());

  // In easy scenario, don't append result if in one message
  if (payload.length() == total_payload_size) {
    done(E::OK, config_version, payload);
    return;
  }

  // Only the first message will contain the full payload length
  if (total_payload_size != 0) {
    ld_check(response_payload_.empty());
    ld_check(response_expected_size_ == 0);
    response_payload_.reserve(total_payload_size);
    response_expected_size_ = total_payload_size;
  }
  response_payload_.append(payload);
  if (response_payload_.length() >= response_expected_size_) {
    ld_debug("LOGS_CONFIG_API_REPLY received full message");
    done(E::OK, config_version, response_payload_);
  } else {
    ld_debug("LOGS_CONFIG_API_REPLY waiting for more data "
             "(recv: %zu, total: %zu)",
             response_payload_.length(),
             response_expected_size_);
  }
}

void LogsConfigApiRequest::done(Status status,
                                uint64_t config_version,
                                std::string response_payload) {
  // cancel all the timers.
  cancelRetryTimer();
  cancelTimeoutTimer();
  ld_check(!callback_called_);
  callback_called_ = true;
  callback_(status, config_version, response_payload);

  // delete this
  auto& rqmap = Worker::onThisThread()->runningLogsConfigApiRequests().map;
  auto it = rqmap.find(id_);
  ld_check(it != rqmap.end());
  rqmap.erase(it); // destroys unique_ptr which owns this
}

void LogsConfigApiRequest::onError(Status status, std::string failure_reason) {
  if (!last_selected_node_.isNodeID()) {
    // This can happen if the socket close callback caused this method to be
    // called but we already have blacklisted.
    return;
  }
  errors_++;
  WORKER_STAT_INCR(client.logsconfig_api_request_errors);
  if (errors_ >= max_errors_) {
    // we have exhausted all the maximum number of retries, failing.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "LogsConfigApiRequest failed, exhausted all possible "
                   "retries (%zu). Giving up!",
                   max_errors_);
    done(status, 0, failure_reason);
    return;
  }
  // if AGAIN, we need to retry later.
  if (status == E::AGAIN) {
    // retry, the state machine on the server is not ready. This happens in
    // particular if the state machine is still booting (replaying), during
    // that time we keep receiving E::AGAIN and need to keep retrying on the
    // same node.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   10,
                   "Server (%s) is not ready yet, state "
                   "machine is still initializing, "
                   "retrying.",
                   last_selected_node_.toString().c_str());
    activateRetryTimer();
    return;
  } else if (status == E::CONNFAILED || status == E::PEER_CLOSED ||
             status == E::PEER_UNAVAILABLE || status == E::TIMEDOUT ||
             status == E::NOBUFS || status == E::SHUTDOWN ||
             status == E::DISABLED) {
    // retry, but this time we pick a different node unless we are out of nodes
    // in the cluster or we reached the errors limit.
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "Connection failed/dropped to (%s) (%s), retrying another node.",
        last_selected_node_.toString().c_str(),
        error_name(status));
    Worker::getClusterState()->refreshClusterStateAsync();
    if (!blacklistSelectedNode()) {
      done(status, 0, "No more servers left to try!");
    } else {
      activateRetryTimer();
    }
    return;
  } else if (status == E::PROTONOSUPPORT || status == E::NOTSUPPORTED) {
    // we fail immediately in this case, we don't want a storm of messages that
    // will end up being rejected, so we give up right away when we get such
    // errors.
    // Note that the client may explicitly retry later, so if the servers
    // restarted and the problem went away, we will eventually pick it up.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "LogsConfigApiRequest failed, this feature is not "
                   "supported or disabled on the server");
  } else {
    // e.g, E::ACCESS et al.
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "LogsConfigApiRequest failed: %s",
                   error_description(status));
  }
  done(status, 0, failure_reason);
}
}} // namespace facebook::logdevice
