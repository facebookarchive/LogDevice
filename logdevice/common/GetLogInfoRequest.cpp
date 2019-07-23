/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/GetLogInfoRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"

namespace facebook { namespace logdevice {

Request::Execution GetLogInfoRequest::execute() {
  // Everything up to start() runs in the context of a Worker().  Unit tests
  // call start() directly.

  createTimers();
  auto insert_result =
      Worker::onThisThread()->runningGetLogInfo().gli_map.insert(
          std::make_pair(id_, std::unique_ptr<GetLogInfoRequest>(this)));
  ld_check(insert_result.second);

  Worker::onThisThread()->activateClusterStatePolling();

  start();
  return Execution::CONTINUE;
}

void GetLogInfoRequest::start() {
  this->prepareAndPostPerNodeRequest();
}

void GetLogInfoRequest::attemptTargetNodeChange() {
  ld_check(!isRetryTimerActive());
  if (shared_state_->considerReloadingConfig(last_used_shared_state_version_)) {
    if (reloadConfig() != 0) {
      ld_error("Couldn't reload config");
    }
  }
  prepareAndPostPerNodeRequest();
}

void GetLogInfoRequest::changeTargetNode(std::unique_lock<std::mutex>& lock) {
  ld_check(lock.owns_lock() && lock.mutex() == &shared_state_->mutex_);

  // Generating new node ID - selecting a random node
  const auto& nodes_configuration = getNodesConfiguration();
  NodeID exclude = shared_state_->node_id_;

  const auto* worker = Worker::onThisThread(false);
  ClusterState* cluster_state = nullptr;
  if (worker != nullptr) {
    cluster_state = worker->getClusterState();
  }
  const auto new_node = RandomNodeSelector::getAliveNode(
      *nodes_configuration, cluster_state, exclude);
  shared_state_->node_id_ = new_node;
  shared_state_->socket_callback_->deactivate();
  ld_info("Changing GetLogInfoRequest target node to %s",
          new_node.toString().c_str());
  shared_state_->change_node_id_ = false;
}

void GetLogInfoRequest::postPerNodeRequest() {
  std::unique_ptr<Request> req =
      std::make_unique<GetLogInfoFromNodeRequest>(this);
  int rv = Worker::onThisThread()->processor_->postWithRetrying(req);
  if (rv == -1) {
    // This only happens when we shut down
    deleteThis();
  }
}

void GetLogInfoRequest::prepareAndPostPerNodeRequest() {
  ld_check(!isRetryTimerActive());
  std::unique_lock<std::mutex> lock(shared_state_->mutex_);

  if (shared_state_->change_node_id_) {
    changeTargetNode(lock);
    ld_check(shared_state_->change_node_id_ == false);
  }

  if (last_used_shared_state_version_ != shared_state_->current_version_) {
    // We're trying a new node, reset timer for changing a node
    activateTargetNodeChangeTimer();
  }

  // Saving the version of the shared_state that we used
  last_used_shared_state_version_ = shared_state_->current_version_;

  lock.unlock();

  postPerNodeRequest();
}

// This request forwards the result to the target worker, so that the result
// callback is called on the worker thread that initiated the request.
class LogInfoForwardedResultRequest : public Request {
 public:
  LogInfoForwardedResultRequest(worker_id_t worker_id,
                                get_log_info_callback_t callback,
                                Status status,
                                std::string json)
      : Request(RequestType::LOG_INFO_FORWARDED_RESULT),
        worker_id_(worker_id),
        callback_(callback),
        status_(status),
        json_(json) {}

  // returns thread affinity
  int getThreadAffinity(int /*nthreads*/) override {
    return worker_id_.val_;
  }

  Execution execute() override {
    callback_(status_, json_);
    return Execution::COMPLETE;
  }

 private:
  const worker_id_t worker_id_;
  const get_log_info_callback_t callback_;
  const Status status_;
  const std::string json_;
};

void GetLogInfoRequest::finalize(Status status, bool delete_this) {
  ld_check(!finalize_called_);
  finalize_called_ = true;

  if (callback_worker_id_.val_ != -1) {
    // Posting the request to call the callback on the correct thread
    std::unique_ptr<Request> req =
        std::make_unique<LogInfoForwardedResultRequest>(
            callback_worker_id_,
            callback_,
            std::move(status),
            std::move(result_json_));
    int rv = Worker::onThisThread()->processor_->postWithRetrying(req);
    if (rv != 0) {
      // This drops the notification on the floor
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      1,
                      "Couldn't post request, error %d (%s)",
                      int(err),
                      error_description(err));
    }
  } else {
    callback_(status, result_json_);
  }

  if (delete_this) {
    deleteThis();
  }
}

GetLogInfoRequest::~GetLogInfoRequest() {
  const Worker* worker = Worker::onThisThread(false /* enforce_worker */);
  if (!worker) {
    // The request has not made it to a Worker. Do not call the callback.
    return;
  }

  if (!finalize_called_) {
    // This can happen if the request or client gets torn down while the
    // request is still processing
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "GetLogInfoRequest destroyed while still processing");
    finalize(E::SHUTDOWN, false);
  }
}

void GetLogInfoRequest::onReplyFromNode(Status status,
                                        std::string json,
                                        uint64_t version) {
  switch (status) {
    case E::OK:
      result_json_ = json;
      if (shared_state_->useReplyFromVersion(version)) {
        finalize(E::OK);
      } else if (!isRetryTimerActive()) {
        prepareAndPostPerNodeRequest();
      }
      break;

    case E::STALE:
      // the shared state version has changed, retry with new state immediately
      // unless the retry timer is active already
      if (!isRetryTimerActive()) {
        prepareAndPostPerNodeRequest();
      }
      break;

    case E::NOTFOUND:
      finalize(E::NOTFOUND);
      break;

    case E::FAILED:
    case E::NOTREADY:
      // retrying with a different node after some time
      if (!isRetryTimerActive()) {
        activateRetryTimer();
      }
      break;

    default:
      ld_error("received reply with unexpected status %s",
               error_description(status));
      break;
  }
}

void GetLogInfoRequest::onClientTimeout() {
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  2,
                  "timed out (%ld ms) waiting to hear from %s",
                  client_timeout_.count(),
                  shared_state_->node_id_.toString().c_str());
  // retrying with a random node
  attemptTargetNodeChange();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
GetLogInfoRequest::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

int GetLogInfoRequest::reloadConfig() {
  return Worker::onThisThread()
      ->getUpdateableConfig()
      ->updateableLogsConfig()
      ->invalidate();
}

void GetLogInfoRequest::createTimers() {
  target_change_timer_ =
      std::make_unique<Timer>([this] { this->onClientTimeout(); });
  retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

      [this]() { attemptTargetNodeChange(); },
      Worker::settings().on_demand_logs_config_retry_delay);
}

void GetLogInfoRequest::activateTargetNodeChangeTimer() {
  ld_check(!isRetryTimerActive());
  target_change_timer_->activate(client_timeout_);
}

bool GetLogInfoRequest::isRetryTimerActive() {
  return retry_timer_->isActive();
}

void GetLogInfoRequest::activateRetryTimer() {
  target_change_timer_->cancel();
  retry_timer_->activate();
}

void GetLogInfoRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningGetLogInfo().gli_map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

}} // namespace facebook::logdevice
