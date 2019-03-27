/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/Memory.h>
#include <folly/Random.h>

#include "logdevice/common/EventLoop.h"
#include "logdevice/common/GetLogInfoRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"

namespace facebook { namespace logdevice {

GetLogInfoFromNodeRequest::GetLogInfoFromNodeRequest(GetLogInfoRequest* parent)
    : Request(RequestType::GET_LOG_INFO),
      gli_req_id_(parent->id_),
      shared_state_(parent->shared_state_),
      worker_id_(shared_state_->worker_id_),
      response_expected_size_(0) {
  std::lock_guard<std::mutex> lock(shared_state_->mutex_);
  shared_state_version_ = shared_state_->current_version_;
}

Request::Execution GetLogInfoFromNodeRequest::execute() {
  // Everything up to start() runs in the context of a Worker().  Unit tests
  // call start() directly.

  createRetryTimer();

  auto insert_result =
      Worker::onThisThread()->runningGetLogInfo().per_node_map.insert(
          std::make_pair(
              id_, std::unique_ptr<GetLogInfoFromNodeRequest>(this)));
  ld_check(insert_result.second);

  start();
  return Execution::CONTINUE;
}

void GetLogInfoFromNodeRequest::start() {
  this->attemptSend();
}

void GetLogInfoFromNodeRequest::attemptSend() {
  std::unique_lock<std::mutex> lock(shared_state_->mutex_);

  if (!shared_state_->message_sending_enabled_) {
    // We are waiting for a new config. We are waiting for RemoteLogsConfig to
    // change this bool when it is loaded.
    activateRetryTimer();
    return;
  }

  if (shared_state_->change_node_id_ ||
      shared_state_version_ != shared_state_->current_version_) {
    // We can't be sending to the same node anymore
    lock.unlock();
    finalize(E::STALE);
    return;
  }

  // Copying the node id so we can release the lock
  NodeID send_to = shared_state_->node_id_;

  lock.unlock();

  ld_debug("Sending a LOGS_CONFIG_API_Message message to %s",
           send_to.toString().c_str());
  int rv = this->sendOneMessage(send_to);
  if (rv != 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    1,
                    "Failed to send LOGS_CONFIG_API_Message message to %s: %s",
                    send_to.toString().c_str(),
                    error_description(err));
    finalize(E::FAILED);
  }
}

void GetLogInfoFromNodeRequest::onMessageSent(NodeID to, Status status) {
  if (status != E::OK) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "Couldn't send LOGS_CONFIG_API_Message message to %s: %s",
                   to.toString().c_str(),
                   error_description(status));
    finalize(E::FAILED);
    return;
  }
}

int GetLogInfoFromNodeRequest::sendOneMessage(NodeID to) {
  GetLogInfoRequest* parent = findParent();
  if (!parent) {
    // parent is dead, no point in continuing
    deleteThis();
    // pretending everything was fine not to trigger any additional processing
    // after self-destruction
    return 0;
  }

  LOGS_CONFIG_API_Header header = {
      .client_rqid = id_,
      .request_type = parent->request_type_,
      .subscribe_to_config_ = true, /* Subscribes to CONFIG_CHANGED */
      .origin = LogsConfigRequestOrigin::REMOTE_LOGS_CONFIG_REQUEST};
  auto msg =
      std::make_unique<LOGS_CONFIG_API_Message>(header, parent->identifier_);
  auto w = Worker::onThisThread();

  // registering callback in case the socket gets closed, even when we aren't
  // running GetLogInfoRequests. This is needed as we rely on the node to send
  // us CONFIG_CHANGED messages to notify the client of config changes. If
  // that node dies, the client will keep running with an outdated config
  SocketCallback* onclose_to_use = nullptr;
  ld_check(to == shared_state_->node_id_);
  ld_check(shared_state_->socket_callback_);
  if (!shared_state_->socket_callback_->active()) {
    // only register the onclose callback if it isn't already
    onclose_to_use = shared_state_->socket_callback_.get();
  }
  int res = w->sender().sendMessage(std::move(msg), to, onclose_to_use);

  if (res == 0 && !shared_state_->socket_callback_->active()) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(1),
        1,
        "GetLogInfo SocketCallback (to %s) has not been registered, "
        "this client might end up with stale LogsConfig. Destroying this "
        "callback.",
        to.toString().c_str());
    ld_check(false);
  }
  return res;
}

void GetLogInfoFromNodeRequest::processChunk(std::string payload,
                                             size_t total_payload_size) {
  if (payload.length() == total_payload_size) {
    finalize(E::OK, payload);
    return;
  }

  // Only the first message will contain the full payload length
  if (total_payload_size != 0) {
    ld_assert(response_payload_.empty());
    ld_assert(response_expected_size_ == 0);
    response_payload_.reserve(total_payload_size);
    response_expected_size_ = total_payload_size;
  }
  response_payload_.append(payload);
  ld_assert(response_payload_.length() <= response_expected_size_);
  if (response_payload_.length() >= response_expected_size_) {
    ld_debug("LOGS_CONFIG_API_REPLY received full message");
    finalize(E::OK, response_payload_);
  } else {
    ld_debug("LOGS_CONFIG_API_REPLY waiting for more data "
             "(recv: %zu, total: %zu)",
             response_payload_.length(),
             response_expected_size_);
  }
}

void GetLogInfoFromNodeRequest::onReply(NodeID from,
                                        Status status,
                                        uint64_t /* unused */,
                                        std::string payload,
                                        size_t total_payload_size) {
  switch (status) {
    case E::OK:
      processChunk(payload, total_payload_size);
      return;

    case E::NOTFOUND:
      finalize(E::NOTFOUND);
      return;

    case E::SHUTDOWN:
      // retrying with a different node
      finalize(E::FAILED);
      break;

    default:
      ld_error("received LOGS_CONFIG_API_REPLY message from %s with unexpected "
               "status %s",
               from.toString().c_str(),
               error_description(status));
      finalize(E::FAILED);
      break;
  }
}

GetLogInfoRequest* FOLLY_NULLABLE GetLogInfoFromNodeRequest::findParent() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningGetLogInfo().gli_map;
  auto it = map.find(gli_req_id_);
  if (it == map.end()) {
    return nullptr;
  }
  return it->second.get();
}

void GetLogInfoFromNodeRequest::finalize(Status st, std::string json) {
  GetLogInfoRequest* parent = findParent();
  if (parent) {
    parent->onReplyFromNode(st, json, shared_state_version_);
  } else {
    // findParent() can return nullptr if the parent GetLogInfoRequest has
    // finished already. This can happen if another (newer)
    // GetLogInfoFromNodeRequest has received a LOGS_CONFIG_API_REPLY message.
  }
  deleteThis();
}

void GetLogInfoFromNodeRequest::createRetryTimer() {
  retry_timer_ = std::make_unique<ExponentialBackoffTimer>(

      std::bind(&GetLogInfoFromNodeRequest::attemptSend, this),
      Worker::onThisThread()->settings().on_demand_logs_config_retry_delay);
  ld_check(retry_timer_ != nullptr);
}

void GetLogInfoFromNodeRequest::activateRetryTimer() {
  ld_check(retry_timer_);
  retry_timer_->activate();
}

void GetLogInfoFromNodeRequest::deleteThis() {
  Worker* worker = Worker::onThisThread();

  auto& map = worker->runningGetLogInfo().per_node_map;
  auto it = map.find(id_);
  ld_check(it != map.end());

  map.erase(it); // destroys unique_ptr which owns this
}

}} // namespace facebook::logdevice
