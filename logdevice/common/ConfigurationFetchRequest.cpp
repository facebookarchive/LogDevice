/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ConfigurationFetchRequest.h"

#include <folly/Memory.h>

#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

bool ConfigurationFetchRequest::isWaitingForResponse() const {
  return cb_ != nullptr;
}

int ConfigurationFetchRequest::getThreadAffinity(int /*nthreads*/) {
  return cb_worker_id_ == WORKER_ID_INVALID ? -1 : cb_worker_id_.val();
}

Request::Execution ConfigurationFetchRequest::execute() {
  auto& map = Worker::onThisThread()->runningConfigurationFetches().map;
  auto insertion_result =
      map.emplace(id_, std::unique_ptr<ConfigurationFetchRequest>(this));
  ld_check(insertion_result.second);

  initTimeoutTimer();
  sendConfigFetch();

  return Execution::CONTINUE;
}

void ConfigurationFetchRequest::initTimeoutTimer() {
  timeout_timer_.assign([id = id_]() {
    // Access the request via the map instead of binding to this as the request
    // might be destroyed when the timer fires. This timer will only fire on the
    // worker on which it's created.
    Worker* worker = Worker::onThisThread();
    auto& map = worker->runningConfigurationFetches().map;
    auto it = map.find(id);
    if (it == map.end()) {
      return;
    }
    it->second->onError(E::TIMEDOUT);
  });

  if (timeout_.hasValue()) {
    timeout_timer_.activate(timeout_.value());
  }
}

void ConfigurationFetchRequest::cancelTimeoutTimer() {
  timeout_timer_.cancel();
}

void ConfigurationFetchRequest::sendConfigFetch() {
  CONFIG_FETCH_Header hdr{
      isWaitingForResponse() ? id_ : REQUEST_ID_INVALID,
      config_type_,
      (conditional_poll_version_.hasValue() ? conditional_poll_version_.value()
                                            : 0)};

  std::unique_ptr<Message> msg = std::make_unique<CONFIG_FETCH_Message>(hdr);
  int rv = sendMessageTo(std::move(msg), node_id_);

  if (rv != 0) {
    ld_error("Unable to fetch config from server %s with error %s",
             node_id_.toString().c_str(),
             error_description(err));
    onError(err);
    return;
  }
  if (!isWaitingForResponse()) {
    finalize();
  }
}

void ConfigurationFetchRequest::onError(Status status) {
  onDone(status, CONFIG_CHANGED_Header(), "");
}

void ConfigurationFetchRequest::onReply(CONFIG_CHANGED_Header header,
                                        std::string config) {
  auto status = header.status;
  onDone(status, std::move(header), std::move(config));
}

void ConfigurationFetchRequest::onDone(Status status,
                                       CONFIG_CHANGED_Header header,
                                       std::string config) {
  if (isWaitingForResponse()) {
    cb_(status, std::move(header), std::move(config));
  }
  finalize();
}

void ConfigurationFetchRequest::finalize() {
  cancelTimeoutTimer();
  deleteThis();
}

void ConfigurationFetchRequest::deleteThis() {
  auto& map = Worker::onThisThread()->runningConfigurationFetches().map;
  auto it = map.find(id_);
  if (it != map.end()) {
    // Delete the unique reference to this
    map.erase(it);
  }
}

int ConfigurationFetchRequest::sendMessageTo(std::unique_ptr<Message> msg,
                                             NodeID to) {
  return Worker::onThisThread()->sender().sendMessage(std::move(msg), to);
}

}} // namespace facebook::logdevice
