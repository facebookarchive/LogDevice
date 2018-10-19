/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <cstring>
#include <memory>
#include <unordered_map>
#include <vector>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/GetLogInfoRequestSharedState.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Request that is posted by a GetLogInfoRequest and handles fetching logs
 * config information from a particular target node. If the target node in
 * shared_state changes, this request will finish with E::STALE, otherwise it
 * will keep trying to fetch information from one node until it either
 * receives a reply or there is a connection/message send failure.
 */

class GetLogInfoRequest;

class GetLogInfoFromNodeRequest : public Request {
 public:
  explicit GetLogInfoFromNodeRequest(GetLogInfoRequest* parent);

  Execution execute() override;

  /**
   * Called by the messaging layer after it successfully sends out our
   * LOGS_CONFIG_API message, or fails to do so.
   */
  void onMessageSent(NodeID, Status);

  /**
   * LOGS_CONFIG_API_REPLY messages support chunking. This will either add a
   * new block to the final response or finalize the request
   */
  void processChunk(std::string payload, size_t total_payload_size);
  /**
   * Called when we receive a LOGS_CONFIG_API_REPLY message from a storage node.
   */
  void onReply(NodeID from,
               Status status,
               uint64_t config_version,
               std::string payload,
               size_t total_payload_size);

  /**
   * Starts execution of the request (execute() includes registering it on the
   * Worker which is not needed in tests)
   */
  void start();

  // broadcasts a message to the node specified in shared_state_, or finalizes
  // with E::STALE if that node has to be changed
  void attemptSend();

  // returns thread affinity
  int getThreadAffinity(int /*nthreads*/) override {
    ld_check(shared_state_->worker_id_ == worker_id_);
    return worker_id_.val_;
  }

  // forwards the result to the GetLogInfoRequest parent and destroys the
  // request
  void finalize(Status, std::string json = "");

  ~GetLogInfoFromNodeRequest() override {}

 protected: // tests can override
  virtual void createRetryTimer();
  virtual void activateRetryTimer();
  virtual void deleteThis();

  // This timer is used to schedule retrying message sends. This will only
  // happen when we are waiting for a config change to propagate to this worker
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;

  // returns the GetLogInfoRequest that posted this one. Can return nullptr if
  // that GetLogInfoRequest doesn't exist anymore. This can happen if another,
  // newer, GetLogInfoFromNodeRequest delivered a response before this one did.
  virtual GetLogInfoRequest* FOLLY_NULLABLE findParent();

 private:
  // ID of the parent GetLogInfoRequest
  request_id_t gli_req_id_;

  // state shared with all GetLogInfoRequests and RemoteLogsConfig
  std::shared_ptr<GetLogInfoRequestSharedState> shared_state_;

  // worker ID that the request will run on
  const worker_id_t worker_id_;

  // The version of the shared state that this request was created with. If
  // the shared state version changes, this request should terminate and return
  // E::STALE
  uint64_t shared_state_version_;

  // Sends a single LOGS_CONFIG_API message to the specified node
  int sendOneMessage(NodeID to);

  // The response body can be returned in chunks
  // Keeps track of the intermediate result until completed
  size_t response_expected_size_;
  std::string response_payload_;
};

}} // namespace facebook::logdevice
