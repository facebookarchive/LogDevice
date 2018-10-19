/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>
#include <unordered_set>

#include "logdevice/common/Request.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/protocol/LOGS_CONFIG_API_Message.h"

namespace facebook { namespace logdevice {

class LogsConfigApiRequest;
class ExponentialBackoffTimer;
class Timer;

// Wrapper instead of typedef to allow forward-declaration in Worker.h
struct LogsConfigApiRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<LogsConfigApiRequest>,
                     request_id_t::Hash>
      map;
};

/**
 * A Request State machine that runs in the client library and manages one
 * LogsConfig API request (of any type).
 */
class LogsConfigApiRequest : public Request {
 public:
  constexpr static size_t MAX_ERRORS = 20;
  using log_management_request_callback_t =
      std::function<void(Status,
                         uint64_t /* config version */,
                         std::string /* response payload*/)>;

  LogsConfigApiRequest(LOGS_CONFIG_API_Header::Type request_type,
                       const std::string& request_payload,
                       std::chrono::milliseconds timeout,
                       size_t max_errors,
                       uint64_t host_selection_seed,
                       log_management_request_callback_t callback)
      : Request(RequestType::LOGS_CONFIG_API),
        request_type_(request_type),
        payload_(std::move(request_payload)),
        timeout_(timeout),
        max_errors_(max_errors),
        host_selection_seed_(host_selection_seed),
        callback_(callback),
        response_expected_size_(0) {}

  Execution execute() override;

  int getThreadAffinity(int /* unused */) override {
    // Binding to a specific worker ensures that all requests executed by the
    // client are executed in a serial fashion. This improves the
    // predictability of the behaviour. This is specially important to make
    // sure that async calls are executed in order which is an implicit
    // assumption by the user of this API. However, in the future this might be
    // removed and the processor will decide the worker.
    return 0; /* always bind to worker 0 */
  }

  /* This gets called by LOGS_CONFIG_API_REPLY_Message when we receive a
   * reply for this request.
   */
  void onReply(NodeID from,
               Status status,
               uint64_t config_version,
               std::string payload,
               size_t total_payload_size);

  /* A callback when an error happens to this request. e.g., Cannot Send the
   * LOGS_CONFIG_API_Message.
   */
  void onError(Status status, std::string failure_reason = std::string());

 private:
  void start();

  NodeID pickNode();
  /**
   * Blacklists the current selected node, this returns false if we have
   * blacklisted all possible nodes in the cluster and there are no more to try
   */
  bool blacklistSelectedNode();

  void sendRequestTo(NodeID to);

  void done(Status status,
            uint64_t config_version,
            std::string response_payload);

  void activateTimeoutTimer();
  void activateRetryTimer();
  void cancelTimeoutTimer();
  void cancelRetryTimer();
  void onTimeout();
  void onRetry();

  LOGS_CONFIG_API_Header::Type request_type_;
  std::string payload_;
  std::chrono::milliseconds timeout_;
  const size_t max_errors_;
  const uint64_t host_selection_seed_;
  log_management_request_callback_t callback_;
  // a timeout timer to avoid waiting indefinitely for a response from the RSM
  std::unique_ptr<Timer> timeout_timer_;
  // a retry timer that calls onRetry(), this is mainly used to try different
  // server nodes if connection has failed or if the RSM responded with
  // E::AGAIN which means that the RSM is still replaying and not ready for
  // requests
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;
  bool callback_called_{false};
  size_t errors_{0};
  NodeID last_selected_node_ = NodeID();
  // A list of nodes that we have tried to reach out to and failed, we don't
  // want to keep retrying on these nodes for this particular Request instance.
  std::unordered_set<NodeID, NodeID::Hash> blacklisted_nodes_;
  // The response body can be returned in chunks
  // Keeps track of the intermediate result until completed
  size_t response_expected_size_;
  std::string response_payload_;
  // callback for a socket close after we've sent a message
  std::unique_ptr<SocketCallback> onclose_callback_;
};

}} // namespace facebook::logdevice
