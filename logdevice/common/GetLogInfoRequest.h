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
#include "logdevice/common/GetLogInfoFromNodeRequest.h"
#include "logdevice/common/GetLogInfoRequestSharedState.h"
#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {

/**
 * @file
 *
 * Request that runs in the client library in order to satisfy an
 * application's call to the getLogByID() or getLogRangeByName() APIs when the
 * log group configuration isn't present on the client. Creates a
 * GetLogInfoFromNodeRequest that harasses a particular node for this
 * information. If that times out or fails, changes the target node and posts
 * a new GetLogInfoFromNodeRequest.
 */

class GetLogInfoRequest;
using get_log_info_callback_t = std::function<void(Status, const std::string&)>;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct GetLogInfoRequestMaps {
  std::unordered_map<request_id_t,
                     std::unique_ptr<GetLogInfoRequest>,
                     request_id_t::Hash>
      gli_map;
  std::unordered_map<request_id_t,
                     std::unique_ptr<GetLogInfoFromNodeRequest>,
                     request_id_t::Hash>
      per_node_map;
};

class GetLogInfoRequest : public Request {
 public:
  GetLogInfoRequest(LOGS_CONFIG_API_Header::Type request_type,
                    std::string identifier,
                    std::chrono::milliseconds client_timeout,
                    std::shared_ptr<GetLogInfoRequestSharedState> shared_state,
                    get_log_info_callback_t callback,
                    worker_id_t callback_worker_id)
      : Request(RequestType::GET_LOG_INFO),
        shared_state_(shared_state),
        request_type_(request_type),
        identifier_(std::move(identifier)),
        client_timeout_(client_timeout),
        callback_(callback),
        worker_id_(shared_state->worker_id_),
        callback_worker_id_(callback_worker_id) {
    ld_check(shared_state_);
  }

  Execution execute() override;

  /**
   * Called when target_change_timer_ fires, attempts to change the target node.
   */
  void onClientTimeout();

  // Initializes state and broadcasts initial messages to all servers.  Public
  // for tests.
  void start();

  // returns thread affinity - all of GetLogInfoRequests and
  // GetLogInfoFromNodeRequests should run on the same worker thread
  int getThreadAffinity(int /*nthreads*/) override {
    ld_check(shared_state_->worker_id_ == worker_id_);
    return worker_id_.val_;
  }

  // Called by GetLogInfoFromNodeRequest whenever there is a reply from a node
  void onReplyFromNode(Status status, std::string json, uint64_t version);

  // forwards the result to the worker thread that is the target for the
  // result callback, and (in most cases) destroys the Request
  void finalize(Status, bool delete_this = true);

  ~GetLogInfoRequest() override;

  std::shared_ptr<GetLogInfoRequestSharedState> shared_state_;

  // Type of request (e.g. log group, directory or namespace)
  const LOGS_CONFIG_API_Header::Type request_type_;

  // Identifier for the type of request,
  // e.g. for log group this is the log group name
  const std::string identifier_;

 protected: // tests can override
  // Changes the target node if necessary, saves some metadata in the shared
  // state and calls postPerNodeRequest()
  void prepareAndPostPerNodeRequest();

  // Schedules a target node change (and invalidates the config if necessary) if
  // another worker hasn't done it yet and if the rate limit on target changes
  // hasn't been hit yet.
  void attemptTargetNodeChange();

  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual int reloadConfig();
  virtual void createTimers();
  virtual void activateTargetNodeChangeTimer();
  // Cancels target node change timer and activates retry timer
  virtual void activateRetryTimer();
  virtual bool isRetryTimerActive();
  virtual void deleteThis();

  // Posts a GetLogInfoFromNodeRequest to this worker
  virtual void postPerNodeRequest();
  uint64_t last_used_shared_state_version_{0};

 private:
  // Changes the target node. Expects the mutex of the shared_state_ structure
  // to be locked and the lock to be passed in as the arg
  void changeTargetNode(std::unique_lock<std::mutex>& lock);

  const std::chrono::milliseconds client_timeout_;
  const get_log_info_callback_t callback_;

  // Worker that this request runs on
  const worker_id_t worker_id_;

  // Worker thread affinity for result callbacks
  const worker_id_t callback_worker_id_;

  // This timer gets reset every time we post a new GetLogInfoFromNodeRequest.
  // If we haven't received a response from that request until client_timeout_
  // expires, this timer will fire and attempt to change our target and post a
  // new GetLogInfoFromNodeRequest
  std::unique_ptr<Timer> target_change_timer_;

  // This timer is activated whenever we need to retry posting a new
  // GetLogInfoFromNodeRequest after the previous one failed. Note that only
  // one of target_change_timer_ and retry_timer_ should be active at any time
  // during the lifetime of the request
  std::unique_ptr<ExponentialBackoffTimer> retry_timer_;

  // Result JSON
  std::string result_json_;

  // Make sure to call finalize() (and hence the client callback) exactly once
  bool finalize_called_ = false;
};

}} // namespace facebook::logdevice
