/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClusterState.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class GetClusterStateRequest;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct GetClusterStateRequestMap {
  std::unordered_map<request_id_t,
                     std::unique_ptr<GetClusterStateRequest>,
                     request_id_t::Hash>
      map;
};

class GetClusterStateRequest : public Request {
 public:
  using get_cs_callback_t =
      std::function<void(Status status,
                         std::vector<uint8_t> nodes_state,
                         std::vector<node_index_t> boycotted_nodes)>;

  GetClusterStateRequest(std::chrono::milliseconds timeout,
                         std::chrono::milliseconds wave_timeout,
                         get_cs_callback_t cb,
                         folly::Optional<NodeID> dest = folly::none)
      : Request(RequestType::GET_CLUSTER_STATE),
        timeout_(timeout),
        wave_timeout_(wave_timeout),
        callback_(std::move(cb)),
        dest_(dest) {}

  Request::Execution execute() override;

  /**
   * Callbacks for timeout, wave timeout, response and error.
   *
   * @return true if the object was destroyed, and the execution
   * of this request should terminate, false otherwise.
   */
  bool onTimeout();
  bool onWaveTimeout();
  bool onReply(const Address& from,
               Status status,
               std::vector<uint8_t> nodes_state,
               std::vector<node_index_t> boycotted_nodes);
  bool onError(Status status);

 protected:
  virtual void initTimers();
  virtual void activateWaveTimer();
  void initNodes();
  bool done(Status status,
            std::vector<uint8_t> nodes_state,
            std::vector<node_index_t> boycotted_nodes);
  bool start();

  virtual NodeID getMyNodeID() const;
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  virtual ClusterState* getClusterState() const;
  virtual bool sendTo(NodeID to);

  virtual void attachToWorker();
  virtual void destroyRequest();

 private:
  virtual const Settings& getSettings() const;
  const static size_t kMinWaveSize = 2;
  // kWaveScaleFactor is used to increase the size of the waves
  // sent after the initial one, by multiplying the scale factor
  // and the previous wave size.
  const static size_t kWaveScaleFactor = 2;

  std::chrono::milliseconds timeout_;
  std::chrono::milliseconds wave_timeout_;
  std::unique_ptr<Timer> timer_;
  std::unique_ptr<Timer> wave_timer_;
  std::vector<node_index_t> nodes_;
  size_t next_node_pos_{0};
  size_t wave_size_{kMinWaveSize};
  size_t errors_{0};
  bool callback_called_{false};
  get_cs_callback_t callback_;
  folly::Optional<NodeID> dest_;
};

}} // namespace facebook::logdevice
