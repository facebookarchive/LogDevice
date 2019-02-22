/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <set>

#include <folly/Function.h>
#include <folly/container/F14Map.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/RequestType.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

namespace facebook { namespace logdevice {

class ConfigurationFetchRequest;

// Wrapper instead of typedef to allow forward-declaring in Worker.h
struct ConfigurationFetchRequestMap {
  folly::F14FastMap<request_id_t,
                    std::unique_ptr<ConfigurationFetchRequest>,
                    request_id_t::Hash>
      map;
};

/**
 * @file Fetches a config from a given node. If a callback is provided, it will
 * be called with the content of the fetched config.
 */

class ConfigurationFetchRequest : public Request {
 public:
  using ConfigType = CONFIG_FETCH_Header::ConfigType;

  using config_cb_t = folly::Function<
      void(Status status, CONFIG_CHANGED_Header header, std::string config)>;

  ConfigurationFetchRequest(
      NodeID node_id,
      ConfigType config_type,
      folly::Optional<uint64_t> conditional_poll_version = folly::none)
      : Request(RequestType::CONFIGURATION_FETCH),
        node_id_(node_id),
        config_type_(config_type),
        conditional_poll_version_(conditional_poll_version) {}

  ConfigurationFetchRequest(
      NodeID node_id,
      ConfigType config_type,
      config_cb_t cb,
      worker_id_t cb_worker_id,
      std::chrono::milliseconds timeout,
      folly::Optional<uint64_t> conditional_poll_version = folly::none)
      : Request(RequestType::CONFIGURATION_FETCH),
        node_id_(node_id),
        config_type_(config_type),
        conditional_poll_version_(conditional_poll_version),
        cb_(std::move(cb)),
        cb_worker_id_(cb_worker_id),
        timeout_(timeout) {}

  ~ConfigurationFetchRequest() override {}

  int getThreadAffinity(int /*nthreads*/) override;

  Request::Execution execute() override;

  void onReply(CONFIG_CHANGED_Header header, std::string config);
  void onError(Status status);

 protected:
  virtual int sendMessageTo(std::unique_ptr<Message> msg, NodeID to);

  // Builds and send the CONFIG_FETCH message to the node with id node_id_.
  void sendConfigFetch();

  // Calls the callback (if there's any) and then finalizes the request.
  void onDone(Status status, CONFIG_CHANGED_Header header, std::string config);

  // finalizes the request by cancel the timeout timer, and deleteing the
  // request object from the worker map, which eventually deletes this.
  void finalize();

  virtual void deleteThis();

  void initTimeoutTimer();
  virtual void cancelTimeoutTimer();

  // Returns true if this request is waiting for a CONFIG_CHANGED response.
  // This is only true when a callback is passed to the request.
  bool isWaitingForResponse() const;

 private:
  NodeID node_id_;
  CONFIG_FETCH_Header::ConfigType config_type_;
  const folly::Optional<uint64_t> conditional_poll_version_;

  // A callback to be called when the config is ready.
  config_cb_t cb_{};

  // An optional to worker_id to execute the request on. If the worker ID is
  // WORKER_ID_INVALID, the request is executed on a random worker.
  worker_id_t cb_worker_id_{WORKER_ID_INVALID};

  Timer timeout_timer_;

  // This is a timeout for how long we should wait for the callback to be
  // called. This is only relevant when a callback is specified.
  folly::Optional<std::chrono::milliseconds> timeout_{folly::none};
};

}} // namespace facebook::logdevice
