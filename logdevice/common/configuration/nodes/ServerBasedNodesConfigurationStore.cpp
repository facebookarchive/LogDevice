/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/ServerBasedNodesConfigurationStore.h"

#include <chrono>

#include <folly/synchronization/Baton.h>

#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class NodesConfigurationOneTimePollRequest : public FireAndForgetRequest {
 public:
  NodesConfigurationOneTimePollRequest(
      NodesConfigurationPoller::Poller::Options options,
      NodesConfigurationStore::value_callback_t cb,
      folly::Optional<NodesConfigurationStore::version_t> base_version)
      : FireAndForgetRequest(RequestType::NODES_CONFIGURATION_ONETIME_POLL),
        options_(std::move(options)),
        cb_(std::move(cb)),
        base_version_(base_version) {}

  void executionBody() override {
    poller_ = std::make_unique<NodesConfigurationPoller>(
        std::move(options_),
        NodesConfigurationCodec::extractConfigVersion,
        [this](Status st,
               NodesConfigurationPoller::Poller::RoundID /*round*/,
               folly::Optional<std::string> config_str) {
          onPollerCallback(st, std::move(config_str));
        },
        base_version_);
    poller_->start();
  }

  void onPollerCallback(Status st, folly::Optional<std::string> config_str) {
    if (st == Status::PARTIAL) {
      // consider a PARTIAL response as success
      RATELIMIT_INFO(std::chrono::seconds(10),
                     2,
                     "NodesConfiguration polling got a partial response.");
      st = Status::OK;
    }
    if (st == Status::OK && !config_str.hasValue()) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      2,
                      "NodesConfiguration polling no-op: UPTODATE");
      st = E::UPTODATE;
    }
    cb_(st, std::move(config_str).value_or(""));
    destroy();
  }

 private:
  NodesConfigurationPoller::Poller::Options options_;
  NodesConfigurationStore::value_callback_t cb_;
  const folly::Optional<NodesConfigurationStore::version_t> base_version_;
  std::unique_ptr<NodesConfigurationPoller> poller_;
};

/*static*/
NodesConfigurationPoller::Poller::Options
ServerBasedNodesConfigurationStore::genPollerOptions(
    NodesConfigurationPoller::Poller::Mode mode,
    const Settings& settings,
    const configuration::nodes::NodesConfiguration& nodes_configuration) {
  NodesConfigurationPoller::Poller::Options options;
  options.mode = mode;
  // cap the required response for each round by the cluster size
  options.round_timeout =
      settings.server_based_nodes_configuration_store_timeout;
  options.num_responses_required_round = std::min(
      settings.server_based_nodes_configuration_store_polling_responses,
      nodes_configuration.clusterSize());
  options.extras_request_each_wave =
      settings.server_based_nodes_configuration_store_polling_extra_requests;
  options.wave_timeout =
      settings.server_based_nodes_configuration_polling_wave_timeout;

  // TODO: support customize polling round internal, graylist / blacklist ttl,
  // etc
  return options;
}

void ServerBasedNodesConfigurationStore::getConfig(
    value_callback_t callback,
    folly::Optional<version_t> base_version) const {
  if (shutdown_signaled_.load()) {
    callback(E::SHUTDOWN, "");
    return;
  }

  auto worker = Worker::onThisThread();
  // TODO T44484704: genPollerOptions is currently using the NodesConfiguration
  // from the ServerConfig, later this will be switched to use the
  // NodesConfiguration maintained by NCM, and we need to make sure such NC is
  // always available, even in the special bootstrapping processor
  std::unique_ptr<Request> rq =
      std::make_unique<NodesConfigurationOneTimePollRequest>(
          genPollerOptions(
              NodesConfigurationPoller::Poller::Mode::ONE_TIME,
              *worker->processor_->settings(),
              *worker->processor_->config_->getServerConfig()
                   ->getNodesConfigurationFromServerConfigSource()),
          std::move(callback),
          base_version);
  worker->processor_->postRequest(rq);
}

Status ServerBasedNodesConfigurationStore::getConfigSync(
    std::string* /* value_out */,
    folly::Optional<version_t>) const {
  return Status::NOTSUPPORTED;
}

void ServerBasedNodesConfigurationStore::getLatestConfig(
    value_callback_t /* callback */) const {
  throw std::runtime_error("unsupported");
}

void ServerBasedNodesConfigurationStore::updateConfig(
    std::string /* value */,
    folly::Optional<version_t> /* base_version */,
    write_callback_t /* callback */) {
  throw std::runtime_error("unsupported");
}

Status ServerBasedNodesConfigurationStore::updateConfigSync(
    std::string /* value */,
    folly::Optional<version_t> /* base_version */,
    version_t* /* version_out */,
    std::string* /* value_out */) {
  throw std::runtime_error("unsupported");
}

void ServerBasedNodesConfigurationStore::shutdown() {
  shutdown_signaled_.store(true);
  // ServerBased NCS runs on the Processor, we assume the Processor::shutdown()
  // will cancel the remaining requests and join the worker threads after
  // signaling NCM / NCS shutdown.
}

}}}} // namespace facebook::logdevice::configuration::nodes
