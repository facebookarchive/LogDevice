/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/NodesConfigurationPoller.h"

#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

NodesConfigurationPoller::NodesConfigurationPoller(
    Poller::Options options,
    VersionExtFn version_fn,
    Callback cb,
    folly::Optional<Version> conditional_base_version)
    : options_(std::move(options)),
      version_fn_(std::move(version_fn)),
      cb_(std::move(cb)),
      conditional_base_version_(std::move(conditional_base_version)),
      callback_helper_(this) {
  ld_check(version_fn_ != nullptr);
  ld_check(cb_ != nullptr);
}

void NodesConfigurationPoller::start() {
  // initialize the highest seen config version
  onNodesConfigurationChanged();
  if (poller_ == nullptr) {
    poller_ = createPoller();
  }
  poller_->start();
}

void NodesConfigurationPoller::stop() {
  if (poller_) {
    poller_->stop();
  }
}

std::unique_ptr<NodesConfigurationPoller::Poller>
NodesConfigurationPoller::createPoller() {
  auto selection_fn = [this](const NodeSourceSet& candidates,
                             const NodeSourceSet& existing,
                             const NodeSourceSet& blacklist,
                             const NodeSourceSet& graylist,
                             size_t num_required,
                             size_t num_extras) {
    return RandomNodeSelector::select(candidates,
                                      existing,
                                      blacklist,
                                      graylist,
                                      num_required,
                                      num_extras,
                                      getClusterState());
  };

  auto req_fn = [this](Poller::RoundID round, node_index_t node) {
    // it's safe to capture `this' as Poller is owned by this object
    return sendRequestToNode(round, node);
  };

  auto aggr_fn = [this](const std::string* config,
                        NodeResponse response) -> folly::Optional<std::string> {
    return aggregateConfiguration(config, std::move(response));
  };

  auto callback_wrapper = [this](Status st,
                                 Poller::RoundID round,
                                 folly::Optional<std::string> str) {
    onPollerCallback(st, round, std::move(str));
  };

  return std::make_unique<Poller>(
      candidatesFromNodesConfiguration(*getNodesConfiguration(), getMyNodeID()),
      std::move(selection_fn),
      std::move(req_fn),
      std::move(aggr_fn),
      std::move(callback_wrapper),
      options_);
}

void NodesConfigurationPoller::onNodesConfigurationChanged() {
  if (isBootstrapping()) {
    // do not update highest_seen or update source candidate nodes
    // in bootstrapping environment as it uses a different bootstrapping
    // nodes configuration
    return;
  }

  const auto& nodes_configuration = getNodesConfiguration();
  const auto new_version = nodes_configuration->getVersion();
  if (new_version > highest_seen_) {
    // update highest seen version for conditional polling
    highest_seen_ = new_version;
    if (poller_ != nullptr) {
      // refresh the list of host to poll from based on the
      // new nodes configuration
      poller_->setSourceCandidates(candidatesFromNodesConfiguration(
          *nodes_configuration, getMyNodeID()));
    }
  }
}

folly::Optional<std::string>
NodesConfigurationPoller::aggregateConfiguration(const std::string* config,
                                                 NodeResponse response) const {
  if (response.st == Status::UPTODATE || response.config_str.empty()) {
    return folly::none;
  }

  // a successful reply can only be either OK or UPTODATE
  ld_check(response.st == Status::OK);
  ld_check(!response.config_str.empty());

  if (config == nullptr) {
    const auto version = version_fn_(response.config_str);
    if (!version.hasValue()) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Got a success reply for NodesConfiguration polling but "
                      "cannot extract version from the config string!");
      return folly::none;
    }

    return folly::Optional<std::string>(std::move(response.config_str));
  }

  const auto existing_version = version_fn_(*config);
  const auto response_version = version_fn_(response.config_str);
  // version_fn_ should be deterministic and existing config was picked by the
  // same function eariler
  ld_check(existing_version.hasValue());
  if (!response_version.hasValue() ||
      response_version.value() < existing_version.value()) {
    // existing version is newer, no-op
    return folly::none;
  }
  return folly::Optional<std::string>(std::move(response.config_str));
}

void NodesConfigurationPoller::onConfigurationFetchResult(Poller::RoundID round,
                                                          node_index_t source,
                                                          Status st,
                                                          std::string config) {
  Poller::RequestResult result = Poller::RequestResult::OK;
  if (st == Status::OK || st == Status::UPTODATE) {
    result = Poller::RequestResult::OK;
  } else {
    // in all other cases (e.g., E::TIMEOUT), graylist the source
    result = Poller::RequestResult::FAILURE_GRAYLIST;
  }

  if (poller_) {
    poller_->onSourceReply(
        round,
        source,
        result,
        folly::Optional<NodeResponse>({st, std::move(config)}));
  }
}

void NodesConfigurationPoller::onPollerCallback(
    Status st,
    Poller::RoundID round,
    folly::Optional<std::string> config_str) {
  // bump stats for the polling results
  if (st == Status::OK) {
    WORKER_STAT_INCR(nodes_configuration_polling_success);
  } else if (st == Status::PARTIAL) {
    WORKER_STAT_INCR(nodes_configuration_polling_partial);
  } else {
    WORKER_STAT_INCR(nodes_configuration_polling_failed);
  }
  cb_(st, round, std::move(config_str));
}

folly::Optional<NodesConfigurationPoller::Version>
NodesConfigurationPoller::getConditionalPollVersion() const {
  if (isBootstrapping()) {
    return folly::none;
  }

  if (conditional_base_version_.hasValue()) {
    // if conditional_base_version_ is set, use the given
    // conditional_base_version_
    return conditional_base_version_.value();
  }

  // otherwise, use the highest seen nodes configuration version,
  // if any
  return highest_seen_.val() > 0 ? highest_seen_ : folly::Optional<Version>();
}

//////////// protected functions ///////////////

ClusterState* NodesConfigurationPoller::getClusterState() {
  return Worker::getClusterState();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
NodesConfigurationPoller::getNodesConfiguration() const {
  if (isBootstrapping()) {
    // for bootstrapping environment, currently only server config
    // based NC is available
    // TODO T44484704: use NC for seed hosts in NodesConfigurationInit
    // bootstrapping
    return Worker::onThisThread()
        ->getNodesConfigurationFromServerConfigSource();
  }

  // Otherwise, NodesConfigurationPoller is used by NCM so it must use
  // NCM based NC for conditional polling
  return Worker::onThisThread()->getNodesConfigurationFromNCMSource();
}

folly::Optional<node_index_t> NodesConfigurationPoller::getMyNodeID() const {
  auto processor = Worker::onThisThread()->processor_;
  return (processor->hasMyNodeID()
              ? folly::Optional<node_index_t>(processor->getMyNodeID().index())
              : folly::none);
}

bool NodesConfigurationPoller::isBootstrapping() const {
  return Worker::settings().bootstrapping;
}

NodesConfigurationPoller::Poller::RequestResult
NodesConfigurationPoller::sendRequestToNode(Poller::RoundID round,
                                            node_index_t node) {
  auto worker = Worker::onThisThread();
  const auto& nodes_configuration = getNodesConfiguration();
  NodeID nid = nodes_configuration->getNodeID(node);

  auto ticket = callback_helper_.ticket();
  auto cb_wrapper = [ticket, round, node](Status status,
                                          CONFIG_CHANGED_Header /* header */,
                                          std::string config) {
    ticket.postCallbackRequest([round, node, status, cfg = std::move(config)](
                                   NodesConfigurationPoller* poller) mutable {
      if (poller != nullptr) {
        poller->onConfigurationFetchResult(round, node, status, std::move(cfg));
      }
    });
  };

  folly::Optional<uint64_t> conditional_poll_version_msg;
  auto conditional_poll_version = getConditionalPollVersion();
  if (conditional_poll_version.hasValue()) {
    conditional_poll_version_msg.assign(conditional_poll_version.value().val());
  }

  std::unique_ptr<Request> rq = std::make_unique<ConfigurationFetchRequest>(
      nid,
      ConfigurationFetchRequest::ConfigType::NODES_CONFIGURATION,
      std::move(cb_wrapper),
      // it doesn't matter where ConfigurationFetchRequest will be executed
      // as we always route the callback back to the poller context
      WORKER_ID_INVALID,
      // use the full round timeout as the RPC request timeout
      options_.round_timeout,
      conditional_poll_version_msg);

  int rv = worker->processor_->postRequest(rq);
  if (rv != 0 && err == E::NOBUFS) {
    // this is not the fault of the source, do not graylist
    return Poller::RequestResult::FAILURE_TRANSIENT;
  }

  // successfully posted or shutting down
  return Poller::RequestResult::OK;
}

/*static*/
NodesConfigurationPoller::NodeSourceSet
NodesConfigurationPoller::candidatesFromNodesConfiguration(
    const configuration::nodes::NodesConfiguration& config,
    folly::Optional<node_index_t> my_node_id) {
  // gather all nodes that have service discovery information
  const auto& serv_disc = config.getServiceDiscovery();
  NodeSourceSet result;
  for (const auto kv : *serv_disc) {
    result.insert(kv.first);
  }
  if (my_node_id.hasValue()) {
    result.erase(my_node_id.value());
  }
  return result;
}

}} // namespace facebook::logdevice
