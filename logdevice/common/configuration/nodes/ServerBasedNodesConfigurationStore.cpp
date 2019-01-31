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

#include "logdevice/common/ConfigurationFetchRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RandomNodeSelector.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

void ServerBasedNodesConfigurationStore::getConfig(
    value_callback_t callback) const {
  auto worker = Worker::onThisThread();
  auto server_config = worker->getServerConfig();

  // If I'm a server, exclude me from the node selection
  auto my_node_id =
      server_config->hasMyNodeID() ? server_config->getMyNodeID() : NodeID();

  std::unique_ptr<Request> rq = std::make_unique<ConfigurationFetchRequest>(
      // TODO implement a better node selection mechanism and a retry mechanism.
      RandomNodeSelector::getNode(*server_config, /* exclude= */ my_node_id),
      ConfigurationFetchRequest::ConfigType::NODES_CONFIGURATION,
      [cb{std::move(callback)}](
          Status status,
          CONFIG_CHANGED_Header /* header */,
          std::string config) mutable { cb(status, std::move(config)); },
      /* NCM doesn't care where the callback is going to get exectued. */
      WORKER_ID_INVALID,
      worker->settings().server_based_nodes_configuration_store_timeout);
  worker->processor_->postRequest(rq);
}

Status ServerBasedNodesConfigurationStore::getConfigSync(
    std::string* /* value_out */) const {
  return Status::NOTSUPPORTED;
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

}}}} // namespace facebook::logdevice::configuration::nodes
