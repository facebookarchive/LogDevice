/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/NodesConfigAPIHandler.h"

#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"

using namespace facebook::logdevice;

namespace facebook { namespace logdevice {

void NodesConfigAPIHandler::getNodesConfig(
    thrift::NodesConfigResponse& out,
    std::unique_ptr<thrift::NodesFilter> filter) {
  // The idea is that we keep filtering the nodes based on the supplied filter
  // until we get a final list to return.

  auto nodes_configuration = processor_->getNodesConfiguration();
  std::vector<thrift::NodeConfig> result_nodes;

  forFilteredNodes(*nodes_configuration, filter.get(), [&](node_index_t index) {
    thrift::NodeConfig node;
    fillNodeConfig(node, index, *nodes_configuration);
    result_nodes.push_back(std::move(node));
  });
  out.set_nodes(std::move(result_nodes));
  out.set_version(
      static_cast<int64_t>(nodes_configuration->getVersion().val()));
}

}} // namespace facebook::logdevice
