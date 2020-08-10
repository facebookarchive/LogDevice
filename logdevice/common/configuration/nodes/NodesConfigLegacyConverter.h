/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

class NodesConfigLegacyConverter {
 public:
  static membership::StorageState
  fromLegacyStorageState(configuration::StorageState ls);

  static std::shared_ptr<const NodesConfiguration>
  fromLegacyNodesConfig(const NodesConfig& config_legacy,
                        const MetaDataLogsConfig& meta_config,
                        config_version_t version);
};

}}}} // namespace facebook::logdevice::configuration::nodes
