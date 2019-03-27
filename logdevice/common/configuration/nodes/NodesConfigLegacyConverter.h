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

  static configuration::StorageState
  toLegacyStorageState(membership::StorageState ss);

  static int toLegacyNodesConfig(const NodesConfiguration& config,
                                 NodesConfig* legacy_config_out);

  static std::shared_ptr<const NodesConfiguration>
  fromLegacyNodesConfig(const NodesConfig& config_legacy,
                        const MetaDataLogsConfig& meta_config,
                        config_version_t version);

  // take a server config, convert its nodes config to the new type,
  // and convert it back, create a new server config with the config that
  // got converted back, compare with the original. return true if the
  // generated server is the same as the original.
  static bool testWithServerConfig(const ServerConfig& server_config);

  static bool testSerialization(const ServerConfig& server_config,
                                bool compress);
};

}}}} // namespace facebook::logdevice::configuration::nodes
