/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/ServerConfig.h"

namespace facebook { namespace logdevice {
class RandomNodeSelector {
 public:
  /**
   * @params cfg      The server config, used to get the nodes
   * @params exclude  Exclude this node. If exclude is the _only_ node in the
   *                  config, it will still be chosen
   * @returns Returns a random node among the nodes in the config, excluding
   *          exclude if there are other options
   */
  static NodeID getNode(const ServerConfig& cfg, NodeID exclude = NodeID());
};
}} // namespace facebook::logdevice
