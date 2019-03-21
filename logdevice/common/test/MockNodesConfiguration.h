/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

struct MockNodesConfiguration : public NodesConfiguration {
  MOCK_METHOD1(getNodeID, NodeID(node_index_t));
  MOCK_CONST_METHOD1(getNodeServiceDiscovery,
                     const NodeServiceDiscovery*(node_index_t));
};
}}}} // namespace facebook::logdevice::configuration::nodes
