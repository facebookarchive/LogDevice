/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

// A class to allocate node indices for new nodes. The indices allocated fill
// the gaps in the config first and then starts allocating new indices.
// In other words, this class recycles node indices of removed nodes before
// allocating new ones.
class NodeIndicesAllocator {
 public:
  // Allocates *num_ids* based on the current service discovery config.
  // The returned deque will contain exactly *num_ids* elements.
  std::deque<node_index_t> allocate(const ServiceDiscoveryConfig& svc,
                                    size_t num_ids) const;
};

}}}} // namespace facebook::logdevice::configuration::nodes
