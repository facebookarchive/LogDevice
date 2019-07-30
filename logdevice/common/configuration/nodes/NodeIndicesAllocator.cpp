/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"

#include <folly/container/F14Set.h>

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

std::deque<node_index_t>
NodeIndicesAllocator::allocate(const ServiceDiscoveryConfig& svc,
                               size_t num_ids) const {
  folly::F14FastSet<node_index_t> current_indices;
  current_indices.reserve(svc.numNodes());
  for (const auto& sd : svc) {
    current_indices.insert(sd.first);
  }

  std::deque<node_index_t> ret;

  node_index_t id = 0;
  while (ret.size() < num_ids) {
    if (current_indices.count(id) == 0) {
      ret.push_back(id);
    }
    id++;
  }

  ld_assert(ret.size() == num_ids);
  return ret;
}

}}}} // namespace facebook::logdevice::configuration::nodes
