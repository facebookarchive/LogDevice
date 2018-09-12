/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeSetSelectorFactory.h"

namespace facebook { namespace logdevice {

/**
 * @file utils for nodeset selectors
 */

// Returns the actual nodeset size that would be used by a given nodeset
// selector type with a given config, nodeset size target and sync replication
// scope
storage_set_size_t
get_real_nodeset_size(NodeSetSelectorType nodeset_selector_type,
                      logid_t log_id,
                      const std::shared_ptr<Configuration>& cfg,
                      folly::Optional<int> target,
                      ReplicationProperty replication) {
  auto selector = NodeSetSelectorFactory::create(nodeset_selector_type);
  if (!selector) {
    ld_check(false);
    err = E::INVALID_PARAM;
    return 0;
  }
  return selector->getStorageSetSize(log_id, cfg, target, replication);
}

}} // namespace facebook::logdevice
