/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice { namespace ClusterMarkerChecker {

/**
 * Reads ClusterMarkerMetadata from each shard of local log store and checks
 * that it's compatible with configuration. See ClusterMarkerMetadata.
 */

// @return whether the check passed.
bool check(ShardedLocalLogStore& sharded_store,
           ServerConfig& config,
           const NodeID& my_node_id);

}}} // namespace facebook::logdevice::ClusterMarkerChecker
