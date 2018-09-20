/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

/**
 * @file  Implements the legacy mapping from log_ids to shards. Newer code
 *        should use the ShardID's in the storage set
 */

inline int getLegacyShardIndexForLog(logid_t log_id, shard_size_t num_shards) {
  return log_id.val_ % num_shards;
}
}} // namespace facebook::logdevice
