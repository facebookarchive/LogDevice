/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/IOTracing.h"

#include "logdevice/common/chrono_util.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

IOTracing::IOTracing(shard_index_t shard_idx) : shardIdx_(shard_idx) {}

void IOTracing::reportCompletedOp(
    std::chrono::steady_clock::duration duration) {
  ld_info("[io:S%d] %s  %.3fms",
          static_cast<int>(shardIdx_),
          state_->context.c_str(),
          to_sec_double(duration) * 1e3);
}

}} // namespace facebook::logdevice
