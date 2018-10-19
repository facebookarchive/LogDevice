/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>

#include "logdevice/common/Request.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

/**
 * @file Processor request to send compaction tasks to local log storage
 */

class CompactionRequest : public Request {
 public:
  explicit CompactionRequest(folly::Optional<int> shard_idx,
                             std::function<void(Status)> callback = nullptr)
      : Request(RequestType::COMPACTION),
        shard_idx_(shard_idx),
        callback_(callback) {}

  Request::Execution execute() override;

 private:
  // If shard_idx_ is supplied with value, compact the storage shard indexed
  // by shard_idx_. Otherwise, compact all storage shards available
  folly::Optional<int> shard_idx_;

  // callback function to pass to the Storage task
  std::function<void(Status)> callback_;
};

}} // namespace facebook::logdevice
