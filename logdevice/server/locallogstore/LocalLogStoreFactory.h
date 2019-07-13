/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/server/locallogstore/LocalLogStore.h"

namespace facebook { namespace logdevice {

/**
 * @file Factory for generating LocalLogStore instances.
 */

class LocalLogStoreFactory {
 public:
  virtual ~LocalLogStoreFactory() {}

  // On failure returns nullptr.
  virtual std::unique_ptr<LocalLogStore>
  create(uint32_t shard_idx,
         uint32_t num_shards,
         std::string path,
         IOTracing* io_tracing) const = 0;
};

}} // namespace facebook::logdevice
