/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

namespace facebook { namespace logdevice {

/**
 * @file  Specify different status shard regarding the
 *        replication state of a record.
 */

enum class ShardDataHealth : uint8_t {

  UNKNOWN = 0,
  HEALTHY,
  UNAVAILABLE,
  LOST_REGIONS,
  LOST_ALL,
  EMPTY,
  Count
};

std::string toString(const ShardDataHealth& st);

}} // namespace facebook::logdevice
