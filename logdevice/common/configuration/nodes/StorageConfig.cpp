/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/StorageConfig.h"

#include <folly/Format.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

bool StorageNodeAttribute::isValid() const {
  if (num_shards <= 0 || num_shards > MAX_SHARDS) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10), 5, "invalid num_shards: %d.", num_shards);
    return false;
  }

  // TODO T33035439: enforce that storage capacity must be positive
  if (capacity < 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10), 5, "invalid storage capacity: %f.", capacity);
    return false;
  }

  if (generation <= 0) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10), 5, "invalid generation: %d.", generation);
    return false;
  }

  return true;
}

std::string StorageNodeAttribute::toString() const {
  return folly::sformat("[cap:{},ns:{},gen:{},ex:{}]",
                        capacity,
                        num_shards,
                        generation,
                        exclude_from_nodesets);
}

template <>
bool StorageAttributeConfig::attributeSpecificValidate() const {
  return true;
}

template <>
bool StorageConfig::roleSpecificValidate() const {
  return true;
}

}}}} // namespace facebook::logdevice::configuration::nodes
