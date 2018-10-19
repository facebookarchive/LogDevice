/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>
#include <folly/hash/Hash.h>

#include "logdevice/common/NodeSetSelector.h"
#include "logdevice/common/NodeSetSelectorType.h"

namespace facebook { namespace logdevice {

/**
 * @file  create a NodeSetSelector of the given type
 */

class NodeSetSelectorFactory {
 public:
  static std::unique_ptr<NodeSetSelector> create(NodeSetSelectorType type);
};

}} // namespace facebook::logdevice
