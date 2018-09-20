/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

namespace facebook { namespace logdevice {
/**
 * @file NodeLocation specifies the location of a Node in a
 *       LogDevice cluster. The location is expressed by a set of
 *       hierarchical scopes. LogDevice assumes the cluster topology to be
 *       a tree-like structure: two nodes sharing a lower-level scope must
 *       be within the same higher level scope.
 */

/**
 * Type of different scopes, must be ordered by increasing size of
 * scopes. The smallest scope, which is node itself, has a value of 0.
 */
enum class NodeLocationScope : uint8_t {
#define NODE_LOCATION_SCOPE(name) name,
#include "logdevice/include/node_location_scopes.inc"
  INVALID
};

}} // namespace facebook::logdevice
