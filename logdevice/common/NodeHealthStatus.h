/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

namespace facebook { namespace logdevice {

/**
 * @file  Specify health status of a node. Determined by various factors and
 * detected on server side.
 */
enum class NodeHealthStatus {
  // Used when health status detecting is disabled, node is dead or for
  // backwards compatibility.
  UNDEFINED,
  HEALTHY,
  OVERLOADED,
  UNHEALTHY
};
std::string toString(NodeHealthStatus status);
}} // namespace facebook::logdevice
