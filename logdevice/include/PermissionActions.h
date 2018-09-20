/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

namespace facebook { namespace logdevice {
/**
 * The list of actions that a PermissionChecker can use to verify permissions.
 */
enum class ACTION {
  APPEND = 0,
  READ = 1,
  TRIM = 2,
  LOG_MANAGEMENT = 3,
  MAX // should always be last
};
}} // namespace facebook::logdevice
