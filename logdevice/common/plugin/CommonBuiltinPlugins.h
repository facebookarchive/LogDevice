/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/plugin/Plugin.h"

namespace facebook { namespace logdevice {

/**
 * A helper to create a vector of built-in plugins commonly loaded by servers,
 * clients and tests, augmented by specified types of plugins.
 */
template <class... Types>
PluginVector createAugmentedCommonBuiltinPluginVector() {
  return createPluginVector<BuildInfo, Types...>();
}

}} // namespace facebook::logdevice
