/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/ServerBuiltinPluginProvider.h"
#include "logdevice/server/ServerPluginPack.h"

namespace facebook { namespace logdevice {

PluginVector ServerBuiltinPluginProvider::getPlugins() {
  PluginVector res;
  res.push_back(std::make_unique<ServerPluginPack>());
  return res;
}

}} // namespace facebook::logdevice
