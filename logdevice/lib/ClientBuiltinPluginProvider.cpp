/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ClientBuiltinPluginProvider.h"

#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"
#include "logdevice/lib/ClientPluginPack.h"

namespace facebook { namespace logdevice {

PluginVector ClientBuiltinPluginProvider::getPlugins() {
  return createAugmentedCommonBuiltinPluginVector<ClientPluginPack>();
}

PluginVector getClientPluginProviders() {
  return createPluginVector<StaticPluginLoader, ClientBuiltinPluginProvider>();
}

}} // namespace facebook::logdevice
