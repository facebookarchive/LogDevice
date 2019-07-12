/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/ClientPluginHelper.h"

#include "logdevice/common/plugin/BuiltinClientDefaultSettingsProvider.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/plugin/DynamicPluginLoader.h"
#include "logdevice/common/plugin/StaticPluginLoader.h"

namespace facebook { namespace logdevice {

PluginVector getClientPluginProviders() {
  return createPluginVector<DynamicPluginLoader,
                            StaticPluginLoader,
                            BuiltinPluginProvider,
                            BuiltinClientDefaultSettingsProvider>();
}

}} // namespace facebook::logdevice
