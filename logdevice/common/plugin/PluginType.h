/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/include/EnumMap.h"

namespace facebook { namespace logdevice {

enum class PluginType : uint32_t {
  // used by EnumMap
  INVALID = 0,

#define PLUGIN_TYPE(name, value, _, __) name = value,
#include "logdevice/common/plugin/plugin_types.inc"

  // Used by EnumMap. No values should be specified after MAX.
  MAX
};

struct PluginTypeDescriptor {
  std::string name;
  bool allow_multiple;
  bool operator!=(const PluginTypeDescriptor& o) const {
    return o.name != name || o.allow_multiple != allow_multiple;
  }
};

EnumMap<PluginType, PluginTypeDescriptor>& pluginTypeDescriptors();

inline const std::string& toString(PluginType t) {
  return pluginTypeDescriptors()[t].name;
}

}} // namespace facebook::logdevice
