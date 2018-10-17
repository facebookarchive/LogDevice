/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/plugin/PluginType.h"

namespace facebook { namespace logdevice {

EnumMap<PluginType, PluginTypeDescriptor>& pluginTypeDescriptors() {
  static EnumMap<PluginType, PluginTypeDescriptor> inst;
  return inst;
}

template <>
/* static */
const PluginTypeDescriptor&
EnumMap<PluginType, PluginTypeDescriptor>::invalidValue() {
  static const PluginTypeDescriptor inst{"INVALID", true};
  return inst;
}

template <>
void EnumMap<PluginType, PluginTypeDescriptor>::setValues() {
#define PLUGIN_TYPE(name, value, description, allow_multiple_active) \
  set(PluginType::name,                                              \
      PluginTypeDescriptor{description, allow_multiple_active});
#include "logdevice/common/plugin/plugin_types.inc" // nolint
}

}} // namespace facebook::logdevice
