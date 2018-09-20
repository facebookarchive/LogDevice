/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

/**
 * Create a SettingsBundle with default values. SettingBundles should not be
 * instantiated manually as bundles need to go through UpdateableSettings to be
 * initialized with default values.
 */

template <typename T>
T create_default_settings() {
  UpdateableSettings<T> s;
  return *s.get();
}

}} // namespace facebook::logdevice
