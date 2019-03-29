/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

include "logdevice/admin/if/common.thrift"

namespace cpp2 facebook.logdevice.thrift
namespace py3 logdevice.admin
namespace php LogDevice
namespace wiki Thriftdoc.LogDevice.Settings


/*
 * Source where a setting comes from
 */
enum SettingSource {
  CLI = 0,
  CONFIG = 1,
  ADMIN_OVERRIDE = 2,
}

/*
 * Settings structure, part of SettingsResponse
 */
struct Setting {
  /*
   * The currently applied setting value
   */
  1: string currentValue,
  /*
   * The default setting value
   */
  2: string defaultValue,
  /*
   * The setting as set by each SettingSource
   */
  3: map<SettingSource, string> sources,
}

struct SettingsResponse {
  1: map<string, Setting> settings,
}

struct SettingsRequest {
  /*
   * Get all settings if left empty
   */
  1: optional set<string> settings;
}
