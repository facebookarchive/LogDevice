/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {
// scope in which command line parameters that applies to different
// types of nodes. Must be defined continuously.
enum class ParamScope : uint8_t { ALL = 0, SEQUENCER = 1, STORAGE_NODE = 2 };

using ParamValue = folly::Optional<std::string>;
using ParamMap = std::unordered_map<std::string, ParamValue>;
using ParamMaps = std::map<ParamScope, ParamMap>;

class ParamSpec {
 public:
  std::string key_;
  std::string value_;
  ParamScope scope_;

  /* implicit */ ParamSpec(std::string key, ParamScope scope = ParamScope::ALL)
      : ParamSpec(std::move(key), "true", scope) {}

  ParamSpec(std::string key,
            std::string value,
            ParamScope scope = ParamScope::ALL)
      : key_(key), value_(value), scope_(scope) {
    ld_check(!key_.empty());
  }
};

}}} // namespace facebook::logdevice::IntegrationTestUtils
