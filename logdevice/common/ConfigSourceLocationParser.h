/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <vector>

#include <folly/Range.h>

#include "logdevice/common/ConfigSource.h"

namespace facebook { namespace logdevice {

class ConfigSourceLocationParser {
 public:
  constexpr static folly::StringPiece kLocationSchemeDelimiter =
      folly::StringPiece(":");

  // Parses a location of the form "scheme:path" and finds the appropriate
  // config source from the passed sources. If none is found, returns nullptr.
  static std::pair<ConfigSource*, std::string>
  parse(std::vector<std::unique_ptr<ConfigSource>>& sources,
        const std::string& location);
};

}} // namespace facebook::logdevice
