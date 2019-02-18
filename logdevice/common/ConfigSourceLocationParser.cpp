/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/ConfigSourceLocationParser.h"

#include "logdevice/common/ConfigSource.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {
constexpr folly::StringPiece
    ConfigSourceLocationParser::kLocationSchemeDelimiter;

std::pair<ConfigSource*, std::string> ConfigSourceLocationParser::parse(
    std::vector<std::unique_ptr<ConfigSource>>& sources,
    const std::string& location) {
  size_t pos = location.find(kLocationSchemeDelimiter.toString());
  std::string scheme, path;
  if (pos == std::string::npos) {
    scheme = "";
    path = location;
  } else {
    scheme = location.substr(0, pos);
    path = location.substr(pos + kLocationSchemeDelimiter.size());
  }

  // Look for an appropriate source
  for (const auto& source : sources) {
    for (const auto& source_scheme : source->getSchemes()) {
      if (source_scheme == scheme) {
        // Success!  This source is registered for the location's scheme.
        return std::make_pair(source.get(), std::move(path));
      }
    }
  }

  ld_error(
      "Unable to parse config location \"%s\": no config source is registered "
      "for scheme \"%s\"",
      location.c_str(),
      scheme.c_str());
  return std::make_pair(nullptr, std::string());
}

}} // namespace facebook::logdevice
