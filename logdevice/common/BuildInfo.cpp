/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/BuildInfo.h"

#include <folly/dynamic.h>
#include <folly/json.h>

namespace facebook { namespace logdevice {

std::string BuildInfo::getBuildInfoJson() {
  folly::dynamic json(folly::dynamic::object);
  addBuildJsonFields(json);
  return folly::toJson(json);
}

void BuildInfo::addFieldIfNotEmpty(folly::dynamic& json,
                                   const char* name,
                                   const std::string& value) {
  if (value.length() > 0 && !value.empty()) {
    json[name] = value;
  }
}

void BuildInfo::addUIntIfNotZero(folly::dynamic& json,
                                 const char* name,
                                 const uint64_t value) {
  if (value) {
    json[name] = value;
  }
}

void BuildInfo::addBuildJsonFields(folly::dynamic& json) {
  addFieldIfNotEmpty(json, BUILD_USER_KEY, buildUser());
  addFieldIfNotEmpty(json, BUILD_PACKAGE_NAME_KEY, packageName());
  addFieldIfNotEmpty(json, BUILD_PACKAGE_VERSION, packageVersion());
  addFieldIfNotEmpty(json, BUILD_REVISION_KEY, buildRevision());
  addFieldIfNotEmpty(
      json, BUILD_UPSTREAM_REVISION_KEY, buildUpstreamRevision());
  addUIntIfNotZero(json, BUILD_REVISION_TIME_KEY, buildRevisionTime());
  addUIntIfNotZero(json, BUILD_TIME_KEY, buildTime());
  addUIntIfNotZero(
      json, BUILD_UPSTREAM_REVISION_TIME_KEY, buildUpstreamRevisionTime());
}

}} // namespace facebook::logdevice
