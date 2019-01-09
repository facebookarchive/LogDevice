/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/Utils.h"

namespace facebook { namespace logdevice { namespace ldquery {

std::string s(const folly::Optional<int>& val) {
  return s(val ? *val : 0);
}

std::string s(const bool& val) {
  return s((int)val & 1);
}

std::string s(const folly::Optional<std::chrono::seconds>& val) {
  return s(val ? val->count() : 0);
}

std::string s(const folly::Optional<std::chrono::milliseconds>& val) {
  return s(val ? val->count() : 0);
}

}}} // namespace facebook::logdevice::ldquery
