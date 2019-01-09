/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/PrincipalsConfig.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/SecurityInformation.h"

namespace facebook { namespace logdevice { namespace configuration {

folly::dynamic PrincipalsConfig::toFollyDynamic() const {
  folly::dynamic principal_list = folly::dynamic::array;
  for (const auto& kv : principals) {
    principal_list.push_back(kv.second->toFollyDynamic());
  }
  return principal_list;
}

std::shared_ptr<const Principal>
PrincipalsConfig::getPrincipalByName(const std::string* name) const {
  if (name != nullptr) {
    auto it = principals.find(*name);
    if (it != principals.end()) {
      return it->second;
    }
  }
  err = E::NOTFOUND;
  return nullptr;
}

}}} // namespace facebook::logdevice::configuration
