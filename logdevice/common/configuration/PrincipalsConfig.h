/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "logdevice/include/Err.h"

namespace folly {
struct dynamic;
}

namespace facebook { namespace logdevice {

class Principal;

namespace configuration {

struct PrincipalsConfig {
  folly::dynamic toFollyDynamic() const;

  std::shared_ptr<const Principal> getPrincipalByName(const std::string*) const;

  std::unordered_map<std::string, std::shared_ptr<const Principal>> principals;
};

} // namespace configuration
}} // namespace facebook::logdevice
