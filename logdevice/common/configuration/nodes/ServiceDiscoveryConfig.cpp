/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "ServiceDiscoveryConfig.h"

#include <folly/Range.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

namespace {
template <typename F>
bool isFieldValid(const F& field, folly::StringPiece name) {
  if (!field.valid()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10), 5, "invalid %s.", name.str().c_str());
    return false;
  }
  return true;
}

template <typename F>
bool isOptionalFieldValid(const F& field, folly::StringPiece name) {
  return !field.hasValue() || isFieldValid(field.value(), name);
}

} // namespace

bool NodeServiceDiscovery::isValid() const {
  if (!isFieldValid(address, "address") &&
      !isFieldValid(gossip_address, "gossip_address") &&
      !isOptionalFieldValid(ssl_address, "ssl_address") &&
      !isOptionalFieldValid(admin_address, "admin_address")) {
    return false;
  }

  if (roles.count() == 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "no role is set. expect at least one role.");
    return false;
  }

  return true;
}

template <>
bool ServiceDiscoveryConfig::attributeSpecificValidate() const {
  // check for address duplication in service discovery
  std::unordered_map<Sockaddr, node_index_t, Sockaddr::Hash> seen_addresses;
  for (const auto& kv : node_states_) {
    auto res =
        seen_addresses.insert(std::make_pair(kv.second.address, kv.first));
    if (!res.second) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Multiple nodes with the same address (idx: %hd, %hd).",
                      res.first->second,
                      kv.first);
      return false;
    }
  }

  return true;
}

}}}} // namespace facebook::logdevice::configuration::nodes
