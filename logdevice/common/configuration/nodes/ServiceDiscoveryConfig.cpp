/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"

#include <folly/Format.h>
#include <folly/Range.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

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
      !isOptionalFieldValid(ssl_address, "ssl_address")) {
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

std::string NodeServiceDiscovery::toString() const {
  return folly::sformat("[A:{},G:{},S:{},L:{},R:{},H:{}]",
                        address.toString(),
                        gossip_address.toString(),
                        ssl_address.hasValue() ? ssl_address->toString() : "",
                        location.hasValue() ? location->toString() : "",
                        logdevice::toString(roles),
                        hostname);
}

const Sockaddr&
NodeServiceDiscovery::getSockaddr(SocketType type,
                                  ConnectionType conntype) const {
  switch (type) {
    case SocketType::GOSSIP:
      return gossip_address;

    case SocketType::DATA:
      if (conntype == ConnectionType::SSL) {
        if (!ssl_address.hasValue()) {
          return Sockaddr::INVALID;
        }
        return ssl_address.value();
      } else {
        return address;
      }

    default:
      RATELIMIT_CRITICAL(
          std::chrono::seconds(1), 2, "Unexpected Socket Type:%d!", (int)type);
      ld_check(false);
  }

  return Sockaddr::INVALID;
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
