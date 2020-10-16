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

#include "logdevice/common/configuration/nodes/utils.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook::logdevice::configuration::nodes {

namespace {
template <typename F>
bool isFieldValid(const F& field, folly::StringPiece name) {
  if (!field.valid()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Invalid %s: %s.",
                    name.str().c_str(),
                    field.toString().c_str());
    return false;
  }
  return true;
}

template <typename F>
bool isMapFieldValid(const F& field, folly::StringPiece name) {
  for (const auto& [k, v] : field) {
    if (!isFieldValid(v, name)) {
      return false;
    }
  }
  return true;
}

template <typename F>
bool isOptionalFieldValid(const F& field, folly::StringPiece name) {
  return !field.hasValue() || isFieldValid(field.value(), name);
}

} // namespace

std::string NodeServiceDiscovery::networkPriorityToString(
    const NodeServiceDiscovery::ClientNetworkPriority& priority) {
  using ClientNetworkPriority = NodeServiceDiscovery::ClientNetworkPriority;
  switch (priority) {
    case ClientNetworkPriority::HIGH:
      return "H";
    case ClientNetworkPriority::MEDIUM:
      return "M";
    case ClientNetworkPriority::LOW:
      return "L";
  }
  ld_check(false);
  folly::assume_unreachable();
}

const Sockaddr& NodeServiceDiscovery::getGossipAddress() const {
  return gossip_address.has_value() ? gossip_address.value()
                                    : default_client_data_address;
}

bool NodeServiceDiscovery::isValid() const {
  if (!isFieldValid(
          default_client_data_address, "default_client_data_address") ||
      !isOptionalFieldValid(gossip_address, "gossip_address") ||
      !isOptionalFieldValid(ssl_address, "ssl_address") ||
      !isOptionalFieldValid(admin_address, "admin_address") ||
      !isOptionalFieldValid(
          server_to_server_address, "server_to_server_address") ||
      !isOptionalFieldValid(
          server_thrift_api_address, "server_thrift_api_address") ||
      !isOptionalFieldValid(
          client_thrift_api_address, "client_thrift_api_address") ||
      !isMapFieldValid(addresses_per_priority, "addresses_per_priority")) {
    return false;
  }

  if (roles.count() == 0) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "no role is set. expect at least one role.");
    return false;
  }

  if (!addresses_per_priority.empty()) {
    if (!addresses_per_priority.count(ClientNetworkPriority::MEDIUM)) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "If an address for any priority is defined, the config "
                      "should also contain the MEDIUM priority address.");
      return false;
    }
  }

  std::string name_invalid_reason;
  if (!name.empty() && !nodes::isValidServerName(name, &name_invalid_reason)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Invalid server name: %s: %s",
                    name.c_str(),
                    name_invalid_reason.c_str());
    return false;
  }

  // TODO(T44427489): Check that the name field is not empty.
  return true;
}

bool NodeServiceDiscovery::isValidForReset(
    const NodeServiceDiscovery& current) const {
  // The proposed version can't be less than the current one
  if (current.version > version) {
    ld_error("A node can't decrease its version. Current value: %lu, "
             "requested update: %lu",
             current.version,
             version);
    return false;
  }

  // Roles are immutable
  if (current.roles != roles) {
    ld_error("Node's roles are assumed to be immutable. Current value: '%s', "
             "requested update: '%s'",
             logdevice::toString(current.roles).c_str(),
             logdevice::toString(roles).c_str());
    return false;
  }

  // TODO Uncomment this after the SFZ location string migration has completed
  // fleetwide. This allows for a safe migration of the location string.
  // The CWS migration script safely starts a maintenance, stops the nodes, and
  // disables expands on a per scope basis. T76073606
  // // The node can't change its location if it's a storage node AND its
  // version
  // // remains the same.
  // // TODO(T57564225): Agree on location string update policy
  // if (current.version == version && current.location != location &&
  //     hasRole(NodeRole::STORAGE)) {
  //   ld_error(
  //       "Storage nodes' location is assumed to be immutable to maintain the "
  //       "correctness of the replication property of the historical nodesets.
  //       " "Current value: '%s', requested update: '%s'",
  //       current.location.has_value() ? current.location->toString().c_str()
  //                                    : "",
  //       location.has_value() ? location->toString().c_str() : "");
  //   return false;
  // }

  // If we reached this point, all the eligible fields can be mutated freely.
  return true;
}

std::string NodeServiceDiscovery::toString() const {
  std::vector<std::string> addresses_strs;
  for (const auto& [priority, sock_addr] : addresses_per_priority) {
    addresses_strs.push_back(folly::sformat(
        "{}:{}", networkPriorityToString(priority), sock_addr.toString()));
  }

  return folly::sformat(
      "[{} => "
      "A:{},G:{},S:{},AA:{},S2SA:{},STA:{},CTA:{},APNP:{{{}}},L:{},R:{},V:{},T:"
      "{}]",
      name,
      default_client_data_address.toString(),
      gossip_address.has_value() ? gossip_address->toString() : "",
      ssl_address.has_value() ? ssl_address->toString() : "",
      admin_address.has_value() ? admin_address->toString() : "",
      server_to_server_address.has_value()
          ? server_to_server_address->toString()
          : "",
      server_thrift_api_address.has_value()
          ? server_thrift_api_address->toString()
          : "",
      client_thrift_api_address.has_value()
          ? client_thrift_api_address->toString()
          : "",
      folly::join(",", addresses_strs),
      location.has_value() ? location->toString() : "",
      logdevice::toString(roles),
      version,
      logdevice::toString(tags));
}

namespace {
bool validateAddressUniqueness(ServiceDiscoveryConfig::MapType node_states) {
  std::unordered_map<Sockaddr, node_index_t, Sockaddr::Hash> seen_addresses;
  for (const auto& kv : node_states) {
    if (!kv.second.default_client_data_address.valid()) {
      // This should have been caught in a better check, but let's avoid
      // crashing in the following lines by returning false here.
      RATELIMIT_CRITICAL(
          std::chrono::seconds(10),
          5,
          "THIS IS A BUG: Got an invalid address for N%hd when validating "
          "the uniquness of the ServiceDiscovery addresses. This should have "
          "been caught in an earlier validation on the ServiceDiscoveryConfig "
          "struct.",
          kv.first);
      ld_assert(kv.second.default_client_data_address.valid());
      return false;
    }
    auto res =
        seen_addresses.emplace(kv.second.default_client_data_address, kv.first);
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

bool validateNameUniqueness(ServiceDiscoveryConfig::MapType node_states) {
  std::unordered_map<std::string, node_index_t> seen_names;
  for (const auto& kv : node_states) {
    if (kv.second.name.empty()) {
      continue;
    }
    auto res =
        seen_names.emplace(normalizeServerName(kv.second.name), kv.first);
    if (!res.second) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Multiple nodes with the same name '%s' (idx: %hd, %hd).",
                      kv.second.name.c_str(),
                      res.first->second,
                      kv.first);
      return false;
    }
  }
  return true;
}

} // namespace

template <>
bool ServiceDiscoveryConfig::attributeSpecificValidate() const {
  return validateAddressUniqueness(node_states_) &&
      validateNameUniqueness(node_states_);
}

} // namespace facebook::logdevice::configuration::nodes
