/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cassert>
#include <string>

namespace facebook { namespace logdevice {

enum class NodeSetSelectorType : uint8_t {
  INVALID = 0,
  SELECT_ALL,
  SELECT_ALL_SHARDS,
  RANDOM,
  RANDOM_CROSSDOMAIN,
  RANDOM_V2,
  RANDOM_CROSSDOMAIN_V2,
  WEIGHT_AWARE,
  WEIGHT_AWARE_V2,
  CONSISTENT_HASHING,
  CONSISTENT_HASHING_V2,
};

// Deserializes the string representation of nodeset selector type (found in
// configuration) to a NodeSetSelectorType value
inline NodeSetSelectorType
NodeSetSelectorTypeFromString(const std::string& str) {
  if (str == "select-all") {
    return NodeSetSelectorType::SELECT_ALL;
  } else if (str == "select-all-shards") {
    return NodeSetSelectorType::SELECT_ALL_SHARDS;
  } else if (str == "random") {
    return NodeSetSelectorType::RANDOM;
  } else if (str == "random-crossdomain") {
    return NodeSetSelectorType::RANDOM_CROSSDOMAIN;
  } else if (str == "random-v2") {
    return NodeSetSelectorType::RANDOM_V2;
  } else if (str == "random-crossdomain-v2") {
    return NodeSetSelectorType::RANDOM_CROSSDOMAIN_V2;
  } else if (str == "weight-aware") {
    return NodeSetSelectorType::WEIGHT_AWARE;
  } else if (str == "weight-aware-v2") {
    return NodeSetSelectorType::WEIGHT_AWARE_V2;
  } else if (str == "consistent-hashing") {
    return NodeSetSelectorType::CONSISTENT_HASHING;
  } else if (str == "consistent-hashing-v2") {
    return NodeSetSelectorType::CONSISTENT_HASHING_V2;
  }
  return NodeSetSelectorType::INVALID;
}

// Serializes the NodeSetSelectorType value into a string (used to serialize
// configuration)
inline std::string NodeSetSelectorTypeToString(NodeSetSelectorType t) {
  switch (t) {
    case NodeSetSelectorType::SELECT_ALL:
      return "select-all";
    case NodeSetSelectorType::SELECT_ALL_SHARDS:
      return "select-all-shards";
    case NodeSetSelectorType::RANDOM:
      return "random";
    case NodeSetSelectorType::RANDOM_CROSSDOMAIN:
      return "random-crossdomain";
    case NodeSetSelectorType::RANDOM_V2:
      return "random-v2";
    case NodeSetSelectorType::RANDOM_CROSSDOMAIN_V2:
      return "random-crossdomain-v2";
    case NodeSetSelectorType::WEIGHT_AWARE:
      return "weight-aware";
    case NodeSetSelectorType::WEIGHT_AWARE_V2:
      return "weight-aware-v2";
    case NodeSetSelectorType::CONSISTENT_HASHING:
      return "consistent-hashing";
    case NodeSetSelectorType::CONSISTENT_HASHING_V2:
      return "consistent-hashing-v2";
    case NodeSetSelectorType::INVALID:
      // This is not serializable to a string
      break;
  }
  ld_check(false);
  return "";
}

}} // namespace facebook::logdevice
