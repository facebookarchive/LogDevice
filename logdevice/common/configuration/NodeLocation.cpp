/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/NodeLocation.h"

#include <algorithm>

#include <folly/String.h>

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

template <>
/* static */
const std::string& NodeLocation::ScopeNameEnumMap::invalidValue() {
  static const std::string invalidScopeName("UNKNOWN");
  return invalidScopeName;
}

template <>
void NodeLocation::ScopeNameEnumMap::setValues() {
#define NODE_LOCATION_SCOPE(c) set(NodeLocationScope::c, #c);
#include "logdevice/include/node_location_scopes.inc"
}

const NodeLocation::ScopeNameEnumMap& NodeLocation::scopeNames() {
  static ScopeNameEnumMap scope_names_;
  return scope_names_;
}

constexpr size_t NodeLocation::NUM_SCOPES;
constexpr const char* NodeLocation::DELIMITER;

size_t NodeLocation::effectiveScopes(NodeLocationScope scope) {
  if (scope == NodeLocationScope::ROOT) {
    return 0;
  }
  if (scope == NodeLocationScope::NODE) {
    return NUM_SCOPES;
  }
  return scopeToIndex(scope) + 1;
}

std::string
NodeLocation::getDomain(NodeLocationScope scope,
                        folly::Optional<node_index_t> node_idx) const {
  if (isEmpty() && !node_idx.hasValue()) {
    return "";
  }

  // how many scopes to look at
  const size_t effective_scopes = effectiveScopes(scope);
  ld_check(effective_scopes <= NUM_SCOPES);

  std::string result = folly::join(
      DELIMITER, labels_.begin(), labels_.begin() + effective_scopes);

  // complete the domain name by adding empty scope labels at the end
  for (size_t i = 0; i < NUM_SCOPES - std::max(effective_scopes, 1ul); ++i) {
    result += DELIMITER;
  }

  ld_assert(validDomain(result));

  if (scope == NodeLocationScope::NODE && node_idx.hasValue()) {
    result += ":N";
    result += std::to_string(node_idx.value());
  }

  return result;
}

bool NodeLocation::matchesPrefix(const std::string& prefix) const {
  const std::string domain = getDomain();
  if (domain == prefix) {
    return true;
  }
  std::string d = prefix;
  if (d.empty()) {
    return false;
  }
  if (d.back() != '.') {
    d.append(".");
  }
  return domain.size() >= d.size() &&
      std::equal(d.begin(), d.end(), domain.begin());
}

const std::string& NodeLocation::getLabel(NodeLocationScope scope) const {
  if (scope == NodeLocationScope::ROOT || scope == NodeLocationScope::NODE) {
    // empty label for special scopes
    static const std::string empty = "";
    return empty;
  }

  return labels_[scopeToIndex(scope)];
}

bool NodeLocation::sharesScopeWith(const NodeLocation& other_location,
                                   NodeLocationScope scope) const {
  if (scope == NodeLocationScope::ROOT) {
    // ROOT is always shared
    return true;
  }
  if (scope == NodeLocationScope::NODE) {
    // NODE is an implicit scope and not considered shared
    return false;
  }

  // how many scopes to look at
  const size_t effective_scopes = effectiveScopes(scope);
  ld_check(effective_scopes <= NUM_SCOPES);

  return std::equal(labels_.begin(),
                    labels_.begin() + effective_scopes,
                    other_location.labels_.begin());
}

NodeLocationScope
NodeLocation::closestSharedScope(const NodeLocation& other_location) const {
  size_t label_idx = 0;
  for (; label_idx < NUM_SCOPES; label_idx++) {
    if (labels_[label_idx].empty() ||
        labels_[label_idx] != other_location.labels_[label_idx]) {
      break;
    }
  }

  size_t scope_idx = static_cast<size_t>(NodeLocationScope::ROOT) - label_idx;
  return static_cast<NodeLocationScope>(scope_idx);
}

int NodeLocation::fromDomainString(const std::string& str) {
  // clear the internal state first
  reset();

  if (str.empty()) {
    err = E::INVALID_PARAM;
    return -1;
  }

  std::vector<std::string> tokens;
  folly::split(DELIMITER, str, tokens, /* ignoreEmpty */ false);
  if (tokens.size() != NUM_SCOPES) {
    ld_error("Wrong number of scopes in location domain string: \"%s\". Got: "
             "%lu, required: %lu",
             str.c_str(),
             tokens.size(),
             NUM_SCOPES);
    err = E::INVALID_PARAM;
    return -1;
  }

  size_t n_tokens = 0;
  for (; n_tokens < NUM_SCOPES; ++n_tokens) {
    if (tokens[n_tokens].empty()) {
      break;
    }
    labels_[n_tokens] = tokens[n_tokens];
  }

  if (std::any_of(tokens.begin() + n_tokens,
                  tokens.end(),
                  [](const std::string& s) { return !s.empty(); })) {
    ld_error("non-empty label exists after an empty label in location "
             "domain string: \"%s\"",
             str.c_str());
    err = E::INVALID_PARAM;
    return -1;
  }

  num_specified_ = n_tokens;
  return 0;
}

}} // namespace facebook::logdevice
