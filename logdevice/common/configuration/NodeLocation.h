/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <string>

#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/include/EnumMap.h"
#include "logdevice/include/NodeLocationScope.h"

namespace facebook { namespace logdevice {

/**
 *  NodeLocation class stores location information of non-special scopes in
 *  range (NODE, ROOT) exclusive. Identifies some domain, by giving a path
 *  from root to the domain.
 *
 *  It's a vector of labels for scopes from biggest to smallest.
 *  E.g. {"ash", ash1", "21", "L", "hi"} identifies rack "hi" in row "L"
 *  in cluster "21" in datacenter "ash1" region "ash1".
 *  The vector always starts at the biggest possible (non-ROOT) scope,
 *  but doesn't necessarily go all the way to the smallest (non-NODE) scope.
 *  E.g. {"ash1", "21"} is a valid NodeLocation, and it identifies
 *  cluster "21" in region "ash1".
 */
class NodeLocation {
 public:
  // number of non-special location scopes stored in the object
  // (excluding ROOT and NODE)
  static constexpr size_t NUM_SCOPES =
      static_cast<size_t>(NodeLocationScope::ROOT) - 1;

  // number of all location scopes including NODE and ROOT
  static constexpr size_t NUM_ALL_SCOPES =
      static_cast<size_t>(NodeLocationScope::ROOT) + 1;

  // returns the next bigger scope, INVALID if the given scope
  // is ROOT or already outside the enum's range.
  static NodeLocationScope nextGreaterScope(NodeLocationScope scope) {
    ld_check(scope <= NodeLocationScope::ROOT);
    return scope >= NodeLocationScope::ROOT
        ? NodeLocationScope::INVALID
        : static_cast<NodeLocationScope>(static_cast<size_t>(scope) + 1);
  }

  // returns the next smaller scope, INVALID if the given scope
  // is NODE or already out of the enum's range
  static NodeLocationScope nextSmallerScope(NodeLocationScope scope) {
    ld_check(scope <= NodeLocationScope::ROOT);
    return (scope == NodeLocationScope::NODE || scope > NodeLocationScope::ROOT)
        ? NodeLocationScope::INVALID
        : static_cast<NodeLocationScope>(static_cast<size_t>(scope) - 1);
  }

  // provide the human-readable name of the scope
  using ScopeNameEnumMap = EnumMap<NodeLocationScope,
                                   std::string,
                                   NodeLocationScope::INVALID,
                                   NodeLocation::NUM_ALL_SCOPES>;

  static const ScopeNameEnumMap& scopeNames();

  // delimiter for location scopes in a location domain string
  static constexpr const char* DELIMITER = ".";

  /**
   * Fill the NodeLocation object from the given location domain string in which
   * labels of each location scope are specified.
   *
   * Definition:
   * Location Domain: a string that identifies the location of a node, it
   *                  consists of labels of all location scopes, separated by
   *                  DELIMITER.
   * Label:           part of the location domain string that belongs to
   *                  one location scope.
   *
   * Notes: the location domain string must have all NUM_SCOPES separated by
   *        DELIMITER. The left most scope must be the biggest scope defined
   *        (excluding ROOT). An empty label is allowed, meaning that location
   *        for the scope and all subscopes are not specified.
   *
   * Legit domain string examples:
   *               "ash.2.08.k.z", "ash.2.08..", "ash....", "...."
   * Invalid examples:
   *               "ash", "ash...", "ash.....", "ash.2.08..z"
   *
   * @return       0 success, -1 for failure and state of the object is
   *                          undefined. err is set to
   *               INVALID_PARAM    input str is not a valid domain string
   */
  int fromDomainString(const std::string& str);

  /**
   * Given a scope specified in @param scope, returns a domain string that
   * identifies the location of the node. The domain string will include the
   * name for the specified scope as well as names for all parent scopes.
   * E.g. a possible output for scope CLUSTER is "ash.ash1.21..".
   * If @param node_idx is folly::none, scope NODE is treated differently:
   * it's equivalent to RACK; i.e. the returned string will identify a rack,
   * not node. If @param node_idx is givem, and scope is NODE, the returned
   * string will be identify a node, e.g. "ash.ash1.21.L.hi:N42" or "....:N42".
   */
  std::string
  getDomain(NodeLocationScope scope = NodeLocationScope::NODE,
            folly::Optional<node_index_t> node_idx = folly::none) const;

  /**
   * @return true if this node location matches the given string prefix.
   */
  bool matchesPrefix(const std::string& prefix) const;

  /**
   * @return  the label of the specific scope for the NodeLocation object
   */
  const std::string& getLabel(NodeLocationScope scope) const;

  /**
   * @return  a boolean value indicating if the node location is under the same
   *          @param scope with the other node of @param other_location
   *          Note that NodeLocationScope::ROOT is always considered shared
   *          with any locations, while NodeLocationScope::NODE is always not
   *          shared.
   */
  bool sharesScopeWith(const NodeLocation& other_location,
                       NodeLocationScope scope) const;

  /**
   * @return The most specific scope shared between this node location
   *         and other_location.
   *
   * @note Since NodeLocation does not include node id, this routine will
   *       return NodeLocationScope::RACK even if other_location was taken
   *       from the same node as "this" location. Callers concerned about
   *       NodeLocationScope::NODE should check for a "same node" condition
   *       before calling this method.
   */
  NodeLocationScope closestSharedScope(const NodeLocation& other) const;

  bool isEmpty() const {
    return num_specified_ == 0;
  }

  /**
   * @return  number of non-empty scopes specified for the location
   */
  size_t numScopes() const {
    return num_specified_;
  }

  /**
   * @return  the smallest specified scope for the location
   */
  NodeLocationScope lastScope() const {
    ld_check(num_specified_ <= NUM_SCOPES);
    return static_cast<NodeLocationScope>(
        static_cast<size_t>(NodeLocationScope::ROOT) - num_specified_);
  }

  /**
   * @return  a boolean value indicating if the location has a non-empty
   *          label at the given scope
   */
  bool scopeSpecified(NodeLocationScope scope) const {
    if (scope == NodeLocationScope::NODE || scope == NodeLocationScope::ROOT) {
      return false;
    }
    return scopeToIndex(scope) < num_specified_;
  }

  // reset the NodeLocation object to the empty state
  void reset() {
    labels_.fill("");
    num_specified_ = 0;
  }

  // @return   a std::string of the normalized location domain name
  std::string toString() const {
    return getDomain();
  }

  // check if a given domain string is a valid one
  static bool validDomain(const std::string& location_str) {
    NodeLocation tmp;
    return tmp.fromDomainString(location_str) == 0;
  }

  bool operator==(const NodeLocation& rhs) const {
    return labels_ == rhs.labels_ && num_specified_ == rhs.num_specified_;
  }

 private:
  // internal storage for labels of all scopes, the order of scopes in the array
  // is the reverse order of their type value: largest scope gets stored at
  // index 0, and so on...
  std::array<std::string, NUM_SCOPES> labels_;

  // number of (non-empty) scope labels specified
  size_t num_specified_{0};

  // helper function that returns the label index of the given scope in
  // labels_ vector
  static size_t scopeToIndex(NodeLocationScope scope) {
    ld_check(scope != NodeLocationScope::ROOT &&
             scope != NodeLocationScope::NODE);
    ld_check(static_cast<size_t>(scope) >= 1 &&
             static_cast<size_t>(scope) <= NUM_SCOPES);
    return NUM_SCOPES - static_cast<size_t>(scope);
  }

  // given a scope, return how many non-special scopes are along the hierarchy
  // that identifies the scope
  static size_t effectiveScopes(NodeLocationScope scope);
};

}} // namespace facebook::logdevice
