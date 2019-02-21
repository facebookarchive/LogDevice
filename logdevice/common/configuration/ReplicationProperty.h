/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <array>
#include <vector>

#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

namespace logsconfig {
class LogAttributes;
}

class ReplicationProperty {
 public:
  using ScopeReplication = std::pair<NodeLocationScope, int>;

  struct OldRepresentation {
    copyset_size_t replication_factor{};
    NodeLocationScope sync_replication_scope{NodeLocationScope::INVALID};

    OldRepresentation() = default;
    OldRepresentation(copyset_size_t r, NodeLocationScope s)
        : replication_factor(r), sync_replication_scope(s) {}
  };

  ReplicationProperty() = default;

  ReplicationProperty(size_t replication,
                      NodeLocationScope sync_replication_scope) {
    if (replication == 1 && sync_replication_scope != NodeLocationScope::NODE) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "Replication factor is 1 while sync replication scope "
          "is %s, ignoring sync replication scope and considering"
          " the scope as NODE.",
          NodeLocation::scopeNames()[sync_replication_scope].c_str());
      setReplication(NodeLocationScope::NODE, 1);
    } else {
      setReplication(NodeLocationScope::NODE, replication);
      if (sync_replication_scope != NodeLocationScope::NODE) {
        setReplication(sync_replication_scope, 2);
      }
    }

    if (!isValid()) {
      ld_critical("Invalid replication property appears to have made it past "
                  "validation. This is a bug. Assertion failure in 3..2..1..");
      ld_check(false);
    }
  }

  explicit ReplicationProperty(std::vector<ScopeReplication> scopes) {
    int rv = assign(std::move(scopes));
    if (rv != 0) {
      ld_critical("Invalid replication property appears to have made it past "
                  "validation. This is a bug. Assertion failure in 3..2..1..");
      ld_check(false);
    }
  }

  explicit ReplicationProperty(std::initializer_list<ScopeReplication> il)
      : ReplicationProperty(
            std::vector<ScopeReplication>(il.begin(), il.end())) {}

  static ReplicationProperty
  fromLogAttributes(const logsconfig::LogAttributes& log);

  // Returns 0 if log attributes define a valid replication property.
  // Otherwise sets *out_error to a human-readable error string and returns -1.
  // Only validates the replication-related things: replicationFactor,
  // syncReplicationScope, replicateAcross, extraCopies, syncedCopies.
  static int validateLogAttributes(const logsconfig::LogAttributes& log,
                                   std::string* out_error = nullptr);

  bool isValid() const {
    size_t cur = std::numeric_limits<size_t>::max();
    for (auto scope = NodeLocationScope::NODE; scope < NodeLocationScope::ROOT;
         scope = NodeLocation::nextGreaterScope(scope)) {
      const size_t r = getReplication(scope);
      if (r == 0) {
        continue;
      }
      if (cur < r) {
        RATELIMIT_DEBUG(std::chrono::seconds(10),
                        10,
                        "Replication for scope %s is %lu but a lower scope "
                        "has replication %lu",
                        NodeLocation::scopeNames()[scope].c_str(),
                        r,
                        cur);
        return false;
      }
      cur = r;
    }
    if (cur == std::numeric_limits<size_t>::max()) {
      RATELIMIT_DEBUG(std::chrono::seconds(10),
                      10,
                      "Need to specify replication for at least one scope");
      return false;
    }

    return true;
  }

  // check if the object is empty like just default initialized.
  // `empty' is considered as a special invalid state.
  bool isEmpty() const {
    return replication_ == ReplicationProperty().replication_;
  }

  void setReplication(NodeLocationScope scope, size_t replication) {
    ld_check(scope < NodeLocationScope::ROOT);
    ld_check(replication > 0);
    replication_[scopeToInt(scope)] = replication;
  }

  int getReplication(NodeLocationScope scope) const {
    return replication_[scopeToInt(scope)];
  }

  int getReplicationFactor() const {
    for (auto scope = NodeLocationScope::NODE; scope < NodeLocationScope::ROOT;
         scope = NodeLocation::nextGreaterScope(scope)) {
      const int r = getReplication(scope);
      if (r != 0) {
        return r;
      }
    }
    ld_check(false);
    return 0;
  }

  void clear() {
    replication_.fill(0);
  }

  // @return  0 if success, -1 if error.
  int assign(std::vector<ScopeReplication> scopes) {
    std::sort(scopes.begin(), scopes.end());
    NodeLocationScope prev = NodeLocationScope::INVALID;
    for (auto p : scopes) {
      if (p.first == prev || p.first >= NodeLocationScope::ROOT ||
          p.second <= 0) {
        clear();
        return -1;
      }
      prev = p.first;

      setReplication(p.first, p.second);
    }
    return isValid() ? 0 : -1;
  }

  // Returns a vector of all different replication factors with their highest
  // scopes. Returned vector is in order of increasing replication factor.
  // Example: {{REGION, 2}, {RACK, 3}, {NODE, 4}}, which means "replicate to
  // at least 4 nodes, spanning at least 3 racks, spanning at least 2 regions".
  //
  // There's a subtle special case with replication factor 1: the biggest
  // scope may have replication factor 1 even if it's not NODE.
  // E.g. {{RACK, 1}} and {{REGION, 1}, {NODE, 3}} are allowed and
  // considered different from {{NODE, 1}} and {{NODE, 3}} respectively.
  // There's no difference for the read side, but on the write side this
  // distinction is (ab)used for controlling sequencer locality in
  // nodeset and copyset selectors. If sequencer locality is enabled,
  // {{RACK, 3}} means "copyset must span 3 racks, and it's
  // preferable that one of these racks is the rack where sequencer is", while
  // {{REGION, 1}, {RACK, 3}} means "copyset must span 3 racks, and it's
  // preferable that these racks are in the same region as the sequencer
  // (but it doesn't matter whether the sequencer is in one of these racks or
  // not)".
  std::vector<ScopeReplication> getDistinctReplicationFactors() const {
    if (isEmpty()) {
      // allow normalizing an invalid object only in empty state
      return {};
    }

    if (!isValid()) {
      ld_critical("Invalid replication property appears to have made it past "
                  "validation. This is a bug. Assertion failure in 3..2..1..");
      ld_check(false);
      return {};
    }
    std::vector<ScopeReplication> res;
    int prev = 0;
    for (auto scope = NodeLocation::nextSmallerScope(NodeLocationScope::ROOT);
         scope != NodeLocationScope::INVALID;
         scope = NodeLocation::nextSmallerScope(scope)) {
      const int r = getReplication(scope);
      if (r == 0 || r == prev) {
        continue;
      }
      ld_check(r > prev);
      prev = r;

      res.emplace_back(scope, r);
    }
    return res;
  }

  // Returns the biggest scope with replication factor >= 2. If there are none
  // (i.e. the total replication factor is 1), returns NODE.
  // E.g. {{RACK, 2}, {NODE, 3}} => RACK,
  //      {{NODE, 1}} => NODE
  NodeLocationScope getBiggestReplicationScope() const {
    auto replicationFactors = getDistinctReplicationFactors();
    if (replicationFactors.size() == 0) {
      return NodeLocationScope::NODE;
    }
    return replicationFactors[0].first;
  }

  // Returns the "minimum" of the two replication properties. More precisely,
  // returns a replication property that is satisfied by every copyset
  // that satisfies at least one of the two properties {*this, rhs}.
  // E.g.:
  //  * {node: 3}, {node: 2}  =>  {node: 2}
  //  * {node: 3, rack: 2}, {node: 4}  =>  {node: 3}
  //  * {rack: 3}, {region: 2}  => {rack: 2}
  ReplicationProperty narrowest(const ReplicationProperty& rhs) const {
    // if both are empty, return either.
    if (isEmpty() && rhs.isEmpty()) {
      return *this;
    }
    // if any is invalid, return the valid.
    if (!rhs.isValid() && isValid()) {
      return *this;
    } else if (!isValid() && rhs.isValid()) {
      return rhs;
    }
    // both has to be valid.
    ld_check(isValid());
    ld_check(rhs.isValid());

    // Fill in the missing replication factors in both *this and rhs
    // (e.g. turn {0,2,0,0,3,0} into {0,2,2,2,3,3}), then take
    // elementwise minimum.
    ReplicationProperty res;
    int cur[2] = {};
    int prev = 0;
    for (auto scope = NodeLocation::nextSmallerScope(NodeLocationScope::ROOT);
         scope != NodeLocationScope::INVALID;
         scope = NodeLocation::nextSmallerScope(scope)) {
      int r[2] = {getReplication(scope), rhs.getReplication(scope)};
      for (int i = 0; i < 2; ++i) {
        if (r[i] != 0) {
          ld_check(r[i] >= cur[i]);
          cur[i] = r[i];
        }
      }

      int mn = std::min(cur[0], cur[1]);
      if (mn != prev) {
        // minimum of two nondecreasing sequences is nondecreasing
        ld_check(mn > prev);
        res.setReplication(scope, mn);
        prev = mn;
      }
    }

    ld_check(res.isValid());
    return res;
  }

  // Returns folly::none if this replication property can't be represented
  // in the more restrictive old form.
  folly::Optional<OldRepresentation> toOldRepresentation() const {
    ld_check(isValid());
    auto r = getDistinctReplicationFactors();
    ld_check(r.size() >= 1);
    if (r[0].first == NodeLocationScope::NODE || // [node: <R>]
        r.back().second == 1) {                  // [<scope>: 1]
      // Cross-domain replication disabled.
      ld_check(r.size() == 1);
      return OldRepresentation(r[0].second, NodeLocationScope::NODE);
    } else if (r[0].second == 2 &&
               (r.size() == 1 || // [<scope>: 2]
                r[1].first ==
                    NodeLocationScope::NODE)) { // [<scope>: 2, node: <R>]
      // Cross-domain replication enabled.
      ld_check(r.size() <= 2);
      return OldRepresentation(r.back().second, r[0].first);
    } else {
      return folly::none;
    }
  }

  std::string toString() const {
    std::string res = "[";
    for (auto p : getDistinctReplicationFactors()) {
      if (res.size() > 1) {
        res += ", ";
      }
      std::string scope = NodeLocation::scopeNames()[p.first];
      std::transform(scope.begin(), scope.end(), scope.begin(), ::tolower);
      res += scope + ": " + std::to_string(p.second);
    }
    res += "]";
    return res;
  }

  bool operator==(const ReplicationProperty& rhs) const {
    return getDistinctReplicationFactors() ==
        rhs.getDistinctReplicationFactors();
  }
  bool operator!=(const ReplicationProperty& rhs) const {
    return !(*this == rhs);
  }

 private:
  // Set to 0 for a scope if there is no replication constraint for it.
  std::array<int, NodeLocation::NUM_ALL_SCOPES - 1> replication_{};

  static int scopeToInt(NodeLocationScope scope) {
    return static_cast<int>(scope);
  }
};

}} // namespace facebook::logdevice
