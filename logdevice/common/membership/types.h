/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace membership {

// membership state is versioned and any changes will cause a version
// bumps to the membership.
namespace MembershipVersion {
using Type = vcs_config_version_t;

// initial state of the membership, which is an empty set. The state is still
// considered as valid but will never get published to subscribers
constexpr Type EMPTY_VERSION{0};
// first non-empty valid membership version
constexpr Type MIN_VERSION{1};
} // namespace MembershipVersion

// membership changes are usually associated with maintenance events; we use the
// MaintenanceID type for identifying them
namespace MaintenanceID {
LOGDEVICE_STRONG_TYPEDEF(uint64_t, Type);

constexpr Type MAINTENANCE_NONE{0};
} // namespace MaintenanceID

// there should be one membership type for each NodeRole
// (defined in common/configuration/Node.h) that requires a membership
using MembershipType = configuration::nodes::NodeRole;

// a simple wrapper for a const iterator of a map type that only
// expose keys of the map
template <typename Map>
class ConstMapKeyIterator {
 public:
  using InnerIt = typename Map::const_iterator;
  explicit ConstMapKeyIterator(InnerIt&& it) : it_(std::move(it)) {}
  ConstMapKeyIterator& operator++() {
    it_++;
    return *this;
  }
  bool operator==(const ConstMapKeyIterator& rhs) const {
    return it_ == rhs.it_;
  }
  bool operator!=(const ConstMapKeyIterator& rhs) const {
    return !(*this == rhs);
  }
  typename Map::key_type operator*() {
    return it_->first;
  }

 private:
  InnerIt it_;
};

}}} // namespace facebook::logdevice::membership
