/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace membership {

// membership state is versioned and any changes will cause a version
// bumps to the membership.
namespace MembershipVersion {
LOGDEVICE_STRONG_TYPEDEF(uint64_t, Type);

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
// the very first maintenance operation that creates the new cluster
constexpr Type MAINTENANCE_PROVISION{1};

} // namespace MaintenanceID

// there should be one membership type for each NodeRole
// (defined in common/configuration/Node.h) that requires a membership
enum class MembershipType { SEQUENCER, STORAGE };

}}} // namespace facebook::logdevice::membership
