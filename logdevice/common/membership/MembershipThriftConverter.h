/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/membership/Membership.h"
#include "logdevice/common/membership/SequencerMembership.h"
#include "logdevice/common/membership/StorageMembership.h"
#include "logdevice/common/membership/gen-cpp2/Membership_types.h"

namespace facebook { namespace logdevice { namespace membership {

class MembershipThriftConverter {
 public:
  using ProtocolVersion = uint32_t;

  // Will be prepended to the serialized membership for forward and
  // backward compatibility;
  //
  // Note: flatbuffers handles backward and forward compatibility of
  // add/deprecate fields in tables. This version is only needed when extra
  // compatibility handling (e.g., adding a new enum value of an existing enum
  // class) is needed.
  static constexpr ProtocolVersion CURRENT_PROTO_VERSION = 1;

  static thrift::StorageMembership
  toThrift(const StorageMembership& storage_membership);

  static std::shared_ptr<StorageMembership>
  fromThrift(const thrift::StorageMembership& storage_membership);

  static thrift::SequencerMembership
  toThrift(const SequencerMembership& sequencer_membership);

  static std::shared_ptr<SequencerMembership>
  fromThrift(const thrift::SequencerMembership& sequencer_membership);

 private:
  static ShardState fromThrift(const thrift::ShardState& shard_state);
};

}}} // namespace facebook::logdevice::membership
