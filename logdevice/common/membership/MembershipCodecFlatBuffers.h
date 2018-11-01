/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/membership/Membership.h"
#include "logdevice/common/membership/MembershipCodec_generated.h"
#include "logdevice/common/membership/SequencerMembership.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace membership {

class MembershipCodecFlatBuffers {
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

  static flatbuffers::Offset<flat_buffer_codec::StorageMembership>
  serialize(flatbuffers::FlatBufferBuilder& b,
            const StorageMembership& storage_membership);

  static std::shared_ptr<StorageMembership>
  deserialize(const flat_buffer_codec::StorageMembership* storage_membership);

  static flatbuffers::Offset<flat_buffer_codec::SequencerMembership>
  serialize(flatbuffers::FlatBufferBuilder& b,
            const SequencerMembership& sequencer_membership);

  static std::shared_ptr<SequencerMembership> deserialize(
      const flat_buffer_codec::SequencerMembership* sequencer_membership);

 private:
  static ShardState
  deserialize(const flat_buffer_codec::ShardState* shard_state);
};

}}} // namespace facebook::logdevice::membership
