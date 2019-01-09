/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/membership/MembershipCodecFlatBuffers.h"

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace membership {

constexpr MembershipCodecFlatBuffers::ProtocolVersion
    MembershipCodecFlatBuffers::CURRENT_PROTO_VERSION;

static_assert(static_cast<uint32_t>(flat_buffer_codec::StorageState::NONE) ==
                  static_cast<uint32_t>(StorageState::NONE),
              "");
static_assert(static_cast<uint32_t>(flat_buffer_codec::StorageState::INVALID) ==
                  static_cast<uint32_t>(StorageState::INVALID),
              "");
static_assert(
    static_cast<uint32_t>(flat_buffer_codec::MetaDataStorageState::NONE) ==
        static_cast<uint32_t>(MetaDataStorageState::NONE),
    "");
static_assert(
    static_cast<uint32_t>(flat_buffer_codec::MetaDataStorageState::INVALID) ==
        static_cast<uint32_t>(MetaDataStorageState::INVALID),
    "");

/* static */
flatbuffers::Offset<flat_buffer_codec::StorageMembership>
MembershipCodecFlatBuffers::serialize(
    flatbuffers::FlatBufferBuilder& b,
    const StorageMembership& storage_membership) {
  std::vector<flatbuffers::Offset<flat_buffer_codec::StorageNodeState>>
      node_states;
  for (const auto& node_kv : storage_membership.node_states_) {
    std::vector<flatbuffers::Offset<flat_buffer_codec::ShardState>>
        shard_states;
    for (const auto& shard_kv : node_kv.second.shard_states) {
      const auto& shard_state = shard_kv.second;
      shard_states.push_back(flat_buffer_codec::CreateShardState(
          b,
          shard_kv.first,
          static_cast<flat_buffer_codec::StorageState>(
              shard_state.storage_state),
          shard_state.flags,
          static_cast<flat_buffer_codec::MetaDataStorageState>(
              shard_state.metadata_state),
          shard_state.active_maintenance.val(),
          shard_state.since_version.val()));
    }

    node_states.push_back(flat_buffer_codec::CreateStorageNodeState(
        b, node_kv.first, b.CreateVector(shard_states)));
  }

  std::vector<flat_buffer_codec::ShardID> metadata_shards;
  for (const auto meta_shard : storage_membership.metadata_shards_) {
    metadata_shards.push_back(
        flat_buffer_codec::ShardID(meta_shard.node(), meta_shard.shard()));
  }

  return flat_buffer_codec::CreateStorageMembership(
      b,
      CURRENT_PROTO_VERSION,
      storage_membership.getVersion().val(),
      b.CreateVector(node_states),
      b.CreateVectorOfStructs(metadata_shards.data(), metadata_shards.size()));
}

/* static */
ShardState MembershipCodecFlatBuffers::deserialize(
    const flat_buffer_codec::ShardState* shard_state) {
  assert(shard_state != nullptr);
  StorageState storage_state =
      static_cast<StorageState>(shard_state->storage_state());
  StorageStateFlags::Type flags = shard_state->flags();
  MetaDataStorageState metadata_state =
      static_cast<MetaDataStorageState>(shard_state->metadata_state());
  MaintenanceID::Type active_maintenance{shard_state->active_maintenance()};
  MembershipVersion::Type since_version{shard_state->since_version()};
  return ShardState{
      storage_state, flags, metadata_state, active_maintenance, since_version};
}

/* static */
std::shared_ptr<StorageMembership> MembershipCodecFlatBuffers::deserialize(
    const flat_buffer_codec::StorageMembership* storage_membership) {
  ld_check(storage_membership != nullptr);
  MembershipCodecFlatBuffers::ProtocolVersion pv =
      storage_membership->proto_version();
  if (pv > CURRENT_PROTO_VERSION) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "Received codec protocol version %u is larger than current "
        "codec protocol version %u. There might be incompatible data, "
        "aborting deserialization",
        pv,
        CURRENT_PROTO_VERSION);
    err = E::NOTSUPPORTED;
    return nullptr;
  }

  auto result = std::make_shared<StorageMembership>();
  result->version_ =
      MembershipVersion::Type(storage_membership->membership_version());

  auto node_states = storage_membership->node_states();
  if (node_states) {
    for (size_t i = 0; i < node_states->Length(); ++i) {
      auto node_state = node_states->Get(i);
      node_index_t node = node_state->node_idx();
      auto shard_states = node_state->shard_states();
      if (shard_states) {
        for (size_t j = 0; j < shard_states->Length(); ++j) {
          auto shard_state = shard_states->Get(j);
          result->setShardState(ShardID(node, shard_state->shard_idx()),
                                deserialize(shard_state));
        }
      }
    }
  }

  auto metadata_shards = storage_membership->metadata_shards();
  if (metadata_shards) {
    for (size_t i = 0; i < metadata_shards->Length(); ++i) {
      auto meta_shard = metadata_shards->Get(i);
      result->metadata_shards_.insert(
          ShardID(meta_shard->node_idx(), meta_shard->shard_idx()));
    }
  }

  if (!result->validate()) {
    err = E::BADMSG;
    return nullptr;
  }

  return result;
}

/* static */
flatbuffers::Offset<flat_buffer_codec::SequencerMembership>
MembershipCodecFlatBuffers::serialize(
    flatbuffers::FlatBufferBuilder& b,
    const SequencerMembership& sequencer_membership) {
  std::vector<flatbuffers::Offset<flat_buffer_codec::SequencerNodeState>>
      node_states;
  for (const auto& node_kv : sequencer_membership.node_states_) {
    node_states.push_back(flat_buffer_codec::CreateSequencerNodeState(
        b,
        node_kv.first,
        node_kv.second.weight,
        node_kv.second.active_maintenance.val()));
  }

  return flat_buffer_codec::CreateSequencerMembership(
      b,
      CURRENT_PROTO_VERSION,
      sequencer_membership.getVersion().val(),
      b.CreateVector(node_states));
}

/* static */
std::shared_ptr<SequencerMembership> MembershipCodecFlatBuffers::deserialize(
    const flat_buffer_codec::SequencerMembership* sequencer_membership) {
  ld_check(sequencer_membership != nullptr);
  MembershipCodecFlatBuffers::ProtocolVersion pv =
      sequencer_membership->proto_version();
  if (pv > CURRENT_PROTO_VERSION) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "Received codec protocol version %u is larger than current "
        "codec protocol version %u. There might be incompatible data, "
        "aborting deserialization",
        pv,
        CURRENT_PROTO_VERSION);
    err = E::NOTSUPPORTED;
    return nullptr;
  }

  auto result = std::make_shared<SequencerMembership>();
  result->version_ =
      MembershipVersion::Type(sequencer_membership->membership_version());

  auto node_states = sequencer_membership->node_states();
  if (node_states) {
    for (size_t i = 0; i < node_states->Length(); ++i) {
      auto node_state = node_states->Get(i);
      node_index_t node = node_state->node_idx();
      double weight = node_state->weight();
      MaintenanceID::Type active_maintenance{node_state->active_maintenance()};
      result->setNodeState(node, {weight, active_maintenance});
    }
  }

  if (!result->validate()) {
    err = E::BADMSG;
    return nullptr;
  }

  return result;
}

}}} // namespace facebook::logdevice::membership
