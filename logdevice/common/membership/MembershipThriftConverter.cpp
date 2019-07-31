/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/membership/MembershipThriftConverter.h"

#include "logdevice/common/debug.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace membership {

constexpr MembershipThriftConverter::ProtocolVersion
    MembershipThriftConverter::CURRENT_PROTO_VERSION;

static_assert(static_cast<uint32_t>(thrift::StorageState::NONE) ==
                  static_cast<uint32_t>(StorageState::NONE),
              "");
static_assert(static_cast<uint32_t>(thrift::StorageState::INVALID) ==
                  static_cast<uint32_t>(StorageState::INVALID),
              "");
static_assert(static_cast<uint32_t>(thrift::StorageState::PROVISIONING) ==
                  static_cast<uint32_t>(StorageState::PROVISIONING),
              "");
static_assert(static_cast<uint32_t>(thrift::MetaDataStorageState::NONE) ==
                  static_cast<uint32_t>(MetaDataStorageState::NONE),
              "");
static_assert(static_cast<uint32_t>(thrift::MetaDataStorageState::INVALID) ==
                  static_cast<uint32_t>(MetaDataStorageState::INVALID),
              "");

/* static */
thrift::StorageMembership MembershipThriftConverter::toThrift(
    const StorageMembership& storage_membership) {
  std::map<thrift::node_idx, std::vector<thrift::ShardState>> node_states;
  for (const auto& node_kv : storage_membership.node_states_) {
    std::vector<thrift::ShardState> shard_states;
    for (const auto& shard_kv : node_kv.second.shard_states) {
      const auto& shard_state = shard_kv.second;
      thrift::ShardState state;
      state.set_shard_idx(shard_kv.first);
      state.set_storage_state(
          static_cast<thrift::StorageState>(shard_state.storage_state));
      state.set_flags(shard_state.flags);
      state.set_metadata_state(static_cast<thrift::MetaDataStorageState>(
          shard_state.metadata_state));
      state.set_active_maintenance(shard_state.active_maintenance.val());
      state.set_since_version(shard_state.since_version.val());

      shard_states.push_back(std::move(state));
    }

    node_states.emplace(node_kv.first, std::move(shard_states));
  }

  std::vector<thrift::ShardID> metadata_shards;
  for (const auto meta_shard : storage_membership.metadata_shards_) {
    thrift::ShardID shard_id;
    shard_id.set_node_idx(meta_shard.node());
    shard_id.set_shard_idx(meta_shard.shard());
    metadata_shards.push_back(std::move(shard_id));
  }

  thrift::StorageMembership membership;
  membership.set_proto_version(CURRENT_PROTO_VERSION);
  membership.set_membership_version(storage_membership.getVersion().val());
  membership.set_node_states(std::move(node_states));
  membership.set_bootstrapping(storage_membership.bootstrapping_);
  membership.set_metadata_shards(std::move(metadata_shards));

  return membership;
}

/* static */
ShardState
MembershipThriftConverter::fromThrift(const thrift::ShardState& shard_state) {
  StorageState storage_state =
      static_cast<StorageState>(shard_state.storage_state);
  StorageStateFlags::Type flags = shard_state.flags;
  MetaDataStorageState metadata_state =
      static_cast<MetaDataStorageState>(shard_state.metadata_state);
  MaintenanceID::Type active_maintenance{shard_state.active_maintenance};
  MembershipVersion::Type since_version{shard_state.since_version};
  return ShardState{
      storage_state, flags, metadata_state, active_maintenance, since_version};
}

/* static */
std::shared_ptr<StorageMembership> MembershipThriftConverter::fromThrift(
    const thrift::StorageMembership& storage_membership) {
  MembershipThriftConverter::ProtocolVersion pv =
      storage_membership.proto_version;
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
      MembershipVersion::Type(storage_membership.membership_version);

  for (const auto& state : storage_membership.node_states) {
    node_index_t node = state.first;
    auto shard_states = state.second;
    for (const auto& shard_state : shard_states) {
      result->setShardState(
          ShardID(node, shard_state.shard_idx), fromThrift(shard_state));
    }
  }

  for (const auto& meta_shard : storage_membership.metadata_shards) {
    result->metadata_shards_.insert(
        ShardID(meta_shard.node_idx, meta_shard.shard_idx));
  }
  result->bootstrapping_ = storage_membership.get_bootstrapping();

  if (!result->validate()) {
    err = E::BADMSG;
    return nullptr;
  }

  return result;
}

/* static */
thrift::SequencerMembership MembershipThriftConverter::toThrift(
    const SequencerMembership& sequencer_membership) {
  std::map<thrift::node_idx, thrift::SequencerNodeState> node_states;
  for (const auto& node_kv : sequencer_membership.node_states_) {
    thrift::SequencerNodeState state;
    state.set_sequencer_enabled(node_kv.second.sequencer_enabled);
    state.set_weight(node_kv.second.getConfiguredWeight());
    state.set_active_maintenance(node_kv.second.active_maintenance.val());
    node_states.emplace(node_kv.first, std::move(state));
  }

  thrift::SequencerMembership membership;
  membership.set_proto_version(CURRENT_PROTO_VERSION);
  membership.set_membership_version(sequencer_membership.getVersion().val());
  membership.set_node_states(std::move(node_states));
  membership.set_bootstrapping(sequencer_membership.bootstrapping_);
  return membership;
}

/* static */
std::shared_ptr<SequencerMembership> MembershipThriftConverter::fromThrift(
    const thrift::SequencerMembership& sequencer_membership) {
  MembershipThriftConverter::ProtocolVersion pv =
      sequencer_membership.proto_version;
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
      MembershipVersion::Type(sequencer_membership.membership_version);

  for (auto const& node_state : sequencer_membership.node_states) {
    node_index_t node = node_state.first;
    bool sequencer_enabled = node_state.second.sequencer_enabled;
    double weight = node_state.second.weight;
    MaintenanceID::Type active_maintenance{
        node_state.second.active_maintenance};
    result->setNodeState(node, {sequencer_enabled, weight, active_maintenance});
  }
  result->bootstrapping_ = sequencer_membership.get_bootstrapping();

  if (!result->validate()) {
    err = E::BADMSG;
    return nullptr;
  }

  return result;
}

}}} // namespace facebook::logdevice::membership
