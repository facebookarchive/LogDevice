/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/ShardStateTracker.h"

#include "logdevice/common/debug.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

namespace {
ShardState::Update
transitionToNonIntermediaryState(const ShardState& shard_state) {
  ShardState::Update update{};
  update.transition = StorageStateTransition::OVERRIDE_STATE;
  update.conditions = Condition::FORCE;

  ShardState::Update::StateOverride state_override{};
  state_override.storage_state =
      toNonIntermediaryState(shard_state.storage_state);
  state_override.metadata_state =
      toNonIntermediaryState(shard_state.metadata_state);
  // TODO: handle UNRECOVERABLE flag
  update.state_override = std::move(state_override);
  ld_assert(update.isValid());
  return update;
}
} // namespace

folly::Optional<ShardStateTracker::Entry>
ShardStateTracker::getEntry(node_index_t node_idx, shard_index_t shard_idx) {
  folly::Optional<ShardStateTracker::Entry> entry = folly::none;
  auto nit = node_states_.find(node_idx);
  if (nit != node_states_.end()) {
    auto& per_node_map = nit->second;
    auto sit = per_node_map.find(shard_idx);
    if (sit != per_node_map.end()) {
      entry = sit->second;
    }
  }
  return entry;
}

void ShardStateTracker::onNewConfig(
    std::shared_ptr<const NodesConfiguration> config) {
  if (!config || config->getVersion() <= last_known_nc_version_) {
    return;
  }
  ld_assert(config->validate());
  last_known_nc_version_ = config->getVersion();
  SystemTimestamp since_timestamp = config->getLastChangeTimestamp();

  // Create a new state map so that we don't need to worry about cleaning up
  // shards / nodes that are no longer in intermediary states or even service
  // discovery info.
  NodeStatesMap new_node_states;

  const auto& storage_membership_ptr = config->getStorageMembership();
  ld_check(storage_membership_ptr);
  const auto& storage_membership = *storage_membership_ptr;
  if (storage_membership.getVersion() <
      last_known_storage_membership_version_) {
    RATELIMIT_CRITICAL(
        std::chrono::seconds(10),
        5,
        "Got a newer NodesConfiguration with an older StorageMembership "
        "version. last known StorageMembership version: %s. Got "
        "StorageMembership (version: %s) in new NodesConfiguration (version: "
        "%s).",
        toString(last_known_storage_membership_version_).c_str(),
        toString(storage_membership.getVersion()).c_str(),
        toString(config->getVersion()).c_str());
    ld_assert(false);
    return;
  }
  last_known_storage_membership_version_ = storage_membership.getVersion();
  for (const auto& np : storage_membership.node_states_) {
    node_index_t node_idx = np.first;
    for (const auto& sp : np.second.shard_states) {
      shard_index_t shard_idx = sp.first;
      const auto& shard_state = sp.second;

      if (!isIntermediaryState(shard_state.storage_state) &&
          !isIntermediaryState(shard_state.metadata_state)) {
        continue;
      }

      auto new_entry = Entry{shard_state, since_timestamp};
      auto entry_opt = getEntry(node_idx, shard_idx);
      if (entry_opt.hasValue() &&
          entry_opt.value().shard_state_ == shard_state) {
        new_entry.since_timestamp_ = entry_opt.value().since_timestamp_;
      }
      new_node_states[node_idx][shard_idx] = std::move(new_entry);
    }
  }

  node_states_ = std::move(new_node_states);
}

folly::Optional<NodesConfiguration::Update>
ShardStateTracker::extractNCUpdate(SystemTimestamp till_timestamp) const {
  StorageMembership::Update update{last_known_storage_membership_version_};

  for (const auto& np : node_states_) {
    node_index_t node_idx = np.first;
    const auto& per_node_map = np.second;
    for (const auto& sp : per_node_map) {
      shard_index_t shard_idx = sp.first;
      const auto& entry = sp.second;

      if (entry.since_timestamp_ > till_timestamp) {
        continue;
      }

      ShardState::Update shard_state_update =
          transitionToNonIntermediaryState(entry.shard_state_);
      ShardID shard_id{node_idx, shard_idx};
      update.addShard(shard_id, std::move(shard_state_update));
      ld_assert(update.isValid());
    }
  }

  if (update.shard_updates.empty()) {
    return folly::none;
  }

  // construct an NC update to return
  NodesConfiguration::Update nc_update;
  nc_update.storage_config_update = std::make_unique<StorageConfig::Update>();
  nc_update.storage_config_update->membership_update =
      std::make_unique<StorageMembership::Update>(std::move(update));
  return std::move(nc_update);
}

}}}} // namespace facebook::logdevice::configuration::nodes
