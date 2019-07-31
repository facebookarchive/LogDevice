/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/membership/StorageMembership.h"

#include <algorithm>

#include <folly/Format.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/membership/utils.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace membership {

using facebook::logdevice::toString;
using namespace MembershipVersion;
using namespace MaintenanceID;

std::string ShardState::Update::toString() const {
  return folly::sformat("[T:{}, C:{}, M:{}]",
                        membership::toString(transition),
                        Condition::toString(conditions),
                        membership::toString(maintenance));
}

bool ShardState::Update::isValid() const {
  if (transition >= StorageStateTransition::Count) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Invalid transition %lu for update %s.",
                    static_cast<size_t>(transition),
                    toString().c_str());
    return false;
  }

  if (transition == StorageStateTransition::OVERRIDE_STATE &&
      !state_override.hasValue()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "Invalid transition %lu for update %s: state_override not provided.",
        static_cast<size_t>(transition),
        toString().c_str());
    return false;
  }

  return true;
}

bool ShardState::isValid() const {
  if (storage_state == StorageState::INVALID ||
      metadata_state == MetaDataStorageState::INVALID ||
      since_version == EMPTY_VERSION) {
    // since_version should never be 0 (EMPTY)
    return false;
  }

  if (metadata_state == MetaDataStorageState::PROMOTING &&
      storage_state != StorageState::READ_WRITE) {
    // PROMOTING metadata storage state should only happen in READ_WRITE
    return false;
  }

  if ((flags & StorageStateFlags::UNRECOVERABLE) &&
      (storage_state == StorageState::NONE ||
       storage_state == StorageState::NONE_TO_RO)) {
    return false;
  }

  return true;
}

std::string ShardState::toString() const {
  return folly::sformat("[S:{}, F:{}, M:{}, A:{}, V:{}]",
                        membership::toString(storage_state),
                        StorageStateFlags::toString(flags),
                        membership::toString(metadata_state),
                        membership::toString(active_maintenance),
                        membership::toString(since_version));
}

/* static */
int ShardState::transition(const ShardState& current_state,
                           Update update,
                           MembershipVersion::Type new_since_version,
                           ShardState* state_out) {
  if (!update.isValid()) {
    err = E::INVALID_PARAM;
    return -1;
  }

  if (!isAddingNewShard(update.transition) && !current_state.isValid()) {
    // current source state must be valid except for the two transitions that
    // add a new shard
    err = E::INVALID_PARAM;
    return -1;
  }

  ShardState target_shard_state(current_state);

  // Step 1: Check the current storage state and conditions, and determine
  // the target storage_state by looking up the TransitionTable
  const StorageState expected_state = source_state(update.transition);
  if (expected_state != StorageState::INVALID &&
      expected_state != current_state.storage_state) {
    err = E::SOURCE_STATE_MISMATCH;
    return -1;
  }

  const StateTransitionCondition expected_conditions =
      required_conditions(update.transition);

  // check transition conditions; Also if the FORCE flag is presented, bypass
  // the condition check
  if (!Condition::hasAllConditionsOrForce(
          update.conditions, expected_conditions)) {
    err = E::CONDITION_MISMATCH;
    return -1;
  }

  const StorageState target_storage_state = target_state(update.transition);
  if (target_storage_state != StorageState::INVALID) {
    target_shard_state.storage_state = target_storage_state;
  }

  // Step 2: Determine the flags and MetaDataStorageState, as well as certain
  //         transition specific processing
  StorageStateFlags::Type target_flags = current_state.flags;
  MetaDataStorageState target_metadata_state = current_state.metadata_state;
  switch (update.transition) {
    case StorageStateTransition::BOOTSTRAP_ENABLE_SHARD:
    case StorageStateTransition::BOOTSTRAP_ENABLE_METADATA_SHARD: {
      // provisioning new shards for the newly created cluster

      ld_check(current_state.storage_state == StorageState::NONE);
      ld_check(target_shard_state.storage_state == StorageState::READ_WRITE);
      target_flags = StorageStateFlags::NONE;

      // The shard is either a metadata shard or not. It can't be
      // INVALID because it's already in the config and it can't be PROMOTING
      // because PROMOTING requires a RW shard.
      ld_check(current_state.metadata_state == MetaDataStorageState::NONE ||
               current_state.metadata_state == MetaDataStorageState::METADATA);

      // If it's a metadata shard, let's respect that. Even if the
      // transition is BOOTSTRAP_ENABLE_SHARD.
      target_metadata_state = update.transition ==
                  StorageStateTransition::BOOTSTRAP_ENABLE_METADATA_SHARD ||
              current_state.metadata_state == MetaDataStorageState::METADATA
          ? MetaDataStorageState::METADATA
          : MetaDataStorageState::NONE;
    } break;

    case StorageStateTransition::ADD_EMPTY_SHARD: {
      ld_check(target_shard_state.storage_state == StorageState::PROVISIONING);
      target_flags = StorageStateFlags::NONE;
      target_metadata_state = MetaDataStorageState::NONE;
    } break;

    case StorageStateTransition::DISABLING_WRITE: {
      // 1) If the metadata storage state of the shard is METADATA, then require
      // additional checks on metadata write availability and capacity;
      // 2) If the metadata storage state of the shard is PROMOTING, abort the
      // promoting and rever the state back to NONE
      switch (current_state.metadata_state) {
        case MetaDataStorageState::METADATA:
          if (!Condition::hasAllConditionsOrForce(
                  update.conditions,
                  (Condition::WRITE_AVAILABILITY_CHECK |
                   Condition::METADATA_CAPACITY_CHECK))) {
            err = E::CONDITION_MISMATCH;
            return -1;
          }
          break;
        case MetaDataStorageState::PROMOTING:
          target_metadata_state = MetaDataStorageState::NONE;
          break;
        case MetaDataStorageState::NONE:
          break;
        case MetaDataStorageState::INVALID:
          ld_check(false);
          break;
      }
    } break;

    case StorageStateTransition::DATA_MIGRATION_COMPLETED: {
      ld_check(current_state.storage_state == StorageState::DATA_MIGRATION);
      // if the shard has the UNRECOVERABLE flag, clear it
      target_flags &= ~(StorageStateFlags::UNRECOVERABLE);
    } break;

    case StorageStateTransition::PROMOTING_METADATA_SHARD: {
      // enforced by the transition table
      ld_check(current_state.storage_state == StorageState::READ_WRITE);
      ld_check(target_shard_state.storage_state == StorageState::READ_WRITE);

      // additional conditions: current metadata storage state must be NONE
      if (current_state.metadata_state != MetaDataStorageState::NONE) {
        err = E::SOURCE_STATE_MISMATCH;
        return -1;
      }
      // metadata state: NONE -> PROMOTING
      target_metadata_state = MetaDataStorageState::PROMOTING;
    } break;

    case StorageStateTransition::COMMIT_PROMOTION_METADATA_SHARD: {
      // enforced by the transition table
      ld_check(current_state.storage_state == StorageState::READ_WRITE);
      ld_check(target_shard_state.storage_state == StorageState::READ_WRITE);
      // additional conditions: check the current metadata state must
      // be PROMOTING
      if (current_state.metadata_state != MetaDataStorageState::PROMOTING) {
        err = E::SOURCE_STATE_MISMATCH;
        return -1;
      }
      // metadata state: PROMOTING -> METADATA
      target_metadata_state = MetaDataStorageState::METADATA;
    } break;

    case StorageStateTransition::ABORT_PROMOTING_METADATA_SHARD: {
      // enforced by the transition table
      ld_check(current_state.storage_state == StorageState::READ_WRITE);
      ld_check(target_shard_state.storage_state == StorageState::READ_WRITE);

      // additional conditions: current metadata storage state must be PROMOTING
      if (current_state.metadata_state != MetaDataStorageState::PROMOTING) {
        err = E::SOURCE_STATE_MISMATCH;
        return -1;
      }
      // metadata state: PROMOTING -> NONE
      target_metadata_state = MetaDataStorageState::NONE;
    } break;

    case StorageStateTransition::MARK_SHARD_UNRECOVERABLE: {
      // additional conditions: 1) current storage state must be one of
      // {ro, rw, rw2ro, dm, none2ro} state. 2) existing shard state
      // shouldn't have the UNRECOVERABLE flag. 3) if the shard is in none2ro
      // state, marking it as UNRECOVERABLE should automatically abort the
      // none->ro transition and make the shard transition back to NONE
      switch (current_state.storage_state) {
        case StorageState::PROVISIONING:
          ld_check(target_shard_state.storage_state ==
                   StorageState::PROVISIONING);
          // for storage state PROVISIONING this operation is a no-op
          break;
        case StorageState::NONE:
          ld_check(target_shard_state.storage_state == StorageState::NONE);
          // for storage state NONE this operation is a no-op
          break;
        case StorageState::NONE_TO_RO:
          // abort the enabling read attempt and make the storage state go back
          // to NONE
          target_shard_state.storage_state = StorageState::NONE;
          break;
        case StorageState::READ_ONLY:
        case StorageState::READ_WRITE:
        case StorageState::RW_TO_RO:
        case StorageState::DATA_MIGRATION: {
          // In these situations, this operation shouldn't change the storage
          // state
          ld_check(target_shard_state.storage_state ==
                   current_state.storage_state);
          if (current_state.flags & StorageStateFlags::UNRECOVERABLE) {
            err = E::ALREADY;
            return -1;
          }

          target_flags |= StorageStateFlags::UNRECOVERABLE;
        } break;
        case StorageState::INVALID:
          err = E::INTERNAL;
          return -1;
      }
      // metadata storage state keeps the same
    } break;

    case StorageStateTransition::OVERRIDE_STATE: {
      // must have the force flag in conditions
      ld_check(update.conditions & Condition::FORCE);
      ld_check(update.state_override.hasValue());
      const auto& state_override = update.state_override.value();

      // blindly overwrite the shard state to the target overrid state
      target_shard_state.storage_state = state_override.storage_state;
      target_flags = state_override.flags;
      target_metadata_state = state_override.metadata_state;
    } break;
    case StorageStateTransition::REMOVE_EMPTY_SHARD:
      if (current_state.storage_state != StorageState::PROVISIONING &&
          current_state.storage_state != StorageState::NONE) {
        err = E::SOURCE_STATE_MISMATCH;
        return -1;
      }
      break;

    case StorageStateTransition::MARK_SHARD_PROVISIONED:
    case StorageStateTransition::ENABLING_READ:
    case StorageStateTransition::COMMIT_READ_ENABLED:
    case StorageStateTransition::ENABLE_WRITE:
    case StorageStateTransition::COMMIT_WRITE_DISABLED:
    case StorageStateTransition::START_DATA_MIGRATION:
    case StorageStateTransition::ABORT_ENABLING_READ:
    case StorageStateTransition::ABORT_DISABLING_WRITE:
    case StorageStateTransition::CANCEL_DATA_MIGRATION:
      break;

    case StorageStateTransition::Count:
      err = E::INTERNAL;
      return -1;
  }

  target_shard_state.flags = target_flags;
  target_shard_state.metadata_state = target_metadata_state;
  target_shard_state.since_version = new_since_version;
  target_shard_state.active_maintenance = update.maintenance;

  if (!target_shard_state.isValid() &&
      update.transition != StorageStateTransition::REMOVE_EMPTY_SHARD) {
    err = E::INTERNAL;
    return -1;
  }

  if (state_out != nullptr) {
    *state_out = target_shard_state;
  }

  return 0;
}

bool StorageMembership::Update::isValid() const {
  // An update is valid if it modifies at least a single shard or changes the
  // the bootstrapping status of the membership
  return (!shard_updates.empty() || finalize_bootstrapping) &&
      std::all_of(shard_updates.cbegin(),
                  shard_updates.cend(),
                  [](const auto& kv) { return kv.second.isValid(); });
}

std::string StorageMembership::Update::toString() const {
  std::string shard_str;
  bool first = true;
  for (const auto& kv : shard_updates) {
    if (!first) {
      shard_str += ", ";
    }
    shard_str += folly::sformat(
        "{{{}, {}}}", membership::toString(kv.first), kv.second.toString());
    first = false;
  }

  return folly::sformat(
      "[V:{}, {{{}}}]", membership::toString(base_version), shard_str);
}

folly::Optional<ShardState>
StorageMembership::getShardState(ShardID shard) const {
  const auto nit = node_states_.find(shard.node());
  if (nit == node_states_.cend()) {
    return folly::none;
  }

  const auto& shard_map = nit->second.shard_states;
  const auto sit = shard_map.find(shard.shard());
  if (sit == shard_map.cend()) {
    return folly::none;
  }

  return sit->second;
}

std::unordered_map<shard_index_t, ShardState>
StorageMembership::getShardStates(node_index_t node) const {
  std::unordered_map<shard_index_t, ShardState> result;
  const auto nit = node_states_.find(node);
  if (nit == node_states_.cend()) {
    return result;
  }

  return nit->second.shard_states;
}

void StorageMembership::setShardState(ShardID shard, ShardState state) {
  // caller must ensure that the given state is valid
  ld_check(state.isValid());
  node_states_[shard.node()].shard_states[shard.shard()] = state;
  if (state.metadata_state != MetaDataStorageState::NONE) {
    // also update the metadata shard index
    metadata_shards_.insert(shard);
  } else {
    metadata_shards_.erase(shard);
  }
}

void StorageMembership::eraseShardState(ShardID shard) {
  auto nit = node_states_.find(shard.node());
  if (nit != node_states_.end()) {
    auto& shard_map = nit->second.shard_states;
    shard_map.erase(shard.shard());
    if (shard_map.empty()) {
      // no shard exist anymore, also erase the entire node state
      node_states_.erase(nit);
    }
  }
  // also erase the shard from metadata shard index
  metadata_shards_.erase(shard);
}

int StorageMembership::applyUpdate(const Membership::Update& membership_update,
                                   Membership* new_membership_out) const {
  if (membership_update.getType() != MembershipType::STORAGE ||
      (new_membership_out != nullptr &&
       new_membership_out->getType() != MembershipType::STORAGE)) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "Expect update and/or out params of storage membership type!");
    err = E::INVALID_PARAM;
    return -1;
  }

  const StorageMembership::Update& update =
      checked_downcast<const StorageMembership::Update&>(membership_update);
  StorageMembership* new_storage_membership_out =
      checked_downcast_or_null<StorageMembership*>(new_membership_out);

  if (!update.isValid()) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Cannnot apply invalid membership update: %s.",
                    update.toString().c_str());
    err = E::INVALID_PARAM;
    return -1;
  }

  if (version_ != update.base_version) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "Cannnot apply membership update with wrong base version."
                    "current version: %s, update: %s.",
                    membership::toString(version_).c_str(),
                    update.toString().c_str());
    err = E::VERSION_MISMATCH;
    return -1;
  }

  StorageMembership target_membership_state(*this);
  // bump the version in the target state
  target_membership_state.version_ =
      MembershipVersion::Type(version_.val() + 1);

  for (const auto& kv : update.shard_updates) {
    const ShardID shard = kv.first;
    const ShardState::Update& shard_update = kv.second;

    // Step 1: get the current ShardState of the shard of the requested shard
    auto current_shard_state = getShardState(shard);
    bool shard_exist = current_shard_state.hasValue();

    if (!shard_exist) {
      if (!isAddingNewShard(shard_update.transition)) {
        // shard must exist in the current membership with the only exception of
        // ADD_EMPTY_(METADATA_)SHARD or PROVISION_(METADATA_)SHARD
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        5,
                        "Cannnot apply membership update for shard %s as it "
                        "does not exist in membership. current version: %s, "
                        "update: %s.",
                        membership::toString(shard).c_str(),
                        membership::toString(version_).c_str(),
                        update.toString().c_str());
        err = E::NOTINCONFIG;
        return -1;
      }
      // create an initial current state
      current_shard_state = ShardState{StorageState::INVALID,
                                       StorageStateFlags::NONE,
                                       MetaDataStorageState::NONE,
                                       MAINTENANCE_NONE,
                                       EMPTY_VERSION};
    } else {
      if (isAddingNewShard(shard_update.transition)) {
        RATELIMIT_ERROR(
            std::chrono::seconds(10),
            5,
            "Cannnot apply membership update for shard %s as the shard "
            "requested to add already exists in membership. current "
            "version: %s, update: %s.",
            membership::toString(shard).c_str(),
            membership::toString(version_).c_str(),
            update.toString().c_str());
        err = E::EXISTS;
        return -1;
      }
    }

    if (isBootstrappingShard(shard_update.transition) && !bootstrapping_) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Cannnot apply a bootstrapping enable update in a non "
                      "bootstrapping cluster. bootstrapping: %d Update: %s",
                      bootstrapping_,
                      update.toString().c_str());
      err = E::INVALID_PARAM;
      return -1;
    }

    ShardState target_shard_state;
    int rv = ShardState::transition(*current_shard_state,
                                    shard_update,
                                    target_membership_state.version_,
                                    &target_shard_state);
    if (rv != 0) {
      // err set by transition()
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      5,
                      "Failed to apply membership update for shard %s: %s. "
                      "current version: %s, current shard state: %s, "
                      "update: %s.",
                      membership::toString(shard).c_str(),
                      error_description(err),
                      membership::toString(version_).c_str(),
                      current_shard_state->toString().c_str(),
                      update.toString().c_str());
      return -1;
    }

    // commit the target ShardState, also maintain the metadata_shards_ index.
    if (shard_update.transition == StorageStateTransition::REMOVE_EMPTY_SHARD) {
      target_membership_state.eraseShardState(shard);
    } else {
      ld_check(target_shard_state.isValid());
      target_membership_state.setShardState(shard, target_shard_state);
    }
  }
  if (update.finalize_bootstrapping) {
    if (!bootstrapping_) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          5,
          "Cannnot apply membership update as it's trying to finalize "
          "bootstrapping for a cluster that's not bootstrapping already."
          "version: %s, update: %s.",
          membership::toString(version_).c_str(),
          update.toString().c_str());
      err = E::ALREADY;
      return -1;
    }
    target_membership_state.bootstrapping_ = false;
  }

  if (new_storage_membership_out != nullptr) {
    *new_storage_membership_out = target_membership_state;
  }

  dcheckConsistency();
  return 0;
}

bool StorageMembership::validate() const {
  if (version_ == EMPTY_VERSION && !isEmpty()) {
    RATELIMIT_ERROR(
        std::chrono::seconds(10),
        5,
        "validation failed! Memership is not empty with empty version: %s. "
        "Number of nodes: %lu.",
        membership::toString(version_).c_str(),
        numNodes());
    return false;
  }

  std::set<ShardID> expected_metadata_shard;
  for (const auto& np : node_states_) {
    for (const auto& sp : np.second.shard_states) {
      ShardID shard(np.first, sp.first);
      const ShardState& st = sp.second;
      if (!st.isValid()) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        5,
                        "validation failed! invalid shard state for shard %s: "
                        "%s. membership version: %s.",
                        membership::toString(shard).c_str(),
                        st.toString().c_str(),
                        membership::toString(version_).c_str());
        return false;
      }

      if (st.since_version > version_) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        5,
                        "validation failed! invalid effective since version "
                        "for shard %s: %s. membership version: %s.",
                        membership::toString(shard).c_str(),
                        st.toString().c_str(),
                        membership::toString(version_).c_str());
        return false;
      }

      if (st.metadata_state != MetaDataStorageState::NONE) {
        expected_metadata_shard.insert(shard);
      }
    }
  }

  if (expected_metadata_shard != metadata_shards_) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    5,
                    "validation failed! inconsistent metadata shards index, "
                    "expected: %s, actual %s. membership version: %s.",
                    membership::toString(expected_metadata_shard).c_str(),
                    membership::toString(metadata_shards_).c_str(),
                    membership::toString(version_).c_str());
    return false;
  }

  return true;
}

std::vector<node_index_t> StorageMembership::getMembershipNodes() const {
  std::vector<node_index_t> res;
  res.reserve(node_states_.size());
  for (const auto& kv : node_states_) {
    res.push_back(kv.first);
  }
  return res;
}

bool StorageMembership::canWriteToShard(ShardID shard) const {
  auto result = getShardState(shard);
  return result.hasValue() ? canWriteTo(result->storage_state) : false;
}

bool StorageMembership::shouldReadFromShard(ShardID shard) const {
  auto result = getShardState(shard);
  return result.hasValue() ? shouldReadFrom(result->storage_state) : false;
}

bool StorageMembership::hasWritableShard(node_index_t node) const {
  const auto nit = node_states_.find(node);
  if (nit == node_states_.cend()) {
    return false;
  }

  const auto& shards = nit->second.shard_states;
  return std::any_of(shards.begin(), shards.end(), [](const auto& kv) {
    return canWriteTo(kv.second.storage_state);
  });
}

bool StorageMembership::hasShardShouldReadFrom(node_index_t node) const {
  const auto nit = node_states_.find(node);
  if (nit == node_states_.cend()) {
    return false;
  }

  const auto& shards = nit->second.shard_states;
  return std::any_of(shards.begin(), shards.end(), [](const auto& kv) {
    return shouldReadFrom(kv.second.storage_state);
  });
}

bool StorageMembership::canWriteMetaDataToShard(ShardID shard) const {
  auto result = getShardState(shard);
  return (result.hasValue() ? canWriteMetaDataTo(
                                  result->storage_state, result->metadata_state)
                            : false);
}

bool StorageMembership::shouldReadMetaDataFromShard(ShardID shard) const {
  auto result = getShardState(shard);
  return (result.hasValue() ? shouldReadMetaDataFrom(
                                  result->storage_state, result->metadata_state)
                            : false);
}

StorageMembership::StorageMembership() : Membership(EMPTY_VERSION) {
  dcheckConsistency();
}

namespace {

template <typename Container, typename Pred>
StorageSet storageset_filter(const Container& container, Pred pred) {
  StorageSet result;
  std::copy_if(
      container.begin(), container.end(), std::back_inserter(result), pred);
  return result;
}

} // namespace

StorageSet StorageMembership::writerView(const StorageSet& storage_set) const {
  return storageset_filter(storage_set, [this](ShardID shard) {
    return this->canWriteToShard(shard);
  });
}

StorageSet StorageMembership::readerView(const StorageSet& storage_set) const {
  return storageset_filter(storage_set, [this](ShardID shard) {
    return this->shouldReadFromShard(shard);
  });
}

StorageSet StorageMembership::writerViewMetaData() const {
  return storageset_filter(metadata_shards_, [this](ShardID shard) {
    return this->canWriteMetaDataToShard(shard);
  });
}

StorageSet StorageMembership::readerViewMetaData() const {
  return storageset_filter(metadata_shards_, [this](ShardID shard) {
    return this->shouldReadMetaDataFromShard(shard);
  });
}

StorageSet StorageMembership::getMetaDataStorageSet() const {
  return storageset_filter(
      metadata_shards_, [](ShardID shard) { return true; });
}

bool StorageMembership::isInMetadataStorageSet(ShardID shard) const {
  return metadata_shards_.count(shard);
}

std::set<node_index_t> StorageMembership::getMetaDataNodeSet() const {
  std::set<node_index_t> result;
  for (const auto shard : metadata_shards_) {
    result.insert(shard.node());
  }
  return result;
}

std::vector<node_index_t> StorageMembership::getMetaDataNodeIndices() const {
  const std::set<node_index_t> meta_set = getMetaDataNodeSet();
  return std::vector<node_index_t>(meta_set.begin(), meta_set.end());
}

bool StorageMembership::operator==(const StorageMembership& rhs) const {
  return version_ == rhs.getVersion() && node_states_ == rhs.node_states_ &&
      bootstrapping_ == rhs.bootstrapping_ &&
      metadata_shards_ == rhs.metadata_shards_;
}

std::shared_ptr<const StorageMembership>
StorageMembership::withIncrementedVersion(
    folly::Optional<membership::MembershipVersion::Type> new_version) const {
  auto new_membership = std::make_shared<StorageMembership>(*this);
  auto new_membership_version =
      new_version.value_or(MembershipVersion::Type{version_.val() + 1});
  if (new_membership_version <= version_) {
    return nullptr;
  }
  new_membership->version_ = new_membership_version;
  return new_membership;
}

bool StorageMembership::hasShard(ShardID shard) const {
  const auto nit = node_states_.find(shard.node());
  if (nit == node_states_.cend()) {
    return false;
  }
  return nit->second.hasShard(shard.shard());
}

}}} // namespace facebook::logdevice::membership
