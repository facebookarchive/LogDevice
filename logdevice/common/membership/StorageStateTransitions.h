/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <tuple>

#include "logdevice/common/membership/StorageState.h"

namespace facebook { namespace logdevice { namespace membership {

/**
 * Enumerates all possible state transitions for a single storage shard.
 */
enum class StorageStateTransition : uint8_t {

  // add an empty shard to the storage membership with NONE state
  // transition: INVALID -> PROVISIONING
  ADD_EMPTY_SHARD = 0,

  // remove an empty shard of NONE/PROVISIONING state from the storage
  // membership
  // transition: NONE/PROVISIONING -> INVALID
  REMOVE_EMPTY_SHARD,

  // enabling reading on an empty shard in NONE state
  // transition: NONE -> NONE_TO_RO
  ENABLING_READ,

  // finalized the transition that the empty shard is becoming read only
  // transition: NONE_TO_RO -> READ_ONLY
  COMMIT_READ_ENABLED,

  // enabling writes on a shard that is previously read-only
  // transition: READ_ONLY -> READ_WRITE
  ENABLE_WRITE,

  // disabling writes of a shard that is previously in READ_WRITE
  // transition: READ_WRITE -> RW_TO_RO
  DISABLING_WRITE,

  // finalize the transition that disables writes for a READ_WRITE shard
  // transition: RW_TO_RO -> READ_ONLY
  COMMIT_WRITE_DISABLED,

  // start migrating copies of data stored on the shard in READ_ONLY
  // transition: READ_ONLY -> DATA_MIGRATION
  START_DATA_MIGRATION,

  // all copies of data stored on the shard have been migrated, the storage
  // shard can be treated as an empty shard
  // transition: DATA_MIGRATION -> NONE
  DATA_MIGRATION_COMPLETED,

  // abort the effort of enabling read on an empty shard.
  // transition: NONE_TO_RO -> NONE
  ABORT_ENABLING_READ,

  // abort the effort of disabling write on a shard in READ_WRITE state
  // transition: RW_TO_RO -> READ_WRITE
  ABORT_DISABLING_WRITE,

  // cancel the ongoing data migration
  // transition: DATA_MIGRATION -> READ_ONLY
  CANCEL_DATA_MIGRATION,

  // promoting a regular storage shard in READ_WRITE state to a metadata
  // storage shard.
  // metadata storage state: NONE -> PROMOTING
  PROMOTING_METADATA_SHARD,

  // finalize the transition that promotes a none metadata shard to
  // a metadata shard. The shard should stay in READ_WRITE state.
  // metadata storage state: PROMOTING -> METADATA
  COMMIT_PROMOTION_METADATA_SHARD,

  // abort the effort of promoting a metadata shard.
  // metadata storage state: PROMOTING -> NONE
  ABORT_PROMOTING_METADATA_SHARD,

  // apply the UNRECOVERABLE storage state flag to the storage shard. See the
  // doc block in StorageStateFlags::UNRECOVRABLE
  MARK_SHARD_UNRECOVERABLE,

  // Transitions the shard out of the PROVISIONING state to NONE. Refer to the
  // PROVISIONING state documentation for more info.
  // transition: PROVISIONING -> NONE
  MARK_SHARD_PROVISIONED,

  // convenience transition for creating the cluster for the first time.
  // Only applicable if the cluster is still bootstrapping.
  // transform the shard to READ_WRITE state.
  // transition: NONE -> READ_WRITE
  BOOTSTRAP_ENABLE_SHARD,

  // same as above, except that the storage shard is a metadata shard. The
  // metadata
  // Only applicable if the cluster is still bootstrapping.
  // metadata state is also directly transitioned into METADATA.
  // transition: NONE -> READ_WRITE
  // metadata storage state: NONE -> METADATA
  BOOTSTRAP_ENABLE_METADATA_SHARD,

  // force to override the storage state of a shard to the provided target
  // state, should only be used in emergency; always require the FORCE condition
  // to be supplied
  OVERRIDE_STATE,

  Count
};

static_assert(static_cast<size_t>(StorageStateTransition::Count) == 20,
              "There are 20 state transitions in the design spec.");

/**
 * return    true if the transition is adding a new shard which is not part of
 *           the current membership
 */
constexpr bool isAddingNewShard(StorageStateTransition transition) {
  return transition == StorageStateTransition::ADD_EMPTY_SHARD;
}

constexpr bool isBootstrappingShard(StorageStateTransition transition) {
  return transition == StorageStateTransition::BOOTSTRAP_ENABLE_SHARD ||
      transition == StorageStateTransition::BOOTSTRAP_ENABLE_METADATA_SHARD;
}

using StateTransitionCondition = uint64_t;

namespace Condition {

static constexpr StateTransitionCondition NONE = 0;

// The storage shard is empty and does not contain other data
// (e.g., records from another cluster) that may corrupt or confuse the system
static constexpr StateTransitionCondition EMPTY_SHARD = 1ul << 0;

// The local storage of the shard is healthy and can serve reads
static constexpr StateTransitionCondition LOCAL_STORE_READABLE = 1ul << 1;

// The storage shard must _not_ be in the state that it is self-reporting
// missing data.
static constexpr StateTransitionCondition NO_SELF_REPORT_MISSING_DATA = 1ul
    << 2;

// The storage shard has its local perceived storage membership version
// larger or equal to the base version upon which the change is proposed
static constexpr StateTransitionCondition CAUGHT_UP_LOCAL_CONFIG = 1ul << 3;

// An intermediate state (e.g, NONE_TO_RO, PROMOTING_METADATA_SHARD) of a
// storage shard has been observed by a subset of storage shards which satisfies
// the copyset property of affected logs.
// Note: applies to both StorageState and MetaDataStorageState transitions
static constexpr StateTransitionCondition COPYSET_CONFIRMATION = 1ul << 4;

// The local storage of the shard is healthy and can serve writes
// (e.g., not running out of space)
static constexpr StateTransitionCondition LOCAL_STORE_WRITABLE = 1ul << 5;

// Used when disabling writes, all writers should still be able to complete
// writes with the shard disabled writes
static constexpr StateTransitionCondition WRITE_AVAILABILITY_CHECK = 1ul << 6;

// Used when 1) disabling writes and 2) data migration. There are enough
// resource (e.g., processing capacity and storage space) left in the remaining
// READ_WRITE nodes in the membership for accepting writes and store migrated
// data
static constexpr StateTransitionCondition CAPACITY_CHECK = 1ul << 7;

// An intermediate state (e.g, NONE_TO_RO, PROMOTING_METADATA_SHARD) of a
// storage shard has been observed by a subset of storage shards which satisfies
// the copyset property of affected logs.
// Note: applies to both StorageState and MetaDataStorageState transitions
static constexpr StateTransitionCondition FMAJORITY_CONFIRMATION = 1ul << 8;

// All copies previously stored on the storage shard has been migrated to other
// shards in the membership
static constexpr StateTransitionCondition DATA_MIGRATION_COMPLETE = 1ul << 9;

// The opposite of NO_SELF_REPORT_MISSING_DATA. The storage shard must be
// persistently (i.e., across restarts) self-aware that there are data missing
// from its local storage and never ship NO_RECORD responses.
static constexpr StateTransitionCondition SELF_AWARE_MISSING_DATA = 1ul << 10;

// The storage shard must persistently (i.e., across restarts) reject all
// incoming write copies from writers.
static constexpr StateTransitionCondition CANNOT_ACCEPT_WRITES = 1ul << 11;

// The following two checks are similar to WRITE_AVAILABILITY_CHECK and
// CAPACITY_CHECK, but with regard to metadata storage instead
static constexpr StateTransitionCondition METADATA_WRITE_AVAILABILITY_CHECK =
    1ul << 12;
static constexpr StateTransitionCondition METADATA_CAPACITY_CHECK = 1ul << 13;

//// Note: change toString() in util.cpp accordingly when adding new flags

// Bypass all condition checks and force the transition to happen.
// Used by emergency tooling.
static constexpr StateTransitionCondition FORCE = 1ul << 63;

// returns true if @param source has all condition flags specificed in
// @param required
constexpr bool hasAllConditions(StateTransitionCondition source,
                                StateTransitionCondition required) {
  return (source & required) == required;
}

// returns true if @param source has all required condition flags or has
// the FORCE flag
constexpr bool hasAllConditionsOrForce(StateTransitionCondition source,
                                       StateTransitionCondition required) {
  return (source & Condition::FORCE) || hasAllConditions(source, required);
}

std::string toString(StateTransitionCondition conditions);
//// Note: change toString() implementation in utils.cpp accordingly
//// when adding new conditions

}; // namespace Condition

// Table columns (from left to right):
// 1) source storage state; 2) target storage state; 3) transition conditions.
// StorageState::INVALID in source state means that the check or value is
// ignored

// TODO: stop using std::make_tuple and use list initialization in C++17
#define _t std::make_tuple

static constexpr std::array<
    std::tuple<StorageState, StorageState, StateTransitionCondition>,
    static_cast<size_t>(StorageStateTransition::Count)>
    TransitionTable{{

        // ADD_EMPTY_SHARD
        _t(StorageState::INVALID, StorageState::PROVISIONING, Condition::NONE),

        // REMOVE_EMPTY_SHARD
        _t(StorageState::INVALID, StorageState::INVALID, Condition::NONE),

        // ENABLING_READ
        _t(StorageState::NONE,
           StorageState::NONE_TO_RO,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA |
            Condition::CAUGHT_UP_LOCAL_CONFIG)),

        // COMMIT_READ_ENABLED
        _t(StorageState::NONE_TO_RO,
           StorageState::READ_ONLY,
           Condition::COPYSET_CONFIRMATION),

        // ENABLE_WRITE
        _t(StorageState::READ_ONLY,
           StorageState::READ_WRITE,
           Condition::LOCAL_STORE_WRITABLE),

        // DISABLING_WRITE  (clear promote flag)
        _t(StorageState::READ_WRITE,
           StorageState::RW_TO_RO,
           Condition::WRITE_AVAILABILITY_CHECK | Condition::CAPACITY_CHECK),

        // COMMIT_WRITE_DISABLED
        _t(StorageState::RW_TO_RO,
           StorageState::READ_ONLY,
           Condition::FMAJORITY_CONFIRMATION),

        // START_DATA_MIGRATION
        _t(StorageState::READ_ONLY,
           StorageState::DATA_MIGRATION,
           Condition::CAPACITY_CHECK),

        // DATA_MIGRATION_COMPLETED
        _t(StorageState::DATA_MIGRATION,
           StorageState::NONE,
           Condition::DATA_MIGRATION_COMPLETE),

        // ABORT_ENABLING_READ
        _t(StorageState::NONE_TO_RO, StorageState::NONE, Condition::NONE),

        // ABORT_DISABLING_WRITE
        _t(StorageState::RW_TO_RO,
           StorageState::READ_WRITE,
           Condition::LOCAL_STORE_WRITABLE),

        // CANCEL_DATA_MIGRATION
        _t(StorageState::DATA_MIGRATION,
           StorageState::READ_ONLY,
           Condition::NONE),

        // PROMOTING_METADATA_SHARD
        _t(StorageState::READ_WRITE, StorageState::READ_WRITE, Condition::NONE),

        // COMMIT_PROMOTION_METADATA_SHARD
        _t(StorageState::READ_WRITE,
           StorageState::READ_WRITE,
           Condition::COPYSET_CONFIRMATION),

        // ABORT_PROMOTING_METADATA_SHARD
        _t(StorageState::READ_WRITE, StorageState::READ_WRITE, Condition::NONE),

        // MARK_SHARD_UNRECOVERABLE (source/target state not applicable)
        _t(StorageState::INVALID,
           StorageState::INVALID,
           (Condition::SELF_AWARE_MISSING_DATA |
            Condition::CANNOT_ACCEPT_WRITES)),

        // MARK_SHARD_PROVISIONED
        _t(StorageState::PROVISIONING,
           StorageState::NONE,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA)),

        // BOOTSTRAP_ENABLE_SHARD
        _t(StorageState::NONE,
           StorageState::READ_WRITE,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA |
            Condition::LOCAL_STORE_WRITABLE)),

        // BOOTSTRAP_ENABLE_METADATA_SHARD
        _t(StorageState::NONE,
           StorageState::READ_WRITE,
           (Condition::EMPTY_SHARD | Condition::LOCAL_STORE_READABLE |
            Condition::NO_SELF_REPORT_MISSING_DATA |
            Condition::LOCAL_STORE_WRITABLE)),

        // OVERRIDE_STATE (source/target state not applicable)
        _t(StorageState::INVALID,
           StorageState::INVALID,
           // always require force condition
           (Condition::FORCE)),
    }};

#undef _t

static_assert(TransitionTable.size() == 20,
              "There are 20 state transitions in the design spec.");

//// utility functions for accessing the transition table

constexpr StorageState source_state(StorageStateTransition transition) {
  return std::get<0>(TransitionTable[static_cast<size_t>(transition)]);
}

constexpr StorageState target_state(StorageStateTransition transition) {
  return std::get<1>(TransitionTable[static_cast<size_t>(transition)]);
}

constexpr StateTransitionCondition
required_conditions(StorageStateTransition transition) {
  return std::get<2>(TransitionTable[static_cast<size_t>(transition)]);
}

}}} // namespace facebook::logdevice::membership
