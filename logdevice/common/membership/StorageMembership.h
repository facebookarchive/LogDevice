/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace membership {

/**
 * @file Storage membership describes the collection of storage shards in
 *       various different storage states. It is the _dynamic_ and mutable
 *       part of the storage configuration.
 *
 *       Note: all state and transition functions defined are not thread-safe.
 *       Upper layer is responsible for atomically update the state and
 * propagate them to all execution contexts.
 */

// Storage membership state is versioned and any changes will cause a version
// bumps to the membership.
LOGDEVICE_STRONG_TYPEDEF(uint64_t, StorageMembershipVersion);
static constexpr StorageMembershipVersion INVALID_VERSION{0};
// first valid membership version
static constexpr StorageMembershipVersion MIN_VERSION{1};

// used by identifying the active maintenance happening on the shard
LOGDEVICE_STRONG_TYPEDEF(uint64_t, MaintenanceID);
static constexpr MaintenanceID MAINTENANCE_NONE{0};

// Describe the per-shard state of a storage membership
struct ShardState {
  StorageState storage_state;

  StorageStateFlags::Type flags;

  MetaDataStorageState metadata_state;

  // identifier for the maintenance event that correspond to the
  // current shard state. Used by the maintenance state machine
  MaintenanceID active_maintenance;

  // storage membership version since which the ShardState is
  // effective for the shard
  StorageMembershipVersion since_version;

  // Describe the update that can apply to the ShardState
  struct Update {
    StorageStateTransition transition;

    // collection of condition checks done, provided by the proposor of
    // the update
    StateTransitionCondition conditions;

    // identifier for the new maintenance requesting the state transition
    MaintenanceID maintenance;

    bool isValid() const;
    std::string toString() const;
  };

  // return true if the shard state is a legitimate shard state of
  // a storage membership
  bool isValid() const;

  std::string toString() const;

  /**
   * Perform state transition by applying an update to the current shard state
   *
   * @param current_state        the current ShardState on which update is
   *                             applied. Must be valid except for the
   *                             ADD_EMPTY_(METADATA_)SHARD transitions.
   *
   * @param update               Update to apply. must be valid.
   *
   * @param new_since_version    new storage membership version after update is
   *                             applied
   *
   * @param state_out            output parameter for the target state
   *
   * @return   0 for success, and the updated state is written to *state_out.
   *          -1 for failure, with err set to:
   *             INVALID_PARAM   current state or update is not valid
   *             SOURCE_STATE_MISMATCH  current state doesn't match the source
   *                                    state required by the requested
   *                                    transition
   *             CONDITION_MISMATCH     conditions provided by the update do
   *                                    not meet all conditons required by the
   *                                    transition
   *             ALREADY                the shard has already been marked as
   *                                    UNRECOVERABLE
   */
  static int transition(const ShardState& current_state,
                        Update update,
                        StorageMembershipVersion new_since_version,
                        ShardState* state_out);
};

class StorageMembership {
 public:
  class Update {
   public:
    using ShardMap = std::map<ShardID, ShardState::Update>;

    // each storage membership update is strictly conditioned on a base
    // membership version of which the update can only be applied
    StorageMembershipVersion base_version;

    // a batch of per-shard updates
    ShardMap shard_updates;

    explicit Update(StorageMembershipVersion base) : base_version(base) {}

    Update(StorageMembershipVersion base,
           std::map<ShardID, ShardState::Update> updates)
        : base_version(base), shard_updates(std::move(updates)) {}

    int addShard(ShardID shard, ShardState::Update update) {
      auto res = shard_updates.insert({shard, update});
      return res.second ? 0 : -1;
    }

    bool isValid() const;
    std::string toString() const;
  };

  /**
   * create an empty storage membership object with MIN_VERSION.
   */
  explicit StorageMembership();

  /**
   * Apply a StorageMembership::Update to the membership and output the new
   * storage membership.
   *
   * @param update          update to apply, must be valid
   * @new_membership_out    output parameter for the new storage membership
   *
   * @return           0 for success, and write the new storage membership to
   *                   *new_membership_out. -1 for failure, with err set to:
   *                      all possible errors in ShardState::transition(); and
   *                      VERSION_MISMATCH  base version of the update doesn't
   *                                        match the current version
   *                      NOTINCONFIG       (for transitions other than
   *                                        adding shard) the requested shard
   *                                        does not exist in the config
   */
  int applyUpdate(Update update, StorageMembership* new_membership_out) const;

  /**
   * Perform validation of the storage membership and return true if the
   * membership is valid.
   *
   * Note: the function has a cost of O(n) where n is the number of shards
   * in the membership.
   */
  bool validate() const;

  /**
   * Get the shard state of a given storage shard.
   *
   * @return   a pair of (exist, ShardState) in which _exist_ is true if the
   *           request shard exists in the membership. In such case, its
   *           ShardState is also returned.
   */
  std::pair<bool, ShardState> getShardState(ShardID shard) const;

  /**
   * Check if writers and readers have access to a given shard. See the
   * definition of canWriteTo() and shouldReadFrom() in StorageState.h.
   */
  bool canWriteToShard(ShardID shard) const;
  bool shouldReadFromShard(ShardID shard) const;

  /**
   * Get the writer/reader's view of a given storage set. This is computed
   * by intersecting the storage set with the set of shards in the membership
   * that satisfies canWriteTo() and shouldReadFrom(), respectively.
   */
  StorageSet writerView(const StorageSet& storage_set) const;
  StorageSet readerView(const StorageSet& storage_set) const;

  /**
   * Similar to the ones above, return the writer/reader's view of storage
   * shards that store metadata.
   */
  bool canWriteMetaDataToShard(ShardID shard) const;
  bool shouldReadMetaDataFromShard(ShardID shard) const;
  StorageSet writerViewMetaData() const;
  StorageSet readerViewMetaData() const;

  /**
   * Return the list of storage shards whose MetaDataStorageState is either
   * METADATA or PROMOTING (i.e., not NONE).
   */
  StorageSet getMetaDataStorageSet() const;

  StorageMembershipVersion getVersion() const {
    return version_;
  }

  std::set<ShardID> getMetaDataShards() const {
    return metadata_shards_;
  }

  size_t numNodes() const {
    return node_states_.size();
  }

  std::string toString() const;

 private:
  class NodeState {
   public:
    std::unordered_map<shard_index_t, ShardState> shard_states;
    size_t numShards() const {
      return shard_states.size();
    }
  };

  StorageMembershipVersion version_;
  std::unordered_map<node_index_t, NodeState> node_states_;

  // a separated index of storage shards whose MetaDataStorageState is not NONE
  std::set<ShardID> metadata_shards_;

  // update the shard state of the given _shard_; also update the
  // metadata_shards_ index as needed. If _shard_ doesn't exist in
  // membership, create an entry for it.
  // Note: caller must guarantee that _state_ is valid
  void setShardState(ShardID shard, ShardState state);

  // remove the given _shard_ from the storage membership if the shard exists;
  // also update the metadata_shards_ index as needed.
  void eraseShardState(ShardID shard);

  // run internal validate() checks in DEBUG mode
  void dcheckConsistency() const;
};

}}} // namespace facebook::logdevice::membership
