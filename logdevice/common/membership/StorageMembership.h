/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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

#include <folly/Optional.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/membership/Membership.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/membership/StorageStateTransitions.h"

namespace facebook { namespace logdevice {
namespace configuration { namespace nodes {
class ShardStateTracker;
}} // namespace configuration::nodes

namespace membership {

/**
 * @file Storage membership describes the collection of storage shards in
 *       various different storage states. It is the _dynamic_ and mutable
 *       part of the storage configuration.
 *
 *       Note: all state and transition functions defined are not thread-safe.
 *       Upper layer is responsible for atomically update the state and
 *       propagate them to all execution contexts.
 */

// Describe the per-shard state of a storage membership
struct ShardState {
  StorageState storage_state{StorageState::INVALID};

  StorageStateFlags::Type flags{StorageStateFlags::NONE};

  MetaDataStorageState metadata_state{MetaDataStorageState::INVALID};

  // identifier for the maintenance event that correspond to the
  // current shard state. Used by the maintenance state machine
  MaintenanceID::Type active_maintenance{MaintenanceID::MAINTENANCE_NONE};

  // storage membership version since which the ShardState is
  // effective for the shard
  MembershipVersion::Type since_version{MembershipVersion::EMPTY_VERSION};

  // Describe the update that can apply to the ShardState
  struct Update {
    StorageStateTransition transition{StorageStateTransition::Count};

    // collection of condition checks done, provided by the proposor of
    // the update
    StateTransitionCondition conditions{Condition::NONE};

    // identifier for the new maintenance requesting the state transition
    MaintenanceID::Type maintenance{MaintenanceID::MAINTENANCE_NONE};

    // see state_override below
    struct StateOverride {
      StorageState storage_state;
      StorageStateFlags::Type flags;
      MetaDataStorageState metadata_state;

      bool operator==(const StateOverride& rhs) const {
        return storage_state == rhs.storage_state && flags == rhs.flags &&
            metadata_state == rhs.metadata_state;
      }
    };

    // forcefully update the shard state. Used only in the OVERRIDE_STATE
    // transition for emergency tools.
    folly::Optional<StateOverride> state_override{};

    bool isValid() const;
    std::string toString() const;

    bool operator==(const Update& rhs) const {
      return transition == rhs.transition && conditions == rhs.conditions &&
          maintenance == rhs.maintenance &&
          state_override == rhs.state_override;
    }
  };

  // return true if the shard state is a legitimate shard state of
  // a storage membership
  bool isValid() const;

  std::string toString() const;

  bool operator==(const ShardState& rhs) const {
    return storage_state == rhs.storage_state && flags == rhs.flags &&
        metadata_state == rhs.metadata_state &&
        active_maintenance == rhs.active_maintenance &&
        since_version == rhs.since_version;
  }

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
   *             VERSION_MISMATCH       the given since version does not match
   *                                    transition (e.g., provision transition
   *                                    requires the min base version)
   */
  static int transition(const ShardState& current_state,
                        Update update,
                        MembershipVersion::Type new_since_version,
                        ShardState* state_out);
};

class StorageMembership : public Membership {
 public:
  class Update : public Membership::Update {
   public:
    using ShardMap = std::map<ShardID, ShardState::Update>;

    // each storage membership update is strictly conditioned on a base
    // membership version of which the update can only be applied
    MembershipVersion::Type base_version{MembershipVersion::EMPTY_VERSION};

    // a batch of per-shard updates
    ShardMap shard_updates{};

    explicit Update(MembershipVersion::Type base) : base_version(base) {}

    Update(MembershipVersion::Type base,
           std::map<ShardID, ShardState::Update> updates)
        : base_version(base), shard_updates(std::move(updates)) {}

    int addShard(ShardID shard, ShardState::Update update) {
      auto res = shard_updates.insert({shard, update});
      return res.second ? 0 : -1;
    }

    bool isValid() const override;
    MembershipType getType() const override {
      return MembershipType::STORAGE;
    }

    std::string toString() const override;

    bool operator==(const Update& rhs) const {
      return base_version == rhs.base_version &&
          shard_updates == rhs.shard_updates;
    }
  };

  /**
   * create an empty storage membership object with EMPTY_VERSION.
   */
  explicit StorageMembership();

  MembershipType getType() const override {
    return MembershipType::STORAGE;
  }

  /**
   * See Membership::applyUpdate().
   *
   * @return           0 for success, and write the new storage membership to
   *                   *new_membership_out. -1 for failure, with err set to:
   *                      all possible errors in ShardState::transition(); and
   *                      VERSION_MISMATCH  base version of the update doesn't
   *                                        match the current version
   *                      NOTINCONFIG       (for transitions other than
   *                                        adding shard) the requested shard
   *                                        does not exist in the config
   *                      EXISTS            requested to add shard which already
   *                                        exists in the membership
   */
  int applyUpdate(const Membership::Update& membership_update,
                  Membership* new_membership_out) const override;

  /**
   * See Membership::validate().
   *
   * Note: the function has a cost of O(n) where n is the number of shards
   * in the membership.
   */
  bool validate() const override;

  /**
   * See Membership::getMembershipNodes().
   */
  std::vector<node_index_t> getMembershipNodes() const override;

  /**
   * Get the shard state of a given storage shard.
   *
   * @return   a pair of (exist, ShardState) in which _exist_ is true if the
   *           request shard exists in the membership. In such case, its
   *           ShardState is also returned.
   */
  folly::Optional<ShardState> getShardState(ShardID shard) const;

  /**
   * Get the shard states of a given node
   *
   * @return a map of shard_index to ShardState. If node is not part
   *         of membership, an enmpty map is returned.
   */
  std::unordered_map<shard_index_t, ShardState>
  getShardStates(node_index_t node) const;

  /**
   * Check if writers and readers have access to a given shard. See the
   * definition of canWriteTo() and shouldReadFrom() in StorageState.h.
   */
  bool canWriteToShard(ShardID shard) const;
  bool shouldReadFromShard(ShardID shard) const;

  /**
   * Return true if at least one shard of the node is writable
   * (canWriteTo(shard) == true);
   */
  bool hasWritableShard(node_index_t node) const;

  /**
   * Return true if at least one shard of the node reader should read from.
   * (shouldReadFrom(shard) == true);
   */
  bool hasShardShouldReadFrom(node_index_t node) const;

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

  const std::set<ShardID>& getMetaDataShards() const {
    return metadata_shards_;
  }

  bool isInMetadataStorageSet(ShardID shard) const;

  // the following functions return a list/set of node indices of
  // metadata storage nodes. Useful in practice as LogDevice currently
  // uses its own implict sharding scheme (e.g., use data log shards)
  // for metadata logs.
  std::vector<node_index_t> getMetaDataNodeIndices() const;
  std::set<node_index_t> getMetaDataNodeSet() const;

  size_t numNodes() const {
    return node_states_.size();
  }

  bool isEmpty() const override {
    return node_states_.empty();
  }

  bool hasNode(node_index_t node) const override {
    return node_states_.count(node) > 0;
  }

  bool hasShard(ShardID shard) const;

  bool operator==(const StorageMembership& rhs) const;

  // Should only be used for testing.
  //
  // returns a new config with an incremented version; or a nullptr if
  // new_version <= current_version
  //
  // @param new_version should either be folly::none, in which case the new
  // version will be the current version + 1, or be strictly greater than the
  // current version.
  std::shared_ptr<const StorageMembership> withIncrementedVersion(
      folly::Optional<membership::MembershipVersion::Type> new_version) const;

 private:
  class NodeState {
   public:
    std::unordered_map<shard_index_t, ShardState> shard_states{};
    size_t numShards() const {
      return shard_states.size();
    }

    bool hasShard(shard_index_t shard) const {
      return shard_states.count(shard) > 0;
    }

    bool operator==(const NodeState& rhs) const {
      return shard_states == rhs.shard_states;
    }
  };

  using MapType = std::unordered_map<node_index_t, NodeState>;
  MapType node_states_{};

  // a separated index of storage shards whose MetaDataStorageState is not NONE
  std::set<ShardID> metadata_shards_{};

  // update the shard state of the given _shard_; also update the
  // metadata_shards_ index as needed. If _shard_ doesn't exist in
  // membership, create an entry for it.
  // Note: caller must guarantee that _state_ is valid
  void setShardState(ShardID shard, ShardState state);

  // remove the given _shard_ from the storage membership if the shard exists;
  // also update the metadata_shards_ index as needed.
  void eraseShardState(ShardID shard);

 public:
  ConstMapKeyIterator<MapType> begin() const {
    return ConstMapKeyIterator<MapType>(node_states_.cbegin());
  }
  ConstMapKeyIterator<MapType> end() const {
    return ConstMapKeyIterator<MapType>(node_states_.cend());
  }

  friend class configuration::nodes::NodesConfigLegacyConverter;
  friend class configuration::nodes::ShardStateTracker;
  friend class MembershipThriftConverter;
};

} // namespace membership
}} // namespace facebook::logdevice
