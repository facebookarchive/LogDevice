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

#include "logdevice/common/NodeID.h"
#include "logdevice/common/membership/Membership.h"
#include "logdevice/common/membership/StorageState.h"
#include "logdevice/common/membership/StorageStateTransitions.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace membership {

/**
 * @file Sequencer membership describes the collection of sequencer nodes with
 *       various different sequencer weights. It is the _dynamic_ and mutable
 *       part of the sequencer configuration.
 *
 *       Note: all state and transition functions defined are not thread-safe.
 *       Upper layer is responsible for atomically update the state and
 *       propagate them to all execution contexts.
 */

enum class SequencerMembershipTransition : uint8_t {
  ADD_NODE = 0,
  REMOVE_NODE,
  SET_WEIGHT,

  Count
};

struct SequencerNodeState {
  /**
   * A non-negative value indicating how many logs this node will run
   * sequencers for relative to other nodes in the cluster.  A value of
   * zero means sequencing is disabled on this node.
   */
  double weight;

  // identifier for the maintenance event that correspond to the
  // current node state. Used by the maintenance state machine
  MaintenanceID::Type active_maintenance;

  std::string toString() const;

  bool isValid() const;

  bool operator==(const SequencerNodeState& rhs) const {
    return weight == rhs.weight && active_maintenance == rhs.active_maintenance;
  }

  bool operator!=(const SequencerNodeState& rhs) const {
    return !(*this == rhs);
  }

  // Describe the update that can apply to SequencerNodeState
  struct Update {
    SequencerMembershipTransition transition;

    // set the weight for adding new node or resetting an existing node
    double weight;

    // identifier for the new maintenance requesting the state transition
    MaintenanceID::Type maintenance;

    bool isValid() const;
    std::string toString() const;
  };
};

class SequencerMembership : public Membership {
 public:
  class Update : public Membership::Update {
   public:
    using NodeMap = std::map<node_index_t, SequencerNodeState::Update>;

    // each sequencer membership update is strictly conditioned on a base
    // membership version of which the update can only be applied
    MembershipVersion::Type base_version;

    // a batch of per-node updates
    NodeMap node_updates;

    int addNode(node_index_t node, SequencerNodeState::Update update) {
      auto res = node_updates.emplace(node, std::move(update));
      return res.second ? 0 : -1;
    }

    explicit Update(MembershipVersion::Type base) : base_version(base) {}

    bool isValid() const override;
    MembershipType getType() const override {
      return MembershipType::SEQUENCER;
    }
    std::string toString() const override;
  };

  /**
   * create an empty sequencer membership object with EMPTY_VERSION.
   */
  explicit SequencerMembership();

  MembershipType getType() const override {
    return MembershipType::SEQUENCER;
  }

  /**
   * See Membership::applyUpdate().
   *
   * @return           0 for success, and write the new sequencer membership to
   *                   *new_membership_out. -1 for failure, with err set to:
   *                      VERSION_MISMATCH  base version of the update doesn't
   *                                        match the current version
   *                      NOTINCONFIG       (for transitions other than
   *                                        adding node) the requested node
   *                                        does not exist in the config
   *                      EXISTS            requested to add shard which already
   *                                        exists in the membership
   */
  int applyUpdate(const Membership::Update& membership_update,
                  Membership* new_membership_out) const override;

  /**
   * See Membership::validate().
   *
   * Note: the function has a cost of O(n) where n is the number of nodes
   * in the membership.
   */
  bool validate() const override;

  /**
   * See Membership::getMembershipNodes().
   */
  std::vector<node_index_t> getMembershipNodes() const override;

  /**
   * Get the node state of a given sequencer node.
   *
   * @return   a pair of (exist, SequencerNodeState) in which _exist_ is true if
   *           the request node exists in the membership. In such case, its
   *           SequencerNodeState is also returned.
   */
  std::pair<bool, SequencerNodeState> getNodeState(node_index_t node) const;
  const SequencerNodeState* getNodeStatePtr(node_index_t node) const;

  size_t numNodes() const {
    return node_states_.size();
  }

  bool hasNode(node_index_t node) const override {
    return node_states_.count(node) > 0;
  }

  /**
   * @return  true if node is in the membership and has a positive sequencer
   *          weight.
   */
  bool isSequencingEnabled(node_index_t node) const;

  std::string toString() const;

  bool isEmpty() const override {
    return node_states_.empty();
  }

  using MapType = std::unordered_map<node_index_t, SequencerNodeState>;
  ConstMapKeyIterator<MapType> begin() const {
    return ConstMapKeyIterator<MapType>(node_states_.cbegin());
  }
  ConstMapKeyIterator<MapType> end() const {
    return ConstMapKeyIterator<MapType>(node_states_.cend());
  }

  bool operator==(const SequencerMembership& rhs) const;

 private:
  MapType node_states_;

  // update the sequencer node state of the given node; If _node_ doesn't exist
  // in membership, create an entry for it.
  void setNodeState(node_index_t node, SequencerNodeState state);

  // remove the given _node_ from the sequencer membership if the node exists;
  // @return  true if the removal actual happened
  bool eraseNodeState(node_index_t node);

  friend class MembershipCodecFlatBuffers;
  friend class configuration::nodes::NodesConfigLegacyConverter;
};

}}} // namespace facebook::logdevice::membership
