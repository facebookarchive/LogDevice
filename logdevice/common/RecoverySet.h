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
#include <vector>

#include "logdevice/common/EpochMetaData.h"
#include "logdevice/common/FailureDomainNodeSet.h"
#include "logdevice/common/RecoveryNode.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class EpochRecovery;

/**
 * @file RecoverySet is a container of RecoveryNode objects that are eligible
 *       to participate in the recovery of a specific epoch of a log. Its
 *       main tasks are (1) to route state transition requests to the
 *       appropriate RecoveryNode objects identified by ShardIDs, and
 *       (2) to determine if the set of RecoveryNodes in a particular state
 *       satisfy certain replication requirements (e.g., f-majority and
 *       can replicate) to the EpochRecovery object responsible for the
 *       recovery of that epoch. This is done by maintaining the epoch
 *       metadata (storage set + replication property) as well authoritative
 *       status of storage shards via a FailureDomainNodeSet object.
 */

class RecoverySet {
 public:
  /**
   * @param epoch_metadata           metadata of the epoch we're recovering
   * @param nodes_configuration      cluster configuration
   * @param recovery                 EpochRecovery object that owns this set
   */
  RecoverySet(
      const EpochMetaData& epoch_metadata,
      const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
      EpochRecovery* recovery);

  size_t size() const {
    return nodes_.size();
  }

  bool containsShard(ShardID shard) {
    return nodes_.count(shard);
  }

  /**
   * Transition the RecoveryNode object in nodes_ identified by @param
   * nid into the state @param to.
   *
   * If @param to is SEALED, @param nid is not required to exist in nodes_
   * (no-op).
   *
   * The state of RecoveryNode is not guaranteed to change after this
   * call (for instance, it will stay unchanged if the underlying
   * RecoveryNode::transition() fails to sends a message and schedules
   * a retry.
   *
   * Note that if the current state of node `nid` is DIGESTING or DIGESTED and
   * the node's state is rolled back to something smaller, it is the caller'
   * responsibility to update the Digest accordingly.
   */
  void transition(ShardID shard, RecoveryNode::State to);

  /**
   * @param nid Nid of the node to get the authoritative status for.
   *
   * @return AuthoritativeStatus of a node or
   * AuthoritativeStatus::FULLY_AUTHORITATIVE if the node is not in the nodeset.
   */
  AuthoritativeStatus getNodeAuthoritativeStatus(ShardID shard) const;

  /**
   * @return number of nodes that have the given authoritative status.
   */
  size_t numShardsInAuthoritativeStatus(AuthoritativeStatus status) const;

  /**
   * Change the authoritative status of a node.
   * Called when Worker::shardStatusManager()::shard_status_ is updated or a
   * node sent STARTED(E::REBUILDING) or SEALED(E::REBUILDING).
   *
   * @param nid    Node for which to change the authoritative status.
   * @param status New status for the node.
   */
  void setNodeAuthoritativeStatus(ShardID shard, AuthoritativeStatus status);

  /**
   * Called when the state of a RecoveryNode identified by _shard_ has actually
   * been changed to State _to_.
   */
  void onNodeStateChanged(ShardID shard, RecoveryNode::State to);

  /**
   * If @param nid is in the recovery set, call resendMessage() on the
   * corresponding RecoveryNode. Otherwise do nothing.
   */
  void resendMessage(ShardID shard);

  /**
   * call transition(nid, to) on those RecoveryNode objects in nodes_
   * whose state immediately preceds @param to in the list of
   * RecoveryNode::State constants. For example, for to=DIGESTING only
   * the nodes in state SEALED match. For CLEANING only the nodes in
   * DIGESTED match, and so on.
   *
   * @param to   target state. Must not be SEALING, as it's the first state.
   *
   * @return the number of nodes selected for transition. Whether or not those
   *         nodes actually change state is determined by the rules of
   *         transition() method above.
   */
  size_t advanceMatching(RecoveryNode::State to);

  /**
   * call transition(nid, SEALED) on all RecoveryNode objects in nodes_
   * that are in a state greater than SEALED.
   *
   * @return  the number of RecoveryNode objects selected for transition
   */
  size_t resetToSealed();

  /**
   * Returns the set of ShardIDs of nodes whose state is @param state.
   */
  std::set<ShardID> getNodesInState(RecoveryNode::State state) const;

  /**
   * Returns the set of ShardIDs that have @param pred return true.
   */
  std::set<ShardID>
  getNodes(std::function<bool(const RecoveryNode&)> pred) const;

  /**
   * @return True if the node's state is >= SEALED.
   */
  bool nodeIsSealed(ShardID shard) const;

  /**
   * @return True if the node's state is >= MUTATABLE.
   */
  bool nodeIsInMutationAndCleaningSet(ShardID shard) const;

  /**
   * @return  the number of RecoveryNode objects in nodes_ whose state is
   *          @param state.
   */
  int countShardsInState(RecoveryNode::State state) const;

  /**
   * Find if we have an f-majority of nodes at a given state.
   * @param state State the node must be in
   * @return @see FailureDomainNodeSet::FmajorityResult.
   */
  FmajorityResult fmajorityForNodesInState(RecoveryNode::State state) const;

  /**
   * Find if we have an f-majority of nodes at a given state or higher.
   * @param state State the node must be in
   * @return @see FailureDomainNodeSet::FmajorityResult.
   */
  FmajorityResult
  fmajorityForNodesInStateOrHigher(RecoveryNode::State state) const;

  /**
   * @return  if the subset of recovery nodes that are in @param state can
   *          meet the replication requirement of the recovering epoch,
   *          for example, the subset consist of nodes from at least two
   *          different failure domains in a location scope.
   */
  bool canReplicateNodesInState(RecoveryNode::State state) const;

  /**
   * @return  if the subset of recovery nodes that are in @param state or
   *          higher meet the replication requirement of the recovering epoch.
   */
  bool canReplicateNodesInStateOrHigher(RecoveryNode::State state) const;

  /**
   * See EpochRecovery::onMessageSent() in EpochRecovery.h
   */
  void onMessageSent(ShardID dest,
                     MessageType type,
                     Status status,
                     read_stream_id_t id = READ_STREAM_ID_INVALID);

  /**
   * @return true iff we sent a START with read stream id @param rsid
   *              to node @param from and are currently expecting STARTED
   *              and/or digest records back
   */
  bool digestingReadStream(ShardID from, read_stream_id_t rsid);

  /**
   * @return true iff we sent a CLEAN to node @param from and are currently
   *         expecting a CLEANED message back.
   */
  bool expectingCleaned(ShardID from);

 private:
  // this map contains one RecoveryNode object for each node index in
  // this epoch's nodeset.
  std::map<ShardID, RecoveryNode> nodes_;

  // owning EpochRecovery object, used for error messages
  const EpochRecovery* recovery_;

  using FailureDomain =
      FailureDomainNodeSet<RecoveryNode::State, HashEnum<RecoveryNode::State>>;

  // track the failure domain information of nodes participated in recovery
  FailureDomain failure_domain_nodes_;

  /**
   * @return  a nodes_ element identified by @param nid, if one exists.
   *          Otherwise nullptr.
   */
  RecoveryNode* findNode(ShardID shard);
};

}} // namespace facebook::logdevice
