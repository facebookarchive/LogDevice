/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RecoverySet.h"

#include <algorithm>
#include <utility>

#include "logdevice/common/EpochRecovery.h"
#include "logdevice/common/RecoveryNode.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

RecoverySet::RecoverySet(
    const EpochMetaData& epoch_metadata,
    const std::shared_ptr<const NodesConfiguration>& nodes_configuration,
    EpochRecovery* recovery)
    : recovery_(recovery),
      failure_domain_nodes_(epoch_metadata.shards,
                            *nodes_configuration,
                            epoch_metadata.replication) {
  for (const ShardID& shard : epoch_metadata.shards) {
    if (!nodes_configuration->getStorageMembership()->shouldReadFromShard(
            shard)) {
      continue;
    }
    auto result = nodes_.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(shard),
                                 std::forward_as_tuple(shard, recovery));
    ld_check(result.second);

    // nodes starts with their state in SEALING
    onNodeStateChanged(shard, RecoveryNode::State::SEALING);
  }

  ld_check(recovery_ != nullptr);
}

std::set<ShardID>
RecoverySet::getNodes(std::function<bool(const RecoveryNode&)> pred) const {
  std::set<ShardID> nodeset;

  for (const auto& it : nodes_) {
    if (pred(it.second)) {
      nodeset.insert(it.second.getShardID());
    }
  }

  return nodeset;
}

std::set<ShardID>
RecoverySet::getNodesInState(RecoveryNode::State state) const {
  return getNodes(
      [state](const RecoveryNode& n) { return n.getState() == state; });
}

bool RecoverySet::nodeIsSealed(ShardID shard) const {
  auto it = nodes_.find(shard);
  if (it == nodes_.end()) {
    return false;
  }
  return it->second.getState() >= RecoveryNode::State::SEALED;
}

bool RecoverySet::nodeIsInMutationAndCleaningSet(ShardID shard) const {
  auto it = nodes_.find(shard);
  if (it == nodes_.end()) {
    return false;
  }
  return it->second.getState() >= RecoveryNode::State::MUTATABLE;
}

int RecoverySet::countShardsInState(RecoveryNode::State state) const {
  return failure_domain_nodes_.countShards(state);
}

FmajorityResult
RecoverySet::fmajorityForNodesInState(RecoveryNode::State state) const {
  return failure_domain_nodes_.isFmajority(state);
}

FmajorityResult
RecoverySet::fmajorityForNodesInStateOrHigher(RecoveryNode::State state) const {
  auto ret = failure_domain_nodes_.isFmajority(
      [state](RecoveryNode::State node_state) { return node_state >= state; });
  return ret;
}

bool RecoverySet::canReplicateNodesInState(RecoveryNode::State state) const {
  return failure_domain_nodes_.canReplicate(state);
}

bool RecoverySet::canReplicateNodesInStateOrHigher(
    RecoveryNode::State state) const {
  return failure_domain_nodes_.canReplicate(
      [state](RecoveryNode::State node_state) { return node_state >= state; });
}

void RecoverySet::onNodeStateChanged(ShardID shard, RecoveryNode::State to) {
  auto it = nodes_.find(shard);
  if (it == nodes_.end()) {
    // currently nodes are never removed from the RecoverySet
    ld_critical("Shard %s has changed state but it is not in the recovery set "
                "for epoch recovery %s",
                shard.toString().c_str(),
                recovery_->identify().c_str());
    ld_check(false);
    return;
  }

  ld_check(to == it->second.getState());
  failure_domain_nodes_.setShardAttribute(shard, to);
}

void RecoverySet::transition(ShardID shard, RecoveryNode::State to) {
  auto it = nodes_.find(shard);

  if (it == nodes_.end()) {
    // the only target state for which we allow `shard` to not be in the
    // storage set of the epoch is SEALED. An EpochRecovery machine may attempt
    // such a transition in response to onSealed() from its controlling
    // LogRecoveryRequest. It will get that event if the LogRecoveryRequest is
    // recovering multiple epochs and the storage set of those epochs are not
    // identical. Nothing to do.
    ld_check(to == RecoveryNode::State::SEALED);
    return;
  }

  RecoveryNode& node = it->second;
  ld_check(node.getShardID() == shard);
  node.transition(to);
}

AuthoritativeStatus
RecoverySet::getNodeAuthoritativeStatus(ShardID shard) const {
  if (!nodes_.count(shard)) {
    return AuthoritativeStatus::FULLY_AUTHORITATIVE;
  }
  return failure_domain_nodes_.getShardAuthoritativeStatus(shard);
}

size_t
RecoverySet::numShardsInAuthoritativeStatus(AuthoritativeStatus status) const {
  return failure_domain_nodes_.numShards(status);
}

void RecoverySet::setNodeAuthoritativeStatus(ShardID shard,
                                             AuthoritativeStatus status) {
  auto it = nodes_.find(shard);
  if (it == nodes_.end()) {
    return;
  }
  failure_domain_nodes_.setShardAuthoritativeStatus(shard, status);
}

RecoveryNode* RecoverySet::findNode(ShardID shard) {
  auto it = nodes_.find(shard);

  if (it == nodes_.end()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Got an unexpected shard %s while recovering %s.",
                    shard.toString().c_str(),
                    recovery_->identify().c_str());
    return nullptr;
  }

  RecoveryNode* node = &it->second;
  ld_check(node->getShardID() == shard);
  return node;
}

void RecoverySet::resendMessage(ShardID to) {
  RecoveryNode* node = findNode(to);

  if (node) {
    node->resendMessage();
  }
}

size_t RecoverySet::resetToSealed() {
  size_t n_matching = 0;

  for (auto& m : nodes_) {
    if (m.second.getState() > RecoveryNode::State::SEALED) {
      m.second.transition(RecoveryNode::State::SEALED);
      n_matching++;
    }
  }

  return n_matching;
}

size_t RecoverySet::advanceMatching(RecoveryNode::State to) {
  static_assert((uint8_t)RecoveryNode::State::SEALING == 0,
                "RecoveryNode::State::SEALING must be 0");
  ld_check(to != RecoveryNode::State::SEALING);

  // transition only nodes that are in the state immediately preceding _to_
  RecoveryNode::State pre = (RecoveryNode::State)((int8_t)to - 1);

  ld_check((int8_t)pre >= 0);

  size_t n_matching = 0;

  for (auto& m : nodes_) {
    if (m.second.getState() == pre) {
      m.second.transition(to);
      n_matching++;
    }
  }

  return n_matching;
}

void RecoverySet::onMessageSent(ShardID dest,
                                MessageType type,
                                Status status,
                                read_stream_id_t id) {
  auto it = nodes_.find(dest);
  if (it == nodes_.end()) {
    // This is very unlikely, but may happen if a message was queued before the
    // cluster was shrank and LogRecoveryRequest preempted.
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "called with status %s for a shard %s which "
                      "doesn't exist in the config",
                      error_name(status),
                      dest.toString().c_str());
    return;
  }

  RecoveryNode& node = it->second;
  if (node.getExpectedReadStreamID() != id) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "called for a %s message to %s during recovery of %s with "
                   "an unexpected read stream id (got %lu, expected %lu). "
                   "Ignoring.",
                   messageTypeNames()[type].c_str(),
                   dest.toString().c_str(),
                   recovery_->identify().c_str(),
                   id.val_,
                   node.getExpectedReadStreamID().val_);
    return;
  }

  node.onMessageSent(type, status, id);
}

bool RecoverySet::digestingReadStream(ShardID from, read_stream_id_t rsid) {
  const RecoveryNode* node = findNode(from);

  return node && node->digestingReadStream(rsid);
}

bool RecoverySet::expectingCleaned(ShardID from) {
  const RecoveryNode* node = findNode(from);

  if (!node) {
    return false;
  }

  if (node->getState() != RecoveryNode::State::CLEANING) {
    RATELIMIT_INFO(std::chrono::seconds(10),
                   10,
                   "RecoveryNode %s of %s is not expecting a CLEANED message."
                   "It is in state %s.",
                   from.toString().c_str(),
                   recovery_->identify().c_str(),
                   RecoveryNode::stateName(node->getState()));
    return false;
  }

  return true;
}

}} // namespace facebook::logdevice
