/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NodeAvailabilityChecker.h"

#include <chrono>

#include <folly/CppAttributes.h>

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

NodeAvailabilityChecker::NodeStatus
NodeAvailabilityChecker::checkNode(NodeSetState* nodeset_state,
                                   ShardID shard,
                                   StoreChainLink* destination_out,
                                   bool ignore_nodeset_state,
                                   bool allow_unencrypted_connections) const {
  ld_check(destination_out != nullptr);
  const auto& storage_membership =
      getNodesConfiguration()->getStorageMembership();
  if (!storage_membership->hasShard(shard)) {
    // shard is no longer in the membership
    return NodeStatus::NOT_AVAILABLE;
  }

  if (!ignore_nodeset_state) {
    const auto now = std::chrono::steady_clock::now();
    // atomically check the notAvailableUntil with the current time, as well
    // as reset the expired timestamp if needed.
    auto not_available_until =
        checkNotAvailableUntil(nodeset_state, shard, now);

    if (not_available_until != std::chrono::steady_clock::time_point::min()) {
      // Destination node is temporarily not available (No space, overloaded, or
      // rebuilding). Skip the node.
      return NodeStatus::NOT_AVAILABLE;
    }
  }

  if (!checkFailureDetector(shard.node())) {
    // FailureDetector identifies the node as dead
    return NodeStatus::NOT_AVAILABLE;
  }

  if (checkIsGraylisted(shard.node())) {
    // Outlier based graylisting marked this node as graylisted
    return NodeStatus::NOT_AVAILABLE;
  }

  NodeID dest_nid(shard.node(), 0);
  ClientID our_name_at_peer;

  NodeStatus result;

  int rv = checkConnection(
      dest_nid, &our_name_at_peer, allow_unencrypted_connections);
  if (rv != 0) {
    switch (err) {
      case E::NOTFOUND:
      case E::NEVER_CONNECTED:
        // We never tried/managed to connect to this node and connecting attempt
        // is not in progress. Let's try to connect and report as unavailable if
        // it fails immediately.
        rv = connect(dest_nid, allow_unencrypted_connections);
        if (rv != 0) {
          result = NodeStatus::NOT_AVAILABLE;
        } else {
          result = NodeStatus::AVAILABLE_NOCHAIN;
        }
        break;
      case E::ALREADY:
        // We are in the process of connecting to the node.
        // The destination is still good, but we can't chain-send.
        result = NodeStatus::AVAILABLE_NOCHAIN;
        break;
      case E::NOTCONN:
      case E::SSLREQUIRED:
        // We don't have a working connection to the node yet. Skip this
        // destination for now, but make sure a reconnection attempt is in
        // progress.
        connect(dest_nid, allow_unencrypted_connections);
        // fall-through
        FOLLY_FALLTHROUGH;
      case E::DISABLED:
        // destination is temporarily marked down. Skip it.
        FOLLY_FALLTHROUGH;
      case E::NOBUFS:
        // socket to the destination has reached its buffer limit.
        // skip the node for now.
        result = NodeStatus::NOT_AVAILABLE;
        break;
      default:
        // anything else is an internal error
        RATELIMIT_CRITICAL(std::chrono::seconds(1),
                           1,
                           "INTERNAL ERROR: Sender::checkConnection() failed "
                           "with unexpected error %s for NodeID %s",
                           error_name(err),
                           dest_nid.toString().c_str());
        ld_check(false);
        result = NodeStatus::NOT_AVAILABLE;
    }
  } else { // rv == 0
    result = NodeStatus::AVAILABLE;
  }

  if (result == NodeStatus::AVAILABLE ||
      result == NodeStatus::AVAILABLE_NOCHAIN) {
    ld_check(dest_nid.isNodeID());
    // our_name_at_peer may not be valid if chain-sending is not allowed,
    // but it must be valid when chain-sending is allowed. assert in debug
    // build
    ld_check(our_name_at_peer.valid() ||
             result == NodeStatus::AVAILABLE_NOCHAIN);
    *destination_out = {shard, our_name_at_peer};
  }

  return result;
}

int NodeAvailabilityChecker::checkConnection(NodeID nid,
                                             ClientID* our_name_at_peer,
                                             bool allow_unencrypted) const {
  return Worker::onThisThread()->sender().checkConnection(
      nid, our_name_at_peer, allow_unencrypted);
}

// `nodeset_state` is nullptr in tests.
std::chrono::steady_clock::time_point
NodeAvailabilityChecker::checkNotAvailableUntil(
    NodeSetState* nodeset_state,
    ShardID shard,
    std::chrono::steady_clock::time_point now) const {
  return nodeset_state ? nodeset_state->checkNotAvailableUntil(shard, now)
                       : std::chrono::steady_clock::time_point::min();
}

bool NodeAvailabilityChecker::checkFailureDetector(node_index_t index) const {
  Processor* processor = Worker::onThisThread()->processor_;
  if (!processor->isNodeAlive(index)) {
    return false;
  }

  // This only happens in tests.
  const std::vector<node_index_t>& do_not_pick =
      processor->settings()->test_do_not_pick_in_copysets;
  if (do_not_pick.empty()) {
    return true;
  }
  return std::find(do_not_pick.begin(), do_not_pick.end(), index) ==
      do_not_pick.end();
}

bool NodeAvailabilityChecker::checkIsGraylisted(node_index_t index) const {
  return getGraylistedNodes().count(index) != 0;
}

const std::unordered_set<node_index_t>&
NodeAvailabilityChecker::getGraylistedNodes() const {
  return Worker::onThisThread()->getGraylistedNodes();
}

std::shared_ptr<const configuration::nodes::NodesConfiguration>
NodeAvailabilityChecker::getNodesConfiguration() const {
  return Worker::onThisThread()->getNodesConfiguration();
}

int NodeAvailabilityChecker::connect(NodeID nid, bool allow_unencrypted) const {
  return Worker::onThisThread()->sender().connect(nid, allow_unencrypted);
}

const NodeAvailabilityChecker* NodeAvailabilityChecker::instance() {
  static NodeAvailabilityChecker a;
  return &a;
}

}} // namespace facebook::logdevice
