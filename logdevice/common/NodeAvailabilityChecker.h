/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeSetState.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file Does the checks needed before trying to store a record on a node:
 *        - checks if the node is marked down,
 *        - checks if we have a woking connection to the node; if not,
 *          tries to initiate a connection,
 *        - resolves node idex to a ChainLink by looking up generation and
 *          our client ID at peer.
 *       Can be mocked by tests.
 */

class NodeAvailabilityChecker {
 public:
  enum class NodeStatus {
    // Node is available to store record can do chain-sending if needed
    AVAILABLE,
    // Node is available, but cannot do chain-send
    AVAILABLE_NOCHAIN,
    // Node is not available to store any record
    NOT_AVAILABLE
  };

  /**
   * Check the available status of a single node in the node set. Also check if
   * the node is marked down on the socket of the current Worker. If the node is
   * available to store a copy, the destination StoreChainLink is written to.
   *
   * @param destination_out. Must be called from a worker thread.
   * @param ignore_nodeset_state  if true, don't ask nodeset.nodeset_state_
   *                              whether the node is marked as unavailable;
   *                              assume it's available
   * @param allow_unencrypted_connections if true, will accept a plaintext
   *                                      connection as suitable even if
   *                                      settings generally mandate an SSL one.
   */
  virtual NodeStatus
  checkNode(NodeSetState* nodeset_state,
            ShardID shard,
            StoreChainLink* destination_out,
            bool ignore_nodeset_state = false,
            bool allow_unencrypted_connections = false) const;

  virtual ~NodeAvailabilityChecker() {}

  // Returns a singleton instance.
  static const NodeAvailabilityChecker* instance();

 protected:
  // Proxy for Sender::checkConnection(). override in tests
  virtual int checkConnection(NodeID nid,
                              ClientID* our_name_at_peer,
                              bool allow_unencrypted) const;

  // Proxy for NodeSetState::checkNotAvailableUntil(). override in tests.
  // @param now is provided for test override.
  virtual std::chrono::steady_clock::time_point
  checkNotAvailableUntil(NodeSetState* nodeset_state,
                         ShardID shard,
                         std::chrono::steady_clock::time_point now) const;

  // check if the node with @param index is healthy in failure detector
  // @return true    if failure detector is not available or the node is alive
  //                 in failure detector
  //         false   node is dead in failure detector
  virtual bool checkFailureDetector(node_index_t index) const;

  // check if the node with @param index is graylisted by the outlier based
  // graylisting
  // @return true    if the node is graylisted and the node should be considered
  //                unavailable
  //         false when the node is not graylisted
  virtual bool checkIsGraylisted(node_index_t index) const;

  virtual const std::unordered_set<node_index_t>& getGraylistedNodes() const;

  // get the NodesConfiguration of the cluster. override in tests
  virtual std::shared_ptr<const configuration::nodes::NodesConfiguration>
  getNodesConfiguration() const;

  // Proxy for Sender::connect(). override in tests
  virtual int connect(NodeID nid, bool allow_unencrypted) const;
};

}} // namespace facebook::logdevice
