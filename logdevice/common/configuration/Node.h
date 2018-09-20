/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <unordered_map>

#include <bitset>
#include <folly/Optional.h>
#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/Socket-fwd.h"

#define NUM_ROLES 2

/**
 * @file Config reading and parsing.
 */

namespace facebook { namespace logdevice { namespace configuration {

enum class StorageState {
  // This is a storage node, it should serve reads and accept writes
  READ_WRITE = 0,

  // This node should get reads, but not writes. Note that it can still take
  // recovery writes (and rebuilding writes if it's not in the rebuilding set)
  READ_ONLY,

  // This is not a storage node
  NONE,
};

// Roles available for any given node
// A node can have more than one role at any given time
enum class NodeRole : unsigned int {
  // Sequencers nodes receive records from log writers
  // and create a total ordering on records
  SEQUENCER,

  // Storage nodes store log records persistently
  // and deliver them to log readers
  STORAGE,
};

std::string storageStateToString(StorageState);
bool storageStateFromString(const std::string&, StorageState* out);

std::string toString(NodeRole& v);
bool nodeRoleFromString(const std::string&, NodeRole* out);

struct Node {
  /**
   * The IP (v4 or v6) address, including port number.
   */
  Sockaddr address;
  Sockaddr gossip_address;

  /**
   * The IP (v4 or v6) address, including port number, for SSL communication.
   * In production this will mostly be identical to address, except for the
   * port. We need both address and ssl_address, so the server could serve
   * both non-SSL and SSL clients.
   */
  folly::Optional<Sockaddr> ssl_address;

  /**
   * The IP (v4 or v6) Admin address, including port number,
   * for Admin API communication.
   */
  folly::Optional<Sockaddr> admin_address;

  /**
   * If this node is a storage node, this is a positive value indicating how
   * much traffic it will get relative to other nodes in the cluster.
   */
  folly::Optional<double> storage_capacity;

  /**
   * See the definition of StorageState above
   */
  StorageState storage_state = StorageState::READ_WRITE;

  /**
   * If true, the node will not be selected into any newly generated nodesets
   */
  bool exclude_from_nodesets = false;

  /**
   * Generation number of this slot.  Hosts in a cluster are uniquely
   * identified by (index, generation) where index is into the array of
   * nodes.
   *
   * When a server is replaced, the generation number for the slot is
   * bumped by the config management tool.  Upon encountering an (index,
   * generation) pair where the generation is less than what is in the
   * config, the system knows that the host referred to by the pair is
   * dead.
   */
  int generation;

  /**
   * Number of shards on this storage server.
   */
  shard_size_t num_shards = 1;

  /**
   * A non-negative value indicating how many logs this node will run
   * sequencers for relative to other nodes in the cluster.  A value of
   * zero means this is not a sequencer node.
   */
  double sequencer_weight = 1;

  /**
   * Location information of the node.
   */
  folly::Optional<NodeLocation> location;

  /**
   * Data retention duration for the node. Used by NodeSetSelector to
   * determine node set for a log.
   */
  folly::Optional<std::chrono::seconds> retention;

  /**
   * Settings overridden for this node.
   */
  std::unordered_map<std::string, std::string> settings;

  /**
   * Bitmap storing node roles
   */
  std::bitset<NUM_ROLES> roles;

  bool hasRole(NodeRole r) const {
    auto id = static_cast<size_t>(r);
    return (
        // By default, if no roles are provided, the node is both a sequencer
        // and a storage node
        (roles.none() &&
         (r == NodeRole::SEQUENCER || r == NodeRole::STORAGE)) ||
        roles.test(id));
  }
  void setRole(NodeRole r) {
    auto id = static_cast<size_t>(r);
    roles.set(id);
  }
  bool isReadableStorageNode() const {
    return hasRole(NodeRole::STORAGE) &&
        (storage_state == StorageState::READ_WRITE ||
         storage_state == StorageState::READ_ONLY);
  }
  bool isWritableStorageNode() const {
    return hasRole(NodeRole::STORAGE) &&
        storage_state == StorageState::READ_WRITE;
  }
  bool includeInNodesets() const {
    return isWritableStorageNode() && !exclude_from_nodesets;
  }
  bool isSequencingEnabled() const {
    return hasRole(NodeRole::SEQUENCER) && sequencer_weight > 0;
  }
  bool isDisabled() const {
    return !isReadableStorageNode() && !isSequencingEnabled();
  }
  double getWritableStorageCapacity() const {
    if (storage_state != StorageState::READ_WRITE) {
      return 0.0;
    }
    return storage_capacity.value_or(DEFAULT_STORAGE_CAPACITY);
  }

  // Should only be used for backwards-compatible config serialization
  int getLegacyWeight() const {
    switch (storage_state) {
      case StorageState::READ_WRITE:
      case StorageState::READ_ONLY: {
        double res = getWritableStorageCapacity();
        return res == 0.0 ? 0 : std::max(1, int(std::lround(res)));
      }
      case StorageState::NONE:
        return -1;
    }
    ld_check(false);
    return -1;
  }

  // return a human-readable string for the location info
  std::string locationStr() const;

  // return the corresponding sockaddr for the given socket type
  const Sockaddr& getSockaddr(SocketType type, ConnectionType conntype) const;

  // storage capacity that would be used for a storage node if no value is
  // specified
  static constexpr double DEFAULT_STORAGE_CAPACITY = 1;

  // storage state that would be used for a node if no value is specified
  static constexpr StorageState DEFAULT_STORAGE_STATE = StorageState::NONE;
};

using Nodes = std::unordered_map<node_index_t, Node>;

// see ServerConfig::validStorageSet()
bool validStorageSet(const Nodes& cluster_nodes,
                     const StorageSet& storage_set,
                     ReplicationProperty replication,
                     bool strict = false);

}}} // namespace facebook::logdevice::configuration
