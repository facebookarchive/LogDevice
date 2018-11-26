/**
 * Copyright (c) 2017-present, Facebook, Inc. and its tffiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>
#include <chrono>
#include <memory>
#include <unordered_map>

#include <folly/Optional.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/SocketTypes.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/ReplicationProperty.h"

#define NUM_ROLES 2

/**
 * @file Config reading and parsing.
 */

namespace facebook { namespace logdevice { namespace configuration {

enum class StorageState {
  // This storage node can currently serve reads and accept writes
  READ_WRITE = 0,

  // This storage node can currently serve reads, but not writes from
  // clients.  Recovery writes and rebuilding writes (unless excluded by
  // its membership in the rebuilding set) are allowed.
  READ_ONLY,

  // Storage operations are completely disabled on this node.
  DISABLED
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

std::string toString(NodeRole& v);
bool nodeRoleFromString(const std::string&, NodeRole* out);

std::string storageStateToString(StorageState);
bool storageStateFromString(const std::string&, StorageState* out);

struct SequencerNodeAttributes {
  /**
   * A non-negative value indicating how many logs this node will run
   * sequencers for relative to other nodes in the cluster.  A value of
   * zero means sequencing is disabled on this node.
   */
  double weight = 1;
};

struct StorageNodeAttributes {
  /**
   * A positive value indicating how much store traffic this node will
   * receive relative to other nodes in the cluster.
   */
  double capacity = 1;

  /**
   * See the definition of StorageState above
   */
  StorageState state = StorageState::READ_WRITE;

  /**
   * Number of storage shards on this node.
   */
  shard_size_t num_shards = 1;

  /**
   * If true, the node will not be selected into any newly generated nodesets
   */
  bool exclude_from_nodesets = false;
};

struct Node {
  explicit Node(const Node&);

  Node() = default;
  Node(Node&&) = default;
  Node& operator=(Node&&) = default;

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
   * Generation number of this slot.  Hosts in a cluster are uniquely
   * identified by (index, generation) where index is into the array of
   * nodes.
   *
   * When a server is replaced, the generation number for the slot is
   * bumped by the config management tool.  Upon encountering an (index,
   * generation) pair where the generation is less than what is in the
   * config, the system knows that the host referred to by the pair is
   * dead.
   *
   * Note: default to 1 as generation <= 0 doesn't make sense.
   */
  int generation{1};

  /**
   * Location information of the node.
   */
  folly::Optional<NodeLocation> location;

  /**
   * Settings overridden for this node.
   */
  std::unordered_map<std::string, std::string> settings;

  /**
   * Bitmap storing node roles
   */
  std::bitset<NUM_ROLES> roles;

  std::unique_ptr<SequencerNodeAttributes> sequencer_attributes;
  std::unique_ptr<StorageNodeAttributes> storage_attributes;

  bool hasRole(NodeRole r) const {
    auto id = static_cast<size_t>(r);
    return roles.test(id);
  }

  void setRole(NodeRole r) {
    auto id = static_cast<size_t>(r);
    roles.set(id);
  }

  double getSequencerWeight() const {
    if (hasRole(NodeRole::SEQUENCER)) {
      return sequencer_attributes->weight;
    } else {
      return 0;
    }
  }
  bool includeInNodesets() const {
    return isWritableStorageNode() &&
        !storage_attributes->exclude_from_nodesets;
  }
  bool isSequencingEnabled() const {
    return getSequencerWeight() > 0;
  }

  bool isReadableStorageNode() const {
    return hasRole(NodeRole::STORAGE) &&
        storage_attributes->state != StorageState::DISABLED;
  }
  bool isWritableStorageNode() const {
    return hasRole(NodeRole::STORAGE) &&
        storage_attributes->state == StorageState::READ_WRITE;
  }
  double getWritableStorageCapacity() const {
    if (!isWritableStorageNode()) {
      return 0.0;
    }
    return storage_attributes->capacity;
  }
  StorageState getStorageState() const {
    return !hasRole(NodeRole::STORAGE) ? StorageState::DISABLED
                                       : storage_attributes->state;
  }

  bool isDisabled() const {
    return !isReadableStorageNode() && !isSequencingEnabled();
  }
  shard_size_t getNumShards() const {
    return !hasRole(NodeRole::STORAGE) ? 0 : storage_attributes->num_shards;
  }
  // Should only be used for backwards-compatible config serialization
  int getLegacyWeight() const {
    switch (getStorageState()) {
      case StorageState::READ_WRITE:
      case StorageState::READ_ONLY: {
        double res = getWritableStorageCapacity();
        return res == 0.0 ? 0 : std::max(1, int(std::lround(res)));
      }
      case StorageState::DISABLED:
        return -1;
    }
    ld_check(false);
    return -1;
  }

  void addSequencerRole(double weight = 1.0) {
    setRole(NodeRole::SEQUENCER);
    sequencer_attributes = std::make_unique<SequencerNodeAttributes>();
    sequencer_attributes->weight = weight;
  }

  void addStorageRole(shard_size_t num_shards = 1) {
    setRole(NodeRole::STORAGE);
    storage_attributes = std::make_unique<StorageNodeAttributes>();
    storage_attributes->num_shards = num_shards;
  }

  // return a human-readable string for the location info
  std::string locationStr() const;

  // return the corresponding sockaddr for the given socket type
  const Sockaddr& getSockaddr(SocketType type, ConnectionType conntype) const;
};

using Nodes = std::unordered_map<node_index_t, Node>;

// see ServerConfig::validStorageSet()
bool validStorageSet(const Nodes& cluster_nodes,
                     const StorageSet& storage_set,
                     ReplicationProperty replication,
                     bool strict = false);

}}} // namespace facebook::logdevice::configuration
