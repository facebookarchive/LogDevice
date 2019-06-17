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
#include "logdevice/common/configuration/nodes/NodeRole.h"

#define NUM_ROLES 2

/**
 * @file Config reading and parsing.
 */

namespace facebook { namespace logdevice { namespace configuration {

enum class StorageState {
  // This storage node can currently serve reads and accept writes
  READ_WRITE = 0,

  // This storage node can currently serve reads, but not writes.
  // Recovery writes are still allowed.
  // The node is still eligible to be included in nodesets, unless
  // exclude_from_nodesets is set. With the exception of "random[-v2]" and
  // "random-crossdomain[-v2]" nodeset selector types, which for historical
  // reasons don't pick read-only nodes.
  READ_ONLY,

  // Storage operations are completely disabled on this node, and the node
  // doesn't have any useful data to send to readers.
  // The node is still included in nodesets, unless exclude_from_nodesets
  // is set.
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

static_assert((size_t)NodeRole::SEQUENCER == (size_t)nodes::NodeRole::SEQUENCER,
              "NodeRole value mismatch between old and new nodes configuration "
              "definition.");
static_assert((size_t)NodeRole::STORAGE == (size_t)nodes::NodeRole::STORAGE,
              "NodeRole value mismatch between old and new nodes configuration "
              "definition.");

std::string toString(NodeRole& v);
bool nodeRoleFromString(const std::string&, NodeRole* out);

std::string storageStateToString(StorageState);
bool storageStateFromString(const std::string&, StorageState* out);

class SequencerNodeAttributes {
 public:
  SequencerNodeAttributes(bool enabled, double weight)
      : weight_(weight), enabled_(enabled) {}

  SequencerNodeAttributes() {}

  double getConfiguredWeight() const {
    return weight_;
  }

  double getEffectiveWeight() const {
    return enabled_ ? weight_ : 0;
  }

  bool enabled() {
    return enabled_;
  }

  void setEnabled(bool enabled) {
    enabled_ = enabled;
  }

  void setWeight(double weight) {
    weight_ = weight;
  }

  bool operator==(const SequencerNodeAttributes& rhs) const {
    return weight_ == rhs.weight_ && enabled_ == rhs.enabled_;
  }

  bool operator!=(const SequencerNodeAttributes& rhs) const {
    return !(*this == rhs);
  }

 private:
  /**
   * A non-negative value indicating how many logs this node will run
   * sequencers for relative to other nodes in the cluster.  A value of
   * zero means sequencing is disabled on this node.
   * The weight is assumed to be zero, when enable=false.
   */
  double weight_ = 1;

  /**
   * Determines if a sequencer is enabled or not. If the sequencer is not
   * enabled, it's similar to giving it a weight of zero. It's done this way
   * to be able to enable/disable sequencers without memorizing its previous
   * weight.
   */
  bool enabled_ = true;
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

  /*
   * This is a unique name for the node in the cluster. This is currently not a
   * required field and can be empty.
   */
  std::string name{""};

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
   * Note: we are in the middle of getting rid of `generation'. Currently
   * generation should only be used in storage node replacement. For nodes
   * w/o a storage role, their generation should always be set to 1.
   */
  int generation{1};

  /**
   * Location information of the node.
   */
  folly::Optional<NodeLocation> location;

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
      return sequencer_attributes->getEffectiveWeight();
    } else {
      return 0;
    }
  }
  bool includeInNodesets() const {
    return hasRole(NodeRole::STORAGE) &&
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

  void addSequencerRole(bool enabled = true, double weight = 1.0) {
    setRole(NodeRole::SEQUENCER);
    sequencer_attributes = std::make_unique<SequencerNodeAttributes>();
    sequencer_attributes->setEnabled(enabled);
    sequencer_attributes->setWeight(weight);
  }

  void addStorageRole(shard_size_t num_shards = 1) {
    setRole(NodeRole::STORAGE);
    storage_attributes = std::make_unique<StorageNodeAttributes>();
    storage_attributes->num_shards = num_shards;
  }

  // return a human-readable string for the location info
  std::string locationStr() const;
};

using Nodes = std::unordered_map<node_index_t, Node>;

}}} // namespace facebook::logdevice::configuration
