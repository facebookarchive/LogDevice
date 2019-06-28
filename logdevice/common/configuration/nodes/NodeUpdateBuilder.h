/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/configuration/NodeLocation.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/membership/types.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

/**
 * A helper class that builds an "Add Node" NodesConfiguration update in a
 * builder style.
 */
class NodeUpdateBuilder {
 public:
  struct Result {
    Status status;
    // If status != Status::OK, message will contain a human readable error
    std::string message;
  };

  NodeUpdateBuilder& setNodeIndex(node_index_t);
  NodeUpdateBuilder& setDataAddress(Sockaddr);
  NodeUpdateBuilder& setGossipAddress(Sockaddr);
  NodeUpdateBuilder& setSSLAddress(Sockaddr);
  NodeUpdateBuilder& setLocation(NodeLocation);
  NodeUpdateBuilder& setName(std::string);
  NodeUpdateBuilder& isSequencerNode();
  NodeUpdateBuilder& isStorageNode();
  NodeUpdateBuilder& setNumShards(shard_size_t);
  NodeUpdateBuilder& setStorageCapacity(double);
  NodeUpdateBuilder& setSequencerWeight(double);

  /**
   * Validates the correctness of the update by checking:
   *  1. The required fields (index, address, name and roles) are set.
   *  2. If the node is a sequencer, then sequencer weight should be set.
   *  3. If the node is a storage node, then the capacity and num shards are
   *     set.
   */
  Result validate() const;

  /**
   * Build a new node update and add it to the passed update structure.
   */
  Result
  buildAddNodeUpdate(NodesConfiguration::Update& update,
                     membership::MembershipVersion::Type sequencer_version,
                     membership::MembershipVersion::Type storage_version) &&;

  // Build an attributes update for the node with index *node_index_* by
  // comparing its current attributes versus the one in the builder.
  Result buildUpdateNodeUpdate(
      NodesConfiguration::Update& update,
      const configuration::nodes::NodesConfiguration& nodes_configuration) &&;

 private:
  std::unique_ptr<StorageNodeAttribute> buildStorageAttributes();
  std::unique_ptr<SequencerNodeAttribute> buildSequencerAttributes();
  std::unique_ptr<NodeServiceDiscovery> buildNodeServiceDiscovery();

 private:
  // Service Discovery
  folly::Optional<node_index_t> node_index_;
  folly::Optional<Sockaddr> data_address_;
  folly::Optional<Sockaddr> gossip_address_;
  folly::Optional<Sockaddr> ssl_address_;
  folly::Optional<NodeLocation> location_;
  configuration::nodes::RoleSet roles_;
  folly::Optional<std::string> name_;

  // Storage Attributes
  folly::Optional<shard_size_t> num_shards_;
  folly::Optional<double> storage_capacity_;

  // Sequencer Attributes
  folly::Optional<double> sequencer_weight_;
};

}}}} // namespace facebook::logdevice::configuration::nodes
