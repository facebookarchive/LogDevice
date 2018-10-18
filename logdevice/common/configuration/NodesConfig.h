/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <unordered_map>

#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice { namespace configuration {

struct MetaDataLogsConfig;

class NodesConfig {
 public:
  explicit NodesConfig()
      : nodes_configuration_(
            std::make_shared<const nodes::NodesConfiguration>()) {}

  explicit NodesConfig(Nodes nodes)
      : nodes_configuration_(
            std::make_shared<const nodes::NodesConfiguration>()) {
    setNodes(std::move(nodes));
  }

  void setNodes(Nodes nodes) {
    nodes_ = std::move(nodes);
    calculateHash();
    calculateNumShards();
  }
  const Nodes& getNodes() const {
    return nodes_;
  }
  uint64_t getStorageNodeHash() const {
    return hash_;
  }
  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  shard_size_t getNumShards() const {
    return num_shards_;
  }

  ////////////////////// New NodesConfiguration ///////////////////////

  bool hasNodesConfiguration() const {
    return nodes_configuration_ != nullptr;
  }

  const std::shared_ptr<const nodes::NodesConfiguration>&
  getNodesConfiguration() const {
    return nodes_configuration_;
  }

  void setNodesConfigurationVersion(config_version_t version) {
    // TODO(T33035439): set the nodes config version to be the same as the
    // config version_ during the migration period. Will be deprecated.
    if (nodes_configuration_) {
      nodes_configuration_ = nodes_configuration_->withVersion(
          membership::MembershipVersion::Type(version.val()));
    }
  }

  // generate the new NodesConfiguration representation based on `this',
  // and the given @param meta_config and @param version.
  // @return   true if the new NodesConfiguration is successfully generated
  bool generateNodesConfiguration(const MetaDataLogsConfig& meta_config,
                                  config_version_t version);

 private:
  // calculates a hash of storage-relevant attributes of nodes: for all nodes,
  // hashes their node_ids, weights, num_shards and locations and stores it in
  // `hash_`
  void calculateHash();

  // TODO(T15517759): remove when Flexible Log Sharding is fully implemented.
  void calculateNumShards();

  Nodes nodes_;

  uint64_t hash_{0};

  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member as well as the check in NodesConfigParser will be removed when the
  // Flexible Log Sharding project is fully implemented.
  // In the mean time, this member is used by state machines that need to
  // convert node_index_t values to ShardID values.
  shard_size_t num_shards_{0};

  // NOTE: NodesConfig is the current nodes config data structure in use, which
  // will be replaced by nodesConfiguration_ in the future. nodesConfiguration_
  // is the new format, and it co-exists with current representation.
  std::shared_ptr<const nodes::NodesConfiguration> nodes_configuration_;
};

}}} // namespace facebook::logdevice::configuration
