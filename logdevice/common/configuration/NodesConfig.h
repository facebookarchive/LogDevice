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

#include <folly/dynamic.h>

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
  }
  const Nodes& getNodes() const {
    return nodes_;
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

  folly::dynamic toJson() const;

  /**
   * NOTE: Only used for consistency checks in NodesConfiguration.
   * NodesConfiguration::getMaxNodeIndex() is what you're looking for.
   *
   * Returns the maximum key in getNodes().
   */
  size_t getMaxNodeIdx_DEPRECATED() const;

 private:
  // NOTE: Only used for consistency checks in NodesConfiguration.
  // NodesConfiguration::getStorageNodesHash() is what you're looking for.
  //
  // calculates a hash of storage-relevant attributes of nodes: for all nodes,
  // hashes their node_ids, weights, num_shards and locations and stores it in
  // `hash_`
  uint64_t calculateHash() const;

  // NOTE: Only used for consistency checks in NodesConfiguration.
  // NodesConfiguration::getNumShards() is what you're looking for.
  //
  // TODO(T15517759): NodesConfigParser currently verifies that all nodes in the
  // config have the same amount of shards, which is also stored here. This
  // member as well as the check in NodesConfigParser will be removed when the
  // Flexible Log Sharding project is fully implemented.
  // In the mean time, this member is used by state machines that need to
  // convert node_index_t values to ShardID values.
  shard_size_t calculateNumShards() const;

  Nodes nodes_;

  // NOTE: NodesConfig is the current nodes config data structure in use, which
  // will be replaced by nodesConfiguration_ in the future. nodesConfiguration_
  // is the new format, and it co-exists with current representation.
  std::shared_ptr<const nodes::NodesConfiguration> nodes_configuration_;

  friend class nodes::NodesConfigLegacyConverter;
};

}}} // namespace facebook::logdevice::configuration
