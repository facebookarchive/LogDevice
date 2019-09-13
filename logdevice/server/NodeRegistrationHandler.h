/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Expected.h>

#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"
#include "logdevice/common/configuration/nodes/NodeUpdateBuilder.h"
#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationStore.h"
#include "logdevice/server/ServerSettings.h"

namespace facebook { namespace logdevice {

/**
 * NodeRegistrationHandler is the class responsible for registering the node
 * on startup in the NodesConfiguration and updating its attributes if they
 * changed. The node registers itself using the attributes that was passed to it
 * via CLI arguments.
 */
class NodeRegistrationHandler {
 public:
  NodeRegistrationHandler(
      ServerSettings settings,
      std::shared_ptr<const configuration::nodes::NodesConfiguration>
          nodes_configuration,
      std::shared_ptr<configuration::nodes::NodesConfigurationStore> store)
      : server_settings_(std::move(settings)),
        nodes_configuration_(std::move(nodes_configuration)),
        store_(std::move(store)) {}

  /**
   * Register the node in the NodesConfiguration and returns the index that this
   * node got on success. Node indicies are allocated via @param allocator.
   */
  folly::Expected<node_index_t /* my_node_id */, E>
  registerSelf(configuration::nodes::NodeIndicesAllocator allocator);

  /**
   * Makes sure that the attributes of the node in the NodesConfiguration
   * matches the desired attributes in the passed CLI settings.
   */
  Status updateSelf(node_index_t my_idx);

 private:
  /**
   * Builds a NodeUpdateBuilder from the settings that was passed to the
   * constructor of this node.
   */
  configuration::nodes::NodeUpdateBuilder
  updateBuilderFromSettings(node_index_t my_idx) const;

  /**
   * Creates the NodesConfiguration update that will be proposed to add the node
   * to the NodesConfiguration. Return folly::none if building the update fails.
   */
  folly::Optional<configuration::nodes::NodesConfiguration::Update>
  buildSelfUpdate(node_index_t my_idx, bool is_update) const;

  /**
   * Applies the NodesConfiguration update to the NodesConfigurationStore.
   */
  Status applyUpdate(configuration::nodes::NodesConfiguration::Update) const;

  ServerSettings server_settings_;
  std::shared_ptr<const configuration::nodes::NodesConfiguration>
      nodes_configuration_;
  std::shared_ptr<configuration::nodes::NodesConfigurationStore> store_;
};

}} // namespace facebook::logdevice
