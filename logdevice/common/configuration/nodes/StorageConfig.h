/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/NodeID.h"
#include "logdevice/common/configuration/nodes/NodeAttributesConfig.h"
#include "logdevice/common/configuration/nodes/PerRoleConfig.h"
#include "logdevice/common/membership/StorageMembership.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

struct StorageNodeAttribute {
  /**
   * A positive value indicating how much store traffic this node will
   * receive relative to other nodes in the cluster.
   */
  double capacity;

  /**
   * Number of storage shards on this node.
   */
  shard_size_t num_shards;

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
  node_gen_t generation;

  /**
   * If true, the node will not be selected into any newly generated nodesets
   */
  bool exclude_from_nodesets;

  bool isValid() const;

  bool operator==(const StorageNodeAttribute& rhs) const {
    return capacity == rhs.capacity && num_shards == rhs.num_shards &&
        generation == rhs.generation &&
        exclude_from_nodesets == rhs.exclude_from_nodesets;
  }
};

using StorageAttributeConfig =
    NodeAttributesConfig<StorageNodeAttribute, /*Mutable=*/true>;

using StorageConfig = PerRoleConfig<NodeRole::STORAGE,
                                    membership::StorageMembership,
                                    StorageAttributeConfig>;

}}}} // namespace facebook::logdevice::configuration::nodes
