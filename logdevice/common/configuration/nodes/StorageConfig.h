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
  double capacity{0.0};

  /**
   * Number of storage shards on this node.
   */
  shard_size_t num_shards{};

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
   * Note: we are in the middle of getting rid of `generation'. Currently
   * generation should only be used in storage node replacement. For nodes
   * w/o a storage role, their generation should always be set to 1.
   */
  node_gen_t generation{};

  /**
   * If true, the node will not be selected into any newly generated nodesets
   */
  bool exclude_from_nodesets;

  bool isValid() const;
  bool isValidForReset(const StorageNodeAttribute& current) const {
    // All changes to the storage node attribute is allowed.
    return true;
  }
  std::string toString() const;

  bool operator==(const StorageNodeAttribute& rhs) const {
    return capacity == rhs.capacity && num_shards == rhs.num_shards &&
        generation == rhs.generation &&
        exclude_from_nodesets == rhs.exclude_from_nodesets;
  }

  bool operator!=(const StorageNodeAttribute& rhs) const {
    return !(*this == rhs);
  }
};

using StorageAttributeConfig = NodeAttributesConfig<StorageNodeAttribute>;

using StorageConfig = PerRoleConfig<NodeRole::STORAGE,
                                    membership::StorageMembership,
                                    StorageAttributeConfig>;

}}}} // namespace facebook::logdevice::configuration::nodes
