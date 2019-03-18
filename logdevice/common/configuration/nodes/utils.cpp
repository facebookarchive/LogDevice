/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/utils.h"

#include "logdevice/common/FailureDomainNodeSet.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

bool shouldIncludeInNodesetSelection(const NodesConfiguration& nodes_config,
                                     ShardID shard) {
  // if `shard' is in membership then it must have an attribute
  // defined, thus direct dereference is used
  return nodes_config.getStorageMembership()->hasShard(shard) &&
      !nodes_config.getNodeStorageAttribute(shard.node())
           ->exclude_from_nodesets;
}

bool validStorageSet(const NodesConfiguration& nodes_configuration,
                     const StorageSet& storage_set,
                     ReplicationProperty replication,
                     bool strict) {
  if (!replication.isValid()) {
    return false;
  }

  // attribute is whether the node is writable
  FailureDomainNodeSet<bool> failure_domain(
      storage_set, nodes_configuration, replication);

  for (auto shard : storage_set) {
    const auto* serv_disc =
        nodes_configuration.getNodeServiceDiscovery(shard.node());
    if (strict &&
        (serv_disc == nullptr || !serv_disc->hasRole(NodeRole::STORAGE))) {
      ld_error("Invalid nodeset: %s is referenced from the nodeset but "
               "doesn't exist in nodes configuration with storage role",
               shard.toString().c_str());
      return false;
    }

    if (nodes_configuration.getStorageMembership()->canWriteToShard(shard)) {
      failure_domain.setShardAttribute(shard, true);
    }
  }

  // return true if the subset of writable storage nodes can satisfy
  // replication property
  return failure_domain.canReplicate(true);
}

}}}} // namespace facebook::logdevice::configuration::nodes
