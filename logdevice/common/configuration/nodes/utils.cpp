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

bool shouldIncludeInNodesetSelection(
    const NodesConfiguration& nodes_configuration,
    ShardID shard) {
  // if `shard' is in membership then it must have an attribute
  // defined, thus direct dereference is used
  return nodes_configuration.getStorageMembership()->hasShard(shard) &&
      !nodes_configuration.getNodeStorageAttribute(shard.node())
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

bool getNodeSSL(const NodesConfiguration& nodes_configuration,
                folly::Optional<NodeLocation> my_location,
                node_index_t node,
                NodeLocationScope diff_level) {
  if (diff_level == NodeLocationScope::ROOT) {
    // Never use SSL
    return false;
  }

  if (diff_level == NodeLocationScope::NODE) {
    // Always use SSL
    return true;
  }

  if (!my_location) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "--ssl-boundary specified, but no location available for "
                    "local machine. Defaulting to SSL.");
    return true;
  }

  const auto* node_sd = nodes_configuration.getNodeServiceDiscovery(node);
  if (!node_sd) {
    RATELIMIT_CRITICAL(std::chrono::seconds(10),
                       10,
                       "node %hu does not exist in nodes configuration.",
                       node);
    ld_check(false);
    return true;
  }

  if (!node_sd->location) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    10,
                    "--ssl-boundary specified, but no location available for "
                    "node %hu. Defaulting to SSL.",
                    node);
    return true;
  }

  if (!my_location->sharesScopeWith(*node_sd->location, diff_level)) {
    if (!node_sd->ssl_address) {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "--ssl-boundary specified, but no SSL address specified "
                      "for node %hu.",
                      node);
    }
    return true;
  }

  return false;
}

bool isNodeDisabled(const NodesConfiguration& nodes_configuration,
                    node_index_t node) {
  if (!nodes_configuration.isNodeInServiceDiscoveryConfig(node)) {
    return true;
  }

  return !nodes_configuration.getSequencerMembership()->isSequencingEnabled(
             node) &&
      !nodes_configuration.getStorageMembership()->hasShardShouldReadFrom(node);
}

bool isValidServerName(const std::string& name, std::string* reason) {
  if (name.empty()) {
    if (reason != nullptr) {
      *reason = "Name can't be empty";
    }
    return false;
  }
  if (contains_whitespaces(name)) {
    if (reason != nullptr) {
      *reason = "Name can't contain whitespaces";
    }
    return false;
  }
  return true;
}

std::string normalizeServerName(const std::string& name) {
  return logdevice::lowerCase(name);
}

}}}} // namespace facebook::logdevice::configuration::nodes
