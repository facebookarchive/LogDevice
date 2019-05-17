/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/Range.h>

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

/**
 * @return  if NodeSetSelector can pick the shard in its generated
 * nodesets/storage sets.
 *
 * TODO T33035439: some of the criterias (e.g., whether or not to pick read-only
 * nodes) should be configuraable per nodeset selector. For now, make this
 * compatible with Node::includeInNodesets() in legacy NodesConfig;
 */
bool shouldIncludeInNodesetSelection(
    const NodesConfiguration& nodes_configuration,
    ShardID shard);

/**
 * Check if nodeset is valid with the given replication property and
 * nodes configuration:
 *    1) there are enough non-zero weight nodes in the nodeset to satisfy
 *       replication property,
 *    2) (if strict == true) all nodes in nodeset are present in config and
 *       are storage nodes.
 */
bool validStorageSet(const NodesConfiguration& nodes_configuration,
                     const StorageSet& storage_set,
                     ReplicationProperty replication,
                     bool strict = false);

/**
 * return true if the node is both disabled in sequencer membership and
 *        does not have any readable shard in storage membership. Node
 *        not exist in nodes_configuration is also considered disabled.
 */
bool isNodeDisabled(const NodesConfiguration& nodes_configuration,
                    node_index_t node);

/**
 * Helper method for determining whether we need to use SSL for connection
 * to a node. The diff_level specifies the level in the location hierarchy,
 * where, if a difference is encountered, we should use SSL. For instance,
 * if diff_level == NodeLocationScope::RACK, the method will return true
 * for any node that is in a rack different to my_location's, and return
 * false otherwise.
 *
 * @param nodes_configuration   nodes configuration of the cluster
 * @param my_location   local NodeLocation
 * @param node          index of the node we are connecting to, must
 *                      exist in the given nodes configuration
 * @param diff_level    The scope of NodeLocation to compare
 */
bool getNodeSSL(const NodesConfiguration& nodes_configuration,
                folly::Optional<NodeLocation> my_location,
                node_index_t node,
                NodeLocationScope diff_level);

/**
 * Checks if the passed value is a valid server name or not. The name is a valid
 * name if:
 *   - It's not empty.
 *   - It doesn't have any whitespaces.
 *
 * @param name   The server name to validate
 * @param reason If the server name is invalid, this string will be filled by
 *                the reason why it's invalid;
 */
bool isValidServerName(const std::string& name, std::string* reason = nullptr);

/**
 * Normalizes the passed server name by:
 *  - Converting it to lower case.
 */
std::string normalizeServerName(const std::string& name);

}}}} // namespace facebook::logdevice::configuration::nodes
