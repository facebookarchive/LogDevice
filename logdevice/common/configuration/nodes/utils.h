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
bool shouldIncludeInNodesetSelection(const NodesConfiguration& nodes_config,
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

}}}} // namespace facebook::logdevice::configuration::nodes
