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

}}}} // namespace facebook::logdevice::configuration::nodes
