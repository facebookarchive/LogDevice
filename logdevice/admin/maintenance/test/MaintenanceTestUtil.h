/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/maintenance/SequencerWorkflow.h"
#include "logdevice/admin/maintenance/ShardWorkflow.h"
#include "logdevice/admin/maintenance/types.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {
class NodesConfiguration;
}}}} // namespace facebook::logdevice::configuration::nodes

namespace facebook { namespace logdevice { namespace maintenance {

std::unique_ptr<thrift::ClusterMaintenanceState> genMaintenanceState();

std::shared_ptr<const configuration::nodes::NodesConfiguration>
genNodesConfiguration();

std::vector<ShardWorkflow> genShardWorkflows();
std::vector<SequencerWorkflow> genSequencerWorkflows();

}}} // namespace facebook::logdevice::maintenance
