/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/configuration/nodes/NodesConfiguration.h"

namespace facebook { namespace logdevice {

extern const configuration::nodes::NodeServiceDiscovery::RoleSet seq_role;
extern const configuration::nodes::NodeServiceDiscovery::RoleSet storage_role;
extern const configuration::nodes::NodeServiceDiscovery::RoleSet both_role;

extern const membership::MaintenanceID::Type DUMMY_MAINTENANCE;

configuration::nodes::NodeServiceDiscovery
genDiscovery(node_index_t n,
             configuration::nodes::NodeServiceDiscovery::RoleSet roles,
             std::string location);

// provision a LD nodes config with:
// 1) four nodes N1, N2, N7, N9
// 2) N1 and N7 have sequencer role; while N1, N2 and N9 have storage role;
// 3) N2 and N9 are metadata storage nodes, metadata logs replicaton is
//    (rack, 2)
configuration::nodes::NodesConfiguration::Update initialProvisionUpdate();

std::shared_ptr<const configuration::nodes::NodesConfiguration>
provisionNodes();

// add new node 17
configuration::nodes::NodesConfiguration::Update addNewNodeUpdate();

}} // namespace facebook::logdevice
