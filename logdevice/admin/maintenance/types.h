/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/admin/if/gen-cpp2/admin_types.h"
#include "logdevice/admin/maintenance/gen-cpp2/MaintenanceDelta_types.h"

namespace facebook { namespace logdevice { namespace maintenance {
using ShardOperationalState = thrift::ShardOperationalState;
using SequencingState = thrift::SequencingState;
using MaintenanceDefinition = thrift::MaintenanceDefinition;
using GroupID = thrift::MaintenanceGroupID;
using MaintenanceStatus = thrift::MaintenanceStatus;
using ShardDataHealth = thrift::ShardDataHealth;
using ClusterMaintenanceState = thrift::ClusterMaintenanceState;

}}} // namespace facebook::logdevice::maintenance
