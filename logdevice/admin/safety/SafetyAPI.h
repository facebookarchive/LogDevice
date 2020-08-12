/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>

#include "logdevice/admin/if/gen-cpp2/safety_types.h"
#include "logdevice/common/AuthoritativeStatus.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/ReplicationProperty.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/NodeLocationScope.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice {

using Impact = thrift::CheckImpactResponse;
using SafetyMargin = std::map<NodeLocationScope, int>;

/**
 * Combines takes an impact object and combines its result with this one.
 * Note that this doesn't combine the total_time.
 * It will also take into account the error_sample_size given to ensure we
 * are not pushing so many samples.
 *
 * If error_sample_size is < 0 we will push all samples.
 */
Impact mergeImpact(Impact i1, const Impact& i2, size_t error_sample_size);

folly::Expected<Impact, Status>
mergeImpact(folly::Expected<Impact, Status> i,
            const folly::Expected<Impact, Status>& i2,
            size_t error_sample_size);

/**
 * Return impact.impact list as comma-separated string.
 **/
std::string impactToString(const Impact& impact);

/**
 * @param descriptor A descriptor describing one safety margin or a set of .
                     safety marings. Safety marging is simular to replication
                     property - it is list of <domain>:<number> pairs.
 *                   For instance, \"rack:1\",\"node:2\".
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 */
int parseSafetyMargin(const std::vector<std::string>& descriptors,
                      SafetyMargin& out);

// Converts a ReplicationProperty object into SafetyMargin
// TODO: We should replace SafetyMargin type with ReplicationProperty
SafetyMargin
safetyMarginFromReplication(const ReplicationProperty& replication);

/**
 * @param descriptors A list of descriptors, @see parseSafetyMargin.
 * @param out        Populated set of NodeLocationScope, int  pairs.
 *
 * @return           0 on success, or -1 on error
 *
 */
int parseSafetyMargin(const std::string& descriptor, SafetyMargin& out);

}} // namespace facebook::logdevice
