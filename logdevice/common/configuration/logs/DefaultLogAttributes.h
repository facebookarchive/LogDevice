/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include "logdevice/common/SlidingWindow.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice { namespace logsconfig {
/*
 * DefaultLogAttributes defines the default set of attributes for LogsConfigTree
 * that can be overridden by the defaults section in the logs config.
 */
class DefaultLogAttributes : public LogAttributes {
 public:
  DefaultLogAttributes()
      : LogAttributes(
            /* replicationFactor >= 1 [REQUIRED] */
            Attribute<int>(),
            /* extraCopies >=0 [Optional] */
            0,
            /* syncedCopies >= 0*/
            0,
            // maxWritesInFlight >= SLIDING_WINDOW_MIN_CAPACITY
            std::max(1000u, facebook::logdevice::SLIDING_WINDOW_MIN_CAPACITY),
            /* singleWriter */
            false,
            /* syncReplicationScope */
            Attribute<NodeLocationScope>(),
            /* replicateAcross */
            Attribute<LogAttributes::ScopeReplicationFactors>(),
            /* backlogDuration [Unset by Default (means INF)] */
            Attribute<folly::Optional<std::chrono::seconds>>(
                folly::Optional<std::chrono::seconds>()),
            /* nodeSetSize */
            Attribute<folly::Optional<int>>(folly::Optional<int>()),
            /* deliveryLatency */
            Attribute<folly::Optional<std::chrono::milliseconds>>(
                folly::Optional<std::chrono::milliseconds>()),
            /* scdEnabled */
            false,
            /* localScdEnabled */
            false,
            /* writeToken */
            Attribute<folly::Optional<std::string>>(
                folly::Optional<std::string>()),
            /* stickyCopySets */
            false,
            /* mutablePerEpochLogMetadataEnabled */
            true,
            /* permissions */
            Attribute<PermissionsMap>(),
            /* acls */
            Attribute<ACLList>(),
            /* acls_shadow */
            Attribute<ACLList>(),
            /* sequencerAffinity */
            Attribute<folly::Optional<std::string>>(
                folly::Optional<std::string>()),
            /* sequencerBatching */
            Attribute<bool>(),
            /* sequencerBatchingTimeTrigger */
            Attribute<std::chrono::milliseconds>(),
            /* sequencerBatchingSizeTrigger */
            Attribute<ssize_t>(),
            /* sequencerBatchingCompression */
            Attribute<Compression>(),
            /* sequencerBatchingPassthruThreshold */
            Attribute<ssize_t>(),
            /* shadow */
            Attribute<Shadow>(),
            /* tailOptimized */
            false,
            /* extras */
            Attribute<ExtrasMap>()) {}
};
}}} // namespace facebook::logdevice::logsconfig
