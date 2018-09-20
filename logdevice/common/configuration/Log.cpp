/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRId64 etc

#include "Log.h"

#include <tuple>

#include <folly/json.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/LogsConfigParser.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/include/LogAttributes.h"
#include "logdevice/common/debug.h"

using namespace facebook::logdevice::configuration::parser;
using namespace facebook::logdevice::logsconfig;

namespace facebook { namespace logdevice { namespace configuration {

Log::Log() : customFields(folly::dynamic::object) {}

bool Log::operator==(const Log& other) const {
  auto as_tuple = [](const Log& l) {
    return std::tie(l.rangeName,
                    l.replicationFactor,
                    l.extraCopies,
                    l.syncedCopies,
                    l.maxWritesInFlight,
                    l.singleWriter,
                    l.syncReplicationScope,
                    l.replicateAcross,
                    l.backlogDuration,
                    l.nodeSetSize,
                    l.deliveryLatency,
                    l.scdEnabled,
                    l.localScdEnabled,
                    l.writeToken,
                    l.permissions,
                    l.acls,
                    l.aclsShadow,
                    l.stickyCopySets,
                    l.mutablePerEpochLogMetadataEnabled,
                    l.sequencerAffinity,
                    l.sequencerBatching,
                    l.sequencerBatchingTimeTrigger,
                    l.sequencerBatchingSizeTrigger,
                    l.sequencerBatchingCompression,
                    l.sequencerBatchingPassthruThreshold,
                    l.tailOptimized,
                    l.customFields);
  };
  return as_tuple(*this) == as_tuple(other);
}

std::unique_ptr<Log> Log::fromLogAttributes(const std::string& rangeName,
                                            const LogAttributes& attrs) {
  auto log = std::make_unique<Log>();
  log->rangeName = rangeName;
#define COPY_ATTR(att)              \
  if (attrs.att()) {                \
    log->att = attrs.att().value(); \
  }

  /*
   * This assumes that all attributes are set. This is true in LogAttributes in
   * a LogsConfigTree since all attributes will inherit values from their
   * parents if an attribute is not set. (assuming that the root has either
   * DefaultLogAttributes or any LogAttributes object where all attributes are
   * set)
   */
  COPY_ATTR(replicationFactor);
  COPY_ATTR(extraCopies);
  COPY_ATTR(syncedCopies);
  COPY_ATTR(maxWritesInFlight);
  COPY_ATTR(singleWriter);
  COPY_ATTR(syncReplicationScope);
  COPY_ATTR(replicateAcross);
  COPY_ATTR(backlogDuration);
  COPY_ATTR(nodeSetSize);
  COPY_ATTR(deliveryLatency);
  COPY_ATTR(scdEnabled);
  COPY_ATTR(localScdEnabled);
  COPY_ATTR(writeToken);
  COPY_ATTR(stickyCopySets);
  COPY_ATTR(mutablePerEpochLogMetadataEnabled);
  COPY_ATTR(permissions);
  COPY_ATTR(acls);
  COPY_ATTR(aclsShadow);
  COPY_ATTR(sequencerAffinity);
  COPY_ATTR(sequencerBatching);
  COPY_ATTR(sequencerBatchingTimeTrigger);
  COPY_ATTR(sequencerBatchingSizeTrigger);
  COPY_ATTR(sequencerBatchingCompression);
  COPY_ATTR(sequencerBatchingPassthruThreshold);
  COPY_ATTR(tailOptimized);
#undef COPY_ATTR
  folly::dynamic customFields = folly::dynamic::object;
  if (attrs.extras().hasValue()) {
    for (const auto& iter : attrs.extras().value()) {
      customFields[iter.first] = iter.second;
    }
  }
  log->customFields = std::move(customFields);
  return log;
}

void Log::toLogAttributes(LogAttributes* out) const {
  LogAttributes::ExtrasMap extras_map;
  for (auto& pair : customFields.items()) {
    // out map is string => string, we serialize whatever in the pair.second
    // into that string because we don't case about it, when we read that
    extras_map[pair.first.asString()] = folly::toJson(pair.second);
  }
  *out = LogAttributes(replicationFactor,
                       extraCopies,
                       syncedCopies,
                       maxWritesInFlight,
                       singleWriter,
                       syncReplicationScope,
                       replicateAcross.empty()
                           ? Attribute<LogAttributes::ScopeReplicationFactors>()
                           : replicateAcross,
                       backlogDuration,
                       nodeSetSize,
                       deliveryLatency,
                       scdEnabled,
                       localScdEnabled,
                       writeToken,
                       stickyCopySets,
                       mutablePerEpochLogMetadataEnabled,
                       permissions,
                       acls,
                       aclsShadow,
                       sequencerAffinity,
                       sequencerBatching,
                       sequencerBatchingTimeTrigger,
                       sequencerBatchingSizeTrigger,
                       sequencerBatchingCompression,
                       sequencerBatchingPassthruThreshold,
                       Attribute<LogAttributes::Shadow>(),
                       tailOptimized,
                       extras_map);
}
}}} // namespace facebook::logdevice::configuration
