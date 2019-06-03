/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/NoopTraceLogger.h"

#include <memory>

#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/TraceSample.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

NoopTraceLogger::NoopTraceLogger(
    const std::shared_ptr<UpdateableConfig> cluster_config,
    const folly::Optional<NodeID>& my_node_id)
    : TraceLogger(cluster_config, my_node_id) {
  ld_info("NOOP TraceLogger is ON, no trace samples will be published.");
}

void NoopTraceLogger::pushSample(const std::string& table,
                                 int32_t /*sample_rate*/,
                                 std::unique_ptr<TraceSample> sample) {
  ld_debug("Pushing on table '%s' sample: %s",
           table.c_str(),
           sample->toJson().c_str());
}

}} // namespace facebook::logdevice
