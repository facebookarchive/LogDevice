/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/nodes/NodesConfigurationTracer.h"

#include <memory>

#include <folly/json.h>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

NodesConfigurationTracer::NodesConfigurationTracer(
    std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

/* static */ std::string NodesConfigurationTracer::toString(
    const NodesConfigurationTracer::Source& source) {
  switch (source) {
    case NodesConfigurationTracer::Source::NCM_UPDATE:
      return "ncm_update";
    case NodesConfigurationTracer::Source::NCM_OVERWRITE:
      return "ncm_overwrite";
    case NodesConfigurationTracer::Source::NC_PUBLISHER:
      return "nc_publisher";
    case NodesConfigurationTracer::Source::UNKNOWN:
      FOLLY_FALLTHROUGH;
    default:
      return "unknonwn";
  };
}

namespace {
int64_t
getLoggableVersion(const std::shared_ptr<const NodesConfiguration>& nc) {
  auto v = nc ? nc->getVersion() : membership::MembershipVersion::EMPTY_VERSION;
  return static_cast<int64_t>(v.val());
}
} // namespace

void NodesConfigurationTracer::trace(NodesConfigurationTracer::Sample sample) {
  SystemTimestamp sample_time = SystemTimestamp::now();
  auto sample_builder =
      [sample = std::move(sample),
       sample_time]() mutable -> std::unique_ptr<TraceSample> {
    auto trace_sample = std::make_unique<TraceSample>();
    trace_sample->addIntValue(
        "sc_nc_version", getLoggableVersion(sample.server_config_nc_));
    trace_sample->addIntValue(
        "ncm_nc_version", getLoggableVersion(sample.ncm_nc_));
    trace_sample->addIntValue(
        "using_ncm_nc", static_cast<int64_t>(sample.using_ncm_nc_));
    trace_sample->addIntValue(
        "is_same", compare_obj_ptrs(sample.server_config_nc_, sample.ncm_nc_));
    if (sample.nc_update_gen_) {
      trace_sample->addNormalValue("nc_update", sample.nc_update_gen_());
    }
    if (!sample.published_nc_) {
      RATELIMIT_ERROR(
          std::chrono::seconds(10),
          1,
          "published NC is null in NodesConfigurationTracer sample...");
    } else {
      trace_sample->addIntValue(
          "published_nc_ctime_ms",
          static_cast<int64_t>(sample.published_nc_->getLastChangeTimestamp()
                                   .toMilliseconds()
                                   .count()));
    }

    trace_sample->addNormalValue("sample_source", toString(sample.source_));
    trace_sample->addIntValue(
        "sample_time_ms",
        static_cast<int64_t>(sample_time.toMilliseconds().count()));
    trace_sample->addNormalValue(
        "timestamps", folly::toJson(sample.timestamps_));

    return trace_sample;
  };
  publish(NODES_CONFIGURATION_TRACER, sample_builder);
}
}}}} // namespace facebook::logdevice::configuration::nodes
