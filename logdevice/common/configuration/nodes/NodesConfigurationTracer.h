/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/Function.h>

#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/membership/types.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class TraceLogger;

namespace configuration { namespace nodes {

constexpr auto NODES_CONFIGURATION_TRACER = "nodes_configuration";

class NodesConfigurationTracer : SampledTracer {
 public:
  enum struct Source {
    UNKNOWN = 0,
    NCM_UPDATE = 1,
    NCM_OVERWRITE = 2,
    NC_PUBLISHER = 3,
  };
  static std::string toString(const Source&);

  struct Sample {
    std::shared_ptr<const NodesConfiguration> server_config_nc_{nullptr};
    std::shared_ptr<const NodesConfiguration> ncm_nc_{nullptr};
    std::shared_ptr<const NodesConfiguration> published_nc_{nullptr};

    // If you're adding columns here, don't forget to update the underlying
    // TraceLogger.
    bool using_ncm_nc_{};
    // Allow the option to delay the generation of the update string unless
    // the sample will be logged.
    folly::Function<std::string()> nc_update_gen_{};
    Source source_{Source::UNKNOWN};
    folly::dynamic timestamps_ = folly::dynamic::object();
  };

  explicit NodesConfigurationTracer(std::shared_ptr<TraceLogger> logger);

  void trace(Sample sample);
};

}} // namespace configuration::nodes
}} // namespace facebook::logdevice
