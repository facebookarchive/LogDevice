/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/TraceLogger.h"
#include "logdevice/common/TraceSample.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include <memory>

namespace facebook { namespace logdevice {

class NoopTraceLogger : public TraceLogger {
 public:
  explicit NoopTraceLogger(
      const std::shared_ptr<UpdateableConfig> cluster_config);
  void pushSample(const std::string& table,
                  int32_t sample_rate,
                  std::unique_ptr<TraceSample> sample) override;
};

}} // namespace facebook::logdevice
