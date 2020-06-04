/**
 * Copyright (c) 2020-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/HistogramBundle.h"

namespace facebook { namespace logdevice {

/* Histograms per monitoring tag */
struct PerMonitoringTagHistograms : public HistogramBundle {
  HistogramBundle::MapType getMap() override {
    return {{"time_stuck", &time_stuck}, {"time_lag", &time_lag}};
  }

  CompactLatencyHistogram time_stuck{
      CompactHistogram::PublishRange{26, 33}}; // ~33 sec to ~71min

  CompactLatencyHistogram time_lag{
      CompactHistogram::PublishRange{26, 33}}; // ~33 sec to ~71min
};
}} // namespace facebook::logdevice
