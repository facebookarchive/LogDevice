/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/stats/Histogram.h"
#include "logdevice/common/stats/HistogramBundle.h"

namespace facebook { namespace logdevice {

/**
 * Client histograms
 */
struct ClientHistograms : public HistogramBundle {
  HistogramBundle::MapType getMap() override {
    return {
        {"append_latency", &append_latency},
        {"findtime_latency", &findtime_latency},
        {"findkey_latency", &findkey_latency},
        {"get_tail_attributes_latency", &get_tail_attributes_latency},
        {"get_head_attributes_latency", &get_head_attributes_latency},
        {"get_tail_lsn_latency", &get_tail_lsn_latency},
        {"is_log_empty_latency", &is_log_empty_latency},
        {"data_size", &data_size},
        {"trim_latency", &trim_latency},
        {"nodes_configuration_manager_propagation_latency",
         &nodes_configuration_manager_propagation_latency},
    };
  }
  LatencyHistogram append_latency;
  LatencyHistogram findtime_latency;
  LatencyHistogram findkey_latency;
  LatencyHistogram get_tail_attributes_latency;
  LatencyHistogram get_head_attributes_latency;
  LatencyHistogram get_tail_lsn_latency;
  LatencyHistogram is_log_empty_latency;
  LatencyHistogram data_size;
  LatencyHistogram trim_latency;
  // How long did it take between when the config is published and when it
  // was received on the client in msec.
  LatencyHistogram nodes_configuration_manager_propagation_latency;
};

}} // namespace facebook::logdevice
