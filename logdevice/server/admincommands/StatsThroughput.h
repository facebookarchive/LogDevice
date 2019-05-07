/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <boost/program_options/options_description.hpp>
#include <boost/program_options/positional_options.hpp>
#include <folly/stats/MultiLevelTimeSeries.h>

#include "logdevice/common/stats/Stats.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/admincommands/Stats.h"

namespace facebook { namespace logdevice { namespace commands {

class StatsThroughput : public AdminCommand {
 public:
  std::string getUsage() override;

  void getOptions(boost::program_options::options_description& opts) override;

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override;

  void run() override;

  using Duration = PerLogTimeSeries::TimeSeries::Duration;

 private:
  // Value of --timeseries (or the first positional argument).
  std::string time_series_;

  // Value of --threshold if provided on the command line, indicating we should
  // output only log groups with throughput above threshold.
  int64_t threshold_{1};

  // Value of --top if provided on the command line, indicating we should
  // output this many log groups with the highest throughput
  uint32_t top_{0};

  // List of time periods (if provided on the command line)
  std::vector<Duration> query_intervals_{std::chrono::minutes{1}};
};

}}} // namespace facebook::logdevice::commands
