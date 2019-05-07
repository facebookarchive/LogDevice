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

class StatsCustomCounters : public AdminCommand {
 public:
  std::string getUsage() override;

  void getOptions(boost::program_options::options_description& opts) override;

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override;

  void run() override;

  using Duration = CustomCountersTimeSeries::TimeSeries::Duration;

 private:
  // Value of --keys if provided on the command line, indicating filtering by
  // key. if uint8_t is used program_options parses numbers as characters
  // storing ASCII values
  std::vector<uint16_t> keys_;

  // Value of --interval if provided on command line. Defaults to max interval
  Duration interval_{
      CustomCountersTimeSeries::getCustomCounterIntervals().back()};
};

}}} // namespace facebook::logdevice::commands
