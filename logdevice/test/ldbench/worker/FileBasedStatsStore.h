/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <fstream>
#include <string>

#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/test/ldbench/worker/StatsStore.h"

namespace facebook { namespace logdevice { namespace ldbench {

class FileBasedStatsStore : public StatsStore {
 public:
  FileBasedStatsStore(std::string dir_name)
      : stats_writer_(dir_name, std::fstream::app) {}

  ~FileBasedStatsStore() {
    stats_writer_.flush();
    stats_writer_.close();
  }

  bool isReady() {
    return stats_writer_.is_open();
  }

  /* *
   *  Write stats to files
   */
  void writeCurrentStats(const folly::dynamic& stats_obj) override {
    // use folly json to serialize folly dynamic object
    // write as json format
    folly::json::serialization_opts opts;
    std::string write_line = folly::json::serialize(stats_obj, opts);
    stats_writer_ << write_line << std::endl;
    return;
  }

 private:
  std::fstream stats_writer_;
};
}}} // namespace facebook::logdevice::ldbench
