/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <chrono>
#include <list>
#include <unordered_map>

#include <folly/stats/MultiLevelTimeSeries.h>
#include <folly/stats/TimeseriesHistogram.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/protocol/STORE_Message.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class WorkerTimeoutStats {
 public:
  enum Levels {
    TEN_SECONDS,
  };

  using Latency = double;

  enum QuantileIndexes { P50, P90, P95, P99, P99_9, P99_99, MAX };
  constexpr static std::array<double, 6> kQuantiles{50,
                                                    90,
                                                    95,
                                                    99,
                                                    99.9,
                                                    99.99};
  using Clock = std::chrono::steady_clock;
  using MessageKey = std::tuple<node_index_t, RecordID, uint32_t>;
  using Timepoint = std::chrono::time_point<std::chrono::steady_clock>;
  using Histogram =
      folly::TimeseriesHistogram<Latency, std::chrono::steady_clock>;
  using HistogramContainer =
      folly::MultiLevelTimeSeries<Latency, std::chrono::steady_clock>;

  WorkerTimeoutStats();

  virtual ~WorkerTimeoutStats() = default;

  void onCopySent(Status status,
                  const ShardID& to,
                  const STORE_Header& header,
                  Clock::time_point now = Clock::now());
  void onReply(const ShardID& from,
               const STORE_Header& header,
               Clock::time_point now = Clock::now());

  void clear();

  folly::Optional<std::array<Latency, WorkerTimeoutStats::kQuantiles.size()>>
  getEstimations(Levels level,
                 int node = -1,
                 Clock::time_point now = Clock::now());

  std::unordered_map<node_index_t, Histogram> histograms_;
  Histogram overall_;

 protected:
  virtual uint64_t getMinSamplesPerBucket() const;

 private:
  void cleanup();

  std::map<MessageKey, std::list<std::pair<MessageKey, Timepoint>>::iterator>
      lookup_table_;

  std::list<std::pair<MessageKey, Timepoint>> outgoing_messages_;
};

}} // namespace facebook::logdevice
