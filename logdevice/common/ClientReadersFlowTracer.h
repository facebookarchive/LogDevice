/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <boost/circular_buffer.hpp>
#include <chrono>
#include <memory>
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/settings/Settings.h"

/**
 * @file ClientReadersFlowTracer keeps track of a ClientReadStream reading
 * performance, submitting counters and sampled traces for monitoring.
 */

namespace facebook { namespace logdevice {

class ClientReadStream;
class LibeventTimer;
struct LogTailAttributes;

constexpr auto READERS_FLOW_TRACER = "readers_flow_tracer";

class ClientReadersFlowTracer
    : public std::enable_shared_from_this<ClientReadersFlowTracer>,
      public SampledTracer {
 public:
  struct AsyncRecords {
    folly::Optional<int64_t> bytes_lagged;
    folly::Optional<int64_t> timestamp_lagged;
  };
  using SystemClock = std::chrono::system_clock;
  using TimePoint = SystemClock::time_point;
  template <typename T>
  using CircularBuffer = boost::circular_buffer_space_optimized<T>;

  ClientReadersFlowTracer(std::shared_ptr<TraceLogger> logger,
                          ClientReadStream* owner);
  virtual ~ClientReadersFlowTracer();

  void traceReaderFlow(size_t num_bytes_read, size_t num_records_read);

  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 0.005;
  }

  folly::Optional<AsyncRecords> getAsyncRecords() const {
    return last_async_records_;
  }

  void onSettingsUpdated();
  std::string lastReportedStatePretty() const;

 private:
  void onTimerTriggered();
  void sendSyncSequencerRequest();
  void updateTimeStuck(lsn_t tail_lsn, Status st = E::OK);
  void updateTimeLagging(Status st = E::OK);
  void maybeBumpStats(bool force_healthy = false);
  void updateAsyncRecords(uint64_t acc_byte_offset,
                          std::chrono::milliseconds last_in_record_ts,
                          lsn_t tail_lsn_approx,
                          LogTailAttributes* attrs);

  std::string log_group_name_;
  std::chrono::milliseconds tracer_period_;

  folly::Optional<AsyncRecords> last_async_records_;
  CircularBuffer<int64_t> ts_lagged_record_;

  size_t sample_counter_{0};
  size_t last_num_bytes_read_{0};
  size_t last_num_records_read_{0};

  enum class State { HEALTHY, STUCK, LAGGING };
  State last_reported_state_{State::HEALTHY};

  TimePoint last_time_stuck_{TimePoint::max()};
  TimePoint last_time_lagging_{TimePoint::max()};
  lsn_t last_next_lsn_to_deliver_{LSN_INVALID};

  std::unique_ptr<LibeventTimer> timer_;

  const ClientReadStream* owner_;
  friend class ClientReadStream;
};

}} // namespace facebook::logdevice
