/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <memory>
#include <chrono>
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/settings/Settings.h"

/**
 * @file ClientReadersFlowTracer is a sampled tracer responsible for tracking
 * ClientReadStream.
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
    folly::Optional<int64_t> bytes_lagged_delta;
    folly::Optional<int64_t> timestamp_lagged;
    folly::Optional<int64_t> timestamp_lagged_delta;
  };
  using SteadyClock = std::chrono::steady_clock;
  using TimePoint = SteadyClock::time_point;

  ClientReadersFlowTracer(std::shared_ptr<TraceLogger> logger,
                          ClientReadStream* owner);
  virtual ~ClientReadersFlowTracer();

  void traceReaderFlow(size_t num_bytes_read, size_t num_records_read);

  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 0.005;
  }

  folly::Optional<AsyncRecords> getAsyncRecords() {
    return last_async_records_;
  }

  void onSettingsUpdated();

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

  UpdateableSettings<Settings> settings_;
  std::chrono::milliseconds tracer_period_;

  folly::Optional<AsyncRecords> last_async_records_;

  size_t last_num_bytes_read_{0};
  size_t last_num_records_read_{0};
  folly::Optional<int64_t> last_bytes_lagged_;
  folly::Optional<int64_t> last_timestamp_lagged_;

  enum class State { HEALTHY, STUCK, LAGGING };
  State last_reported_state_{State::HEALTHY};

  TimePoint last_time_stuck_{TimePoint::max()};
  TimePoint last_time_lagging_{TimePoint::max()};
  lsn_t last_next_lsn_to_deliver_{LSN_INVALID};

  std::unique_ptr<LibeventTimer> timer_;

  const ClientReadStream* owner_;
};

}} // namespace facebook::logdevice
