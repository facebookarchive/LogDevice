/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once
#include <chrono>
#include <memory>

#include <boost/circular_buffer.hpp>

#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/SampledTracer.h"
#include "logdevice/common/WeakRefHolder.h"
#include "logdevice/common/settings/Settings.h"

/**
 * @file ClientReadersFlowTracer keeps track of a ClientReadStream reading
 * performance, submitting counters and sampled traces for monitoring.
 * Lives on the same worker thread as the corresponding ClientReadStream.
 * Not thread safe.
 */

namespace facebook { namespace logdevice {

class ClientReadStream;
class Timer;
struct LogTailAttributes;

constexpr auto READERS_FLOW_TRACER = "readers_flow_tracer";

class ClientReadersFlowTracer : public SampledTracer {
 public:
  using SystemClock = std::chrono::system_clock;
  using TimePoint = SystemClock::time_point;
  template <typename T>
  using CircularBuffer = boost::circular_buffer_space_optimized<T>;

  struct Sample {
    int64_t time_lag;
    int64_t time_lag_correction{
        0}; // `time_lag_correction` is the amount of time lag accumulated due
            // to client rejecting records from the time the sample was first
            // recorded until the next sample is recorded (in
            // `time_lag_record_`).
    uint16_t ttl; // for how many periods do we keep this sample
  };

  struct TailInfo {
    OffsetMap offsets;
    int64_t timestamp;
    lsn_t lsn_approx;

    TailInfo(OffsetMap offset_map, int64_t ts, lsn_t lsn)
        : offsets(std::move(offset_map)), timestamp(ts), lsn_approx(lsn) {}

    TailInfo(const TailInfo& other) = delete;
    TailInfo(TailInfo&& other) noexcept = default;
    TailInfo& operator=(const TailInfo& other) = delete;
    TailInfo& operator=(TailInfo&& other) noexcept = default;
  };

  ClientReadersFlowTracer(std::shared_ptr<TraceLogger> logger,
                          ClientReadStream* owner);
  virtual ~ClientReadersFlowTracer();

  void traceReaderFlow(size_t num_bytes_read, size_t num_records_read);

  void onRedeliveryTimerInactive();
  void onRedeliveryTimerActive();
  void onWindowUpdatePending();
  void onWindowUpdateSent();

  folly::Optional<double> getDefaultSamplePercentage() const override {
    return 0.005;
  }

  folly::Optional<int64_t> estimateTimeLag() const;
  folly::Optional<int64_t> estimateByteLag() const;

  void onSettingsUpdated();
  std::string lastReportedStatePretty() const;
  std::string lastTailInfoPretty() const;
  std::string timeLagRecordPretty() const;

 private:
  void onTimerTriggered();
  void sendSyncSequencerRequest();
  void onSyncSequencerRequestResponse(Status st,
                                      NodeID seq_node,
                                      lsn_t next_lsn,
                                      std::unique_ptr<LogTailAttributes> attrs);
  void updateTimeStuck(lsn_t tail_lsn, Status st = E::OK);
  void updateTimeLagging(Status st = E::OK);
  void updateIsClientReading();
  void maybeBumpStats(bool force_healthy = false);
  double calculateSamplingWeight();
  bool readerIsUnhealthy();

  WeakRefHolder<ClientReadersFlowTracer> ref_holder_;

  std::chrono::milliseconds tracer_period_;

  CircularBuffer<Sample> time_lag_record_;
  folly::Optional<TailInfo> latest_tail_info_;

  size_t sample_counter_{0};
  size_t last_num_bytes_read_{0};
  size_t last_num_records_read_{0};
  bool is_client_reading_{true};

  enum class State { HEALTHY, STUCK, LAGGING };
  State last_reported_state_{State::HEALTHY};

  TimePoint last_time_stuck_{TimePoint::max()};
  TimePoint last_time_lagging_{TimePoint::max()};
  lsn_t last_next_lsn_to_deliver_{LSN_INVALID};

  std::unique_ptr<Timer> timer_;

  const ClientReadStream* owner_;
  friend class ClientReadStream;
};

}} // namespace facebook::logdevice
