/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientReadersFlowTracer.h"

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

inline uint16_t get_initial_ttl(size_t group_size, size_t num_groups) {
  return 1.25 * group_size * num_groups;
}

ClientReadersFlowTracer::ClientReadersFlowTracer(
    std::shared_ptr<TraceLogger> logger,
    ClientReadStream* owner)
    : SampledTracer(std::move(logger)), ref_holder_(this), owner_(owner) {
  timer_ = std::make_unique<Timer>([this] { onTimerTriggered(); });

  // update settings
  onSettingsUpdated();
}

ClientReadersFlowTracer::~ClientReadersFlowTracer() {
  // cleanup ods
  maybeBumpStats(/*force_healthy=*/true);
}

void ClientReadersFlowTracer::traceReaderFlow(size_t num_bytes_read,
                                              size_t num_records_read) {
  auto time_stuck = std::max(msec_since(last_time_stuck_), 0l);
  auto time_lagging = std::max(msec_since(last_time_lagging_), 0l);
  auto shard_status_version = owner_->deps_->getShardStatus().getVersion();
  auto time_lag = estimateTimeLag();
  auto byte_lag = estimateByteLag();

  auto sample_builder =
      [=,
       reading_speed_bytes = num_bytes_read - last_num_bytes_read_,
       reading_speed_records = num_records_read -
           last_num_records_read_]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("log_id", std::to_string(owner_->log_id_.val()));

    sample->addNormalValue("log_group_name", owner_->log_group_name_);

    sample->addNormalValue(
        "read_stream_id",
        std::to_string(owner_->deps_->getReadStreamID().val()));
    sample->addNormalValue("from_lsn", lsn_to_string(owner_->start_lsn_));
    sample->addNormalValue("until_lsn", lsn_to_string(owner_->until_lsn_));
    sample->addNormalValue(
        "epoch_metadata", owner_->epoch_metadata_str_factory_());
    sample->addNormalValue(
        "reader_pointer", lsn_to_string(owner_->next_lsn_to_deliver_));
    sample->addNormalValue(
        "unavailable_shards", owner_->unavailable_shards_str_factory_());
    if (owner_->scd_) {
      sample->addNormalValue(
          "scd_down_shards", toString(owner_->scd_->getShardsDown()));
      sample->addNormalValue(
          "scd_slow_shards", toString(owner_->scd_->getShardsSlow()));
    }
    sample->addNormalValue("storage_set_health_status",
                           owner_->storage_set_health_status_str_factory_());
    sample->addNormalValue("trim_point", lsn_to_string(owner_->trim_point_));
    sample->addIntValue("readset_size", owner_->readSetSize());
    if (byte_lag.has_value()) {
      sample->addIntValue("bytes_lagged", byte_lag.value());
    }
    if (time_lag.has_value()) {
      sample->addIntValue("timestamp_lagged", time_lag.value());
    }
    sample->addIntValue("time_stuck", time_stuck);
    sample->addIntValue("time_lagging", time_lagging);
    sample->addIntValue("reading_speed_bytes", reading_speed_bytes);
    sample->addIntValue("reading_speed_records", reading_speed_records);
    sample->addNormalValue("sender_state", owner_->senderStatePretty());
    sample->addNormalValue("grace_counters", owner_->graceCountersPretty());
    sample->addIntValue("shard_status_version", shard_status_version);
    return sample;
  };
  last_num_bytes_read_ = num_bytes_read;
  last_num_records_read_ = num_records_read;
  publish(READERS_FLOW_TRACER,
          sample_builder,
          /*force=*/false,
          calculateSamplingWeight());
}

double ClientReadersFlowTracer::calculateSamplingWeight() {
  if (readerIsUnhealthy()) {
    return Worker::settings()
        .client_readers_flow_tracer_unhealthy_publish_weight;
  } else {
    return 1.0; /* default weight */
  }
}

bool ClientReadersFlowTracer::readerIsUnhealthy() {
  return last_reported_state_ != State::HEALTHY;
}

void ClientReadersFlowTracer::onSettingsUpdated() {
  auto& settings = Worker::settings();
  tracer_period_ = settings.client_readers_flow_tracer_period;
  if (tracer_period_ != std::chrono::milliseconds::zero()) {
    if (!timer_->isActive()) {
      timer_->activate(std::chrono::milliseconds{0});
    }
  } else {
    if (timer_->isActive()) {
      timer_->cancel();
    }
  }

  const auto len =
      settings.client_readers_flow_tracer_lagging_metric_num_sample_groups;
  if (len != time_lag_record_.capacity()) {
    time_lag_record_.set_capacity(len);
  }
}

void ClientReadersFlowTracer::onTimerTriggered() {
  // For simplicity, we send out the request for tail attributes on every sample
  // submission.
  sendSyncSequencerRequest();

  maybeBumpStats();
  traceReaderFlow(owner_->num_bytes_delivered_, owner_->num_records_delivered_);
  timer_->activate(tracer_period_);
}

void ClientReadersFlowTracer::sendSyncSequencerRequest() {
  auto ssr = std::make_unique<SyncSequencerRequest>(
      owner_->log_id_,
      SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES,
      [weak_ref = ref_holder_.ref()](
          Status st,
          NodeID seq_node,
          lsn_t next_lsn,
          std::unique_ptr<LogTailAttributes> attrs,
          std::shared_ptr<const EpochMetaDataMap> /*unused*/,
          std::shared_ptr<TailRecord> /*unused*/,
          folly::Optional<bool> /*unused*/) {
        if (auto ptr = weak_ref.get()) {
          ptr->onSyncSequencerRequestResponse(
              st, seq_node, next_lsn, std::move(attrs));
        }
      },
      GetSeqStateRequest::Context::READER_MONITORING,
      /*timeout=*/tracer_period_,
      GetSeqStateRequest::MergeType::GSS_MERGE_INTO_OLD);
  ssr->setThreadIdx(Worker::onThisThread()->idx_.val());
  std::unique_ptr<Request> req(std::move(ssr));
  auto res = Worker::onThisThread()->processor_->postRequest(req);
  if (res != 0) {
    onSyncSequencerRequestResponse(
        E::NOBUFS, NodeID(), /*next_lsn=*/LSN_INVALID, /*attrs=*/nullptr);
  }
}

void ClientReadersFlowTracer::onSyncSequencerRequestResponse(
    Status st,
    NodeID seq_node,
    lsn_t next_lsn,
    std::unique_ptr<LogTailAttributes> attrs) {
  if (st == E::OK && attrs) {
    auto tail_lsn_approx = attrs->last_released_real_lsn != LSN_INVALID
        ? attrs->last_released_real_lsn
        : next_lsn - 1; // in case we haven't gotten the
                        // last_released_real_lsn, we use the maximum
                        // possible lsn for the tail record.
    latest_tail_info_ =
        TailInfo(OffsetMap::fromRecord(std::move(attrs->offsets)),
                 attrs->last_timestamp.count(),
                 tail_lsn_approx);
    updateTimeStuck(tail_lsn_approx);
  } else {
    if (st == E::OK) {
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          10,
          "SyncSequencerRequest (sent to sequencer node %s) returned "
          "E::OK for log %lu in read stream %lu but did not provide "
          "tail attributes.",
          seq_node.toString().c_str(),
          owner_->log_id_.val(),
          owner_->deps_->getReadStreamID().val());
    } else {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        10,
                        "SyncSequencerRequest (sent to sequencer node %s) "
                        "failed for log %lu in read stream %lu with error %s",
                        seq_node.toString().c_str(),
                        owner_->log_id_.val(),
                        owner_->deps_->getReadStreamID().val(),
                        error_description(st));
    }
    // If the sequencer is not responding, we assume we are stuck until
    // further notice.
    updateTimeStuck(LSN_INVALID, st);
  }
  updateTimeLagging(st);
}

/**
 * updateTimeStuck() samples the current state to check if we can be considered
 * stuck or not. Note that this sampling happens at roughly a period T (where T
 * = settings_->client_readers_flow_tracer_period) and therefore we might incur
 * in an error of about T secs (or even greater if we fail to obtain
 * LogTailAttributes for a long time because last_time_stuck_ will not get
 * updated).
 */
void ClientReadersFlowTracer::updateTimeStuck(lsn_t tail_lsn, Status st) {
  if (last_next_lsn_to_deliver_ != owner_->next_lsn_to_deliver_) {
    // If we delivered some record in between calls to this function, we just
    // move to an "unstuck" state.
    last_next_lsn_to_deliver_ = owner_->next_lsn_to_deliver_;
    last_time_stuck_ = TimePoint::max();
    maybeBumpStats();
  }

  bool is_stuck = is_client_reading_ &&
      (st != E::OK ||
       owner_->next_lsn_to_deliver_ <= std::min(tail_lsn, owner_->until_lsn_));

  // When we detect that we are stuck, we record the time of that event.
  // Otherwise, we go to the "unstuck" state.
  if (!is_stuck) {
    last_time_stuck_ = TimePoint::max();
  } else if (last_time_stuck_ == TimePoint::max()) {
    last_time_stuck_ = SystemClock::now();
  }
  maybeBumpStats();
}

void ClientReadersFlowTracer::updateTimeLagging(Status st) {
  int64_t cur_ts_lag;
  auto& settings = Worker::settings();
  auto last_lag = estimateTimeLag();
  if (st == E::OK && last_lag.has_value()) {
    cur_ts_lag = last_lag.value();
  } else if (st != E::OK && !time_lag_record_.empty()) {
    // Our last_lag value is computed from stale info, let's repeat the
    // previous lag time lag that we have recorded instead.
    cur_ts_lag = time_lag_record_.back().time_lag;
  } else {
    RATELIMIT_WARNING(
        std::chrono::seconds{10},
        1,
        "Unable to obtain timestamp lagged in read stream with "
        "id %ld for logid %lu. We haven't gotten any log tail information ",
        owner_->getID().val(),
        owner_->log_id_.val());
    return;
  }

  /* pop old samples */
  while (!time_lag_record_.empty() && time_lag_record_.front().ttl == 0) {
    time_lag_record_.pop_front();
  }
  /* update counters */
  for (auto& s : time_lag_record_) {
    --s.ttl;
  }

  if (!is_client_reading_) {
    if (!time_lag_record_.full()) {
      // if we have stale samples, let's go back to not be lagging because the
      // client is purposefully not reading right now.
      last_time_lagging_ = TimePoint::max();
    }
    maybeBumpStats();
    return;
  }

  const auto group_size = std::max(
      settings.client_readers_flow_tracer_lagging_metric_sample_group_size,
      1ul);
  const auto slope_threshold =
      settings.client_readers_flow_tracer_lagging_slope_threshold;
  const auto num_groups =
      settings.client_readers_flow_tracer_lagging_metric_num_sample_groups;

  /* Should we record this sample?
   * We do this now so time_window computation has a nicer expression. */
  if ((sample_counter_++) % group_size == 0) {
    time_lag_record_.push_back(
        {.time_lag = cur_ts_lag,
         .time_lag_correction = 0,
         .ttl = get_initial_ttl(group_size, num_groups)});
  }

  const auto time_window = tracer_period_.count() *
      (group_size * (num_groups - 1) + sample_counter_ % group_size);

  int64_t correction = 0;
  for (auto& x : time_lag_record_) {
    // accumulate corrections
    correction += x.time_lag_correction;
  }

  bool is_catching_up = cur_ts_lag <= tracer_period_.count() ||
      !time_lag_record_.full() ||
      cur_ts_lag - time_lag_record_.front().time_lag + correction <=
          slope_threshold * time_window;

  if (is_catching_up) {
    last_time_lagging_ = TimePoint::max();
  } else if (last_time_lagging_ == TimePoint::max()) {
    last_time_lagging_ = SystemClock::now();
  }

  maybeBumpStats();
}

void ClientReadersFlowTracer::maybeBumpStats(bool force_healthy) {
  auto now = SystemClock::now();
  State state_to_report;
  auto& settings = Worker::settings();

  if (last_time_stuck_ != TimePoint::max()
          ? last_time_stuck_ + settings.reader_stuck_threshold <= now
          : false) {
    state_to_report = State::STUCK;
  } else if ((last_time_lagging_ != TimePoint::max()
                  ? last_time_lagging_ + settings.reader_lagging_threshold <=
                      now
                  : false) &&
             owner_->until_lsn_ == LSN_MAX) {
    // We won't consider a reader lagging if until_lsn is a fixed target because
    // we are not attempting to reach a moving tail.
    state_to_report = State::LAGGING;
  } else {
    state_to_report = State::HEALTHY;
  }

  if (force_healthy) {
    state_to_report = State::HEALTHY;
  }

  auto update_counter_for_state = [](State state, int increment) {
    if (state == State::STUCK) {
      WORKER_STAT_ADD(client.read_streams_stuck, increment);
    } else if (state == State::LAGGING) {
      WORKER_STAT_ADD(client.read_streams_lagging, increment);
    } else {
      /* ignore */
    }
  };

  if (state_to_report != last_reported_state_) {
    update_counter_for_state(last_reported_state_, -1);
    update_counter_for_state(state_to_report, +1);
    last_reported_state_ = state_to_report;
  }
}

std::string ClientReadersFlowTracer::lastReportedStatePretty() const {
  switch (last_reported_state_) {
    case State::HEALTHY:
      return "healthy";
    case State::STUCK:
      return "stuck";
    case State::LAGGING:
      return "lagging";
  }
  ld_check(false);
  return "";
}

std::string ClientReadersFlowTracer::lastTailInfoPretty() const {
  if (latest_tail_info_.has_value()) {
    return folly::sformat("OM={},TS={},LSN={}",
                          latest_tail_info_.value().offsets.toString().c_str(),
                          latest_tail_info_.value().timestamp,
                          lsn_to_string(latest_tail_info_.value().lsn_approx));
  } else {
    return "NONE";
  }
}

std::string ClientReadersFlowTracer::timeLagRecordPretty() const {
  std::vector<std::string> entries_pretty;
  for (auto& s : time_lag_record_) {
    entries_pretty.push_back(folly::sformat("[ts_lag={},ts_lag_cor={},ttl={}]",
                                            s.time_lag,
                                            s.time_lag_correction,
                                            s.ttl));
  }
  return folly::join(",", entries_pretty);
}

folly::Optional<int64_t> ClientReadersFlowTracer::estimateTimeLag() const {
  if (latest_tail_info_.hasValue()) {
    auto tail_lsn = latest_tail_info_->lsn_approx;
    auto tail_ts = latest_tail_info_->timestamp;
    int64_t last_in_record_ts = owner_->last_in_record_ts_.count();

    if (tail_lsn < owner_->next_lsn_to_deliver_) {
      /* If we are at tail, we should report that we have no lag to avoid
       * reporting a reader that is at tail as lagging. This is our last resort
       * for readers that are racing the trim point and might miss the record
       * that was last appended. */
      return 0;
    } else if (last_in_record_ts > 0) {
      return std::max(tail_ts - last_in_record_ts, static_cast<int64_t>(0));
    }
  }
  return folly::none;
}

folly::Optional<int64_t> ClientReadersFlowTracer::estimateByteLag() const {
  if (latest_tail_info_.hasValue()) {
    auto tail_lsn = latest_tail_info_->lsn_approx;
    int64_t tail_byte_offset =
        latest_tail_info_->offsets.getCounter(BYTE_OFFSET);
    int64_t acc_byte_offset =
        owner_->accumulated_offsets_.getCounter(BYTE_OFFSET);

    if (tail_lsn < owner_->next_lsn_to_deliver_) {
      // see comment in estimateTimeLag()
      return 0;
    } else if (acc_byte_offset != BYTE_OFFSET_INVALID &&
               tail_byte_offset != BYTE_OFFSET_INVALID) {
      if (tail_byte_offset >= acc_byte_offset) {
        return tail_byte_offset - acc_byte_offset;
      } else {
        return 0;
      }
    }
  }
  return folly::none;
}

void ClientReadersFlowTracer::updateIsClientReading() {
  bool was_client_reading = is_client_reading_;
  is_client_reading_ =
      !(owner_->redelivery_timer_ && owner_->redelivery_timer_->isActive()) &&
      !owner_->window_update_pending_; // We check window_update_pending_ as a
                                       // best effort attempt to assess if the
                                       // client is reading because a
                                       // synchronous reader that is not
                                       // consuming records (T34286876) might
                                       // hold all records of the CRS buffer
                                       // while not notifying the CRS to slide
                                       // the window, creating a situation where
                                       // the client is not reading,
                                       // next_lsn_to_deliver does not move and
                                       // the redelivery timer is not active.

  /* check if we transitioned to reading */
  if (was_client_reading && !is_client_reading_) {
    auto time_lag = estimateTimeLag();
    if (!time_lag_record_.empty() && time_lag.hasValue()) {
      time_lag_record_.back().time_lag_correction -= time_lag.value();
    }
  } else if (!was_client_reading && is_client_reading_) {
    auto time_lag = estimateTimeLag();
    if (!time_lag_record_.empty() && time_lag.hasValue()) {
      time_lag_record_.back().time_lag_correction += time_lag.value();
    }
  }
}

void ClientReadersFlowTracer::onRedeliveryTimerInactive() {
  updateIsClientReading();
}

void ClientReadersFlowTracer::onRedeliveryTimerActive() {
  updateIsClientReading();
}

void ClientReadersFlowTracer::onWindowUpdatePending() {
  updateIsClientReading();
}

void ClientReadersFlowTracer::onWindowUpdateSent() {
  updateIsClientReading();
}

}} // namespace facebook::logdevice
