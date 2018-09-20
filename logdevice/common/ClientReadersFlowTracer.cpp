/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientReadersFlowTracer.h"

#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/GetSeqStateRequest.h"
#include "logdevice/common/LibeventTimer.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Request.h"
#include "logdevice/common/ShardAuthoritativeStatusMap.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/SyncSequencerRequest.h"
#include "logdevice/common/Worker.h"

namespace facebook { namespace logdevice {

ClientReadersFlowTracer::ClientReadersFlowTracer(
    std::shared_ptr<TraceLogger> logger,
    ClientReadStream* owner)
    : SampledTracer(std::move(logger)),
      settings_(Worker::onThisThread()->processor_->updateableSettings()),
      owner_(owner) {
  timer_ = std::make_unique<LibeventTimer>(
      Worker::onThisThread()->getEventBase(), [this] { onTimerTriggered(); });

  // obtain log group name
  log_group_name_ = "<WAITING LOGSCONFIG>";

  auto config = logger_->getConfiguration();
  auto get_stream_by_id = owner_->deps_->getStreamByIDCallback();
  auto rsid = owner_->getID();
  logger_->getConfiguration()->getLogGroupByIDAsync(
      owner_->log_id_,
      [rsid,
       get_stream_by_id](std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
        auto ptr = get_stream_by_id(rsid);
        if (ptr && ptr->readers_flow_tracer_) {
          if (log_config) {
            ptr->readers_flow_tracer_->log_group_name_ = log_config->name();
          } else {
            ptr->readers_flow_tracer_->log_group_name_ = "<NO CONFIG>";
          }
        }
      });

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

  auto sample_builder =
      [=,
       reading_speed_bytes = num_bytes_read - last_num_bytes_read_,
       reading_speed_records = num_records_read - last_num_records_read_,
       async_data = last_async_records_]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();
    sample->addNormalValue("log_id", std::to_string(owner_->log_id_.val()));

    sample->addNormalValue("log_group_name", log_group_name_);

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
    if (async_data.has_value()) {
      if (async_data->bytes_lagged.has_value()) {
        sample->addIntValue("bytes_lagged", async_data->bytes_lagged.value());
      }
      if (async_data->bytes_lagged_delta.has_value()) {
        sample->addIntValue(
            "bytes_lagged_delta", async_data->bytes_lagged_delta.value());
      }
      if (async_data->timestamp_lagged.has_value()) {
        sample->addIntValue(
            "timestamp_lagged", async_data->timestamp_lagged.value());
      }
      if (async_data->timestamp_lagged_delta.has_value()) {
        sample->addIntValue("timestamp_lagged_delta",
                            async_data->timestamp_lagged_delta.value());
      }
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
  last_async_records_.clear();
  publish(READERS_FLOW_TRACER, sample_builder);
}

void ClientReadersFlowTracer::onSettingsUpdated() {
  tracer_period_ = settings_->client_readers_flow_tracer_period;
  if (tracer_period_ != std::chrono::milliseconds::zero()) {
    if (!timer_->isActive()) {
      timer_->activate(std::chrono::milliseconds{0});
    }
  } else {
    if (timer_->isActive()) {
      timer_->cancel();
    }
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

void ClientReadersFlowTracer::updateAsyncRecords(
    uint64_t acc_byte_offset,
    std::chrono::milliseconds last_in_record_ts,
    lsn_t tail_lsn_approx,
    LogTailAttributes* attrs) {
  folly::Optional<int64_t> bytes_lagged;
  if (attrs->byte_offset != BYTE_OFFSET_INVALID &&
      acc_byte_offset != BYTE_OFFSET_INVALID) {
    bytes_lagged = attrs->byte_offset - acc_byte_offset;
  } else if (tail_lsn_approx < owner_->next_lsn_to_deliver_) {
    // Here, we can assume that we are at the tail and we simply
    // ignore issues with the sequencer not reporting a byte offset.
    // Note, however, that this will appear as a 0 even for clusters
    // that do not normally report byte offset.
    bytes_lagged = 0;
  }

  folly::Optional<int64_t> bytes_lagged_delta;
  if (bytes_lagged.has_value() && last_bytes_lagged_.has_value()) {
    bytes_lagged_delta = bytes_lagged.value() - last_bytes_lagged_.value();
  }
  last_bytes_lagged_ = bytes_lagged;

  folly::Optional<int64_t> timestamp_lagged;
  if (last_in_record_ts.count() > 0) {
    timestamp_lagged = (attrs->last_timestamp - last_in_record_ts).count();
  } else if (tail_lsn_approx < owner_->next_lsn_to_deliver_) {
    // see bytes_lagged
    timestamp_lagged = 0;
  }

  folly::Optional<int64_t> timestamp_lagged_delta;
  if (timestamp_lagged.hasValue() && last_timestamp_lagged_.has_value()) {
    timestamp_lagged_delta =
        timestamp_lagged.value() - last_timestamp_lagged_.value();
  }
  last_timestamp_lagged_ = timestamp_lagged;

  last_async_records_.assign(
      {.bytes_lagged = bytes_lagged,
       .bytes_lagged_delta = bytes_lagged_delta,
       .timestamp_lagged = timestamp_lagged,
       .timestamp_lagged_delta = timestamp_lagged_delta});
}

void ClientReadersFlowTracer::sendSyncSequencerRequest() {
  auto ssr = std::make_unique<SyncSequencerRequest>(
      owner_->log_id_,
      SyncSequencerRequest::INCLUDE_TAIL_ATTRIBUTES,
      [weak_ptr = std::weak_ptr<ClientReadersFlowTracer>(shared_from_this()),
       acc_byte_offset = owner_->accumulated_byte_offset_,
       last_in_record_ts = owner_->last_in_record_ts_](
          Status st,
          NodeID seq_node,
          lsn_t next_lsn,
          std::unique_ptr<LogTailAttributes> attrs,
          std::shared_ptr<const EpochMetaDataMap> /*unused*/,
          std::shared_ptr<TailRecord> /*unused*/) {
        if (auto ptr = weak_ptr.lock()) {
          if (st == E::OK && attrs) {
            auto tail_lsn_approx = attrs->last_released_real_lsn != LSN_INVALID
                ? attrs->last_released_real_lsn
                : next_lsn - 1; // in case we haven't gotten the
                                // last_released_real_lsn, we use the maximum
                                // possible lsn for the tail record.
            ptr->updateAsyncRecords(acc_byte_offset,
                                    last_in_record_ts,
                                    tail_lsn_approx,
                                    attrs.get());
            ptr->updateTimeStuck(tail_lsn_approx);
          } else {
            // If the sequencer is not responding, we assume we are stuck until
            // further notice.
            if (st == E::OK) {
              RATELIMIT_WARNING(
                  std::chrono::seconds(10),
                  10,
                  "SyncSequencerRequest (sent to sequencer node %s) returned "
                  "E::OK for log %lu in read stream %lu but did not provide "
                  "tail attributes.",
                  seq_node.toString().c_str(),
                  ptr->owner_->log_id_.val(),
                  ptr->owner_->deps_->getReadStreamID().val());
            } else {
              RATELIMIT_WARNING(
                  std::chrono::seconds(10),
                  10,
                  "SyncSequencerRequest (sent to sequencer node %s) "
                  "failed for log %lu in read stream %lu with error %s",
                  seq_node.toString().c_str(),
                  ptr->owner_->log_id_.val(),
                  ptr->owner_->deps_->getReadStreamID().val(),
                  error_description(st));
            }
            ptr->updateTimeStuck(LSN_INVALID, st);
          }
          ptr->updateTimeLagging(st);
        }
      },
      GetSeqStateRequest::Context::GET_TAIL_ATTRIBUTES,
      /*timeout=*/tracer_period_,
      GetSeqStateRequest::MergeType::GSS_MERGE_INTO_OLD);
  ssr->setThreadIdx(Worker::onThisThread()->idx_.val());
  std::unique_ptr<Request> req(std::move(ssr));
  Worker::onThisThread()->processor_->postRequest(req);
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

  // We use redelivery_timer_ being active as a hint that the client is not
  // consuming records fast enough and therefore we are not waiting on the
  // server.
  bool is_stuck =
      !(owner_->redelivery_timer_ && owner_->redelivery_timer_->isActive()) &&
      (st != E::OK ||
       owner_->next_lsn_to_deliver_ <= std::min(tail_lsn, owner_->until_lsn_));

  // When we detect that we are stuck, we record the time of that event.
  // Otherwise, we go to the "unstuck" state.
  if (!is_stuck) {
    last_time_stuck_ = TimePoint::max();
  } else if (last_time_stuck_ == TimePoint::max()) {
    last_time_stuck_ = SteadyClock::now();
  }
  maybeBumpStats();
}

void ClientReadersFlowTracer::updateTimeLagging(Status st) {
  int64_t timestamp_lagged_delta =
      (last_async_records_.hasValue()
           ? last_async_records_->timestamp_lagged_delta.value_or(0)
           : 0);
  bool is_lagging =
      !(owner_->redelivery_timer_ && owner_->redelivery_timer_->isActive()) &&
      (st != E::OK || timestamp_lagged_delta > 0);
  if (!is_lagging) {
    last_time_lagging_ = TimePoint::max();
  } else if (last_time_stuck_ == TimePoint::max()) {
    last_time_lagging_ = SteadyClock::now();
  }
  maybeBumpStats();
}

void ClientReadersFlowTracer::maybeBumpStats(bool force_healthy) {
  auto now = SteadyClock::now();
  State state_to_report;

  if (last_time_stuck_ != TimePoint::max()
          ? last_time_stuck_ + settings_->reader_stuck_threshold <= now
          : false) {
    state_to_report = State::STUCK;
  } else if ((last_time_lagging_ != TimePoint::max()
                  ? last_time_lagging_ + settings_->reader_lagging_threshold <=
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

}} // namespace facebook::logdevice
