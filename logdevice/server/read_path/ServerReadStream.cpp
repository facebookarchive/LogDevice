/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/read_path/ServerReadStream.h"

#include <list>
#include <string>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/CatchupQueue.h"
#include "logdevice/server/read_path/IteratorCache.h"
#include "logdevice/server/read_path/LogStorageState.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

namespace facebook { namespace logdevice {

ServerReadStream::ServerReadStream(read_stream_id_t id,
                                   ClientID client_id,
                                   logid_t log_id,
                                   shard_index_t shard,
                                   StatsHolder* stats,
                                   std::shared_ptr<std::string> log_group_path)
    : id_(id),
      client_id_(client_id),
      log_id_(log_id),
      shard_(shard),
      start_lsn_(LSN_INVALID),
      until_lsn_(LSN_INVALID),
      last_delivered_record_(LSN_INVALID),
      last_delivered_lsn_(LSN_INVALID),
      rebuilding_(false),
      in_under_replicated_region_(false),
      version_(0),
      ref_holder_(this),
      filter_version_(0),
      include_extra_metadata_(false),
      fill_cache_(true),
      ignore_released_status_(false),
      digest_(false),
      no_payload_(false),
      csi_data_only_(false),
      proto_(0),
      created_(std::chrono::steady_clock::now()),
      last_rewind_time_(created_),
      last_enqueued_time_(SteadyTimestamp::min()),
      last_batch_started_time_(SteadyTimestamp::min()),
      next_read_time_(SteadyTimestamp::min()),
      read_shaping_cb_(ref_holder_.ref()),
      storage_task_in_flight_(false),
      replication_(0),
      log_group_path_(std::move(log_group_path)),
      read_ptr_({LSN_INVALID}),
      window_high_(LSN_INVALID),
      stats_(stats),
      scd_enabled_(false),
      local_scd_enabled_(false) {
  ld_check(log_id_ != LOGID_INVALID && log_id_ != LOGID_INVALID2);

  WORKER_LOG_STAT_INCR(log_id_, read_streams);
}

ServerReadStream::~ServerReadStream() {
  // We do that for the sole purpose of keeping stats accurate.
  disableSingleCopyDelivery();
  setTrafficClass(TrafficClass::MAX);
  WORKER_LOG_STAT_DECR(log_id_, read_streams);
  if (is_throttled_) {
    STAT_INCR(stats_, read_throttling_num_streams_closed_in_throttle);
  }
}

const SimpleEnumMap<ServerReadStream::RecordSource, const char*>
    ServerReadStream::names{{RecordSource::REAL_TIME, "REAL_TIME"},
                            {RecordSource::NON_BLOCKING, "NON_BLOCKING"},
                            {RecordSource::BLOCKING, "BLOCKING"}};

bool ServerReadStream::isCatchingUp() const {
  return queue_hook_.is_linked() || queue_delayed_hook_.is_linked();
}

bool ServerReadStream::isPastWindow() const {
  return read_ptr_.lsn > window_high_;
}

bool ServerReadStream::isInUnderreplicatedRegion() const {
  return false;
}

void ServerReadStream::setReadPtr(lsn_t lsn) {
  const bool was_past_window = isPastWindow();
  read_ptr_.lsn = lsn;
  if (was_past_window != isPastWindow()) {
    adjustStatWhenWindowChanged();
  }
}

void ServerReadStream::setReadPtr(LocalLogStoreReader::ReadPointer read_ptr) {
  setReadPtr(read_ptr.lsn);
}

const LocalLogStoreReader::ReadPointer& ServerReadStream::getReadPtr() const {
  return read_ptr_;
}

void ServerReadStream::setWindowHigh(lsn_t window_high) {
  const bool was_past_window = isPastWindow();
  window_high_ = window_high;
  if (was_past_window != isPastWindow()) {
    adjustStatWhenWindowChanged();
  }
}

void ServerReadStream::adjustStatWhenWindowChanged() {
  if (traffic_class_ != TrafficClass::MAX) {
    int64_t sign = isPastWindow() ? 1 : -1;
    TRAFFIC_CLASS_STAT_ADD(
        stats_, traffic_class_, read_streams_num_window_end, sign);
  }
}

void ServerReadStream::adjustStatWhenCatchingUpChanged() {
  if (traffic_class_ != TrafficClass::MAX) {
    int64_t sign = isCatchingUp() ? 1 : -1;
    TRAFFIC_CLASS_STAT_ADD(
        stats_, traffic_class_, read_streams_num_catching_up, sign);
  }
}

void ServerReadStream::enableSingleCopyDelivery(
    const small_shardset_t& known_down,
    node_index_t my_node_index) {
  const int diff = known_down.size() - known_down_.size();
  STAT_ADD(stats_, read_streams_total_known_down_size, diff);
  if (!scd_enabled_) {
    STAT_INCR(stats_, read_streams_num_scd);
  }

  scd_enabled_ = true;
  known_down_ = known_down;

  const bool prev = self_in_known_down_;
  self_in_known_down_ =
      std::find(known_down_.begin(),
                known_down_.end(),
                ShardID(my_node_index, shard_)) != known_down_.end();

  STAT_ADD(stats_,
           read_streams_total_self_in_known_down,
           self_in_known_down_ - prev);
}

void ServerReadStream::disableSingleCopyDelivery() {
  if (!scd_enabled_) {
    return;
  }

  STAT_SUB(stats_, read_streams_total_known_down_size, known_down_.size());
  STAT_DECR(stats_, read_streams_num_scd);
  STAT_SUB(stats_, read_streams_total_self_in_known_down, self_in_known_down_);

  scd_enabled_ = false;
  known_down_.clear();
  self_in_known_down_ = false;
}

void ServerReadStream::enableLocalScd(const std::string& client_location) {
  local_scd_enabled_ = true;
  client_location_ = client_location;
}

void ServerReadStream::disableLocalScd() {
  if (!local_scd_enabled_) {
    return;
  }

  local_scd_enabled_ = false;
  client_location_.clear();
}

void ServerReadStream::setTrafficClass(TrafficClass c) {
  if (c == traffic_class_) {
    return;
  }

  auto upd = [this](int sign) {
    if (!stats_ || traffic_class_ == TrafficClass::MAX) {
      return;
    }
    TRAFFIC_CLASS_STAT_ADD(stats_, traffic_class_, read_streams, sign);
    if (isCatchingUp()) {
      TRAFFIC_CLASS_STAT_ADD(
          stats_, traffic_class_, read_streams_num_catching_up, sign);
    }
    if (isPastWindow()) {
      TRAFFIC_CLASS_STAT_ADD(
          stats_, traffic_class_, read_streams_num_window_end, sign);
    }
  };

  upd(-1);
  traffic_class_ = c;
  upd(+1);

  if (traffic_class_ != TrafficClass::MAX) {
    TRAFFIC_CLASS_STAT_INCR(stats_, traffic_class_, read_streams_created);
  }
}

void ServerReadStream::getDebugInfo(InfoReadersTable& table) const {
  std::string known_down;
  if (scdEnabled()) {
    for (size_t i = 0; i < getKnownDown().size(); ++i) {
      if (i != 0) {
        known_down += ",";
      }
      known_down += getKnownDown()[i].toString();
    }
  } else {
    known_down = "ALL_SEND_ALL";
  }

  ServerWorker* worker = ServerWorker::onThisThread();
  LogStorageState& log_state =
      worker->processor_->getLogStorageStateMap().get(log_id_, shard_);
  LogStorageState::LastReleasedLSN last_released_lsn =
      log_state.getLastReleasedLSN();

  table.next()
      .set<0>(shard_)
      .set<1>(client_id_)
      .set<2>(log_id_)
      .set<3>(start_lsn_)
      .set<4>(until_lsn_)
      .set<5>(read_ptr_.lsn)
      .set<6>(last_delivered_lsn_)
      .set<7>(last_delivered_record_)
      .set<8>(window_high_)
      .set<10>(isCatchingUp())
      .set<11>(isPastWindow())
      .set<12>(known_down)
      .set<13>(filter_version_.val());

  if (last_released_lsn.hasValue()) {
    table.set<9>(last_released_lsn.value());
  }

  table.set<14>(last_batch_status_);
  table.set<15>(toSystemTimestamp(created_).toMilliseconds());

  if (last_enqueued_time_ != std::chrono::steady_clock::time_point::min()) {
    table.set<16>(toSystemTimestamp(last_enqueued_time_).toMilliseconds());
  }
  if (last_batch_started_time_ !=
      std::chrono::steady_clock::time_point::min()) {
    table.set<17>(toSystemTimestamp(last_batch_started_time_).toMilliseconds());
  }

  table.set<18>(storage_task_in_flight_);
  table.set<19>(version_.val_);
  table.set<20>(is_throttled_);

  if (is_throttled_) {
    int64_t throttled_since_ms = msec_since(throttling_start_time_);
    table.set<21>(throttled_since_ms);
  }

  size_t current_meter_level = getCurrentMeterLevel();
  table.set<22>(current_meter_level);
}

void ServerReadStream::addReleasedRecords(
    const std::shared_ptr<ReleasedRecords>& ptr) {
  if (!released_records_.empty()) {
    // Assert that new records come after existing records.
    const ReleasedRecords* back = released_records_.back().get();

    ld_check(!same_epoch(back->begin_lsn_, ptr->begin_lsn_) ||
             *back < ptr->begin_lsn_);
  }
  released_records_.push_back(ptr);
}

void ServerReadStream::noteSent(StatsHolder* stats,
                                RecordSource source,
                                size_t msg_size_bytes_approx) {
  switch (source) {
    case RecordSource::REAL_TIME:
      STAT_INCR(stats, read_streams_records_real_time);
      STAT_ADD(stats, read_streams_bytes_real_time, msg_size_bytes_approx);
      break;
    case RecordSource::NON_BLOCKING:
      STAT_INCR(stats, read_streams_records_non_blocking);
      STAT_ADD(stats, read_streams_bytes_non_blocking, msg_size_bytes_approx);
      break;
    case RecordSource::BLOCKING:
      STAT_INCR(stats, read_streams_records_blocking);
      STAT_ADD(stats, read_streams_bytes_blocking, msg_size_bytes_approx);
      break;
    case RecordSource::MAX:
      ld_check(false);
      break;
  }

  if (last_sent_source_.hasValue() && source == last_sent_source_.value()) {
    return;
  }

  switch (source) {
    case RecordSource::REAL_TIME:
      STAT_INCR(stats, real_time_switched_to_real_time);
      break;
    case RecordSource::NON_BLOCKING:
      STAT_INCR(stats, real_time_switched_to_non_blocking);
      break;
    case RecordSource::BLOCKING:
      STAT_INCR(stats, real_time_switched_to_blocking);
      break;
    case RecordSource::MAX:
      break;
  }

  last_sent_source_ = source;
}

std::string ServerReadStream::OnSentState::toString() const {
  return std::string("{") + "fv=" + logdevice::toString(filter_version) +
      ","
      "start=" +
      lsn_to_string(start_lsn) +
      ","
      "min_next=" +
      lsn_to_string(min_next_lsn) +
      ","
      "last_record_lsn=" +
      lsn_to_string(last_record_lsn) +
      ","
      "last_lsn=" +
      lsn_to_string(last_lsn) +
      ","
      "started=" +
      logdevice::toString(started) + "}";
}

std::string ServerReadStream::toString() const {
  std::string known_down;
  if (scdEnabled()) {
    for (size_t i = 0; i < getKnownDown().size(); ++i) {
      if (i != 0) {
        known_down += ",";
      }
      known_down += getKnownDown()[i].toString();
    }
  } else {
    known_down = "ALL_SEND_ALL";
  }

  LogStorageState::LastReleasedLSN last_released_lsn;
  ServerWorker* worker = ServerWorker::onThisThread(/*enforce_worker*/ false);
  if (worker) {
    LogStorageState& log_state =
        worker->processor_->getLogStorageStateMap().get(log_id_, shard_);
    last_released_lsn = log_state.getLastReleasedLSN();
  }

  return std::string("{") +
      "traffic_class=" + trafficClasses()[traffic_class_].c_str() +
      ","
      "id=" +
      logdevice::toString(id_) +
      ","
      "shard=" +
      logdevice::toString(shard_) +
      ","
      "client_id=" +
      logdevice::toString(client_id_) +
      ","
      "log_id=" +
      logdevice::toString(log_id_) +
      ","
      "start=" +
      lsn_to_string(start_lsn_) +
      ","
      "until=" +
      lsn_to_string(until_lsn_) +
      ","
      "read_ptr=" +
      lsn_to_string(read_ptr_.lsn) +
      ","
      "last_lsn=" +
      lsn_to_string(last_delivered_lsn_) +
      ","
      "last_record=" +
      lsn_to_string(last_delivered_record_) +
      ","
      "last_released=" +
      (last_released_lsn.hasValue() ? lsn_to_string(last_released_lsn.value())
                                    : "none") +
      ","
      "window_high=" +
      lsn_to_string(window_high_) +
      ","
      "catching_up=" +
      logdevice::toString(isCatchingUp()) +
      ","
      "past_window=" +
      logdevice::toString(isPastWindow()) +
      ","
      "known_down=" +
      logdevice::toString(known_down) +
      ","
      "fv=" +
      logdevice::toString(filter_version_) +
      ","
      "version=" +
      logdevice::toString(version_) +
      ","
      "sent_state=" +
      logdevice::toString(sent_state) +
      ","
      "created=" +
      logdevice::toString(toSystemTimestamp(created_)) +
      ","
      "last_rewind=" +
      logdevice::toString(toSystemTimestamp(last_rewind_time_)) +
      ","
      "last_queued=" +
      logdevice::toString(toSystemTimestamp(last_enqueued_time_)) +
      ","
      "last_batch_staus=" +
      std::string(last_batch_status_) +
      ","
      "last_batch_time=" +
      logdevice::toString(toSystemTimestamp(last_batch_started_time_)) +
      ","
      "storage_task=" +
      logdevice::toString(storage_task_in_flight_) + "}";
}

}} // namespace facebook::logdevice
