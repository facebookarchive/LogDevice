/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#define __STDC_FORMAT_MACROS // pull in PRIu64 etc

#include "logdevice/common/client_read_stream/ClientReadStream.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <utility>

#include <folly/CppAttributes.h>
#include <folly/Memory.h>
#include <folly/String.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ClientGapTracer.h"
#include "logdevice/common/ClientReadTracer.h"
#include "logdevice/common/ClientReadersFlowTracer.h"
#include "logdevice/common/ClientStalledReadTracer.h"
#include "logdevice/common/ClusterState.h"
#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EpochMetaDataCache.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/SocketCallback.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBuffer.h"
#include "logdevice/common/client_read_stream/ClientReadStreamBufferFactory.h"
#include "logdevice/common/client_read_stream/ClientReadStreamConnectionHealth.h"
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/client_read_stream/ClientReadStreamTracer.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/protocol/STARTED_Message.h"
#include "logdevice/common/protocol/STOP_Message.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Record.h"

namespace facebook { namespace logdevice {

using ConnectionState = ClientReadStreamSenderState::ConnectionState;

// delay to use when retrying reading metadata log if the metadata log is empty
// when start reading
static const std::chrono::milliseconds INITIAL_RETRY_READ_METADATA_DELAY(1000);
static const std::chrono::milliseconds MAX_RETRY_READ_METADATA_DELAY(30000);

// Calculates the actual buffer size to use, given the requested size, start
// and until LSNs.
static size_t actual_buffer_size(size_t requested_size,
                                 lsn_t start_lsn,
                                 lsn_t until_lsn) {
  // This should have been checked by now
  ld_check(start_lsn <= until_lsn);

  // If the client is only reading a small range, we can use a smaller buffer
  // than requested.  Client asked to read (until - start + 1) records; we can
  // make the buffer that small without having to slide the window.
  static_assert(sizeof(size_t) >= sizeof(until_lsn - start_lsn + 1),
                "does not fit, overflow hazard");

  size_t size =
      std::min(until_lsn - start_lsn, std::numeric_limits<size_t>::max() - 1) +
      1;
  size = std::min(size, requested_size);

  // Make sure the buffer is not empty.
  size = std::max(size, size_t(1));

  return size;
}

// Helper method to bump the right `client.gap_*' stat after delivering a gap
static void bumpGapStat(logid_t, StatsHolder*, GapType);

void ClientReadStreamRecordState::reset() {
  ld_check(list.empty());
  record.reset();
  gap = false;
  filtered_out = false;
}

ClientReadStream::ClientReadStream(
    read_stream_id_t id,
    logid_t log_id,
    lsn_t start_lsn,
    lsn_t until_lsn,
    double flow_control_threshold,
    ClientReadStreamBufferType buffer_type,
    size_t buffer_capacity,
    std::unique_ptr<ClientReadStreamDependencies>&& deps,
    std::shared_ptr<UpdateableConfig> config,
    ReaderBridge* reader,
    const ReadStreamAttributes* attrs)
    : id_(id),
      log_id_(log_id),
      start_lsn_(start_lsn),
      next_lsn_to_deliver_(start_lsn),
      until_lsn_(until_lsn),
      flow_control_threshold_(flow_control_threshold),
      buffer_(ClientReadStreamBufferFactory::create(
          buffer_type,
          actual_buffer_size(buffer_capacity, start_lsn, until_lsn),
          next_lsn_to_deliver_)),
      // window size starts at the maximum buffer capacity
      window_size_(buffer_->capacity()),
      deps_(std::move(deps)),
      config_(std::move(config)),
      last_epoch_with_metadata_(EPOCH_INVALID),
      gap_end_outside_window_(LSN_INVALID),
      trim_point_(LSN_INVALID),
      reader_(reader),
      coordinated_proto_(Compatibility::MAX_PROTOCOL_SUPPORTED),
      window_update_pending_(false),
      gap_tracer_(std::make_unique<ClientGapTracer>(nullptr)),
      read_tracer_(std::make_unique<ClientReadTracer>(nullptr)),
      events_tracer_(std::make_unique<ClientReadStreamTracer>(nullptr)) {
  if (attrs != nullptr) {
    attrs_ = *attrs;
  }
  calcWindowHigh();
  calcNextLSNToSlideWindow();
  updateServerWindow();
}

std::string ClientReadStream::senderStatePretty() const {
  std::vector<ShardID> states;
  for (const auto& kv : storage_set_states_) {
    states.push_back(kv.first);
  }
  std::sort(states.begin(), states.end());

  std::vector<std::string> gap_shards_details;
  for (const auto& shard_id : states) {
    const SenderState& state{storage_set_states_.find(shard_id)->second};
    ClientID cid{deps_->getOurNameAtPeer(shard_id.node())};

    std::string detail = shard_id.toString() + "(" + cid.toString() +
        "):" + lsn_to_string(state.getNextLsn());

    switch (state.getGapState()) {
      case GapState::NONE:
        detail += ":N";
        break;
      case GapState::GAP:
        detail += ":G";
        break;
      case GapState::UNDER_REPLICATED:
        detail += ":U";
        break;
    }
    gap_shards_details.push_back(detail);
  }
  return folly::join(',', gap_shards_details);
}

std::string ClientReadStream::unavailableShardsPretty() const {
  std::vector<std::string> unavailable_shards;
  for (const auto& kv : storage_set_states_) {
    std::string detail = kv.first.toString();
    if (kv.second.getAuthoritativeStatus() ==
        AuthoritativeStatus::UNDERREPLICATION) {
      detail += ":UR";
      unavailable_shards.push_back(detail);
    } else if (kv.second.getAuthoritativeStatus() ==
               AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
      detail += ":AE";
      unavailable_shards.push_back(detail);
    } else if (kv.second.getConnectionState() != ConnectionState::READING) {
      switch (kv.second.getConnectionState()) {
        case ConnectionState::INITIAL:
        case ConnectionState::CONNECTING:
        case ConnectionState::RECONNECT_PENDING:
          detail += ":C";
          break;
        case ConnectionState::START_SENT:
          detail += ":S";
          break;
        case ConnectionState::PERSISTENT_ERROR:
          detail += ":F";
          break;
        case ConnectionState::DYING:
          detail += ":D";
          break;
        case ConnectionState::READING:
          ld_check(false);
          break;
      }
      unavailable_shards.push_back(detail);
    } else if (kv.second.getGapState() == GapState::UNDER_REPLICATED) {
      detail += ":U";
      unavailable_shards.push_back(detail);
    }
  }
  return folly::join(',', unavailable_shards);
}

std::string ClientReadStream::graceCountersPretty() const {
  std::vector<std::string> output;
  for (const auto& kv : storage_set_states_) {
    auto& sender = kv.second;
    auto shard = kv.first;
    if (sender.getGapState() == GapState::NONE &&
        sender.isFullyAuthoritative() && sender.grace_counter > 0) {
      output.push_back(shard.toString() + "=" +
                       std::to_string(sender.grace_counter));
    }
  }
  return folly::join(',', output);
}

std::string ClientReadStream::getStorageSetHealthStatusPretty() const {
  if (!healthy_node_set_ ||
      last_epoch_with_metadata_ < epoch_metadata_requested_.value()) {
    return "READING_METADATA";
  } else {
    const auto ret = healthy_node_set_->isFmajority(true);
    switch (ret) {
      case FmajorityResult::AUTHORITATIVE_COMPLETE:
        // All shards that we should be able to communicate to are up.
        return "HEALTHY_AUTHORITATIVE_COMPLETE";
      case FmajorityResult::AUTHORITATIVE_INCOMPLETE:
        // There are some shards down but we should be able to make progess
        // anyway because we can still have an f-majority.
        return "HEALTHY_AUTHORITATIVE_INCOMPLETE";
      case FmajorityResult::NON_AUTHORITATIVE:
        // Al the shards that we should be able to communicate to are up but
        // there are too many shards being rebuilt, we may see data loss.
        return "HEALTHY_NON_AUTHORITATIVE";
      case FmajorityResult::NONE:
        // There are not enough shards up for the reader to be able to make
        // progress.
        return "UNHEALTHY";
      default:
        RATELIMIT_ERROR(std::chrono::seconds(1),
                        1,
                        "Got an invalid health status on read stream %lu.",
                        deps_->getReadStreamID().val_);
        return "INVALID_HEALTH_STATUS";
    }
  }
}

std::string ClientReadStream::getDebugInfoStr() const {
  // Be aware that some of our tooling parses this information. Do not rename
  // fields or change the format. It should however be safe to add new fields.

  std::string shards_down = "ALL_SEND_ALL";
  std::string shards_slow = "ALL_SEND_ALL";
  if (scd_ && scd_->isActive()) {
    shards_down = toString(scd_->getShardsDown());
    shards_slow = toString(scd_->getShardsSlow());
  }
  std::string res;
  res += "next_lsn_to_deliver_=" + lsn_to_string(next_lsn_to_deliver_);
  res += ", window_high_=" + lsn_to_string(window_high_);
  res += ", gap_end_outside_window_=" + lsn_to_string(gap_end_outside_window_);
  res += ", until_lsn_=" + lsn_to_string(until_lsn_);
  res += ", last_epoch_with_metadata_=" +
      std::to_string(last_epoch_with_metadata_.val());
  res += ", trim_point_=" + lsn_to_string(trim_point_);
  res += ", unavailable_shards={" + unavailableShardsPretty();
  res += "}, filter_version=" + std::to_string(filter_version_.val());
  res += ", shards_down=" + shards_down;
  res += ", shards_slow=" + shards_slow;
  // Print sender state at the end because it can be long and get truncated.
  res += ", sender_state={" + senderStatePretty() + "}";

  return res;
}

ClientReadStream::ClientReadStreamDebugInfo
ClientReadStream::getClientReadStreamDebugInfo() const {
  ClientReadStream::ClientReadStreamDebugInfo info(id_);
  std::string shards_down = "ALL_SEND_ALL";
  std::string shards_slow = "ALL_SEND_ALL";
  if (scd_ && scd_->isActive()) {
    shards_down = toString(scd_->getShardsDown());
    shards_slow = toString(scd_->getShardsSlow());
  }

  info.log_id = log_id_;
  info.next_lsn = next_lsn_to_deliver_;
  info.window_high = window_high_;
  info.until_lsn = until_lsn_;
  info.set_size = readSetSize();
  info.gap_end = gap_end_outside_window_;
  info.trim_point = trim_point_;
  info.gap_shards_next_lsn = senderStatePretty();
  info.unavailable_shards = unavailableShardsPretty();
  info.redelivery = redelivery_timer_ ? redelivery_timer_->isActive() : false;
  info.filter_version = filter_version_.val();
  info.shards_down = shards_down;
  info.shards_slow = shards_slow;
  if (healthy_node_set_ && last_epoch_with_metadata_ != EPOCH_INVALID) {
    info.health = getStorageSetHealthStatusPretty();
  }

  if (readers_flow_tracer_) {
    auto last_bytes_lagged = readers_flow_tracer_->estimateByteLag();
    if (last_bytes_lagged.has_value()) {
      info.bytes_lagged = last_bytes_lagged;
    }
    auto last_timestamp_lagged = readers_flow_tracer_->estimateTimeLag();
    if (last_timestamp_lagged.has_value()) {
      info.timestamp_lagged = last_timestamp_lagged;
    }
    info.last_lagging =
        to_msec((readers_flow_tracer_->last_time_lagging_.time_since_epoch()));
    info.last_stuck =
        to_msec((readers_flow_tracer_->last_time_stuck_.time_since_epoch()));
    info.last_report = readers_flow_tracer_->lastReportedStatePretty();
    info.last_tail_info = readers_flow_tracer_->lastTailInfoPretty();
    info.lag_record = readers_flow_tracer_->timeLagRecordPretty();
  }
  return info;
}

void ClientReadStream::sampleDebugInfo(
    const ClientReadStream::ClientReadStreamDebugInfo& info) const {
  if (!deps_->getSettings().enable_all_read_streams_sampling) {
    return;
  }
  auto sample = std::make_unique<TraceSample>();
  sample->addNormalValue("thread_name", ThreadID::getName());
  sample->addIntValue("log_id", info.log_id.val());
  sample->addIntValue("stream_id", info.stream_id.val());
  sample->addIntValue("next_lsn", info.next_lsn);
  sample->addIntValue("window_high", info.window_high);
  sample->addIntValue("until_lsn", info.until_lsn);
  sample->addIntValue("set_size", info.set_size);
  sample->addIntValue("gap_end", info.gap_end);
  sample->addIntValue("trim_point", info.trim_point);
  sample->addNormalValue("gap_shards_next_lsn", info.gap_shards_next_lsn);
  sample->addNormalValue("unavailable_shards", info.unavailable_shards);
  sample->addIntValue("redelivery", info.redelivery);
  sample->addIntValue("filter_version", info.filter_version);
  sample->addNormalValue("shards_down", info.shards_down);
  sample->addNormalValue("shards_slow", info.shards_slow);
  if (info.health.has_value()) {
    sample->addNormalValue("health", info.health.value());
  }
  if (info.bytes_lagged.has_value()) {
    sample->addIntValue("bytes_lagged", info.bytes_lagged.value());
  }
  if (info.timestamp_lagged.has_value()) {
    sample->addIntValue("timestamp_lagged", info.timestamp_lagged.value());
  }
  if (info.last_lagging.has_value()) {
    sample->addIntValue("last_lagging", info.last_lagging.value().count());
  }
  if (info.last_stuck.has_value()) {
    sample->addIntValue("last_stuck", info.last_stuck.value().count());
  }
  if (info.last_report.has_value()) {
    sample->addNormalValue("last_report", info.last_report.value());
  }
  if (info.last_tail_info.has_value()) {
    sample->addNormalValue("last_tail_info", info.last_tail_info.value());
  }
  if (info.lag_record.has_value()) {
    sample->addNormalValue("lag_record", info.lag_record.value());
  }

  Worker::onThisThread()->getTraceLogger()->pushSample(
      "all_read_streams", 0, std::move(sample));
  return;
}

void ClientReadStream::getDebugInfo(InfoClientReadStreamsTable& table) const {
  auto info = getClientReadStreamDebugInfo();
  table.next()
      .set<0>(info.log_id)
      .set<1>(info.stream_id)
      .set<2>(info.next_lsn)
      .set<3>(info.window_high)
      .set<4>(info.until_lsn)
      .set<5>(info.set_size)
      .set<6>(info.gap_end)
      .set<7>(info.trim_point)
      .set<8>(info.gap_shards_next_lsn)
      .set<9>(info.unavailable_shards)
      .set<11>(info.redelivery)
      .set<12>(info.filter_version)
      .set<13>(info.shards_down)
      .set<14>(info.shards_slow);

  if (info.health.has_value()) {
    table.set<10>(info.health.value());
  }
  if (info.bytes_lagged.has_value()) {
    table.set<15>(info.bytes_lagged.value());
  }
  if (info.timestamp_lagged.has_value()) {
    table.set<16>(info.timestamp_lagged.value());
  }
  if (info.last_lagging.has_value()) {
    table.set<17>(info.last_lagging.value());
  }
  if (info.last_stuck.has_value()) {
    table.set<18>(info.last_stuck.value());
  }
  if (info.last_report.has_value()) {
    table.set<19>(info.last_report.value());
  }
  if (info.last_tail_info.has_value()) {
    table.set<20>(info.last_tail_info.value());
  }
  if (info.lag_record.has_value()) {
    table.set<21>(info.lag_record.value());
  }
}

void ClientReadStream::start() {
  ld_check(!started_);
  started_ = true;

  rewind_scheduler_ = std::make_unique<RewindScheduler>(this);

  gap_tracer_ = std::make_unique<ClientGapTracer>(
      Worker::onThisThread(false) ? Worker::onThisThread()->getTraceLogger()
                                  : nullptr);

  read_tracer_ = std::make_unique<ClientReadTracer>(
      Worker::onThisThread(false) ? Worker::onThisThread()->getTraceLogger()
                                  : nullptr);

  events_tracer_ = std::make_unique<ClientReadStreamTracer>(
      Worker::onThisThread(false) ? Worker::onThisThread()->getTraceLogger()
                                  : nullptr);

  connection_health_tracker_ =
      std::make_unique<ClientReadStreamConnectionHealth>(this);

  if (Worker::onThisThread(false) &&
      !MetaDataLog::isMetaDataLog(
          log_id_) // Don't create tracer for metadata logs to avoid issues in
                   // SyncSequencerRequest.
  ) {
    readers_flow_tracer_ = std::make_unique<ClientReadersFlowTracer>(
        Worker::onThisThread()->getTraceLogger(), this);
  }

  auto gap_grace_period = deps_->computeGapGracePeriod();

  if (gap_grace_period > decltype(gap_grace_period)::zero()) {
    grace_period_ =
        deps_->createBackoffTimer(gap_grace_period, gap_grace_period);
    grace_period_->setCallback([this]() {
      // grace period timer should never be active when in single copy
      // delivery mode. Assert that.
      ld_check(!scd_->isActive());
      updateGraceCounters();
      findGapsAndRecords(/*grace_period_expired=*/true);
      disposeIfDone();
    });
  }

  std::shared_ptr<Configuration> config = config_->get();
  read_stream_id_t rsid = this->getID();
  auto get_stream_by_id = deps_->getStreamByIDCallback();
  std::chrono::steady_clock::time_point logid_request_time =
      std::chrono::steady_clock::now();

  // The first stat counts read streams that are alive, second one counts
  // read stream creation events (i.e. it's not decremented when
  // ClientReadStream is destroyed).
  WORKER_STAT_INCR(client.num_read_streams);
  WORKER_STAT_INCR(client.client_read_streams_created);

  // getLogByIDAsync might be synchronously called for LocalLogsConfig and
  // read stream can get destroyed in startContinuation. so make sure to not
  // access `this' after the call
  config->getLogGroupByIDAsync(
      log_id_,
      [get_stream_by_id, rsid, logid_request_time, this](
          const std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
        auto ptr = get_stream_by_id(rsid);
        if (!ptr) {
          ld_warning("Got reply for ClientReadStream %lu that doesn't exist "
                     "anymore.",
                     rsid.val_);
          return;
        }
        // measuring latency for ClientReadsTracer
        logid_request_latency_ =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - logid_request_time);
        ptr->startContinuation(log_config);
      });
}

void ClientReadStream::startContinuation(
    const std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
  if (!log_config) {
    deliverNoConfigGapAndDispose();
    return;
  }

  auto attrs = log_config->attrs();
  sequencer_window_size_ = *attrs.maxWritesInFlight();
  log_group_name_ = log_config->name();

  log_uses_scd_ = attrs.scdEnabled().hasValue() && *attrs.scdEnabled();
  log_uses_local_scd_ =
      attrs.localScdEnabled().hasValue() && *attrs.localScdEnabled();
  ClientReadStreamScd::Mode mode = ClientReadStreamScd::Mode::ALL_SEND_ALL;
  if (!force_no_scd_ && log_uses_scd_) {
    if (log_uses_local_scd_) {
      mode = ClientReadStreamScd::Mode::LOCAL_SCD;
    } else {
      mode = ClientReadStreamScd::Mode::SCD;
    }
  }
  scd_ = std::make_unique<ClientReadStreamScd>(this, mode);

  ensureSkipEpoch0(log_config);
}

void ClientReadStream::ensureSkipEpoch0(
    const std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
  if (currentEpoch() < EPOCH_MIN) {
    // We issue a bridge gap and update next_lsn_to_deliver_:
    int res = handleEpochBegin(compose_lsn(EPOCH_MIN, ESN_MIN));
    if (res == -1) {
      if (reattempt_start_timer_ == nullptr) {
        reattempt_start_timer_ = deps_->createBackoffTimer(
            deps_->getSettings().client_initial_redelivery_delay,
            deps_->getSettings().client_max_redelivery_delay);
        reattempt_start_timer_->setCallback(
            [this, log_config]() { startContinuation(log_config); });
      }
      reattempt_start_timer_->activate();
      return; // will try again later
    } else {
      if (reattempt_start_timer_ != nullptr) {
        // cleanup so we don't hold ownership of a copy of log_config
        reattempt_start_timer_->reset();
      }
    }
    slideSenderWindows();
  }

  /**
   * Request the initial epoch metadata for the current epoch,
   * start reading once the data is available.
   * Noted that it is possible that at this time, epoch metadata for
   * currentEpoch() has not yet been released or even does not exist at all.
   * In such case, the initial epoch metadata may not be correct. However,
   * we can still start reading from the storage set and reobtain the epoch
   * metadata the first time we encounters a gap.
   * See onEpochMetaData() for handling this special case.
   *
   * For the same reason, we allow delivering potentially inconsistent metadata
   * from the metadata cache to save performance on frequently start reading
   * the latest epoch.
   */
  if (!done()) {
    requestEpochMetaData(
        currentEpoch(), /*require_consistent_from_cache=*/false);
  }
  disposeIfDone();
}

void ClientReadStream::sendStart(ShardID shard_id, SenderState& state) {
  ld_check(!done());
  ld_check(last_epoch_with_metadata_ != EPOCH_INVALID);

  SocketCallback* onclose = state.getSocketClosedCallback();
  onclose->deactivate();

  resetGapParametersForSender(state);

  // If we just encountered an epoch bump, window_high will be below start_lsn.
  // This is fine, the storage shards will wait for the WINDOW message we'll
  // send after receiving STARTED.
  START_Header header;
  header.start_lsn = next_lsn_to_deliver_;
  header.until_lsn = until_lsn_;
  header.window_high = server_window_.high;
  header.flags = additional_start_flags_;
  header.required_node_in_copyset = -1; // deprecated.
  header.shard = shard_id.shard();

  if (ignore_released_status_) {
    header.flags |= START_Header::IGNORE_RELEASED_STATUS;
  }

  const auto& filtered_out =
      scd_->isActive() ? scd_->getFilteredOut() : small_shardset_t{};

  header.filter_version = filter_version_;

  ld_check(current_metadata_);
  header.replication = current_metadata_->replication.getReplicationFactor();
  header.scd_copyset_reordering = std::min(
      SCDCopysetReordering::HASH_SHUFFLE_CLIENT_SEED,
      SCDCopysetReordering(deps_->getSettings().scd_copyset_reordering_max));

  if (scd_->isActive()) {
    header.flags |= START_Header::SINGLE_COPY_DELIVERY;
    if (scd_->localScdEnabled()) {
      header.flags |= START_Header::LOCAL_SCD_ENABLED;
    }
  }
  int rv =
      deps_->sendStartMessage(shard_id, onclose, header, filtered_out, &attrs_);

  state.cancelReconnectTimer();
  state.cancelStartedTimer();

  if (rv == 0) {
    state.setConnectionState(ConnectionState::CONNECTING);
    state.setWindowHigh(header.window_high);
    any_start_sent_ = true;
  } else if (err == E::PROTONOSUPPORT) {
    handleStartPROTONOSUPPORT(shard_id);
  } else {
    RATELIMIT_INFO(std::chrono::seconds(1),
                   1,
                   "Cannot send START message to %s for log %lu: %s",
                   shard_id.toString().c_str(),
                   log_id_.val_,
                   error_description(err));
    onConnectionFailure(state, err);
  }
}

void ClientReadStream::onStartSent(ShardID shard_id, Status status) {
  ld_check(!done());

  auto it = storage_set_states_.find(shard_id);
  if (it == storage_set_states_.end()) {
    // It is possible that storage set has changed before START is sent
    // (although quite rare)
    ld_debug("could not find state for %s, possibly storage set "
             "has been changed",
             shard_id.toString().c_str());
    return;
  }

  SenderState& state = it->second;

  if (state.getConnectionState() != ConnectionState::CONNECTING) {
    // Ignore.  This can happen if:
    // (1) an original START timed out waiting for STARTED
    // (2) we enqueued another START
    // (3) the original STARTED came back
    // (4) this function gets called when the START from (2) goes out
    return;
  }

  if (status == E::OK) {
    state.setConnectionState(ConnectionState::START_SENT);
    state.activateStartedTimer();
  } else if (status == E::PROTONOSUPPORT) {
    handleStartPROTONOSUPPORT(shard_id);
  } else {
    RATELIMIT_LEVEL((status == E::TIMEDOUT || status == E::CONNFAILED)
                        ? dbg::Level::DEBUG
                        : dbg::Level::INFO,
                    std::chrono::seconds(10),
                    2,
                    "Cannot send START message to %s: %s",
                    shard_id.toString().c_str(),
                    error_description(status));
    onConnectionFailure(state, status);
  }
}

void ClientReadStream::onStarted(ShardID from, const STARTED_Message& msg) {
  ld_check(!done());
  ld_spew("Received STARTED_Message from %s, for log_:%lu, id_:%lu, status:%s",
          from.toString().c_str(),
          msg.header_.log_id.val(),
          id_.val(),
          errorStrings()[msg.header_.status].name);

  if (permission_denied_) {
    // Already received one STARTED_Message with E::ACCESS status.
    return;
  }

  auto it = storage_set_states_.find(from);
  if (it == storage_set_states_.end()) {
    // it is possible that the storage has changed and this STARTED becomes
    // stale.
    ld_debug("Could not find state for %s. It is possibly a stale "
             "STARTED and the storage set has already changed",
             from.toString().c_str());
    return;
  }

  if (msg.header_.filter_version > filter_version_) {
    ld_error("Received STARTED message from %s for log %lu with filter "
             "version %lu greater than this read stream's filter version %lu. "
             "This should not happen and is likely a bug in the server code.",
             from.toString().c_str(),
             log_id_.val_,
             msg.header_.filter_version.val_,
             filter_version_.val_);
    return;
  }

  SenderState& state = it->second;
  // Discard this response if it is not for the last filter version.
  if (msg.header_.filter_version < filter_version_) {
    ld_debug("Receive STARTED message from %s for log %lu with stale filter "
             "version %lu, last filter version is %lu.",
             from.toString().c_str(),
             log_id_.val_,
             msg.header_.filter_version.val_,
             filter_version_.val_);
    // The node is responding. Give it additional time to catch up to the
    // current filter version.
    state.extendStartedTimer(msg.header_.filter_version);
    return;
  }

  const Status status = msg.header_.status;
  if (status != E::FAILED &&
      state.getConnectionState() == ConnectionState::READING) {
    // Ignore.  This can happen if we resent START because the server took too
    // long to reply with STARTED, and then we got two of them.
    // It is however possible to receive a STARTED(FAILED) while in READING
    // state, this is the storage shard notifying us that it cannot send
    // anymore data because the shard entered an error state.
    return;
  }

  const AuthoritativeStatus auth_status = state.getAuthoritativeStatus();

  if (status == E::REBUILDING &&
      (auth_status == AuthoritativeStatus::FULLY_AUTHORITATIVE ||
       auth_status == AuthoritativeStatus::UNAVAILABLE)) {
    // We are probably lagging behind reading the event log. Hopefully we can
    // read without this node. Worst case, this stream will stall until we
    // catch up reading the event log.
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Node %s sent a STARTED message for log %lu with "
        "E::REBUILDING but event log says the node is %s. "
        "Ignoring this reply. We will keep retrying until what the node says "
        "matches what the event log says."
        "Sender state is %s.",
        from.toString().c_str(),
        log_id_.val_,
        toString(auth_status).c_str(),
        state.getConnectionStateName());
    if (scd_->isActive()) {
      scd_->addToShardsDownAndScheduleRewind(
          state.getShardID(),
          folly::format("{} added to known down list because it sent STARTED "
                        "with E::REBUILDING",
                        state.getShardID().toString())
              .str());
    }
    return;
  }

  state.resetStartedTimer();

  if (status == E::OK && auth_status != AuthoritativeStatus::UNAVAILABLE &&
      auth_status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        2,
        "Shard %s sent a STARTED message for log %lu with status == "
        "E::OK but event log says the shard is %s. "
        "This stream is now considering this shard FULLY_AUTHORITATIVE.",
        from.toString().c_str(),
        log_id_.val_,
        toString(auth_status).c_str());
    // This is an imperfect workaround for a race where a shard completes
    // rebuilding and starts accepting new writes but this stream is still
    // behind reading the event log and decides to make progress without it.
    // Here we decide to consider the shard fully authoritative to reduce the
    // chances of us missing records.
    setShardAuthoritativeStatus(
        state, AuthoritativeStatus::FULLY_AUTHORITATIVE);
  }

  switch (status) {
    case E::OK:
    case E::REBUILDING:
      ld_check(state.filter_version.val_ <= filter_version_.val_);
      state.filter_version = filter_version_;
      state.setConnectionState(ConnectionState::READING);
      state.resetReconnectTimer();
      if (status == E::OK) {
        updateLastReleased(msg.header_.last_released_lsn);
        // Send a quick WINDOW message in case this server missed out on any
        // window updates while the read stream was starting
        sendWindowMessage(state);
      } else if (scd_->isActive()) {
        // Do not activate the reconnect timer here. We'll retry sending START
        // once the connection is closed or if we receive a new STARTED message
        // when the node finishes rebuilding.
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::format("{} added to known down list because it sent STARTED "
                          "with E::REBUILDING",
                          state.getShardID().toString())
                .str());
      }
      break;

    case E::NOTSTORAGE:
    case E::SHUTDOWN:
    case E::FAILED:
      RATELIMIT_LEVEL(
          status == E::FAILED ? dbg::Level::ERROR : dbg::Level::DEBUG,
          std::chrono::seconds(1),
          1,
          "Received STARTED_Message(%s) from:%s (log_:%lu, id_:%lu"
          ", scd active: %s)",
          errorStrings()[msg.header_.status].name,
          from.toString().c_str(),
          log_id_.val(),
          id_.val(),
          scd_->isActive() ? "yes" : "no");

      // Persistent error, stop retrying.
      // This can happen either while in the process of establishing a
      // connection, in that case the STARTED message is a response to the
      // START message we just sent, or this can be a new STARTED message sent
      // while we are in READING state to indicate that the storage shard cannot
      // send anymore data. Either way, we do not try to reconnect to the shard
      // until the other end closes the socket.
      state.setConnectionState(ConnectionState::PERSISTENT_ERROR);
      state.resetReconnectTimer();
      state.resetRetryWindowTimer();

      if (scd_->isActive()) {
        // This shard cannot send us records right now. Add it to the known down
        // list and rewind the stream. Other shards will send the records it was
        // supposed to send. This is a no-op if the shard is already in the
        // known down list.
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::format("{} added to known down list because it sent STARTED "
                          "with E::FAILED",
                          state.getShardID().toString())
                .str());
      }

      // apply or re-apply shard authoritative status, in case we need to reset
      // overwritten value back to what is in the worker's authoritative
      // status map.
      applyShardStatus("onStarted", &state);
      // `this` may be destroyed after this call.
      return;

    case E::AGAIN:
    case E::NOTREADY:
    case E::SYSLIMIT:
      onConnectionFailure(state, status);
      break;
    case E::ACCESS:
      // The read stream is now in the permission denied state. It will be
      // unable to send data records or gap messages except a final
      // ACCESS gap record.
      permission_denied_ = true;
      deliverAccessGapAndDispose();

      // return so that disposeIfDone() is not called.
      return;
    default:
      RATELIMIT_ERROR(std::chrono::seconds(2),
                      2,
                      "got STARTED message from %s with unexpected status %s",
                      from.toString().c_str(),
                      error_description(status));
      onConnectionFailure(state, status);
  }

  disposeIfDone();
}

void ClientReadStream::onDataRecord(
    ShardID shard,
    std::unique_ptr<DataRecordOwnsPayload> record) {
  ld_check(!done());

  // There are several possible actions to take with the record:
  // (1) Ignore:
  // (1a) Record is a copy of one that we already delivered to the
  //      application.
  // (1b) Record came from a sender that we are not requesting records from.
  //      This could be from a server that we were previously reading from, or
  //      a spoofed message.
  // (2) Deliver: this is the exact record we need to deliver to the
  //     application next.
  // (3) Buffer: this is one of the next records we will deliver to the
  //     application.

  lsn_t lsn = record->attrs.lsn;

  ld_spew("Log=%lu,%s%s,%s from %s, next_lsn_to_deliver=%s",
          log_id_.val_,
          lsn_to_string(lsn).c_str(),
          (record->invalid_checksum_ ||
           (record->flags_ & RECORD_Header::UNDER_REPLICATED_REGION))
              ? "(UNDERREPLICATED)"
              : "",
          logdevice::toString(RecordTimestamp(record->attrs.timestamp)).c_str(),
          shard.toString().c_str(),
          lsn_to_string(next_lsn_to_deliver_).c_str());

  if (MetaDataLog::isMetaDataLog(log_id_)) {
    if (wait_for_all_copies_) {
      WORKER_STAT_INCR(client.metadata_log_records_received_wait_for_all);
    } else {
      WORKER_STAT_INCR(client.metadata_log_records_received);
    }
  } else {
    if (wait_for_all_copies_) {
      WORKER_STAT_INCR(client.records_received_wait_for_all);
    } else {
      WORKER_STAT_INCR(client.records_received);
      if (scd_ && scd_->isActive()) {
        WORKER_STAT_INCR(client.records_received_scd);
      } else {
        WORKER_STAT_INCR(client.records_received_noscd);
      }
    }
  }

  auto it = storage_set_states_.find(shard);
  if (it == storage_set_states_.end()) {
    // Ignore (1b)
    return;
  }

  if (permission_denied_) {
    // Ignore, invalid permissions
    return;
  }

  if (!canAcceptRecord(lsn)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    3,
                    "Got LSN %s from shard %s but can only accept up "
                    "to %s.  This should not be possible due to "
                    "flow control.",
                    lsn_to_string(lsn).c_str(),
                    shard.toString().c_str(),
                    lsn_to_string(window_high_).c_str());
    return; // Ignore
  }

  SenderState& sender_state = it->second;

  if (filter_version_.val_ != 1 &&
      sender_state.filter_version != filter_version_) {
    ld_debug("Rejecting record from %s for log %lu because of filter_version "
             "mismatch. Sender acknowledged version %lu but last version is "
             "%lu.",
             shard.toString().c_str(),
             log_id_.val_,
             sender_state.filter_version.val_,
             filter_version_.val_);
    return;
  }

  const AuthoritativeStatus auth_status = sender_state.getAuthoritativeStatus();
  if (auth_status != AuthoritativeStatus::UNAVAILABLE &&
      auth_status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    ld_debug("Shard %s is now fully authoritative for log %lu",
             shard.toString().c_str(),
             log_id_.val_);
    setShardAuthoritativeStatus(
        sender_state, AuthoritativeStatus::FULLY_AUTHORITATIVE);
    sendWindowMessage(sender_state);
    checkConsistency();
  }

  if (sender_state.getConnectionState() != ConnectionState::READING) {
    // We should always have gotten a STARTED before receiving data
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        5,
        "Received a record from %s while in unexpected state %s (%d)",
        shard.toString().c_str(),
        sender_state.getConnectionStateName(),
        static_cast<int>(sender_state.getConnectionState()));
  }

  if (lsn < sender_state.getNextLsn()) {
    // If we reconnect to the same storage shard, it might resend some of the
    // records it already delivered (since the beginning of the window is
    // next_lsn_to_deliver_, and not next_lsn; see #3368006). Ignore those
    // records.
    ld_debug("Rejecting record from %s for log %lu because of lsn %s < %s",
             shard.toString().c_str(),
             log_id_.val_,
             lsn_to_string(lsn).c_str(),
             lsn_to_string(sender_state.getNextLsn()).c_str());
    return;
  }

  if (record->invalid_checksum_ && !ship_corrupted_records_) {
    // issuing a gap instead of shipping a record with an invalid checksum
    GAP_Header gap_header = {log_id_,
                             this->getID(),
                             lsn,
                             lsn,
                             GapReason::CHECKSUM_FAIL,
                             (GAP_flags_t)0,
                             shard.shard()};
    onGap(shard, GAP_Message(gap_header, TrafficClass::READ_BACKLOG));
    return;
  }

  // Update state for the sender.
  bool under_replicated = record->invalid_checksum_ ||
      (record->flags_ & RECORD_Header::UNDER_REPLICATED_REGION);

  sender_state.max_data_record_lsn =
      std::max(sender_state.max_data_record_lsn, lsn);
  if (under_replicated) {
    // Flag UNDER_REPLICATED_REGION means that the shard may be missing some
    // records between this record and the previous reported LSN. Mark this
    // range as underreplicated for this shard. This record itself is clearly
    // not missing, so we don't have to mark its LSN as underreplicated for this
    // sender; but we do it anyway, because it's likely that the next gap will
    // be marked underreplicated again, and we don't want to transition (and
    // potentially rewind) from underreplicated to normal to underreplicated
    // again for just this one record.
    sender_state.under_replicated_until = std::max(
        sender_state.under_replicated_until, std::min(LSN_MAX - 1, lsn) + 1);
  }
  sender_state.grace_counter = 0;

  highest_record_lsn_ = std::max(highest_record_lsn_, lsn);

  // in reading mode other than ignore_released_status_, a received
  // record indicates that the record is released on the storage shard.
  if (!ignore_released_status_) {
    updateLastReleased(lsn);
  }

  // Now decide what to do with the record.
  if (lsn < next_lsn_to_deliver_) {
    // Ignore (1a)
    ld_debug("Discarded record from %s for log %lu with lsn %s < "
             "next_lsn_to_deliver_ %s",
             shard.toString().c_str(),
             log_id_.val_,
             lsn_to_string(lsn).c_str(),
             lsn_to_string(next_lsn_to_deliver_).c_str());

    // Update sender's next LSN even if it's below the window.
    // This is used for a paranoid protocol check: if this sender sends a gap
    // next, we expect its start LSN to be equal to this record's LSN + 1.
    updateGapState(lsn + 1, sender_state);
  } else {
    // Buffer (3) and maybe deliver (2)

    deps_->recordCopyCallback(shard, record.get());

    // buffer must be aligned
    ld_check(next_lsn_to_deliver_ == buffer_->getBufferHead());
    // Update the RecordState for the lsn with the record received
    RecordState* rstate = buffer_->createOrGet(lsn);
    // we already checked that lsn can fit into the buffer
    ld_check(rstate != nullptr);

    read_tracer_->traceRecordDelivery(record->logid,
                                      record->attrs.lsn,
                                      deps_->getReadStreamID(),
                                      sender_state.getShardID().asNodeID(),
                                      epoch_metadata_request_latency_,
                                      logid_request_latency_,
                                      record->payload.size(),
                                      record->attrs.timestamp,
                                      start_lsn_,
                                      until_lsn_,
                                      epoch_metadata_str_factory_,
                                      unavailable_shards_str_factory_,
                                      currentEpoch(),
                                      trim_point_,
                                      readSetSize());

    if (!rstate->record) {
      rstate->record = std::move(record);
      // Updating info reg. buffer usage.
      bytes_buffered_ += rstate->record->payload.size();
    }
    // This shard won't send us anything before `lsn'+1.
    // Use that information for gap detection.
    updateGapState(lsn < LSN_MAX ? lsn + 1 : LSN_MAX, sender_state);
    findGapsAndRecords();
  }

  // This may be a shard that was blacklisted, in that case a rewind will be
  // scheduled.
  if (scd_->isActive()) {
    scd_->scheduleRewindIfShardBackUp(sender_state);
  }

  // This function should leave everything in a consistent state.
  checkConsistency();

  disposeIfDone();
}

void ClientReadStream::onGap(ShardID shard, const GAP_Message& msg) {
  ld_check(!done());
  auto& gap = msg.getHeader();

  ld_spew("%s from %s", gap.identify().c_str(), shard.toString().c_str());

  auto it = storage_set_states_.find(shard);
  if (it == storage_set_states_.end()) {
    // this record is from a sender we're not requesting records from, see (1b)
    // in onDataRecord()
    return;
  }

  if (permission_denied_) {
    // Ignore, invalid permissions
    return;
  }

  SenderState& sender_state = it->second;

  // reset sender's grace counter
  sender_state.grace_counter = 0;

  if (filter_version_.val_ != 1 &&
      sender_state.filter_version != filter_version_) {
    ld_debug("Rejecting gap [%s, %s] from %s for log %lu because of "
             "filter_version mismatch. Sender acknowledged version %lu but "
             "last version is %lu.",
             lsn_to_string(gap.start_lsn).c_str(),
             lsn_to_string(gap.end_lsn).c_str(),
             shard.toString().c_str(),
             log_id_.val_,
             sender_state.filter_version.val_,
             filter_version_.val_);
    return;
  }

  // Paranoid check: this gap should start where the previous gap/record ended.
  // Exception: the sender may fast forward to window's low end; the
  // sender's view of the window may be a little stale, so
  // anything <= server_window_.low is valid.
  if (gap.start_lsn > std::max(sender_state.getNextLsn(), server_window_.low)) {
    RATELIMIT_ERROR(std::chrono::seconds(10),
                    2,
                    "Got gap %s from %s with unexpected start LSN; sender's "
                    "next LSN: %s, window: [%s, %s]",
                    gap.identify().c_str(),
                    shard.toString().c_str(),
                    lsn_to_string(sender_state.getNextLsn()).c_str(),
                    lsn_to_string(server_window_.low).c_str(),
                    lsn_to_string(server_window_.high).c_str());
    // Let's process the gap anyway.
  }

  const AuthoritativeStatus auth_status = sender_state.getAuthoritativeStatus();
  if (auth_status != AuthoritativeStatus::UNAVAILABLE &&
      auth_status != AuthoritativeStatus::FULLY_AUTHORITATIVE) {
    ld_debug("Shard %s is now fully authoritative for log %lu",
             shard.toString().c_str(),
             log_id_.val_);
    setShardAuthoritativeStatus(
        sender_state, AuthoritativeStatus::FULLY_AUTHORITATIVE);
    sendWindowMessage(sender_state);
    checkConsistency();
  }

  bool under_replicated = false;
  bool filtered_out = false;
  switch (gap.reason) {
    case GapReason::TRIM:
      trim_point_ = std::max(trim_point_, gap.end_lsn);
      break;
    case GapReason::UNDER_REPLICATED:
    case GapReason::CHECKSUM_FAIL:
      under_replicated = true;
      break;
    case GapReason::NO_RECORDS:
      break;
    case GapReason::FILTERED_OUT:
      filtered_out = true;
      break;
    default:
      RATELIMIT_WARNING(
          std::chrono::seconds(10),
          3,
          "GapReason provided is not defined. Unexpected gap value: %d"
          "log_id: %lu, Shard: %s ",
          static_cast<int>(gap.reason),
          log_id_.val_,
          shard.toString().c_str());
  }

  if (scd_ && scd_->isActive() && gap.reason == GapReason::CHECKSUM_FAIL) {
    // If we got checksum error, add the shard to known down and rewind.
    // Unlike UNDER_REPLICATED, it's unlikely that we can make progress without
    // rewind, so let's rewind right away instead of waiting for gap detection.
    if (scd_->addToShardsDownAndScheduleRewind(
            sender_state.getShardID(),
            folly::sformat("{} added to known down list because it checksum "
                           "error for lsn {}",
                           sender_state.getShardID().toString(),
                           lsn_to_string(gap.start_lsn).c_str()))) {
      checkConsistency();
      return;
    } else {
      // Already in known down or all send all.
      // Treat this missing records just like UNDER_REPLICATED gap.
    }
  }

  if (!ignore_released_status_) {
    updateLastReleased(gap.end_lsn);
  }

  // If this is a filtered_out gap, we need to set up GapState. Currently, we
  // treat gap message FILTERED_OUT very much like record. The only
  // difference is that we deliver a FILTERED_OUT gap instead of a record
  // message to application.
  // If fail, we return because we cannot set up gap state
  if (filtered_out &&
      setGapStateFilteredOut(gap.start_lsn, gap.end_lsn, it->second) != 0) {
    return;
  }

  if (under_replicated) {
    sender_state.under_replicated_until =
        std::max(sender_state.under_replicated_until,
                 std::min(LSN_MAX - 1, gap.end_lsn) + 1);
  }

  updateGapState(gap.end_lsn < LSN_MAX ? gap.end_lsn + 1 : LSN_MAX, it->second);

  findGapsAndRecords();

  // This function should leave everything in a consistent state.
  checkConsistency();

  disposeIfDone();
}

void ClientReadStream::updateGapState(lsn_t next_lsn, SenderState& state) {
  if (next_lsn < state.getNextLsn()) {
    // Storage shard already sent a record/gap for this LSN.
    return;
  }

  SCOPE_EXIT {
    state.setNextLsn(next_lsn);
    scd_->onShardNextLsnChanged(state.getShardID(), next_lsn);
  };

  if (next_lsn <= next_lsn_to_deliver_) {
    // Storage shard sent a gap message, but we already delivered (or are about
    // to) those records? Nothing to do in this case.
    return;
  }

  /**
   * Gap detection algorithm uses the following pieces of information:
   *   - set S_G of storage shards such that the smallest record each shard in
   *     S_G can deliver is strictly larger than next_lsn_to_deliver; i.e.
   *
   *        S_G := { s \in S : s.getNextLsn() > next_lsn_to_deliver_ }
   *
   *   - smallest LSN L > next_lsn_to_deliver_ such that at least one of the
   *     shards in S_G has a record with that LSN or had sent a gap message
   *     L-1 being the right endpoint of the gap
   *
   * Considering the replication factor, synchronous replication scope of
   * the reading epoch, as well as failure domain information for storage shards
   * participated in reading, if S_G is a f-majority subset of the current read
   * set, we can conclude that such record doesn't exist (or happens to be
   * under-replicated, but the system tries hard to avoid these cases), and
   * report a gap [next_lsn_to_deliver_, L-1]. In order to avoid eagerly
   * reporting data loss when a record is just under-replicated, a grace period
   * is used to allow other shards to deliver the record and save the day.
   * Therefore, depending on the size of |S_G|, one of the following will
   * happen:
   *
   *   a) if |S_G| = |S|: a gap is reported immediately
   *
   *   b) if |S_G| is a f-majority subset: the grace period timer is activated;
   *      if it expires before a record is delivered, a gap is reported
   *
   *   c) if |S_G| is not a f-majority subset: we don't have enough information
   *      to conclude that a record may be missing and will wait for more shards
   *      to respond
   *
   * Size of S_G is maintained throughout the lifetime of the read stream. All
   * methods must maintain the correct GapState for storage shards as some
   * records get delivered to the application and the buffer head gets
   * advanced. In order to keep track of these GapState_s in constant time, the
   * following is used:
   *
   *   - next_lsn, a smallest lsn that was not seen (either by receiving a
   *     record or a gap message), for each shard in the storage set
   *   - at each gap marker in the buffer (i.e. for each gap marker in
   *     [next_lsn_to_deliver_ + 1, window_high_]) a special linked list is
   *     stored containing those storage shards that delivered the record (or a
   *     gap) with LSN next_lsn_to_deliver_ + i + 1; this is an instrusive list
   *     containing ClientReadStreamSenderState objects (one sender state can
   *     be only in one list!)
   *
   * This method adds state to the list in buffer_ corresponding to next_lsn,
   * if the state already isn't in the list for some lsn > next_lsn_to_deliver_
   * (and fits inside the window). See also clearRecordState() which moves
   * shards from one list to another.
   */

  GapState prev_gap_state = state.getGapState();
  bool under_replicated = next_lsn_to_deliver_ < state.under_replicated_until;
  setSenderGapState(
      state, under_replicated ? GapState::UNDER_REPLICATED : GapState::GAP);
  ld_check(state.under_replicated_until <= next_lsn);

  // The next LSN at which GapState may change:
  // either from UNDER_REPLICATED to GAP or from UNDER_REPLICATED/GAP to NONE.
  lsn_t transition_lsn =
      under_replicated ? state.under_replicated_until : next_lsn;

  if (next_lsn > window_high_) {
    // next_lsn is outside of the window, adjust gap_end_outside_window_
    // if needed
    if (gap_end_outside_window_ == LSN_INVALID ||
        next_lsn < gap_end_outside_window_) {
      gap_end_outside_window_ = next_lsn;
    }
  }

  if (transition_lsn <= window_high_) {
    // transition_lsn falls inside the window. If this storage shard is not
    // already in a linked list for some lsn > next_lsn_to_deliver_, add it now.
    // Do this only if this storage shard was not already accounted for
    // GapState::GAP/UNDER_REPLICATED (i.e. it reported a gap with LSN outside
    // of the window first).
    RecordState* rstate = buffer_->createOrGet(transition_lsn);
    // transition_lsn must fit in the buffer
    ld_check(rstate != nullptr);

    if (prev_gap_state == GapState::NONE) {
      rstate->list.push_back(state);
    }

    rstate->gap = true;
  }
}

/**
 * StorageSet and gap detection
 *
 * The logic for gap detection with storage set (and epoch metadata) is quite
 * straight forward: whenever the client read stream sees a potential gap
 * whose left or right end belongs to an epoch _e_ > last_epoch_with_metadata_,
 * it should not immediately deliver the gap but rather start to request
 * epoch metadata for the first epoch with unknown metadata
 * (e.g., last_epoch_with_metadata_ + 1). Once the metadata is fetched, the
 * read stream can resume gap detection and deliver gaps whose both ends
 * are in epochs with known metadata.
 *
 * The reason why the read stream needs to fetch metadata for an epoch after
 * it sees a gap to the epoch is to ensure correctness. See the doc block
 * in MetaDataLogReader for an explanation.
 *
 * In order to effectively detect the end of an epoch, we need to have storage
 * shards explicitly send a gap to a higher epoch when it finishes delivering
 * all records that belong to an epoch in its log store and is sure that no new
 * records will be appended to the epoch, even in case that the shard does not
 * belong to the storage set of the new epoch. The storage shard can infer such
 * condition when it receives a RELEASE message releasing a lsn in a higer
 * epoch.
 *
 * Therefore, it is important to ensure that all storage shards (or at least
 * storage shards that ever participated in storing the log) eventually get
 * all RELEASEs. Currently, it is achieved by PeriodicalReleases, which
 * guarantees that RELEASE messages are eventually delivered to all shards in
 * the cluster.
 */
void ClientReadStream::findGapsAndRecords(bool grace_period_expired,
                                          bool redelivery_in_progress) {
  if (done()) {
    // The stream may be in an inconsistent state so better to return early.
    return;
  }

  if (permission_denied_) {
    return;
  }

  // the read stream must at least have an initial read set when this
  // function gets called
  ld_check(last_epoch_with_metadata_ != EPOCH_INVALID);
  ld_check(current_metadata_ != nullptr);

  // we must have constructed failure domain information at this point
  ld_check(gap_failure_domain_ != nullptr);

  // Look at front of the buffer and see what we should do.
  // Returns true if some progress was made.
  auto try_make_progress = [&] {
    if (next_lsn_to_deliver_ > window_high_) {
      return false;
    }
    if (redelivery_timer_ != nullptr && redelivery_timer_->isActive()) {
      // There is a redelivery timer already pending, meaning we recently failed
      // to deliver and should not keep trying too eagerly.
      return false;
    }

    RecordState* rstate = buffer_->front();

    if (canSkipPartiallyTrimmedSection()) {
      // If a redelivery is in progress and we have a buffered record at the
      // from of the buffer, we cannot deliver a TRIM gap and fast forward the
      // read stream. The reason is that the application has already seen that
      // and may be expecting to receive it later. In particular, the record may
      // be a BufferedWriter batch and may have been partially delivered to the
      // application. If we deliver a TRIM gap here the records will end up out
      // - of - oder.
      if (!redelivery_in_progress || rstate == nullptr ||
          rstate->record == nullptr) {
        lsn_t gap_lsn = std::min(trim_point_, LSN_MAX - 1) + 1;
        if (deliverFastForwardGap(GapType::TRIM, gap_lsn) != 0) {
          return false;
        }

        // return immediately here as the gap is delivered, if we deliver
        // a gap to the outside of the window, the next iterator of
        // try_make_progress() will stop and call slideSenderWindows() which
        // is responsible for clearing gap state for each shard and updating
        // gap parameters such as gap_end_outside_window_.
        return true;
      }
    }

    if (rstate != nullptr) {
      if (rstate->record != nullptr || rstate->filtered_out) {
        // There's a record or FILTERED_OUT gap to ship.
        if (handleRecord(rstate, grace_period_expired, rstate->filtered_out) !=
            0) {
          return false;
        }
      } else {
        // No record - must be a gap marker.
        ld_check(rstate->gap);
        // A gap marker in the front slot is useless.  Since it is exclusive, at
        // this point it means that some shard says it has nothing in the empty
        // interval [next_lsn_to_deliver_, next_lsn_to_deliver_).  See
        // ClientReadStreamTest.MootGapThenProperGap for an example of how this
        // can happen.
        clearRecordState(buffer_->getBufferHead(), *rstate);
        buffer_->popFront();
      }
    } else {
      // the front slot of the buffer is empty: the read stream has not yet
      // gotten any for next_lsn_to_deliver_, see if it can detect a gap to
      // make progress

      const lsn_t current_epoch_begin = checkEpochBegin();
      if (current_epoch_begin != LSN_INVALID) {
        // if the read stream is at the beginning of an epoch, first search
        // to see if it is possible to deliver a bridge gap of epoch begin
        // immediately based on what have already been received in the buffer
        ld_check(lsn_to_epoch(current_epoch_begin) == currentEpoch());
        if (handleEpochBegin(current_epoch_begin) != 0) {
          return false;
        }
      } else {
        // run the f-majority based gap detection algorithm to try to find and
        // deliver potential gaps
        if (detectGap(grace_period_expired) != 0) {
          return false;
        }
      }
    }

    // If detectGap() or handleRecord() started redelivery timer, it wouldn't
    // have returned 0.
    ld_check(redelivery_timer_ == nullptr || !redelivery_timer_->isActive());

    // At this point we have delivered either a record or a gap, the redelivery
    // is no longer in progress. Unset that boolean so it is not considered in
    // the next iteration.
    redelivery_in_progress = false;
    return true;
  };

  while (!done()) {
    while (try_make_progress()) {
      if (grace_period_ != nullptr) {
        // If we are here, we made progress. This means that any pending gap
        // grace period timer needs to be reset.
        grace_period_->cancel();
      }
    }
    if (!slideSenderWindows()) {
      break;
    }
  }

  if (scd_->isActive()) {
    scd_->checkNeedsFailoverToAllSendAll();
  }
}

lsn_t ClientReadStream::checkEpochBegin() const {
  if (lsn_to_esn(next_lsn_to_deliver_) != ESN_INVALID &&
      next_lsn_to_deliver_ != start_lsn_) {
    // only search for the epoch begin record in the following situations:
    // 1) next_lsn_to_deliver_ is at ESN_INVALID, this implies that the read
    //    stream may have just delivered a bridge record in the previous epoch.
    // 2) the read stream just started reading so that
    //    next_lsn_to_deliver_ == start_lsn_
    return LSN_INVALID;
  }

  if (highest_record_lsn_ < next_lsn_to_deliver_) {
    // we haven't received any record beyond next_lsn_to_deliver_ yet, no need
    // to search further
    return LSN_INVALID;
  }

  // peform a search on the buffer to find the first non-empty buffer slot
  std::pair<RecordState*, lsn_t> result = buffer_->findFirstMarker();
  if (result.first == nullptr) {
    // nothing found
    return LSN_INVALID;
  }

  ld_check(result.first->gap || result.first->record ||
           result.first->filtered_out);
  if (result.first->record == nullptr && !result.first->filtered_out) {
    // the first non-empty slot is not a record, let alone epoch begin
    return LSN_INVALID;
  }

  const lsn_t first_record = result.second;
  if (lsn_to_epoch(first_record) != currentEpoch()) {
    // the first record is in a different epoch
    return LSN_INVALID;
  }

  if (lsn_to_esn(first_record) == ESN_MIN ||
      (result.first->record &&
       result.first->record->flags_ & RECORD_Header::EPOCH_BEGIN)) {
    // To be considered as epoch begin, the first record in the buffer is
    // either:
    // 1) located at ESN_MIN (first possible valid record in an epoch)
    // 2) a record that has the EPOCH_BEGIN flag
    ld_debug("Epoch begin detected at %s for log %lu",
             lsn_to_string(first_record).c_str(),
             log_id_.val_);

    return first_record;
  }

  return LSN_INVALID;
}

int ClientReadStream::handleEpochBegin(lsn_t epoch_begin) {
  ld_check(epoch_begin > next_lsn_to_deliver_);

  if (currentEpoch() > last_epoch_with_metadata_) {
    // Just like gap detection, do not deliver anything in an epoch whose
    // metadata is unknown, request its metadata and wait for the result
    // to come back
    requestEpochMetaData(epoch_t(last_epoch_with_metadata_.val_ + 1));
    return -1;
  }

  if (deliverGap(GapType::BRIDGE, next_lsn_to_deliver_, epoch_begin - 1) != 0) {
    ld_debug("failed to deliver a epoch begin bridge gap [%lu, %lu]. ",
             next_lsn_to_deliver_,
             epoch_begin - 1);
    return -1;
  }

  // advanceBufferHead() asserts that there must be no record or gap marker
  // in the slots getting advanced. This is ensured since the epoch begin lsn
  // was computed by search for the first marker in the buffer.
  buffer_->advanceBufferHead(epoch_begin - next_lsn_to_deliver_);
  ld_check(buffer_->getBufferHead() == epoch_begin);
  next_lsn_to_deliver_ = epoch_begin;
  return 0;
}

ClientReadStream::ProgressDecision
ClientReadStream::checkFMajority(bool grace_period_expired) const {
  if (rewind_scheduler_->isScheduled()) {
    // We do not allow running the gap detection algorithm while a rewind is
    // scheduled as that rewind may have been scheduled because the
    // authoritative status of a node changed, and issuing a gap right now is
    // not fully correct. An example is the transition to AUTHORITATIVE_EMPTY
    // which happens when rebuilding completes. If we run the gap-detection
    // algorithm right now we will miss some records that are not in the buffer
    // because they were rebuilt after we received them.
    // See T16944847.
    return ProgressDecision::WAIT;
  }

  auto fmajority_result = gap_failure_domain_->isFmajority(GapState::GAP);

  // We can make progress if we have results from all nodes that are expected
  // to be online and providing information. Include gap results from nodes
  // with under replicated regions in the count of respondents.
  if (fmajority_result == FmajorityResult::NONE ||
      fmajority_result == FmajorityResult::AUTHORITATIVE_INCOMPLETE) {
    const auto under_replicated_fmajority_result =
        gap_failure_domain_->isFmajority([](const GapState& gs) {
          // GAP or UNDER_REPLICATED
          return gs != GapState::NONE;
        });
    switch (under_replicated_fmajority_result) {
      case FmajorityResult::AUTHORITATIVE_COMPLETE:
      case FmajorityResult::NON_AUTHORITATIVE:
        if (fmajority_result == FmajorityResult::AUTHORITATIVE_INCOMPLETE) {
          // An f-majority exists without couting under-replicated nodes.
          // Consider this a complete f-majority since all nodes that can
          // respond have responded.
          fmajority_result = FmajorityResult::AUTHORITATIVE_COMPLETE;
        } else {
          // An f-majority confirming no-record exists at this lsn isn't
          // possible. All nodes that can reply have replied, but too many
          // of these nodes have indicated they may be missing data. The
          // result is complete, but non-authoritative. This is the expected
          // result when more than R nodes experience a failure (localized
          // media defect, loss of full shard, or a crash with in-flight data
          // not yet committed to stable storage).
          ld_check(fmajority_result == FmajorityResult::NONE);
          fmajority_result = FmajorityResult::NON_AUTHORITATIVE;
        }
        break;
      case FmajorityResult::NONE:
      case FmajorityResult::AUTHORITATIVE_INCOMPLETE:
        // Including under replicated nodes doesn't improve things.
        break;
    }
  }

  // If SCD is active, and if the user allows issuing gap while in SCD mode,
  // issue a gap if we have a complete majority, ie all fully authoritative
  // nodes chimed in. Note that when we issue gaps while in SCD, we may miss
  // some records that are underreplicated because of a bug or have inconsistent
  // copysets. This is tracked in T16277257. Issuing gaps while in SCD mode may
  // still be desirable for use cases that are constrained in network bandwidth
  // and can't afford to do too many rewinds in ALL SEND ALL mode when there is
  // data loss.
  if (scd_->isActive()) {
    if (fmajority_result == FmajorityResult::NON_AUTHORITATIVE ||
        fmajority_result == FmajorityResult::AUTHORITATIVE_COMPLETE) {
      if (scd_->getUnderReplicatedShardsNotBlacklisted() != 0) {
        // Some nodes reported underreplication, but we optimistically didn't
        // add them to known down list, in hopes that we'll get through the
        // underreplicated region without encountering gaps. Now we didn
        // encounter a gap and have to add the underreplicated shards to known
        // down list and rewind.
        return ProgressDecision::ADD_TO_KNOWN_DOWN_AND_REWIND;
      }
      if (deps_->getSettings().read_stream_guaranteed_delivery_efficiency) {
        // We've heard from all shards and no one had a record for current LSN
        // that would pass SCD filter. Normally we would rewind to all send all
        // mode in this situation, in case the record is underreplicated or has
        // inconsistent copyset. But in
        // read_stream_guaranteed_delivery_efficiency we just ship the gap
        // because rewinds are slow.
        return ProgressDecision::ISSUE_GAP;
      }
    }
    return ProgressDecision::WAIT;
  }

  if (fmajority_result == FmajorityResult::NONE) {
    // Not enough shards chimed in for us to make any gap decision.
    return ProgressDecision::WAIT;
  }

  // If requireFullReadSet() was called, only report a gap if all shards
  // that are available chimed in. This means either all non empty shards chimed
  // in (AUTHORITATIVE_COMPLETE), or all non empty and non rebuilding shards
  // chimed in (NON_AUTHORITATIVE).
  if (require_full_read_set_ &&
      fmajority_result == FmajorityResult::AUTHORITATIVE_INCOMPLETE) {
    return ProgressDecision::WAIT;
  }

  if (shouldWaitForGracePeriod(fmajority_result, grace_period_expired)) {
    // Let's wait a bit more to give a chance for more shards to chime in (and
    // possibly send records) instead of reporting the gap immediately.
    return ProgressDecision::WAIT_FOR_GRACE_PERIOD;
  }

  if (fmajority_result == FmajorityResult::NON_AUTHORITATIVE) {
    RATELIMIT_WARNING(
        std::chrono::seconds(10),
        1,
        "%lu shards are rebuilding for log %lu. There may "
        "have been records that were replicated on these shards, "
        "such records may be lost. Unavailable shards: %s",
        numShardsInAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION) +
            numShardsInState(GapState::UNDER_REPLICATED),
        log_id_.val_,
        unavailableShardsPretty().c_str());
  }

  return ProgressDecision::ISSUE_GAP;
}

int ClientReadStream::detectGap(bool grace_period_expired) {
  const ProgressDecision decision = checkFMajority(grace_period_expired);

  if (grace_period_ && decision != ProgressDecision::WAIT_FOR_GRACE_PERIOD) {
    // If we are here and gap grace period is active, then f-majority result
    // changed from our past call to detectGap() and we need to cancel it.
    grace_period_->cancel();
  }

  if (decision == ProgressDecision::WAIT) {
    // Not enough shards chimed in, we should wait.
    return -1;
  }

  if (decision == ProgressDecision::ADD_TO_KNOWN_DOWN_AND_REWIND) {
    promoteUnderreplicatedShardsToKnownDown();
    return -1;
  }

  const epoch_t current_epoch = currentEpoch();

  // We encountered a gap whose left end belongs to an epoch whose
  // metadata is unknown. This can happen when:
  // (1) the readstream delivered a record with the last esn (ESN_MAX) of the
  //     previous epoch, and then received a gap/record on the new epoch
  // (2) reading started with an epoch whose metadata is unknown.
  // In either case, we need to figure out the epoch metadata before
  // performing any gap detection in the new epoch.
  // We need to request the metadata only after some record in the new epoch
  // gets released: the metadata log record is guaranteed to be available then.
  // Since the f-majority check above have passed, we know that at least one
  // shard has sent us a gap ending in the new epoch, which means that at least
  // one record is released in the new epoch.
  if (current_epoch > last_epoch_with_metadata_) {
    ld_check(current_epoch.val_ == last_epoch_with_metadata_.val_ + 1);
    requestEpochMetaData(current_epoch);
    return -1;
  }

  // At this point, enough shards told us they don't have the record with LSN
  // next_lsn_to_deliver_. We need to report a potential gap at least for that
  // LSN. Perform a search from the beginning of the buffer to see how far
  // the gap extends (through the smallest LSN with a record or gap point).

  lsn_t gap = gap_end_outside_window_;

  std::pair<RecordState*, lsn_t> result = buffer_->findFirstMarker();

  if (result.first != nullptr) {
    ld_check(result.first->gap || result.first->record ||
             result.first->filtered_out);
    ld_check(result.second > next_lsn_to_deliver_);
    gap = result.second;
  }

  // If trim point is inside gap, split the gap in two: trim gap and data loss.
  // Handle the trim gap here and leave data loss gap for the next call.
  if (trim_point_ < LSN_MAX && trim_point_ >= next_lsn_to_deliver_ &&
      trim_point_ + 1 < gap) {
    gap = trim_point_ + 1;
  }

  if (gap == LSN_INVALID) {
    // At least an f-majority of shards are empty.
    if (ignore_released_status_) {
      // We don't care about releases. Ship gap up to until_lsn.
      gap = until_lsn_ < LSN_MAX ? until_lsn_ + 1 : LSN_MAX;
    } else {
      // Wait for at least one shard to send us something. Otherwise we don't
      // have a lower bound on last released LSN, so can't ship anything.
      return -1;
    }
  }

  if (decision == ProgressDecision::WAIT_FOR_GRACE_PERIOD &&
      (next_lsn_to_deliver_ == start_lsn_ ||
       determineGapType(gap) != GapType::BRIDGE)) {
    // Let's give more time for all shards to chime in in case there is
    // underreplicated data.
    // NOTE: Not waiting for BRIDGE gaps may skip under-replicated records at
    // the end or beginning of an epoch. See T12057764.
    grace_period_->activate();
    return -1;
  }

  const epoch_t gap_epoch = lsn_to_epoch(gap);
  // A potential gap is found with a right end that belongs to an
  // epoch whose metadata is unknown. We know that there will be no more
  // records in the current epoch, but cannot confirm what is the right end
  // beyond currentEpoch(). The read stream needs to request the epoch
  // metadata for the next epoch with unknown metadata
  if (gap_epoch > last_epoch_with_metadata_) {
    // +1 here assumes that there must be at least one gap per each epoch
    // this holds true for logdevice, since:
    // (1) ESN_INVALID (0) is not an valid esn
    // (2) sequencer does not exhaust all esns at the end of the epoch
    //     before switching to a new epoch
    requestEpochMetaData(epoch_t(last_epoch_with_metadata_.val_ + 1));
    return -1;
  }

  ld_check(gap != LSN_INVALID);
  return handleGap(gap);
}

GapType ClientReadStream::determineGapType(lsn_t gap_lsn) {
  if (readSetSize() > 0 && numShardsNotFullyAuthoritative() == readSetSize()) {
    // All shards are rebuilding or empty, skipping everything with one gap.
    return GapType::DATALOSS;
  }

  // Always consider it as a trim gap if the right endpoint is no larger
  // than trim_point_
  if (trim_point_ == LSN_MAX || gap_lsn <= trim_point_ + 1) {
    return GapType::TRIM;
  }

  // Consider the gap that meets the following conditions as BRIDGE gap instead
  // or DATALOSS. This is currently a workaround to avoid false dataloss
  // reporting at the beginning of an epoch.
  // 1. gap_lsn is within [0, sequencer_window_size_]
  // 2. the read stream has not yet delivered anything in the epoch of
  //    gap_lsn
  // TODO 7467469: Update this once sequencers stop starting epochs at ESN > 1,
  //               to detect data loss at start of epoch.
  if (lsn_to_esn(gap_lsn).val_ <= sequencer_window_size_ &&
      (next_lsn_to_deliver_ == start_lsn_ || // nothing delivered yet
       lsn_to_epoch(next_lsn_to_deliver_ - 1) < lsn_to_epoch(gap_lsn))) {
    // note: next_lsn_to_deliver_ - 1 should not overflow since if it is
    // LSN_INVALID, it must be the same as start_lsn_
    return GapType::BRIDGE;
  }

  // Treat the gap as an epoch bump if gap_lsn belongs to a different epoch
  // since storage shards are able to provide last ESN of the epoch through
  // bridge records to detect data loss at end of epoch.

  return lsn_to_epoch(gap_lsn) == lsn_to_epoch(next_lsn_to_deliver_)
      ? GapType::DATALOSS
      : GapType::BRIDGE;
}

bool ClientReadStream::shouldRewindWhenDataLoss() {
  if (log_uses_scd_ &&
      !deps_->getSettings().read_stream_guaranteed_delivery_efficiency &&
      !wait_for_all_copies_) {
    // When SCD is in use, there is a possible race condition that causes an
    // f-majority of storage shards to not send a record. If the remaining
    // storage shard is slow, we can end up here. If this is the first time we
    // are seeing data loss for a gap with this lower bound, rewind the streams
    // to give storage shards another chance. See T5849538.
    if (last_dataloss_gap_retried_ != next_lsn_to_deliver_) {
      ld_check(last_dataloss_gap_retried_ < next_lsn_to_deliver_);
      last_dataloss_gap_retried_ = next_lsn_to_deliver_;
      return true;
    }
  }
  return false;
}

void ClientReadStream::promoteUnderreplicatedShardsToKnownDown() {
  bool added = false;
  for (auto& it : storage_set_states_) {
    SenderState& state = it.second;
    if (state.getGapState() == GapState::UNDER_REPLICATED &&
        !state.should_blacklist_as_under_replicated) {
      state.should_blacklist_as_under_replicated = true;
      added |= scd_->addToShardsDownAndScheduleRewind(
          state.getShardID(),
          folly::sformat("{} added to known down list because it has an "
                         "under replicated region",
                         state.getShardID().toString()));
      ;
    }
  }
  ld_check(added);

  checkConsistency();
}

int ClientReadStream::handleGap(lsn_t gap_lsn) {
  // assert that the read stream must have the epoch metadata for both end of
  // the gap before delivering it
  ld_check(last_epoch_with_metadata_ >= currentEpoch());
  ld_check(last_epoch_with_metadata_ >= lsn_to_epoch(gap_lsn - 1));
  ld_check(next_lsn_to_deliver_ < gap_lsn);

  GapType type = determineGapType(gap_lsn);
  if (type == GapType::BRIDGE) {
    ld_debug("Epoch bump detected for log %lu: new epoch is %lu",
             log_id_.val_,
             static_cast<uint64_t>(lsn_to_epoch(gap_lsn).val_));
  } else if (type == GapType::DATALOSS && shouldRewindWhenDataLoss()) {
    RATELIMIT_WARNING(std::chrono::seconds(1),
                      1,
                      "Rewinding following dataloss in range [%s, %s] "
                      "in log %lu. %s",
                      lsn_to_string(next_lsn_to_deliver_).c_str(),
                      lsn_to_string(gap_lsn).c_str(),
                      log_id_.val_,
                      getDebugInfoStr().c_str());
    rewind(folly::format("DATALOSS [{}, {}]",
                         lsn_to_string(next_lsn_to_deliver_).c_str(),
                         lsn_to_string(gap_lsn).c_str())
               .str());
    WORKER_STAT_INCR(client.read_streams_rewinds_when_dataloss);
    return 0;
  }

  lsn_t hi = gap_lsn - 1;
  if (type == GapType::TRIM && trim_point_ >= LSN_MAX) {
    // special case to avoid overflows when dealing with logs trimmed up to
    // LSN_MAX
    hi = trim_point_;
  }

  int rv = deliverGap(type, next_lsn_to_deliver_, std::min(until_lsn_, hi));
  if (rv != 0) {
    return rv;
  }

  // Advance buffer head so that `gap_lsn' becomes the front slot of the
  // buffer.  There should be no records/gaps inside the buffer for the lsn
  // range of the gap; advanceBufferHead() will assert that.
  buffer_->advanceBufferHead(gap_lsn - next_lsn_to_deliver_);
  ld_check(buffer_->getBufferHead() == gap_lsn);
  next_lsn_to_deliver_ = gap_lsn;

  return 0;
}

void ClientReadStream::onReaderProgress() {
  ld_check(!done());
  if (permission_denied_) {
    return;
  }

  if (!window_update_pending_) {
    return;
  }

  updateServerWindow();
  scd_->onWindowSlid(server_window_.high, filter_version_);

  for (auto& it : storage_set_states_) {
    sendWindowMessage(it.second);
  }

  window_update_pending_ = false;
  if (readers_flow_tracer_) {
    readers_flow_tracer_->onWindowUpdateSent();
  }

  disposeIfDone();
}

void ClientReadStream::updateScdStatus() {
  std::shared_ptr<Configuration> config = config_->get();
  read_stream_id_t rsid = this->getID();
  auto get_stream_by_id = deps_->getStreamByIDCallback();
  config->getLogGroupByIDAsync(
      log_id_,
      [rsid, get_stream_by_id](
          const std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
        auto ptr = get_stream_by_id(rsid);
        if (!ptr) {
          ld_warning("Got reply for ClientReadStream %lu that doesn't exist "
                     "anymore.",
                     rsid.val_);
          return;
        }
        ptr->updateScdStatusContinuation(log_config);
      });
}

void ClientReadStream::updateScdStatusContinuation(
    const std::shared_ptr<LogsConfig::LogGroupNode> log_config) {
  if (!log_config) {
    deliverNoConfigGapAndDispose();
    return;
  }

  auto attrs = log_config->attrs();
  bool scd_enabled = attrs.scdEnabled().hasValue() && *attrs.scdEnabled();
  bool local_scd_enabled =
      attrs.localScdEnabled().hasValue() && *attrs.localScdEnabled();

  if (!scd_->isActive()) {
    // If we just switched from not using scd to using it, and the user
    // allows the scd optimization, switch to scd mode now.
    if (!log_uses_scd_ && scd_enabled && !force_no_scd_) {
      // If the user allows local scd, switch to local scd mode.
      if (local_scd_enabled) {
        ld_debug("Switching to local scd for log:%lu, id:%lu",
                 log_id_.val(),
                 id_.val());
        scd_->scheduleRewindToMode(ClientReadStreamScd::Mode::LOCAL_SCD,
                                   "Switching to LOCAL_SCD: config changed");
      } else {
        ld_debug(
            "Switching to scd for log:%lu, id:%lu", log_id_.val(), id_.val());
        scd_->scheduleRewindToMode(
            ClientReadStreamScd::Mode::SCD, "Switching to SCD: config changed");
      }
    }
  } else {
    ld_check(scd_->isActive());
    ld_check(!force_no_scd_);
    if (!scd_enabled) {
      // Scd is currently active but the log has just been configured to
      // not use it anymore. Switch to all send all mode now.
      ld_debug("Switching to all-send-all mode for log:%lu, id:%lu",
               log_id_.val(),
               id_.val());
      scd_->scheduleRewindToMode(ClientReadStreamScd::Mode::ALL_SEND_ALL,
                                 "Switching to ALL_SEND_ALL: config changed");
    }
  }

  log_uses_scd_ = scd_enabled;
  log_uses_local_scd_ = local_scd_enabled;
  sequencer_window_size_ = *attrs.maxWritesInFlight();
}

void ClientReadStream::noteConfigurationChanged() {
  ld_check(!done());

  if (permission_denied_) {
    return;
  }

  if (last_epoch_with_metadata_ == EPOCH_INVALID) {
    // the read stream is just started without having the inital epoch
    // metadata or read set. There is nothing we can do right now but just
    // return and wait for the initial epoch metadata to be delivered. At
    // that time the new read set will be calculated using the up-to-date
    // cluster configuration.
    ld_check(current_metadata_ == nullptr);
    ld_check(readSetSize() == 0);
    if (scd_) {
      // (scd_ == nullptr) could be true if configuration changes before
      // startContinuation() is called. In that case, we can rely on scd status
      // to be set by startContinuation(). Otherwise we update it here.
      updateScdStatus();
    }
    checkConsistency();
    return;
  }

  updateCurrentReadSet();
  connection_health_tracker_->recalculate();
  findGapsAndRecords();
  updateScdStatus();

  // This function should leave everything in a consistent state.
  checkConsistency();

  disposeIfDone();
}

void ClientReadStream::onSettingsUpdated() {
  if (current_metadata_) {
    findGapsAndRecords();
    if (done()) {
      deps_->dispose();
      return;
    }
  }
  if (scd_) {
    scd_->onSettingsUpdated();
  }
  if (readers_flow_tracer_) {
    readers_flow_tracer_->onSettingsUpdated();
  }
  if (grace_period_) {
    auto gap_grace_period = deps_->computeGapGracePeriod();
    deps_->updateBackoffTimerSettings(
        grace_period_, gap_grace_period, gap_grace_period);
  }
  applyShardStatus("onSettingsUpdated");
  // `this` may be destroyed here.
}

void ClientReadStream::setShardAuthoritativeStatus(SenderState& state,
                                                   AuthoritativeStatus status) {
  auto prev_status = state.getAuthoritativeStatus();
  if (status == prev_status) {
    return;
  }

  ShardID shard = state.getShardID();
  gap_failure_domain_->setShardAuthoritativeStatus(shard, status);
  healthy_node_set_->setShardAuthoritativeStatus(shard, status);
  state.setAuthoritativeStatus(status);

  connection_health_tracker_->recalculate();

  if (scd_) {
    scd_->setShardAuthoritativeStatus(shard, status);
  }
}

void ClientReadStream::applyShardStatus(const char* context,
                                        folly::Optional<SenderState*> state,
                                        bool try_make_progress) {
  const auto shard_status = deps_->getShardStatus();

  if (shard_status.getVersion() == LSN_INVALID) {
    return;
  }

  auto process_shard = [&](SenderState& state) {
    // If the shard has replied with an OK or REBUILDING status, don't change
    // its state: shard knows its rebuilding status better than the event log.
    // Once we lose connection to the shard, we will set its authoritative
    // status back to FULLY_AUTHORITATIVE until we call applyShardStatus() the
    // next time we try to reconnect to it.
    if (state.getConnectionState() == ConnectionState::READING) {
      return;
    }

    const node_index_t nid = state.getShardID().node();
    const shard_index_t shard_idx = state.getShardID().shard();
    auto status = shard_status.getShardStatus(nid, shard_idx);
    auto prev_status = state.getAuthoritativeStatus();

    ld_debug("Applying shard status for %s "
             "(context: %s, prev_status: %s, status: %s)",
             state.getShardID().toString().c_str(),
             context,
             toString(prev_status).c_str(),
             toString(status).c_str());

    if (deps_->getSettings().read_stream_guaranteed_delivery_efficiency &&
        status == AuthoritativeStatus::UNAVAILABLE) {
      status = AuthoritativeStatus::UNDERREPLICATION;
    }

    setShardAuthoritativeStatus(state, status);

    if (prev_status == status) {
      // Status did not change, no need to rewind
      return;
    }

    // If SCD is active and the node status is not fully authoritative,
    // we want to add that node to the filtered_out list and rewind
    if (scd_ && scd_->isActive() &&
        status != AuthoritativeStatus::FULLY_AUTHORITATIVE &&
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::format("{} added to known down list because it is not "
                          "fully authoritative",
                          state.getShardID().toString())
                .str())) {
      // Rewind has been scheduled.
      return;
    }

    // In general, we don't need to trigger rewind when the authoritative
    // status of a node changes in ALL_SEND_ALL mode, except if it
    // transitioned to being empty. In that case, we must rewind in order to
    // read the records that may have been rebuilt onto other nodes
    if (status == AuthoritativeStatus::AUTHORITATIVE_EMPTY) {
      scheduleRewind(folly::format("{} transitioned from {} to {} "
                                   "(context: {})",
                                   state.getShardID().toString().c_str(),
                                   toString(prev_status).c_str(),
                                   toString(status).c_str(),
                                   context)
                         .str());
    } else {
      ld_debug("Not rewinding despite the change of status of %s from %s to %s "
               "(context: %s)",
               state.getShardID().toString().c_str(),
               toString(prev_status).c_str(),
               toString(status).c_str(),
               context);
    }
  };

  if (state.hasValue()) {
    process_shard(*state.value());
  } else {
    for (auto& it : storage_set_states_) {
      process_shard(it.second);
    }
  }

  // We call findGapsAndRecords() here in case the change of status causes
  // the gap detection to find a gap to deliver
  if (try_make_progress) {
    if (current_metadata_) {
      findGapsAndRecords();
    }
    checkConsistency();
    disposeIfDone();
  }
}

void ClientReadStream::requestEpochMetaData(
    epoch_t epoch,
    bool require_consistent_from_cache) {
  // read stream should not request epoch metadata for an epoch whose
  // metadata is already known. Moreover, read stream should request the
  // exact next epoch of last_epoch_with_metadata_ if it is valid
  ld_check(last_epoch_with_metadata_ == EPOCH_INVALID ||
           epoch.val_ == last_epoch_with_metadata_.val_ + 1);

  if (epoch_metadata_requested_.hasValue() &&
      epoch <= epoch_metadata_requested_.value()) {
    // read stream should not request an epoch less than the highest one
    // it already requested
    ld_check(epoch == epoch_metadata_requested_.value());
    // the read stream is already requesting this epoch, return and wait
    // for the result
    return;
  }

  epoch_metadata_requested_.assign(epoch);
  epoch_metadata_request_time_ = std::chrono::steady_clock::now();
  last_released_epoch_when_metadata_requested_ = lsn_to_epoch(last_released_);

  ld_spew("Fetching metadata for epoch %u for log %lu, "
          "last_epoch_with_metadata %u",
          epoch.val_,
          log_id_.val_,
          last_epoch_with_metadata_.val_);

  if (!deps_->getMetaDataForEpoch(
          this->getID(),
          epoch,
          std::bind(&ClientReadStream::onEpochMetaData,
                    this,
                    std::placeholders::_1,
                    std::placeholders::_2),
          // ignore_released_status mode requires the information whether the
          // metadata record is the last record in the metadata log. Do not use
          // metadata cache in this case.
          use_epoch_metadata_cache_ && !ignore_released_status_,
          require_consistent_from_cache)) {
    // Metadata will be fetched asynchronously. Until then, let's update
    // connection health.
    connection_health_tracker_->recalculate();
  } else {
    // onEpochMetaData() was called synchronously.
    // *this may be destroyed here.
  }
}

void ClientReadStream::activateMetaDataRetryTimer() {
  if (!retry_read_metadata_) {
    retry_read_metadata_ = deps_->createBackoffTimer(
        INITIAL_RETRY_READ_METADATA_DELAY, MAX_RETRY_READ_METADATA_DELAY);
    retry_read_metadata_->setCallback([this]() {
      ld_info("epoch_metadata_requested: %u, current epoch: %u",
              epoch_metadata_requested_.hasValue()
                  ? epoch_metadata_requested_.value().val()
                  : 0,
              currentEpoch().val());
      // we must have requested epoch metadata for an epoch before
      ld_check(epoch_metadata_requested_.hasValue());
      epoch_t last_requested_epoch = epoch_metadata_requested_.value();
      // rewind epoch_metadata_requested_
      epoch_metadata_requested_.clear();
      // re-read the metadata log
      requestEpochMetaData(last_requested_epoch);
    });
  }

  retry_read_metadata_->activate();
}

int ClientReadStream::updateCurrentMetaData(
    Status st,
    epoch_t epoch,
    epoch_t until,
    std::unique_ptr<EpochMetaData> metadata) {
  // this function should be called on data log only
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));

  /**
   * Handling data loss or corruption of the metadata log
   *
   * Although quite unlikely, data loss can still happen in metadata logs and
   * the outcome is undesirable. There are multiple possible ways to handle
   * data loss or corruption in epoch interval [requested, until], for
   * example:
   *  - continue to use the existing storage set and replication factor up to
   *    epoch _until_, in such case, data loss may be reported
   *  - fallback to reading from all storage shards, use the replication factor
   *    from the current config. This is undesirable when the cluster size is
   *    large, and may still incur data losses since the replication factor of
   *    the epoch is unknown.
   *  - pick a random storage set and start reading from it to fast-forward the
   *    readstream; data loss may occur in this case
   *  - wait until the metadata log is repaired and back to the normal state
   *
   * Currently the latter mechanism is used to handle metadata loss/corruption
   */
  if (st != E::OK) {
    ld_error("Fetch epoch metadata for epoch %u of log %lu FAILED: %s. "
             "Expect bad metadata until epoch %u",
             epoch.val_,
             log_id_.val_,
             error_description(st),
             until.val_);

    if (current_metadata_ == nullptr) {
      ld_check(last_epoch_with_metadata_ == EPOCH_INVALID);
    }

    if (st == E::NOTFOUND) {
      ld_error("Metadata log for log %lu is empty! It is likely "
               "that the metadata log is not properly provisioned. Retry "
               "reading the metadata log after a timeout...",
               log_id_.val_);
      WORKER_STAT_INCR(client.metadata_log_error_NOTFOUND);
    } else {
      ld_error("Unable to read metadata log for log %lu: %s. Will retry "
               "after a timeout",
               log_id_.val(),
               error_description(st));
    }

    // can't read metadata log. start a backoff timer and keep re-reading
    // the metadata log until it is provisioned / fixed
    activateMetaDataRetryTimer();
    return -1;
  } else { // st == E::OK
    ld_check(metadata != nullptr);
    // MetaDataLogReader guarantees this
    ld_check(metadata->isValid());
    ld_check(until >= epoch);
    ld_debug("Successfully fetched metadata for epoch %u for log %lu. "
             "metadata epoch %u, effective until %u, metadata: %s",
             epoch.val_,
             log_id_.val_,
             metadata->h.epoch.val_,
             until.val_,
             metadata->toString().c_str());

    retry_read_metadata_.reset();

    if (current_metadata_ && metadata->h.epoch == current_metadata_->h.epoch) {
      // same metadata, no need to update
      if (*current_metadata_ != *metadata) {
        // metadata log record has changed to be not consistent with the current
        // metadata we have, this could be caused by (1) we fell back to a
        // different storage set because of a previous metadata corroption, then
        // metadata log is repaired; (2) misbehaved epoch metadata utility
        // that corrupted the metadata log.
        // TODO: distinguish two cases as part of the effort for better
        // handling metadata log corruption
        ld_warning("Existing epoch metadata has changed for log %lu "
                   "on epoch %u, check metadata log!",
                   log_id_.val_,
                   metadata->h.epoch.val_);
        current_metadata_ = std::move(metadata);
      }
    } else {
      current_metadata_ = std::move(metadata);
    }
  }

  ld_check(current_metadata_ && current_metadata_->isValid());
  return 0;
}

void ClientReadStream::updateLastEpochWithMetaData(
    Status /*st*/,
    epoch_t epoch,
    epoch_t until,
    MetaDataLogReader::RecordSource source) {
  // this function should be called on data log only
  ld_check(!MetaDataLog::isMetaDataLog(log_id_));
  // current metadata must have a valid value
  ld_check(current_metadata_ && current_metadata_->isValid());
  const epoch_t metadata_epoch = current_metadata_->h.epoch;

  /**
   * Handling ignoreReleasedStatus().
   * This special reading mode allows storage shards to send all records
   * that it has within the specified range, regardless of whether they were
   * released or not. A storage shard sends a gap to until_lsn once it
   * delivers all records on the log store.
   *
   * However, with storage set, we need client-side logic for handling this as
   * well. Specifically we don't want the client read stream try to figure
   * out epoch metadata for all future epochs upto until_lsn once it
   * receives gaps from all storage shards. Instead, it is sufficient for the
   * read stream to use the snapshot of the metadata log (without
   * considering the release status) at the very moment it starts reading to
   * determine epoch metadata.
   *
   * This still guarantees that the read stream can receive records that
   * meet the following criteria when it starts reading at time _t_:
   *  - ALL records stored (released or unreleased) with epoch _e_ <= last
   *    released epoch at the time of _t_. (This includes all records that
   *    were released at the time of _t_)
   * see doc block in MetaDataLogReader.h for the invariants that could
   * prove the guarantee.
   *
   * Noted that there is no guarantee on records not satisfying the
   * criteria, they may or may not get delivered, or delivered partially.
   *
   * To achieve this, with ignore_released_status_, the read stream stops
   * requesting new epoch metadata when it was delivered the epoch metadata
   * stored in the last record of the metadata log.
   */
  if (ignore_released_status_ &&
      source == MetaDataLogReader::RecordSource::LAST) {
    last_epoch_with_metadata_ = lsn_to_epoch(LSN_MAX);

  } else if (source == MetaDataLogReader::RecordSource::CACHED_SOFT ||
             (last_epoch_with_metadata_ == EPOCH_INVALID &&
              epoch > metadata_epoch &&
              source == MetaDataLogReader::RecordSource::LAST)) {
    /**
     * The read stream attempted to figure out the initial storage set before
     * starts reading from any shard, however, the metadata returned by the
     * reader may not be correct if:
     * (1) the _epoch_ requested is larger than the metadata epoch got; AND
     * (2) MetaDataLogReader gave us the last record in the current metadata
     *     log
     *  OR
     * (3) the metadata is from the epoch metadata cache and it might be
     *     inconsistent (RecordSource::CACHED_SOFT)
     *
     * In such case, we cannot confirm that the epoch metadata is used
     * by _epoch_ since we may start reading from an epoch whose metadata does
     * not exist yet or has not been stored. The following steps are taken to
     * ensure the read stream eventually gets the correct epoch metadata:
     *
     * (1) do not advance last_epoch_with_metadata_ using the epoch metadata
     *     got, instead, set it to currentEpoch() - 1 just to indicate the
     *     initial reading set is obtained.
     * (2) rewind epoch_metadata_requested_ and start reading from the storage
     *     set in the unconfirmed epoch metadata.
     * (3) eventually the readstream will encounter a gap with
     *     gap_lsn >= currentEpoch(). At that time, epoch metadata for current
     *     epoch can be correctly determined by MetaDataReader. As a result,
     *     findGapsAndRecords() will request and eventually get metadata for
     *     currentEpoch() by reading metadata log for the second time.
     */
    ld_check(epoch == currentEpoch());
    ld_check(epoch_metadata_requested_.value() > EPOCH_INVALID);

    // also hold true for CACHED_SOFT case since we only allow inconsistent
    // metadata when start reading
    ld_check(last_epoch_with_metadata_ == EPOCH_INVALID);

    last_epoch_with_metadata_ = epoch_t(epoch.val_ - 1);
    epoch_metadata_requested_ =
        epoch_t(epoch_metadata_requested_.value().val_ - 1);

    // store the inconsistent metadata to the cache if it is not
    // from the cache
    if (use_epoch_metadata_cache_ &&
        !MetaDataLogReader::isCachedSource(source)) {
      deps_->updateEpochMetaDataCache(
          epoch,
          until,
          *current_metadata_,
          MetaDataLogReader::RecordSource::CACHED_SOFT);
    }
  } else {
    // for all other cases, advance last_epoch_with_metadaata_ to the _until_
    // epoch from the result
    ld_check(until > last_epoch_with_metadata_);
    last_epoch_with_metadata_ = until;

    // we are sure that the current epoch metadata is authentic for the request
    // epoch, update the metadata cache if the metdata is read from the metadata
    // log
    if (use_epoch_metadata_cache_ &&
        !MetaDataLogReader::isCachedSource(source)) {
      deps_->updateEpochMetaDataCache(
          epoch,
          until,
          *current_metadata_,
          MetaDataLogReader::RecordSource::CACHED_CONSISTENT);
    }
  }

  ld_check(last_epoch_with_metadata_ != EPOCH_INVALID);
  ld_spew("Last epoch with metadata is updated to %u for log %lu",
          last_epoch_with_metadata_.val_,
          log_id_.val_);
}

void ClientReadStream::handleEmptyStorageSet(
    const MetaDataLogReader::Result& result,
    epoch_t until) {
  const epoch_t epoch = result.epoch_req;
  if (result.source != MetaDataLogReader::RecordSource::NOT_LAST) {
    // This is the last entry in the metadata log, we just have to retry
    // reading it until we encounter something else
    ld_debug("Read an entry in the metadata log for epoch %u of log %lu "
             "with an effectively empty storage set and no other metadata log "
             "entries after it. Retrying metadata log fetch.",
             epoch.val(),
             log_id_.val());
    current_metadata_ = nullptr;
    last_epoch_with_metadata_ = EPOCH_INVALID;
    if (use_epoch_metadata_cache_) {
      reenable_cache_on_valid_metadata_ = true;
    }
    // Disabling metadata cache, since we are trying to refetch from this
    // record onwards, and don't want to be getting it from cache.
    use_epoch_metadata_cache_ = false;
    activateMetaDataRetryTimer();
    return;
  }
  // There is an entry in the metadata log after this one - issue a DATALOSS
  // gap until that entry
  ld_debug("Read an entry in the metadata log for epoch %u of log %lu "
           "with an effectively empty storage set. Issuing data loss until "
           "end of epoch %u",
           epoch.val(),
           log_id_.val(),
           until.val());
  lsn_t gap_until = compose_lsn(until, ESN_MAX);

  // This line does some minimal effort to distinguish between dataloss and trim
  // when storage set is empty. It would only work if we happen to already have
  // a trim point from some previous nonempty storage set, and the trim point
  // completely covers the epochs with empty storage set. This seems like a rare
  // case, but we've encountered it in practice. In the general case we would
  // probably need to connect to a storage set of a higher epoch before shipping
  // this gap, but that's not implemented.
  // TODO 16227919: hide such dataloss with BRIDGE gaps, the hope is to have
  //                metadata logs trimmed so that these empty nodesets won't
  //                stay for long
  GapType gap_type = trim_point_ >= gap_until ? GapType::TRIM : GapType::BRIDGE;

  if (deliverGap(gap_type,
                 next_lsn_to_deliver_,
                 std::min(until_lsn_, gap_until)) != 0) {
    current_metadata_ = nullptr;
    last_epoch_with_metadata_ = EPOCH_INVALID;
    activateMetaDataRetryTimer();
    return;
  }

  // Advance buffer head so that the next epoch becomes the front slot of the
  // buffer.  There should be no records/gaps inside the buffer for the lsn
  // range of the gap; advanceBufferHead() will assert that.
  buffer_->advanceBufferHead(gap_until + 1 - next_lsn_to_deliver_);
  ld_check(buffer_->getBufferHead() == gap_until + 1);
  next_lsn_to_deliver_ = gap_until + 1;
  findGapsAndRecords();
}

void ClientReadStream::onEpochMetaData(Status st,
                                       MetaDataLogReader::Result result) {
  ld_check(!done());
  ld_check(result.log_id == log_id_);

  if (permission_denied_) {
    // Ignore, invalid permissions
    return;
  }

  // measuring latency for ClientReadsTracer
  epoch_metadata_request_latency_ =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - epoch_metadata_request_time_);
  const epoch_t epoch = result.epoch_req;
  epoch_t until = result.epoch_until;
  const MetaDataLogReader::RecordSource source = result.source;

  ld_check(epoch_metadata_requested_.hasValue());
  ld_check(epoch == epoch_metadata_requested_.value());
  ld_check(last_epoch_with_metadata_ < EPOCH_MAX);

  if (last_epoch_with_metadata_ != EPOCH_INVALID &&
      epoch.val_ != last_epoch_with_metadata_.val_ + 1) {
    // except for figuring out the initial epoch, the read stream should always
    // request the very next epoch with unknown metadata
    ld_critical("(%p) Got epoch metadata for epoch %u of log %lu but was "
                "requesting epoch %u. last_epoch_with_metadata_: %u, "
                "start_lsn_: %s, until_lsn_: %s, last_released_: %s, "
                "highest_record_lsn_: %s.",
                this,
                epoch.val_,
                log_id_.val_,
                epoch_metadata_requested_.value_or(EPOCH_INVALID).val_,
                last_epoch_with_metadata_.val_,
                lsn_to_string(start_lsn_).c_str(),
                lsn_to_string(until_lsn_).c_str(),
                lsn_to_string(last_released_).c_str(),
                lsn_to_string(highest_record_lsn_).c_str());
    ld_check(false);
    return;
  }

  // If the status is E::ACCESS then the readstream does not have the correct
  // permission to read from the metadata log. Do not allow the result to be
  // used, and send an ACCESS gap back to the client
  if (st == E::ACCESS) {
    if (!permission_denied_) {
      permission_denied_ = true;
      deliverAccessGapAndDispose();
    }
    return;
  }

  if (st == E::NOTINCONFIG) {
    deliverNoConfigGapAndDispose();
    return;
  }

  // if the log is a metadata log, we expect the function only be called once
  // when the read stream starts, but never after
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    ld_check(st == E::OK);
    ld_check(last_epoch_with_metadata_ == EPOCH_INVALID);
    current_metadata_ = std::move(result.metadata);
    last_epoch_with_metadata_ = until;
    // the epoch metadata, once fetched, should be effective for all future
    // epochs
    ld_check(last_epoch_with_metadata_ >= EPOCH_MAX);
    // metadata should not come from the client cache
    ld_check(!MetaDataLogReader::isCachedSource(source));
  } else { // data log

    /// determine the real `effective until` for this metadata request

    if (until == EPOCH_INVALID) {
      // this only happens when start reading from EPOCH_INVALID, and there are
      // invalid record in the metadata log. In such case, revise until
      // to EPOCH_MIN so that reading can be started
      ld_check(last_epoch_with_metadata_ == EPOCH_INVALID &&
               currentEpoch() == EPOCH_INVALID);
      ld_check(st != E::OK);
      until = EPOCH_MIN;
    }

    if (source == MetaDataLogReader::RecordSource::LAST) {
      // if the metadata is from the last record of the metadata log, it is
      // safe to assume that the epoch metadata is effective until the epoch of
      // the last released lsn at the time when the last metadata request was
      // made. Therefore, use last_released_epoch_when_metadata_requested_ to
      // extend effective_until if possible.
      until = std::max(last_released_epoch_when_metadata_requested_, until);
    } else {
      // Due to the race condition described in t19749400, we cannot simply rely
      // on the effective until delivered by metadata log reader without
      // comparing with the last released epoch of the data log. Specifically,
      // the effective until cannot be larger than the last released
      // epoch of the data log when the metadata was requested. Note that
      // last released epoch can be stale or not available (EPOCH_INVALID),
      // and is at least the `epoch` requested, but we are still guaranteed to
      // make progress since the metadata is at least effective in the requested
      // `epoch'. The worst case is that we will re-read the metadata log for
      // every single epoch encountered.
      //
      // Another edge case is that the client starts reading from EPOCH_INVALID
      // and from the calculation above the until will be EPOCH_INVALID as well.
      // In such case adjust until to EPOCH_MIN which is the first valid epoch.
      epoch_t effective_since =
          (st == E::OK ? result.metadata->h.effective_since : EPOCH_INVALID);

      until = std::max(
          {EPOCH_MIN,
           epoch,
           effective_since,
           std::min(last_released_epoch_when_metadata_requested_, until)});
    }

    // update current metadata and advance last_epoch_with_metadata_ according
    // to the result
    if (updateCurrentMetaData(st, epoch, until, std::move(result.metadata))) {
      // failed to update current metadata, abort and return
      return;
    }
    updateLastEpochWithMetaData(st, epoch, until, source);
  }

  // both current_metadata_ and last_epoch_with_metadata_ have been updated,
  // update the read set and continue gap detection
  updateCurrentReadSet();
  connection_health_tracker_->recalculate();
  findGapsAndRecords();

  if (storage_set_states_.size() == 0) {
    // Special case - no shard in the storage set are in the config, effective
    // storage set is empty.
    handleEmptyStorageSet(result, until);
  } else if (reenable_cache_on_valid_metadata_) {
    use_epoch_metadata_cache_ = true;
    reenable_cache_on_valid_metadata_ = false;
  }

  /**
   * This function is a public method may advance next_lsn_to_deliver_ beyond
   * until_lsn_, dispose the read stream in such case.
   * This is safe since:
   * (1) this function won't be called twice without returning to the event loop
   *     (i.e. at most one frame on the call stack)
   * (2) Call frames above the function on the call stack do not access the read
   *     stream. Typical call chain: event loop -> on message -> metadata read
   *     stream callback -> this function.
   * (3) destructor of the read stream prevents further invocation of the
   *     function by callbacks
   */
  disposeIfDone();
}

void ClientReadStream::updateCurrentReadSet() {
  SCOPE_EXIT {
    checkConsistency();
  };

  // caller must invoke this function with valid epoch metadata
  ld_check(current_metadata_);

  const StorageSet& new_shards = current_metadata_->shards;
  const std::unordered_set<ShardID, ShardID::Hash> next_nodeset(
      new_shards.cbegin(), new_shards.cend());

  // replication property of the current epoch is used to construct
  // the failure domain information for gap detection
  const ReplicationProperty replication = current_metadata_->replication;

  const auto& nodes_configuration = config_->getNodesConfiguration();
  const auto& storage_membership = nodes_configuration->getStorageMembership();

  // find nodes from storage_set_states_ that are:
  // (1) no longer in the nodeset of epoch metadata
  // (2) no longer in the current cluster configuration
  for (auto it = storage_set_states_.begin();
       it != storage_set_states_.end();) {
    const ShardID shard = it->first;

    if (next_nodeset.find(shard) == next_nodeset.end() ||
        !storage_membership->shouldReadFromShard(shard)) {
      // Node is no longer present in the current storage set, or has been
      // removed from the config or reconfigured to not be a storage shard.

      // Try to send a quick STOP.  We don't care if sending succeeds (it
      // won't if the socket no longer exists).  It is just a courtesy to the
      // server so that it can release any resources for the read stream
      // quicker.
      deps_->sendStopMessage(it->second.getShardID());

      // Update gap parameters
      resetGapParametersForSender(it->second);

      // We track the number of shards in the READING state. Move to a different
      // state so that the count is updated if necessary.
      it->second.setConnectionState(ConnectionState::DYING);

      // gap_failure_domain_ must exist since the read set is not empty
      ld_check(gap_failure_domain_ != nullptr);

      // Remove it from the map of states
      it = storage_set_states_.erase(it);
    } else {
      ++it;
    }
  }

  // check for newly eligible shards currently not present in
  // storage_set_states_ but satisfy:
  // (1) present in the current storage set; and
  // (2) present as valid storage shards in current config
  std::vector<std::reference_wrapper<SenderState>> added_shards;
  for (ShardID shard_id : new_shards) {
    if (!storage_membership->shouldReadFromShard(shard_id)) {
      // Either:
      //    1- node is not in the cluster config. This may indicate that the
      // cluster was shrunk but we are reading in epochs that are written
      // before the cluster was shrunk. It is likely dataloss gaps will appear
      // in this epoch.
      //    2- Not a storage node, or it's not currently readable. Ignore.
      continue;
    }

    if (storage_set_states_.find(shard_id) != storage_set_states_.end()) {
      // Shard already exists.
      continue;
    }

    added_shards.push_back(std::ref(createStateForShard(shard_id)));
  }

  StorageSet read_set;
  read_set.reserve(readSetSize());
  for (const auto& kv : storage_set_states_) {
    read_set.push_back(kv.first);
  }

  // read set is updated, rebuild the gap failure domain information with the
  // new read set and the gap state for each shards in the new read set.
  // Do the same thing to the connection failure domain.
  gap_failure_domain_ = std::make_unique<GapFailureDomain>(
      read_set, *nodes_configuration, replication);
  healthy_node_set_ = std::make_unique<HealthyNodeSet>(
      read_set, *nodes_configuration, replication);

  ld_check(healthy_node_set_->numShards() == storage_set_states_.size());
  ld_check(gap_failure_domain_->numShards() == storage_set_states_.size());

  for (const auto& kv : storage_set_states_) {
    const auto shard_id = kv.first;
    const auto gap_state = kv.second.getGapState();
    const auto authoritative_state = kv.second.getAuthoritativeStatus();
    gap_failure_domain_->setShardAttribute(shard_id, gap_state);
    gap_failure_domain_->setShardAuthoritativeStatus(
        shard_id, authoritative_state);
    healthy_node_set_->setShardAttribute(shard_id, kv.second.isHealthy());
    healthy_node_set_->setShardAuthoritativeStatus(
        shard_id, authoritative_state);
  }

  // If *no* START messages have been sent and some sockets to shards already
  // exist, pre-check protocol versions to avoid unnecessary rewinding when
  // servers are on an old version.
  if (!any_start_sent_) {
    for (const SenderState& state : added_shards) {
      folly::Optional<uint16_t> proto =
          deps_->getSocketProtocolVersion(state.getShardID().node());
      if (proto.hasValue()) {
        coordinated_proto_ = std::min(coordinated_proto_, proto.value());
      }
    }
  }

  // If SCD is active, updating the storage shards set may cause us to rewind.
  // If that's the case, return early as the rewind will cause the new shards to
  // be sent a START message.
  scd_->updateStorageShardsSet(read_set, replication);

  // Apply authoritative status from event log. In particular, for a newly
  // created ClientReadStream this does the initial propagation of
  // authoritative status from Worker's authoritative status map to
  // storage_set_states_.
  for (SenderState& state : added_shards) {
    // Use try_make_progress=false to make sure *this is not destroyed here.
    // The caller of updateCurrentReadSet() will try to make progress right
    // after.
    applyShardStatus("updateCurrentReadSet",
                     &state,
                     /* try_make_progress */ false);
  }

  // If either updateStorageShardsSet() or applyShardStatus() scheduled a
  // rewind, no need to send START messages here as they'll be sent by rewind.
  if (rewind_scheduler_->isScheduled()) {
    return;
  }

  // Send START to the new shards.
  for (SenderState& state : added_shards) {
    sendStart(state.getShardID(), state);
  }
  scd_->onWindowSlid(server_window_.high, filter_version_);
}

void ClientReadStream::onSocketClosed(SenderState& state, Status st) {
  onConnectionFailure(state, st);
}

void ClientReadStream::onConnectionHealthChange(ShardID shard, bool healthy) {
  if (healthy_node_set_ == nullptr) {
    return;
  }

  healthy_node_set_->setShardAttribute(shard, healthy);
  connection_health_tracker_->recalculate();
}

void ClientReadStream::checkConsistency() const {
  if (!folly::kIsDebug) {
    return;
  }
  nodeset_size_t real_gap_shards_total = 0;
  nodeset_size_t real_gap_shards_filtered_out = 0;
  nodeset_size_t real_under_replicated_shards_not_blacklisted = 0;

  small_shardset_t filtered_out;

  for (auto& it : storage_set_states_) {
    const SenderState& state = it.second;
    const auto shard = state.getShardID();
    const bool is_linked = state.list_hook_.is_linked();
    GapState gap_state = state.getGapState();

    if (gap_state == GapState::NONE) {
      ld_check(!is_linked);
      ld_check_ge(next_lsn_to_deliver_, state.getNextLsn());
    } else {
      ++real_gap_shards_total;

      ld_check_eq(state.grace_counter, 0);

      if (next_lsn_to_deliver_ <= until_lsn_) {
        ld_check_lt(next_lsn_to_deliver_, state.getNextLsn());
        ld_check_eq(state.getGapState() == GapState::UNDER_REPLICATED,
                    next_lsn_to_deliver_ < state.under_replicated_until);
      } else {
        // Gap state is not updated when reaching until LSN.
      }

      if (gap_state == GapState::GAP) {
        ld_check(!state.should_blacklist_as_under_replicated);
      }
    }

    const auto st = state.getAuthoritativeStatus();
    if (gap_failure_domain_) {
      ld_check(gap_failure_domain_->getShardAuthoritativeStatus(shard) == st);
    }
    if (healthy_node_set_) {
      ld_check(healthy_node_set_->getShardAuthoritativeStatus(shard) == st);
    }

    if (scd_ && scd_->isActive()) {
      if (state.blacklist_state != SenderState::BlacklistState::NONE) {
        filtered_out.push_back(state.getShardID());

        if (state.getGapState() != GapState::NONE) {
          ++real_gap_shards_filtered_out;
        }
      } else {
        real_under_replicated_shards_not_blacklisted +=
            state.getGapState() == GapState::UNDER_REPLICATED;
      }
    }
  }

  if (scd_ && scd_->isActive()) {
    ld_check_eq(scd_->getGapShardsFilteredOut(), real_gap_shards_filtered_out);
    ld_check_eq(scd_->getUnderReplicatedShardsNotBlacklisted(),
                real_under_replicated_shards_not_blacklisted);
    auto filtered_out_expected = scd_->getFilteredOut();
    ld_check_eq(filtered_out.size(), filtered_out_expected.size());
    std::sort(filtered_out_expected.begin(), filtered_out_expected.end());
    std::sort(filtered_out.begin(), filtered_out.end());
    ld_check(std::equal(filtered_out.begin(),
                        filtered_out.end(),
                        filtered_out_expected.begin()));
  }

  if (readSetSize() > 0) {
    ld_check(gap_failure_domain_ != nullptr);
    ld_check(real_gap_shards_total ==
             (numShardsInState(GapState::GAP) +
              numShardsInState(GapState::UNDER_REPLICATED)));
  }
}

void ClientReadStream::resumeReading() {
  if (redelivery_timer_ != nullptr && redelivery_timer_->isActive()) {
    cancelRedeliveryTimer();
    redeliver();
    // `this` may be deleted here.
  }
}

bool ClientReadStream::canDeliverRecordsNow() const {
  const RecordState* rstate = buffer_->front();
  return rstate && (rstate->record || rstate->filtered_out);
}

ClientReadStreamSenderState&
ClientReadStream::createStateForShard(ShardID shard_id) {
  const auto& settings = deps_->getSettings();
  auto insert_result = storage_set_states_.emplace(
      std::piecewise_construct,
      std::forward_as_tuple(shard_id),
      std::forward_as_tuple(
          this,
          shard_id,
          deps_->createBackoffTimer(settings.reader_reconnect_delay),
          deps_->createBackoffTimer(settings.reader_started_timeout),
          deps_->createBackoffTimer(settings.reader_retry_window_delay)));
  ld_check(insert_result.second);
  return insert_result.first->second;
}

void ClientReadStream::setSenderGapState(SenderState& sender, GapState state) {
  // shard must exist in read set and its sender state must match _state_
  ShardID shard = sender.getShardID();
  ld_assert(storage_set_states_.find(shard) != storage_set_states_.end());
  ld_assert(&storage_set_states_.at(shard) == &sender);

  const auto current_state = sender.getGapState();
  ld_check(current_state != GapState::NONE || !sender.list_hook_.is_linked());

  if (current_state == state) {
    return;
  }

  // update GapState in both SenderState and gap failure domain info.
  sender.setGapState(state);
  scd_->onSenderGapStateChanged(sender, current_state);
  ld_check(gap_failure_domain_ != nullptr);
  gap_failure_domain_->setShardAttribute(shard, state);

  // If the new state is NONE and the SenderState is currently linked to a list
  // of some RecordState, remove it from the list.
  if (state == GapState::NONE && sender.list_hook_.is_linked()) {
    sender.list_hook_.unlink();
  }

  if (state == GapState::GAP) {
    // The shard is not reported as underreplicated anymore. Mark it for
    // removing from known down when we receive next record from it.
    sender.should_blacklist_as_under_replicated = false;
  }
}

void ClientReadStream::resetGapParametersForSender(SenderState& state) {
  // We might be reconnecting to the same shard, in which case it's necessary to
  // reset gap-handling parameters.
  state.setNextLsn(LSN_INVALID);
  state.max_data_record_lsn = LSN_INVALID;
  state.grace_counter = 0;
  state.under_replicated_until = LSN_INVALID;
  setSenderGapState(state, GapState::NONE);
}

int ClientReadStream::handleRecord(RecordState* rstate,
                                   bool grace_period_expired,
                                   bool filtered_out) {
  ld_check(next_lsn_to_deliver_ == buffer_->getBufferHead());
  ld_check(rstate);
  ld_check(rstate == buffer_->front());
  ld_check(rstate->record != nullptr || rstate->filtered_out);
  ld_check(!rstate->record ||
           rstate->record->attrs.lsn == next_lsn_to_deliver_);
  // Update gap state.
  // If `next_lsn_to_deliver_` is `next_lsn` for some sender, need to change
  // the sender's gap state to NONE before checking for f-majority.
  unlinkRecordState(next_lsn_to_deliver_, *rstate);

  if (wait_for_all_copies_) {
    switch (checkFMajority(grace_period_expired)) {
      case WAIT_FOR_GRACE_PERIOD:
        grace_period_->activate();
        FOLLY_FALLTHROUGH;
      case WAIT:
        return -1;
      case ISSUE_GAP: {
        // See comment in detectGap().
        const epoch_t current_epoch = currentEpoch();
        if (current_epoch > last_epoch_with_metadata_) {
          ld_check(current_epoch.val_ == last_epoch_with_metadata_.val_ + 1);
          requestEpochMetaData(current_epoch);
          return -1;
        }
        break;
      }
      case ADD_TO_KNOWN_DOWN_AND_REWIND:
        promoteUnderreplicatedShardsToKnownDown();
        return -1;
      default:
        ld_check(false);
    }
  }

  bool bridge =
      rstate->record && rstate->record->flags_ & RECORD_Header::BRIDGE;
  lsn_t record_lsn = LSN_INVALID;
  if (!filtered_out) {
    record_lsn = rstate->record->attrs.lsn;
  }

  int rv = -1;
  if (filtered_out) {
    // Deliver FILTERED_OUT gap here.
    rv = deliverGap(
        GapType::FILTERED_OUT, next_lsn_to_deliver_, next_lsn_to_deliver_);
    if (rv != 0) {
      RATELIMIT_WARNING(std::chrono::seconds(10),
                        3,
                        "Failed to deliver filtered_out"
                        "gap. Gap: [%s, %s]",
                        lsn_to_string(next_lsn_to_deliver_).c_str(),
                        lsn_to_string(next_lsn_to_deliver_).c_str());
      return rv;
    }
  } else if (!bridge ||
             ship_pseudorecords_) { // don't deliver bridges by default
    rv = deliverRecord(rstate->record);
    if (rv != 0) {
      // The record could not be delivered at this time.  Assert that we still
      // have it.  Downstream ought to have left the std::unique_ptr intact.
      ld_check(rstate->record || rstate->filtered_out);
      return rv;
    }
  }

  if (bridge) {
    // The record is a bridge record. Handle it separately.
    return handleBridgeRecord(record_lsn, rstate);
  }

  // Free the record and advance to next LSN.
  ++next_lsn_to_deliver_;
  rstate->reset();
  buffer_->popFront();
  buffer_->advanceBufferHead();
  ld_check(next_lsn_to_deliver_ == buffer_->getBufferHead());

  return 0;
}

/**
 *  Bridge Record
 *
 *  ClientReadStream makes use of bridge records to reliably report dataloss
 *  and bridge gap at epoch transitions. It also leverages these records to
 *  avoid performing f-majority based gap detection algorithm to report bridge
 *  gaps and advance epochs. The clear advantage is that it requires much less
 *  shards participate during epoch transitions, and the read stream will not
 *  fall back from single-copy-delivery mode to all-send-all mode on epoch
 *  bumps. There are two types of record that indicates the beginning and the
 *  end of an epoch, respectively:
 *
 *  1) Records with the flag RECORD_Header::BRIDGE. This indicates that the
 *     record is a special hole plugged by epoch recovery. It is guaranteed
 *     that there is no more valid data record in this epoch after this record
 *     (although there might be hole plugs plugged by epoch recovery instances).
 *     Once it encounters such record, the read stream will fast-forward
 *     directly to the end of the epoch, delivering a bridge gap from the
 *     bridge record esn to ESN_MAX of the epoch (or until_lsn_ if it is
 *     smaller). The next_lsn_to_deliver_, will be advanced to the ESN_INVALID
 *     to the next epoch.
 *
 *  2) Records with RECORD_Header::EPOCH_BEGIN. This flag is placed by the
 *     sequencer with the first record it tried to replicated in an epoch.
 *     However, with the current design, such record is _not_ guaranteed to be
 *     always present or fully replicated. When the read stream is just started
 *     or is at the beginning of an epoch (next_lsn_to_deliver_ has esn of
 *     ESN_INVALID), everytime it tries to make progress, before attempting the
 *     gap detection algorithm, it does a foward search on the read stream
 *     buffer to find if the first marker in the buffer is a record with
 *     EPOCH_BEGIN or a record at ESN_MIN. In either case, the record must be
 *     the beginning of an epoch and the read stream will fast-forward to the
 *     record lsn, delivering a bridge gap from ESN_INVALID (or start_lsn_) to
 *     the lsn before the record.
 *
 *  Note that if bridge records are present, for each epoch transistions, there
 *  are two bridge gaps to be delivered instead of one (as delivered by
 *  gap detection).
 */
int ClientReadStream::handleBridgeRecord(lsn_t bridge_record_lsn,
                                         RecordState* rstate) {
  // next_lsn_to_deliver_ must align with buffer head and it must be
  // a bridge record
  ld_check(next_lsn_to_deliver_ == buffer_->getBufferHead());
  ld_check(bridge_record_lsn == next_lsn_to_deliver_);

  // record state must be unlinked by handleRecord() so that there is no sender
  // state attached to this record state
  ld_check(rstate);
  ld_check(rstate->list.empty());

  // Now we are sure that there are no more data records beyond the
  // next_lsn_to_deliver_ for the current epoch. Try to advance
  // next_lsn_to_deliver_ to the last esn for the current epoch,
  // i.e, e(currentEpoch())n(ESN_MAX).
  const lsn_t gap_lsn =
      std::min(until_lsn_, compose_lsn(currentEpoch(), ESN_MAX));
  const lsn_t next_lsn_target = (gap_lsn == LSN_MAX ? LSN_MAX : gap_lsn + 1);

  ld_check(gap_lsn >= next_lsn_to_deliver_);
  int rv = deliverGap(GapType::BRIDGE, next_lsn_to_deliver_, gap_lsn);
  if (rv != 0) {
    return rv;
  }

  if (last_epoch_with_metadata_ < lsn_to_epoch(LSN_MAX) &&
      lsn_to_epoch(next_lsn_target).val_ > last_epoch_with_metadata_.val_ + 1) {
    last_epoch_with_metadata_ = epoch_t(lsn_to_epoch(next_lsn_target).val_ - 1);
  }

  // fast-forward the read stream to the next_lsn_target.
  namespace arg = std::placeholders;
  buffer_->forEachUpto(
      next_lsn_target,
      std::bind(&ClientReadStream::clearRecordState, this, arg::_1, arg::_2));

  if (next_lsn_target > window_high_) {
    gap_end_outside_window_ = LSN_INVALID;
  }

  ld_debug("bridge record detected for log %lu, advance from "
           "%s to %s.",
           log_id_.val_,
           lsn_to_string(next_lsn_to_deliver_).c_str(),
           lsn_to_string(next_lsn_target).c_str());

  buffer_->advanceBufferHead(next_lsn_target - next_lsn_to_deliver_);
  ld_check(buffer_->getBufferHead() == next_lsn_target);
  next_lsn_to_deliver_ = next_lsn_target;

  if (!done() && currentEpoch() > last_epoch_with_metadata_) {
    requestEpochMetaData(epoch_t(last_epoch_with_metadata_.val_ + 1));
    return -1;
  }

  return 0;
}

int ClientReadStream::deliverRecord(
    std::unique_ptr<DataRecordOwnsPayload>& record) {
  lsn_t lsn = record->attrs.lsn;
  if (last_delivered_lsn_ > LSN_INVALID && last_delivered_lsn_ >= lsn) {
    // make sure we are delivering record in order
    ld_critical("Order guarantee violated! Record %s of log %lu is delivered "
                "after record/gap at lsn %s",
                lsn_to_string(lsn).c_str(),
                log_id_.val(),
                lsn_to_string(last_delivered_lsn_).c_str());
    ld_check(last_delivered_lsn_ < lsn);
  }

  last_in_record_ts_ = record->attrs.timestamp;
  last_received_ts_ = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());

  OffsetMap current_offsets = OffsetMap::fromRecord(record->attrs.offsets);
  OffsetMap payload_size_map;
  // TODO(T33977412) Add record counter offset based on settings
  payload_size_map.setCounter(BYTE_OFFSET, record->payload.size());

  // we should always deliver the record at next_lsn_to_deliver_, which is at
  // the front of the buffer
  ld_check(next_lsn_to_deliver_ == buffer_->getBufferHead());
  ld_check(next_lsn_to_deliver_ == lsn);

  // Try to set byte offset from accumulated clientReadStream value if
  // current record does not have byte_offset information and client
  // requested byte offset.
  if (additional_start_flags_ & START_Header::INCLUDE_BYTE_OFFSET &&
      !record->attrs.offsets.isValid()) {
    current_offsets = accumulated_offsets_.isValid()
        ? OffsetMap::mergeOffsets(
              std::move(accumulated_offsets_), payload_size_map)
        : OffsetMap();
    record->attrs.offsets = OffsetMap::toRecord(current_offsets);
  }

  if ((record->flags_ & RECORD_Header::HOLE) && !ship_pseudorecords_) {
    // This special record type is reported as a HOLE gap to clients
    return deliverGap(GapType::HOLE, lsn, lsn);
  }

  bool bridge_record = (record->flags_ & RECORD_Header::BRIDGE);
  bool success;
  if (reader_) {
    bool notify =
        // After this record is consumed, will it be time to slide the window?
        // If so, tell the Reader to notify us.
        lsn == next_lsn_to_slide_window_ ||
        // If this is the last record to read, also set the flag; not because we
        // need the notification but to unblock Reader and avoid an issue where
        // it gets stuck waiting for more records (t14156907)
        lsn == until_lsn_;
    int rv = reader_->onDataRecord(getID(), std::move(record), notify);
    success = (rv == 0);
  } else {
    inside_callback_ = true;
    // This is tricky.  Upcasting from DataRecordOwnsPayload ...
    std::unique_ptr<DataRecord> record_upcast(std::move(record));
    success = deps_->recordCallback(record_upcast);
    if (success) {
      if (lsn >= until_lsn_) {
        deps_->doneCallback(log_id_);
      }
    } else {
      // Application must not drain the unique_ptr when it rejects
      ld_check(record_upcast != nullptr);
      // Downcast back into buffer
      record = std::unique_ptr<DataRecordOwnsPayload>(
          static_cast<DataRecordOwnsPayload*>(record_upcast.release()));

      if (record->attrs.lsn != lsn) {
        RATELIMIT_CRITICAL(
            std::chrono::seconds(5),
            2,
            "Record lsn %s has been altered in delivery callback "
            "while redelivery is scheduled for log %lu, "
            "original lsn %s.",
            lsn_to_string(record->attrs.lsn).c_str(),
            log_id_.val_,
            lsn_to_string(lsn).c_str());

        // assert here to catch the problem early, if we do not assert here,
        // it is likely that an assert will be triggered in handleRecord()
        // anyway
        ld_check(false);
      }
    }
    inside_callback_ = false;
  }
  if (success) {
    if (!bridge_record) {
      // Update last delivered lsn to keep track of ordering
      // This does not apply to bridge record as they are dummy records that
      // are delivered as GAP. So deliverGap() will update the last delivered
      // lsn appropriately in that case.
      last_delivered_lsn_ = lsn;
    }
    if (MetaDataLog::isMetaDataLog(log_id_)) {
      if (wait_for_all_copies_) {
        WORKER_STAT_INCR(client.metadata_log_records_delivered_wait_for_all);
      } else {
        WORKER_STAT_INCR(client.metadata_log_records_delivered);
      }
      WORKER_STAT_ADD(client.metadata_log_bytes_delivered,
                      payload_size_map.getCounter(BYTE_OFFSET));
    } else {
      if (wait_for_all_copies_) {
        WORKER_STAT_INCR(client.records_delivered_wait_for_all);
      } else {
        WORKER_STAT_INCR(client.records_delivered);
        if (scd_ && scd_->isActive()) {
          WORKER_STAT_INCR(client.records_delivered_scd);
        } else {
          WORKER_STAT_INCR(client.records_delivered_noscd);
        }
      }
      WORKER_STAT_ADD(
          client.bytes_delivered, payload_size_map.getCounter(BYTE_OFFSET));
    }
    num_records_delivered_++;
    num_bytes_delivered_ += payload_size_map.getCounter(BYTE_OFFSET);
    // Updating info reg. buffer usage.
    bytes_buffered_ -= payload_size_map.getCounter(BYTE_OFFSET);
    if (current_offsets.isValid()) {
      accumulated_offsets_ = std::move(current_offsets);
    }
  } else {
    if (!MetaDataLog::isMetaDataLog(log_id_)) {
      WORKER_STAT_INCR(client.records_redelivery_attempted);
    }
  }
  adjustRedeliveryTimer(success);
  return success ? 0 : -1;
}

int ClientReadStream::deliverGap(GapType type, lsn_t lo, lsn_t hi) {
  ld_check(hi <= until_lsn_);
  ld_check(lo <= hi);

  if (type == GapType::DATALOSS) {
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      2,
                      "Delivering DATALOSS gap log %lu lsns [%s, %s]. %s",
                      log_id_.val_,
                      lsn_to_string(lo).c_str(),
                      lsn_to_string(hi).c_str(),
                      getDebugInfoStr().c_str());
  }

  if (last_delivered_lsn_ > LSN_INVALID && last_delivered_lsn_ >= lo) {
    // make sure we are not delivering a gap that overlaps already
    // delivered records
    ld_critical("Order guarantee violated! Gap(%d) [%s, %s] of log %lu is "
                "delivered after record/gap at lsn %s",
                static_cast<int>(type),
                lsn_to_string(lo).c_str(),
                lsn_to_string(hi).c_str(),
                log_id_.val(),
                lsn_to_string(last_delivered_lsn_).c_str());
    ld_check(last_delivered_lsn_ < lo);
  }

  GapRecord gap(log_id_, type, lo, hi);
  bool success;
  if (reader_) {
    bool notify =
        lo <= next_lsn_to_slide_window_ && next_lsn_to_slide_window_ <= hi;
    int rv = reader_->onGapRecord(getID(), gap, notify);
    success = (rv == 0);
  } else {
    inside_callback_ = true;
    success = deps_->gapCallback(gap);
    if (success && hi >= until_lsn_) {
      deps_->doneCallback(log_id_);
    }
    inside_callback_ = false;
  }
  if (success) {
    bumpGapStat(log_id_, Worker::stats(), gap.type);
    // update last delivered LSN in order to check that records
    // are always delivered in order
    last_delivered_lsn_ = gap.hi;
  } else {
    if (!MetaDataLog::isMetaDataLog(log_id_)) {
      WORKER_STAT_INCR(client.gaps_redelivery_attempted);
    }
  }
  adjustRedeliveryTimer(success);
  // Log DATALOSS gaps for investigation.
  if (success && type == GapType::DATALOSS) {
    gap_tracer_->traceGapDelivery(
        log_id_,
        type,
        lo,
        hi,
        start_lsn_,
        current_metadata_ ? current_metadata_->toString() : "[nullptr]",
        getDebugInfoStr(),
        unavailableShardsPretty(),
        currentEpoch(),
        trim_point_,
        readSetSize(),
        current_metadata_->replication.toString());
    epoch_t epoch = lsn_to_epoch(lo);
    if (epoch == lsn_to_epoch(hi)) {
      WORKER_STAT_ADD(
          client.records_lost, lsn_to_esn(hi).val_ - lsn_to_esn(lo).val_);
    }
  }
  return success ? 0 : -1;
}

void ClientReadStream::deliverAccessGapAndDispose() {
  ld_check(permission_denied_);

  if (deliverGap(GapType::ACCESS, next_lsn_to_deliver_, until_lsn_) == 0) {
    // Client received Gap record. ClientReadStream is no longer needed.
    deps_->dispose();
  }
}

void ClientReadStream::deliverNoConfigGapAndDispose() {
  RATELIMIT_WARNING(std::chrono::seconds(1),
                    10,
                    "Reading from log %lu but it is no longer in config!",
                    log_id_.val_);

  // Setting this flags guarantees we will try to issue that gap again if
  // deliverGap() fails.
  log_removed_from_config_ = true;

  if (deliverGap(GapType::NOTINCONFIG, next_lsn_to_deliver_, until_lsn_) == 0) {
    // Client received Gap record. ClientReadStream is no longer needed.
    deps_->dispose();
  }
}

void ClientReadStream::adjustRedeliveryTimer(bool delivery_success) {
  if (delivery_success) {
    resetRedeliveryTimer();
  } else {
    activateRedeliveryTimer();
  }
}

void ClientReadStream::activateRedeliveryTimer() {
  if (!redelivery_timer_) {
    redelivery_timer_ = deps_->createBackoffTimer(
        deps_->getSettings().client_initial_redelivery_delay,
        deps_->getSettings().client_max_redelivery_delay);
    redelivery_timer_->setCallback([this]() {
      if (readers_flow_tracer_) {
        readers_flow_tracer_->onRedeliveryTimerInactive();
      }
      redeliver();
    });
  }
  bool was_active = redelivery_timer_->isActive();
  redelivery_timer_->activate();
  if (readers_flow_tracer_ && !was_active) {
    readers_flow_tracer_->onRedeliveryTimerActive();
  }
}

void ClientReadStream::resetRedeliveryTimer() {
  if (redelivery_timer_) {
    bool was_active = redelivery_timer_->isActive();
    redelivery_timer_->reset();
    if (readers_flow_tracer_ && was_active) {
      readers_flow_tracer_->onRedeliveryTimerInactive();
    }
  }
}

void ClientReadStream::cancelRedeliveryTimer() {
  if (redelivery_timer_) {
    bool was_active = redelivery_timer_->isActive();
    redelivery_timer_->cancel();
    if (readers_flow_tracer_ && was_active) {
      readers_flow_tracer_->onRedeliveryTimerInactive();
    }
  }
}

void ClientReadStream::redeliver() {
  if (log_removed_from_config_) {
    deliverNoConfigGapAndDispose();
  } else if (permission_denied_) {
    deliverAccessGapAndDispose();
  } else if (last_epoch_with_metadata_ == EPOCH_INVALID) {
    // Read stream doesn't have any epoch metadata yet. Nothing to redeliver.
    return;
  } else {
    findGapsAndRecords(/*grace_period_expired=*/false,
                       /*redelivery_in_progress=*/true);
    disposeIfDone();
  }

  // findGapsAndRecords(), deliverAccessGapAndDispose() and
  // deliverNoConfigGapAndDispose() all reactivate the timer on failure.
}

void ClientReadStream::unlinkRecordState(lsn_t lsn, RecordState& rstate) {
  /*
   * When some record is delivered (@see handleRecord()) to the application,
   * each SenderState is moved from the list corresponding to buffer_->front()
   * to the list for next_lsn, assuming next_lsn_to_deliver_ < next_lsn. In this
   * case, |S_G| doesn't change. If next_lsn == next_lsn_to_deliver_, number of
   * shards in |S_G| has to be decremented by one. There's a special case where
   * next_lsn > window_high_ -- in this case, |S_G| remains the same, but the
   * SenderState needs to be unlinked if it was previously linked.
   */
  auto& list = rstate.list;
  for (auto it = list.begin(); it != list.end();) {
    SenderState& state = *it;
    it = list.erase(it);

    ld_check(state.getNextLsn() >= lsn);
    // linked SenderState must be the result of a previously sent record/gap
    ld_check(state.getGapState() != GapState::NONE);
    ld_check(!state.list_hook_.is_linked());

    if (state.getNextLsn() == lsn) {
      // no need to move state to some other list; just remove the shard from
      // |S_G|
      ld_assert((numShardsInState(GapState::GAP) +
                 numShardsInState(GapState::UNDER_REPLICATED)) > 0);
      setSenderGapState(state, GapState::NONE);
      continue;
    }

    // Next LSN at which GapState may change.
    lsn_t transition_lsn;
    if (lsn < state.under_replicated_until) {
      ld_check(state.getGapState() == GapState::UNDER_REPLICATED);
      transition_lsn = state.under_replicated_until;
    } else {
      if (state.getGapState() == GapState::UNDER_REPLICATED) {
        // Not underreplicated anymore.
        setSenderGapState(state, GapState::GAP);
      }
      transition_lsn = state.getNextLsn();
    }

    if (transition_lsn <= window_high_) {
      RecordState* nstate = buffer_->createOrGet(transition_lsn);
      // transition_lsn is in the window, so it should fit in the buffer
      ld_check(nstate != nullptr);
      ld_check(nstate != &rstate);
      nstate->list.push_back(state);
      nstate->gap = true;
    } else {
      // Storage shard will not send anything to us in the current window,
      // its SenderState should be kept unlinked, while its GapState should
      // remain GapState::GAP/UNDER_REPLICATED.
    }
  }
}

size_t ClientReadStream::numShardsInState(GapState st) const {
  if (gap_failure_domain_ == nullptr) {
    return 0;
  }

  return gap_failure_domain_->countShards(st);
}

size_t
ClientReadStream::numShardsInAuthoritativeStatus(AuthoritativeStatus st) const {
  if (gap_failure_domain_ == nullptr) {
    return 0;
  }

  return gap_failure_domain_->numShards(st);
}

size_t ClientReadStream::numShardsNotFullyAuthoritative() const {
  return numShardsInAuthoritativeStatus(AuthoritativeStatus::UNDERREPLICATION) +
      numShardsInAuthoritativeStatus(AuthoritativeStatus::AUTHORITATIVE_EMPTY);
}

void ClientReadStream::calcNextLSNToSlideWindow() {
  // NOTE: this bound is exclusive; we'll slide the window *after* delivering
  // `next_lsn_to_deliver_' to the application
  size_t delta = flow_control_threshold_ * (window_size_ - 1);
  delta = std::min(delta, LSN_MAX - next_lsn_to_deliver_);
  next_lsn_to_slide_window_ = next_lsn_to_deliver_ + delta;

  ld_check(next_lsn_to_slide_window_ >= next_lsn_to_deliver_);
  ld_check(next_lsn_to_slide_window_ <= window_high_ ||
           next_lsn_to_slide_window_ > until_lsn_);
}

void ClientReadStream::calcWindowHigh() {
  // Set the right side of the window by adding window_size_ to
  // the next LSN to deliver, unless until_lsn_ is lesser in which case the
  // window size is effectively reduced
  window_high_ = (next_lsn_to_deliver_ < until_lsn_ - window_size_ + 1)
      ? next_lsn_to_deliver_ + window_size_ - 1
      : until_lsn_;
}

bool ClientReadStream::canAcceptRecord(lsn_t lsn) const {
  return lsn <= window_high_;
}

void ClientReadStream::updateServerWindow() {
  server_window_.low = next_lsn_to_deliver_;
  server_window_.high = window_high_;
  ld_spew("Log=%lu server window is now [%s, %s]",
          log_id_.val_,
          lsn_to_string(server_window_.low).c_str(),
          lsn_to_string(server_window_.high).c_str());
}

void ClientReadStream::updateWindowSize() {
  if (deps_->hasMemoryPressure()) {
    // cut the window size in half
    window_size_ = std::max(size_t(1), window_size_ / 2);
  } else {
    // increment window size but not more than what the buffer can hold
    window_size_ = std::min(buffer_->capacity(), window_size_ + 1);
  }
}

bool ClientReadStream::slideSenderWindows() {
  // This function should leave everything in a consistent state.
  SCOPE_EXIT {
    checkConsistency();
  };

  if (!(next_lsn_to_deliver_ <= until_lsn_ &&
        next_lsn_to_deliver_ > next_lsn_to_slide_window_ &&
        until_lsn_ > window_high_)) {
    return false;
  }

  updateWindowSize();
  calcWindowHigh();
  calcNextLSNToSlideWindow();

  if (reader_) {
    // Let onReaderProgress() know that it needs to send WINDOW messages
    window_update_pending_ = true;
    if (readers_flow_tracer_) {
      readers_flow_tracer_->onWindowUpdatePending();
    }
  } else {
    updateServerWindow();
    scd_->onWindowSlid(server_window_.high, filter_version_);
  }

  // The window has changed and the current value of gap_end_outside_window_
  // should be invalidated. It will be recomputed in the following
  // updateGapState() calls.
  gap_end_outside_window_ = LSN_INVALID;

  // If we're in all send all mode, we can switch back to SCD, unless all shards
  // are down/underreplicated/non-authoritative. Let's check that.
  bool want_to_switch_to_scd =
      !force_no_scd_ && log_uses_scd_ && !scd_->isActive();
  size_t known_down = 0;

  // Now that window has changed, rebuild the GapState and associated
  // SenderState according to the new window using `state.getNextLsn()'.
  // `state.getNextLsn()' tells us the highest LSN of a record or gap that this
  // storage shard has already sent us. We can be sure that the shard will not
  // send any records before `next_lsn' so we call updateGapState() with that
  // LSN. If the information is still interesting, updateGapState() will
  // update gap handling parameters appropriately.
  for (auto& it : storage_set_states_) {
    SenderState& state = it.second;

    if (want_to_switch_to_scd) {
      auto conn_state = state.getConnectionState();
      if (conn_state == ConnectionState::RECONNECT_PENDING ||
          conn_state == ConnectionState::PERSISTENT_ERROR ||
          state.getAuthoritativeStatus() !=
              AuthoritativeStatus::FULLY_AUTHORITATIVE ||
          state.should_blacklist_as_under_replicated) {
        ++known_down;
      }
    }

    if (!reader_) {
      sendWindowMessage(state);
    }

    if (state.getGapState() != GapState::NONE) {
      setSenderGapState(state, GapState::NONE);
    }

    updateGapState(state.getNextLsn(), state);
  }

  if (want_to_switch_to_scd && known_down < storage_set_states_.size()) {
    // After we did failover to all send all mode, we eventually
    // recover to single copy delivery mode when we slide the senders' window.
    if (log_uses_local_scd_) {
      ld_debug("Switching to local scd for log:%lu, id:%lu",
               log_id_.val(),
               id_.val());
      scd_->scheduleRewindToMode(ClientReadStreamScd::Mode::LOCAL_SCD,
                                 "Switching to LOCAL_SCD: sliding window");
    } else {
      ld_debug(
          "Switching to scd for log:%lu, id:%lu", log_id_.val(), id_.val());
      scd_->scheduleRewindToMode(
          ClientReadStreamScd::Mode::SCD, "Switching to SCD: sliding window");
    }
  }

  return true;
}

void ClientReadStream::sendWindowMessage(SenderState& state) {
  // Don't send WINDOW if we're going to send START soon.
  if (rewind_scheduler_->isScheduled()) {
    return;
  }

  // If we are in the READING state, send out the WINDOW message now.
  //
  // If we are in an error state (RECONNECT_PENDING or PERSISTENT_ERROR),
  // resuming reading will involve sending a new START message with an
  // uptodate window.
  //
  // If we are in one of the other states (START is in flight), we will
  // send a WINDOW message in onStarted() when we enter READING.
  const AuthoritativeStatus auth_status = state.getAuthoritativeStatus();
  if (state.getConnectionState() == ConnectionState::READING &&
      (auth_status == AuthoritativeStatus::FULLY_AUTHORITATIVE ||
       auth_status == AuthoritativeStatus::UNAVAILABLE)) {
    if (server_window_.low > server_window_.high) {
      // This can happen if a gap ending with until_lsn is detected (in that
      // case, next_lsn_to_deliver_ is updated, but not window_high_). We're
      // done reading, so there's no need to send a window update.
      ld_check(server_window_.low > until_lsn_);

      state.resetRetryWindowTimer();
      return;
    }

    if (server_window_.high == state.getWindowHigh()) {
      // Server already has an uptodate view of the window.  This usually
      // happens right after we START; onStarted() calls this method in case the
      // window has changed while the shard was processing the START.  But if
      // not, there is nothing to do.
      state.resetRetryWindowTimer();
      return;
    }

    ld_check(server_window_.high >= state.getWindowHigh());

    int rv = deps_->sendWindowMessage(
        state.getShardID(), server_window_.low, server_window_.high);
    if (rv == 0) {
      state.resetRetryWindowTimer();
      state.setWindowHigh(server_window_.high);
    } else {
      state.activateRetryWindowTimer();
    }
  }
}

bool ClientReadStream::canSkipPartiallyTrimmedSection() const {
  return !do_not_skip_partially_trimmed_sections_ &&
      trim_point_ != LSN_INVALID && trim_point_ >= next_lsn_to_deliver_;
}

int ClientReadStream::deliverFastForwardGap(GapType type, lsn_t next_lsn) {
  const epoch_t gap_epoch = lsn_to_epoch(next_lsn);
  if (gap_epoch > last_epoch_with_metadata_) {
    requestEpochMetaData(epoch_t(last_epoch_with_metadata_.val_ + 1));
    return -1;
  }

  ld_check(next_lsn > LSN_OLDEST);
  lsn_t hi = next_lsn - 1;
  int rv = deliverGap(type, next_lsn_to_deliver_, std::min(until_lsn_, hi));
  if (rv != 0) {
    // We'll try again later when redeliver() is called.
    return rv;
  }

  // Clear the record state up to hi. Otherwise advanceBufferHead() call below
  // will complain. Also, we want clearRecordState() to be called for each slot
  // to maintain gap accounting.
  namespace arg = std::placeholders;
  buffer_->forEachUpto(
      hi,
      std::bind(&ClientReadStream::clearRecordState, this, arg::_1, arg::_2));

  buffer_->advanceBufferHead(next_lsn - next_lsn_to_deliver_);
  ld_check(buffer_->getBufferHead() == next_lsn);
  next_lsn_to_deliver_ = next_lsn;

  // If the window needs to be changed, slideSenderWindows() will be changed
  // which will recompute gap_end_outside_window_.

  return 0;
}

void ClientReadStream::updateLastReleased(lsn_t last_released_lsn) {
  if (last_released_lsn > last_released_) {
    last_released_ = last_released_lsn;
  }
}

ClientReadStream::~ClientReadStream() {
  if (started_) {
    WORKER_STAT_DECR(client.num_read_streams);
  }

  // Not safe to destroy while executing a callback
  ld_check(!inside_callback_);

  // Send STOP messages to any storage shards that are still sending.  This is
  // best-effort and not strictly necessary since, if storage shards keep
  // sending us data for a deleted read stream, we will respond with STOPs.
  for (auto& entry : storage_set_states_) {
    SenderState& state = entry.second;
    if (state.getConnectionState() == ConnectionState::CONNECTING ||
        state.getConnectionState() == ConnectionState::START_SENT ||
        state.getConnectionState() == ConnectionState::READING) {
      deps_->sendStopMessage(state.getShardID());
    }
  }
  storage_set_states_.clear();
}

// Used in tests only.
void ClientReadStream::overrideConnectionState(ShardID shard,
                                               ConnectionState state) {
  auto it = storage_set_states_.find(shard);
  ld_check(it != storage_set_states_.end());

  it->second.setConnectionState(state);

  // overrideConnectionState is used only
  // to simulate a change of connection state.
  // We want to enforce this change immediately
  connection_health_tracker_->recalculate(
      state != ConnectionState::READING /* grace_period_expired */);
}

// Used in tests only.
void ClientReadStream::reconnectTimerCallback(ShardID shard) {
  auto it = storage_set_states_.find(shard);
  ld_check(it != storage_set_states_.end());
  it->second.reconnectTimerCallback();
}

// Used in tests only.
bool ClientReadStream::reconnectTimerIsActive(ShardID shard) const {
  auto it = storage_set_states_.find(shard);
  ld_check(it != storage_set_states_.end());
  return it->second.reconnectTimerIsActive();
}

// Used in tests only.
void ClientReadStream::startedTimerCallback(ShardID shard) {
  auto it = storage_set_states_.find(shard);
  ld_check(it != storage_set_states_.end());
  it->second.startedTimerCallback();
}

void ClientReadStream::onConnectionFailure(SenderState& state, Status st) {
  state.resetRetryWindowTimer();
  state.resetStartedTimer();

  if (st == E::ACCESS) {
    if (!permission_denied_) {
      permission_denied_ = true;
      deliverAccessGapAndDispose();
    }
    // If status is E::ACCESS, do not reactive Reconnect Timer
    return;
  }

  // There are several situations:
  // 1/ SCD is active and addToShardsDownAndScheduleRewind() returns true,
  //    meaning a rewind has been scheduled to add this node in the known down
  //    list, do nothing;
  // 2/ SCD is active but addToShardsDownAndScheduleRewind() returned false
  //    because this node is already in known down list. Activate the reconnect
  //    timer so that at least we try re-connecting to that node;
  // 3/ SCD is not active, there is no rewind to do, just activate the reconnect
  //    timer.
  if (!scd_->isActive() || st == E::SSLREQUIRED ||
      !scd_->addToShardsDownAndScheduleRewind(
          state.getShardID(),
          folly::format("{} added to known down list because we cannot connect "
                        "to it, error {}",
                        state.getShardID().toString(),
                        error_name(st))
              .str())) {
    state.activateReconnectTimer();
  }
}

void ClientReadStream::updateGraceCounters() {
  // Bump up grace_counters for all shards that are not in GAP state.
  for (auto& state : storage_set_states_) {
    state.second.grace_counter +=
        (state.second.getGapState() == GapState::NONE);
  }
}

bool ClientReadStream::shouldWaitForGracePeriod(
    FmajorityResult fmajority_result,
    bool grace_period_expired) const {
  if (grace_period_expired || (grace_period_ == nullptr)) {
    // Grace period already expired or grace period is 0. Do not wait.
    return false;
  }

  if (fmajority_result != FmajorityResult::AUTHORITATIVE_INCOMPLETE ||
      trim_point_ >= next_lsn_to_deliver_) {
    // No authoritative incomplete f-majority, or record has already been
    // trimmed. Do not wait.
    return false;
  }

  // We have an f-majority, but it is incomplete.
  const auto grace_counter_limit = deps_->getSettings().grace_counter_limit;
  if (grace_counter_limit < 0) {
    // grace_counter_limit is negative. Fall back to pre-T13689030 logic.
    return true;
  }

  // Wait if there is at least one non-disgraced, fully authoritative shard that
  // is not in GAP state. Any such shard may have a record and a reasonable
  // chance of sending it.
  return std::any_of(storage_set_states_.begin(),
                     storage_set_states_.end(),
                     [grace_counter_limit](const auto& state) {
                       const auto& sender_state = state.second;
                       return sender_state.getGapState() == GapState::NONE &&
                           sender_state.isFullyAuthoritative() &&
                           sender_state.grace_counter <= grace_counter_limit;
                     });
}

void ClientReadStream::handleStartPROTONOSUPPORT(ShardID shard_id) {
  folly::Optional<uint16_t> proto =
      deps_->getSocketProtocolVersion(shard_id.node());
  // Assuming a socket to the server exists, which seems reasonable if we just
  // got a PROTONOSUPPORT error.
  ld_check(proto.hasValue());
  if (proto.value() < coordinated_proto_) {
    RATELIMIT_INFO(
        std::chrono::seconds(10),
        1,
        "%s sent STARTED with E::PROTONOSUPPORT, downgrading coordinated "
        "protocol version from %u to %u and rewinding",
        shard_id.toString().c_str(),
        coordinated_proto_,
        proto.value());
    // Schedule a rewind in a separate libevent event. That rewind will use an
    // older protocol.
    coordinated_proto_ = proto.value();
    scheduleRewind(folly::format("{} sent STARTED with E::PROTONOSUPPORT "
                                 "({} < {})",
                                 shard_id.toString().c_str(),
                                 proto.value(),
                                 coordinated_proto_)
                       .str());
  } else {
    // We already know the protocol version is lower than what we had tried
    // to send to this shard. It's probably a delayed STARTED reply and we
    // have already sent another; ignore.
    RATELIMIT_INFO(std::chrono::seconds(10),
                   1,
                   "%s sent STARTED with E::PROTONOSUPPORT but the coordinated "
                   "protocol version %u is already >= the socket's "
                   "version %u.  Probably a stale reply.",
                   shard_id.toString().c_str(),
                   coordinated_proto_,
                   proto.value());
  }
}

void ClientReadStream::scheduleRewind(std::string reason) {
  ld_check(!reason.empty());
  rewind_scheduler_->schedule(nullptr, std::move(reason));
}

void ClientReadStream::rewind(std::string reason) {
  // Clear the buffer and reset gap parameters.

  events_tracer_->traceEvent(log_id_,
                             deps_->getReadStreamID(),
                             ClientReadStreamTracer::Events::REWIND,
                             reason,
                             start_lsn_,
                             until_lsn_,
                             last_delivered_lsn_,
                             last_in_record_ts_,
                             last_received_ts_,
                             epoch_metadata_str_factory_,
                             unavailable_shards_str_factory_,
                             currentEpoch(),
                             trim_point_,
                             readSetSize());
  ld_debug("Rewinding stream for log %lu: %s", log_id_.val(), reason.c_str());

  for (auto it = storage_set_states_.begin(); it != storage_set_states_.end();
       ++it) {
    resetGapParametersForSender(it->second);
  }

  ld_assert(numShardsInState(GapState::GAP) == 0);
  ld_assert(numShardsInState(GapState::UNDER_REPLICATED) == 0);
  ld_check(scd_->getGapShardsFilteredOut() == 0);
  ld_check(scd_->getUnderReplicatedShardsNotBlacklisted() == 0);

  // clear the entire read stream buffer
  buffer_->clear();

  gap_end_outside_window_ = LSN_INVALID;

  // Doing this prevents any data record sent by all storage nodes to be
  // considered until these storage nodes respond with a STARTED message that
  // contains the same filter_version.
  ++filter_version_.val_;

  bool was_in_all_send_all = !scd_ || !scd_->isActive();

  if (scd_) {
    // This function may transition us between SCD and ALL_SEND_ALL mode and/or
    // change the filtered out list.
    scd_->applyScheduledChanges();
  }

  for (auto& it : storage_set_states_) {
    SenderState& state = it.second;
    state.blacklist_state = SenderState::BlacklistState::NONE;

    if (scd_ && was_in_all_send_all && scd_->isActive()) {
      // Rewinding from all send all to scd. Known down list was cleared.
      // Let's repopulate it before rewinding, to avoid having to rewind again
      // immediately in common cases.
      // (It would probably be better to not clear known down list in the first
      // place, and keep it up to date while in all-send-all mode. I'm not sure
      // why it's not done that way. It may also make sense to make
      // ClientReadStreamSenderState the source of truth for whether the shard
      // should be in known down list or not, instead of having
      // addToShardsDownAndScheduleRewind() sprinkled everywhere.
      // This is a mess.)

      // Blacklist if we couldn't connect to the node.
      auto conn_state = state.getConnectionState();
      if (conn_state == ConnectionState::RECONNECT_PENDING ||
          conn_state == ConnectionState::PERSISTENT_ERROR) {
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::sformat("{} re-added to known down list because we couldn't "
                           "connect to it",
                           state.getShardID().toString()));
      }

      // Blacklist if the shard is not fully authoritative.
      // The applyShardStatus() below will also blacklist if it changes
      // authoritative status to not fully authoritative.
      if (state.getAuthoritativeStatus() !=
          AuthoritativeStatus::FULLY_AUTHORITATIVE) {
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::sformat("{} re-added to known down list because it's not "
                           "fully authoritative",
                           state.getShardID().toString()));
      }

      // Blacklist if the node reported underreplication.
      if (state.should_blacklist_as_under_replicated) {
        scd_->addToShardsDownAndScheduleRewind(
            state.getShardID(),
            folly::sformat("{} re-added to known down list because it has an "
                           "under replicated region",
                           state.getShardID().toString()));
      }

      // We could also blacklist if we received STARTED with E::REBUILDING,
      // but in this case the shard is usually also not fully authoritative in
      // event log, so we skip this case for simplicity.
    }

    // Re-apply shard statuses from event log, removing any overrides we did
    // based on STARTED messages. These overrides are going to be obsolete soon
    // because we'll send new START messages in a moment.

    // First reset connection state because applyShardStatus() skips shards
    // in READING state.
    state.setConnectionState(
        ClientReadStreamSenderState::ConnectionState::CONNECTING);
    // Use try_make_progress=false because there's no progress to be made
    // since we're resetting all sender states.
    applyShardStatus("rewind", &it.second, /* try_make_progress */ false);
  }

  // Apply scheduled SCD changes *again* after because the code above could
  // schedule adding more shards to known down list on next rewind.
  // Since we're doing a rewind already, just pick up those changes now and
  // cancel the scheduled rewind.
  if (scd_) {
    scd_->applyScheduledChanges();
  }
  rewind_scheduler_->cancel();

  if (scd_ && scd_->isActive()) {
    for (const auto& shard_id : scd_->getShardsSlow()) {
      storage_set_states_.at(shard_id).blacklist_state =
          SenderState::BlacklistState::SLOW;
    }
    for (const auto& shard_id : scd_->getShardsDown()) {
      storage_set_states_.at(shard_id).blacklist_state =
          SenderState::BlacklistState::DOWN;
    }
  }

  if (scd_ && scd_->isActive()) {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "Rewinding read stream for log %lu in SCD mode with version "
        "%lu. LSN: %s, metadata effective since e%u. Reason: %s.",
        log_id_.val_,
        filter_version_.val_,
        lsn_to_string(next_lsn_to_deliver_).c_str(),
        current_metadata_ ? current_metadata_->h.effective_since.val() : 0u,
        reason.c_str());
  } else {
    RATELIMIT_INFO(
        std::chrono::seconds(1),
        10,
        "Rewinding read stream for log %lu in ALL_SEND_ALL mode "
        "with version %lu. LSN: %s, metadata effective since e%u. "
        "Reason: %s.",
        log_id_.val_,
        filter_version_.val_,
        lsn_to_string(next_lsn_to_deliver_).c_str(),
        current_metadata_ ? current_metadata_->h.effective_since.val() : 0u,
        reason.c_str());
  }
  RATELIMIT_INFO(std::chrono::seconds(10),
                 1,
                 "Log %lu, Debug info: %s.",
                 log_id_.val_,
                 getDebugInfoStr().c_str());

  // Tell storage nodes to rewind and update the known down list.
  // It is possible sendStart() will schedule another rewind.
  for (auto& it : storage_set_states_) {
    sendStart(it.second.getShardID(), it.second);
  }
  scd_->onWindowSlid(server_window_.high, filter_version_);
}

int ClientReadStream::setGapStateFilteredOut(lsn_t start_lsn,
                                             lsn_t end_lsn,
                                             SenderState& state) {
  if (end_lsn + 1 <= next_lsn_to_deliver_ || end_lsn + 1 < state.getNextLsn()) {
    return -1;
  }

  if (permission_denied_) {
    // Ignore, invalid permissions
    return -1;
  }

  if (end_lsn < start_lsn) {
    RATELIMIT_ERROR(
        std::chrono::seconds(1),
        3,
        "start_lsn: %s, end_lsn: %s end_lsn should not be less than start_lsn",
        lsn_to_string(start_lsn).c_str(),
        lsn_to_string(end_lsn).c_str());
    return -1;
  }

  if (!canAcceptRecord(end_lsn)) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    3,
                    "Got LSN %s from shard %s but can only accept up "
                    "to %s.  This should not be possible due to "
                    "flow control.",
                    lsn_to_string(end_lsn).c_str(),
                    state.getShardID().toString().c_str(),
                    lsn_to_string(window_high_).c_str());
    return -1;
  }

  for (lsn_t i = std::max(start_lsn, next_lsn_to_deliver_); i <= end_lsn; i++) {
    RecordState* rstate = buffer_->createOrGet(i);
    rstate->filtered_out = true;
  }

  // "Filtered out" can be viewed as we have read that record. But we do not
  // necessarily need that record. So we need to update max_data_record_lsn
  // and highest_record_lsn_ as well.
  state.max_data_record_lsn = std::max(state.max_data_record_lsn, end_lsn);
  state.grace_counter = 0;
  highest_record_lsn_ = std::max(highest_record_lsn_, end_lsn);

  return 0;
}

size_t ClientReadStream::getBytesBuffered() const {
  return bytes_buffered_;
}

//
// Production implementations of ClientReadStreamDependencies methods
//

ClientReadStreamDependencies::ClientReadStreamDependencies() = default;

ClientReadStreamDependencies::~ClientReadStreamDependencies() {}

bool ClientReadStreamDependencies::hasMemoryPressure() const {
  // TODO(T6159466): implement memory limits/accounting
  return false;
}

bool ClientReadStreamDependencies::getMetaDataForEpoch(
    read_stream_id_t rsid,
    epoch_t epoch,
    MetaDataLogReader::Callback cb,
    bool allow_from_cache,
    bool require_consistent_from_cache) {
  Worker* w = Worker::onThisThread();
  ld_check(w);

  // If the read stream is used to read a metadata log, simply return its
  // meta storage set and replication factor from the Configuration. Such
  // information will never change and it is safe to set the until_ epoch
  // to EPOCH_MAX so that it will never be called again for the read stream
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    const auto& nodes_configuration = w->getNodesConfiguration();

    std::unique_ptr<EpochMetaData> metadata = std::make_unique<EpochMetaData>(
        EpochMetaData::genEpochMetaDataForMetaDataLog(
            log_id_, *nodes_configuration));
    ld_check(metadata->isValid());

    cb(E::OK,
       {log_id_,
        epoch,
        /*until=*/lsn_to_epoch(LSN_MAX),
        MetaDataLogReader::RecordSource::LAST,
        compose_lsn(epoch, esn_t(1)),
        std::chrono::milliseconds(0),
        std::move(metadata)});
    return true;
  }

  if (allow_from_cache && metadata_cache_ != nullptr) {
    metadata_cached_ = std::make_unique<EpochMetaData>();
    epoch_t until = EPOCH_INVALID;
    MetaDataLogReader::RecordSource source;
    if (metadata_cache_->getMetaData(log_id_,
                                     epoch,
                                     &until,
                                     metadata_cached_.get(),
                                     &source,
                                     require_consistent_from_cache)) {
      // Cache hit

      // metadata must from a cached source
      ld_check(MetaDataLogReader::isCachedSource(source));
      // if consistent record is required, should always return
      // CACHED_CONSISTENT
      ld_check(!require_consistent_from_cache ||
               source == MetaDataLogReader::RecordSource::CACHED_CONSISTENT);

      auto callback = [this, epoch, until, source, cb] {
        cb(E::OK,
           {log_id_,
            epoch,
            until,
            source,
            compose_lsn(epoch, esn_t(1)),
            std::chrono::milliseconds(0),
            std::move(metadata_cached_)});
      };

      if (delivery_timer_ == nullptr) {
        delivery_timer_ = std::make_unique<Timer>(std::move(callback));
      } else {
        delivery_timer_->setCallback(std::move(callback));
      }

      // deliver the metadata on the next event loop iteration to avoid
      // recursively calling findGapsAndRecords()
      delivery_timer_->activate(std::chrono::microseconds(0));

      ld_debug("Got epoch metadata from client cache for epoch %u of log %lu. "
               "metadata epoch %u, effective until %u, metadata: %s",
               epoch.val_,
               log_id_.val_,
               metadata_cached_->h.epoch.val_,
               until.val_,
               metadata_cached_->toString().c_str());
      return false;
    }

    // cache miss, continue to read the metadata log
  }

  if (nodeset_finder_) {
    // Although quite unlikely, there are cases that the ClientReadStream may
    // request epoch metadata for a later epoch while a previous request for a
    // smaller epoch is still in flight. One example scenario:
    //  1) the read stream reading epoch _e_ is performing f-majority-based gap
    //     detection for a potential bridge gap to _e_+1 while the metadata of
    //     epoch _e_ is still unknown.
    //  2) read stream start to request epoch metadata for epoch _e_
    //  3) while the metadata request is still in flight, the read stream
    //     receives a bridge record in _e_. The reason why the brige record
    //     is not part of the f-majority in step 1 could be either 1) the
    //     bridge record is under-replicated; or 2) the read stream had a
    //     non-authoritative f-majority in step 1 and the copyset for the
    //     bridge record is not part of it.
    //  4) the bridge record advances the epoch to _e_+1 regardless of the
    //     f-majority results, and the read stream starts requesting epoch
    //     in _e_+1 while request for _e_ is still in flight
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      10,
                      "Requesting epoch metadata for log %lu epoch %u rsid %lu "
                      "while a previous metadata request is still "
                      "in flight. Cancelling the previous request.",
                      log_id_.val_,
                      epoch.val_,
                      rsid.val_);
    nodeset_finder_.reset();
  }

  // a callback object which essentially wraps the ClientReadStream callback
  // passed in.
  auto cb_wrapper = [this, epoch, cb](Status st) {
    MetaDataLogReader::Result result;
    if (st == E::OK) {
      auto map = nodeset_finder_->getResult();
      epoch_t until;
      std::unique_ptr<EpochMetaData> metadata;
      epoch_t effective_until = map->getEffectiveUntil();
      if (epoch > effective_until) {
        RATELIMIT_INFO(std::chrono::seconds(10),
                       10,
                       "Requested epoch metadata for log %lu epoch %u is "
                       "not available. Highest epoch known is %u.",
                       log_id_.val(),
                       epoch.val(),
                       effective_until.val());
        // The requested epoch does not exist or is not released yet. Return the
        // last known epoch. The client read stream will use that to start
        // reading and will re-request epoch metadata later.
        until = epoch;
        effective_until = epoch;
        metadata = map->getLastEpochMetaData();
      } else {
        auto it = map->find(epoch);
        ld_check(it != map->end());
        auto entry = *it;
        until = entry.first.second;
        metadata = std::make_unique<EpochMetaData>(entry.second);

        // Here we can store higher epochs into the cache to avoid future
        // requests if possible.
        ++it;
        for (; it != map->end(); ++it) {
          auto elem = *it;
          updateEpochMetaDataCache(
              elem.first.first,
              elem.first.second,
              elem.second,
              MetaDataLogReader::RecordSource::CACHED_CONSISTENT);
        }
      }

      result = {log_id_,
                epoch,
                until,
                (until == effective_until)
                    ? MetaDataLogReader::RecordSource::LAST
                    : MetaDataLogReader::RecordSource::NOT_LAST,
                // Metadata record LSN and timestamp are not available through
                // NodeSetFinder. Pass LSN_INVALID and 0 instead.
                LSN_INVALID,
                std::chrono::milliseconds(0),
                std::move(metadata)};
    } else {
      RATELIMIT_ERROR(std::chrono::seconds(10),
                      10,
                      "Failed to get epoch metadata for log %lu epoch %u: "
                      "%s: %s",
                      log_id_.val(),
                      epoch.val(),
                      error_name(st),
                      error_description(st));
      result = {log_id_,
                epoch,
                EPOCH_MAX,
                MetaDataLogReader::RecordSource::LAST,
                LSN_INVALID,
                std::chrono::milliseconds(0),
                nullptr};
    }

    // Clear the nodeset_finder_ while keeping the object alive to avoid
    // ASAN failures. It will be destroyed after the callback returned.
    std::unique_ptr<NodeSetFinder> nf;
    nodeset_finder_.swap(nf);
    cb(st, std::move(result));
  };

  nodeset_finder_ = std::make_unique<NodeSetFinder>(
      log_id_,
      MAX_RETRY_READ_METADATA_DELAY,
      std::move(cb_wrapper),
      getSettings().read_streams_use_metadata_log_only
          ? NodeSetFinder::Source::METADATA_LOG
          : NodeSetFinder::Source::BOTH);

  nodeset_finder_->start();
  return false;
}

void ClientReadStreamDependencies::updateEpochMetaDataCache(
    epoch_t epoch,
    epoch_t until,
    const EpochMetaData& metadata,
    MetaDataLogReader::RecordSource source) {
  ld_check(MetaDataLogReader::isCachedSource(source));
  if (metadata_cache_ != nullptr) {
    metadata_cache_->setMetaData(log_id_, epoch, until, source, metadata);
  }
}

int ClientReadStreamDependencies::sendStartMessage(
    ShardID shard,
    SocketCallback* onclose,
    START_Header header,
    const small_shardset_t& filtered_out,
    const ReadStreamAttributes* attrs) {
  auto w = Worker::onThisThread();
  ld_check(w);
  ld_check(header.start_lsn <= header.until_lsn);
  ld_check(header.window_high <= header.until_lsn);
  // Fill in the boilerplate fields
  header.log_id = log_id_;
  header.read_stream_id = read_stream_id_;

  auto msg = std::make_unique<START_Message>(
      header, filtered_out, attrs, client_session_id_);
  return w->sender().sendMessage(std::move(msg), shard.asNodeID(), onclose);
}

int ClientReadStreamDependencies::sendStopMessage(ShardID shard) {
  auto w = Worker::onThisThread();
  ld_check(w);

  STOP_Header header;
  header.log_id = log_id_;
  header.read_stream_id = read_stream_id_;
  header.shard = shard.shard();

  // Worker is shutting down skip the shutdown message.
  if (!w->isAcceptingWork()) {
    return 0;
  }

  auto msg = std::make_unique<STOP_Message>(header);
  return w->sender().sendMessage(std::move(msg), shard.asNodeID());
}

int ClientReadStreamDependencies::sendWindowMessage(ShardID shard,
                                                    lsn_t window_low,
                                                    lsn_t window_high) {
  auto w = Worker::onThisThread();
  ld_check(w);

  WINDOW_Header header;
  header.log_id = log_id_;
  header.read_stream_id = read_stream_id_;
  header.sliding_window.low = window_low;
  header.sliding_window.high = window_high;
  header.shard = shard.shard();

  ld_check(window_low <= window_high);

  auto msg = std::make_unique<WINDOW_Message>(header);
  return w->sender().sendMessage(std::move(msg), shard.asNodeID());
}

void ClientReadStreamDependencies::dispose() {
  Worker::onThisThread()->clientReadStreams().erase(read_stream_id_);
}

std::unique_ptr<BackoffTimer> ClientReadStreamDependencies::createBackoffTimer(
    const chrono_expbackoff_t<std::chrono::milliseconds>& settings) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(

      std::function<void()>(), // SenderState will change
      settings);

  return std::move(timer);
}

std::unique_ptr<BackoffTimer> ClientReadStreamDependencies::createBackoffTimer(
    std::chrono::milliseconds initial_delay,
    std::chrono::milliseconds max_delay) {
  auto settings =
      chrono_expbackoff_t<std::chrono::milliseconds>(initial_delay, max_delay);
  return createBackoffTimer(std::move(settings));
}

void ClientReadStreamDependencies::updateBackoffTimerSettings(
    std::unique_ptr<BackoffTimer>& timer,
    const chrono_expbackoff_t<std::chrono::milliseconds>& settings) {
  if (timer) {
    auto timer_ptr = dynamic_cast<ExponentialBackoffTimer*>(&*timer);
    ld_check(timer_ptr);
    timer_ptr->updateSettings(settings);
  }
}

void ClientReadStreamDependencies::updateBackoffTimerSettings(
    std::unique_ptr<BackoffTimer>& timer,
    std::chrono::milliseconds initial_delay,
    std::chrono::milliseconds max_delay) {
  auto settings =
      chrono_expbackoff_t<std::chrono::milliseconds>(initial_delay, max_delay);
  updateBackoffTimerSettings(timer, settings);
}

std::chrono::milliseconds
ClientReadStreamDependencies::computeGapGracePeriod() const {
  using std::chrono::milliseconds;
  milliseconds ggp{0};
  if (MetaDataLog::isMetaDataLog(log_id_)) {
    ggp = std::max(ggp, getSettings().metadata_log_gap_grace_period);
  } else {
    ggp = std::max(ggp, getSettings().data_log_gap_grace_period);
  }
  if (ggp == milliseconds::zero()) {
    ggp = getSettings().gap_grace_period;
  }
  return ggp;
}

std::unique_ptr<Timer>
ClientReadStreamDependencies::createTimer(std::function<void()> cb) {
  auto timer = std::make_unique<Timer>(cb);
  return timer;
}

std::function<ClientReadStream*(read_stream_id_t)>
ClientReadStreamDependencies::getStreamByIDCallback() {
  return [](read_stream_id_t rsid) {
    return Worker::onThisThread()->clientReadStreams().getStream(rsid);
  };
}

const Settings& ClientReadStreamDependencies::getSettings() const {
  return Worker::settings();
}

ShardAuthoritativeStatusMap
ClientReadStreamDependencies::getShardStatus() const {
  return Worker::onThisThread()
      ->shardStatusManager()
      .getShardAuthoritativeStatusMap();
}

void ClientReadStreamDependencies::refreshClusterState() {
  auto* cluster_state = Worker::getClusterState();
  // ClusterState might be nullptr if the tier does not support this feature
  // yet.
  if (cluster_state) {
    cluster_state->refreshClusterStateAsync();
  }
}

folly::Optional<uint16_t>
ClientReadStreamDependencies::getSocketProtocolVersion(node_index_t nid) const {
  return Worker::onThisThread()->sender().getSocketProtocolVersion(nid);
}

ClientID
ClientReadStreamDependencies::getOurNameAtPeer(node_index_t node_index) const {
  Worker* w = Worker::onThisThread(false);
  return w ? w->sender().getOurNameAtPeer(node_index) : ClientID::INVALID;
}

void bumpGapStat(logid_t logid, StatsHolder* stats, GapType gap_type) {
  const bool metadata_log = MetaDataLog::isMetaDataLog(logid);
  switch (gap_type) {
#define HANDLE_TYPE(type)                               \
  case GapType::type:                                   \
    if (metadata_log) {                                 \
      STAT_INCR(stats, client.metadata_log_gap_##type); \
    } else {                                            \
      STAT_INCR(stats, client.gap_##type);              \
    }                                                   \
    break;

    HANDLE_TYPE(UNKNOWN)
    HANDLE_TYPE(BRIDGE)
    HANDLE_TYPE(HOLE)
    HANDLE_TYPE(DATALOSS)
    HANDLE_TYPE(TRIM)
    HANDLE_TYPE(ACCESS)
    HANDLE_TYPE(NOTINCONFIG)
    HANDLE_TYPE(FILTERED_OUT)

#undef HANDLE_TYPE

    case GapType::MAX:
      ld_check(false);
      break;
  }
  static_assert(
      static_cast<int>(GapType::MAX) == 8, "keep in sync with GapType");
}

bool ClientReadStream::isStreamStuckFor(std::chrono::milliseconds time) {
  using SystemClock = ClientReadersFlowTracer::SystemClock;
  using TimePoint = ClientReadersFlowTracer::TimePoint;
  if (!readers_flow_tracer_) {
    return false;
  }
  auto last_stuck = readers_flow_tracer_->last_time_stuck_;
  return last_stuck != TimePoint::max() &&
      SystemClock::now() - last_stuck >= time;
}

}} // namespace facebook::logdevice
