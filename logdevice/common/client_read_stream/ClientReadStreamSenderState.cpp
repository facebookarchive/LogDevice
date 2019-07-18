/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/client_read_stream/ClientReadStreamSenderState.h"

#include <algorithm>

#include "logdevice/common/DataRecordOwnsPayload.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/client_read_stream/ClientReadStreamScd.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

ClientReadStreamSenderState::ClientReadStreamSenderState(
    ClientReadStream* client_read_stream,
    ShardID shard_id,
    std::unique_ptr<BackoffTimer>&& reconnect_timer,
    std::unique_ptr<BackoffTimer>&& started_timer,
    std::unique_ptr<BackoffTimer>&& retry_window_timer)
    : max_data_record_lsn(0),
      filter_version(0),
      last_received_filter_version(0),
      client_read_stream_(client_read_stream),
      window_high_(0),
      next_lsn_(0),
      shard_id_(shard_id),
      on_socket_close_(this),
      reconnect_timer_(std::move(reconnect_timer)),
      started_timer_(std::move(started_timer)),
      retry_window_timer_(std::move(retry_window_timer)) {
  reconnect_timer_->setCallback([this]() {
    ld_check(getConnectionState() == ConnectionState::RECONNECT_PENDING);
    RATELIMIT_DEBUG(
        std::chrono::seconds(10),
        5,
        "TIMED out connecting to shard %s after %ldms, log %lu, retrying",
        shard_id_.toString().c_str(),
        reconnect_timer_->getNextDelay().count(),
        client_read_stream_->log_id_.val_);
    reconnectTimerCallback();
  });
  started_timer_->setCallback([this]() { startedTimerCallback(); });
  retry_window_timer_->setCallback(
      [this]() { client_read_stream_->sendWindowMessage(*this); });
}

void ClientReadStreamSenderState::activateReconnectTimer() {
  setConnectionState(ConnectionState::RECONNECT_PENDING);
  reconnect_timer_->activate();
}

void ClientReadStreamSenderState::activateStartedTimer() {
  started_timer_->activate();
}

void ClientReadStreamSenderState::extendStartedTimer(filter_version_t fv) {
  if (fv > last_received_filter_version &&
      fv <= client_read_stream_->filter_version_) {
    last_received_filter_version = fv;
    if (started_timer_->isActive()) {
      started_timer_->cancel();
      started_timer_->activate();
    }
  }
}

void ClientReadStreamSenderState::startedTimerCallback() {
  onConnectionAttemptFailed();
  ld_check(getConnectionState() == ConnectionState::START_SENT);
  RATELIMIT_WARNING(std::chrono::seconds(10),
                    5,
                    "TIMED out waiting for STARTED message from shard %s after "
                    "%ldms, log %lu, "
                    "retrying",
                    shard_id_.toString().c_str(),
                    started_timer_->getNextDelay().count(),
                    client_read_stream_->log_id_.val_);

  reconnectTimerCallback();
}

void ClientReadStreamSenderState::reconnectTimerCallback() {
  onConnectionAttemptFailed();

  const auto& nodes_configuration =
      client_read_stream_->getConfig()->getNodesConfiguration();

  if (!nodes_configuration->isNodeInServiceDiscoveryConfig(shard_id_.node())) {
    // This shouldn't happen unless the cluster was shrunk.
    setConnectionState(ConnectionState::PERSISTENT_ERROR);
    return;
  }

  client_read_stream_->sendStart(shard_id_, *this);
  client_read_stream_->applyShardStatus("reconnectTimerCallback", this);
}

void ClientReadStreamSenderState::onConnectionAttemptFailed() {
  client_read_stream_->onConnectionHealthChange(shard_id_, isHealthy());
}

void ClientReadStreamSenderState::setConnectionState(ConnectionState state) {
  const bool was = isHealthy();
  connection_state_ = state;
  const bool is = isHealthy();
  if (is != was) {
    client_read_stream_->onConnectionHealthChange(shard_id_, is);
  }

  ld_spew("state for shard %s is now %s (%d)",
          shard_id_.toString().c_str(),
          getConnectionStateName(),
          int(connection_state_));
}

void ClientReadStreamSenderState::setGapState(GapState gap_state) {
  gap_state_ = gap_state;
}

void ClientReadStreamSenderState::setWindowHigh(lsn_t new_window_high) {
  window_high_ = new_window_high;
}

void ClientReadStreamSenderState::setNextLsn(lsn_t new_next_lsn) {
  next_lsn_ = new_next_lsn;
}

const char* ClientReadStreamSenderState::getConnectionStateName() const {
  switch (connection_state_) {
    case ConnectionState::INITIAL:
      return "INITIAL";
    case ConnectionState::CONNECTING:
      return "CONNECTING";
    case ConnectionState::START_SENT:
      return "START_SENT";
    case ConnectionState::READING:
      return "READING";
    case ConnectionState::RECONNECT_PENDING:
      return "RECONNECT_PENDING";
    case ConnectionState::PERSISTENT_ERROR:
      return "PERSISTENT_ERROR";
    case ConnectionState::DYING:
      return "DYING";
  }
  return "can't reach this, gcc";
}

void ClientReadStreamSenderState::SocketClosedCallback::
operator()(Status st, const Address& /*name*/) {
  // Using `state_` here is safe.  If the callback fired, we know the instance
  // still exists.  The destructor would have deactivated the callback
  // otherwise.

  state_->filter_version.val_ = 0;

  state_->client_read_stream_->onSocketClosed(*state_, st);
  // *this may be destroyed here.
}

}} // namespace facebook::logdevice
