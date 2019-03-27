/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/client_read_stream/ClientReadStreamConnectionHealth.h"

#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/ClientStalledReadTracer.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/NodeSetSelectorFactory.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"

namespace facebook { namespace logdevice {

ClientReadStreamConnectionHealth::ClientReadStreamConnectionHealth(
    ClientReadStream* owner)
    : owner_(owner),
      stalled_read_tracer_(std::make_unique<ClientStalledReadTracer>(nullptr)) {
  stall_grace_period_ = owner_->deps_->createBackoffTimer(
      owner_->deps_->getSettings().reader_stalled_grace_period,
      owner_->deps_->getSettings().reader_stalled_grace_period);
  stall_grace_period_->setCallback(
      [this]() { recalculate(/*grace_period_expired=*/true); });

  bumpHealthStat(last_health_state_, 1);

  // Immediately call the health callback saying we are healthy (until proven
  // otherwise).
  owner_->deps_->healthCallback(true);
};

ClientReadStreamConnectionHealth::~ClientReadStreamConnectionHealth() {
  bumpHealthStat(last_health_state_, -1);
}

void ClientReadStreamConnectionHealth::bumpHealthStat(FmajorityResult health,
                                                      int d) {
  switch (health) {
    case FmajorityResult::AUTHORITATIVE_COMPLETE:
    case FmajorityResult::AUTHORITATIVE_INCOMPLETE:
      WORKER_STAT_ADD(client.read_streams_healthy, d);
      break;
    case FmajorityResult::NON_AUTHORITATIVE:
      WORKER_STAT_ADD(client.read_streams_non_authoritative, d);
      break;
    case FmajorityResult::NONE:
      WORKER_STAT_ADD(client.read_streams_stalled, d);
      break;
  }
}

void ClientReadStreamConnectionHealth::updateHealthState(
    const FmajorityResult& health_state) {
  // stats should be update on all state changes,
  // including changes from one healthy state to another,
  // e.g. from AUTHORITATIVE_INCOMPLETE to NON_AUTHORITATIVE
  bumpHealthStat(last_health_state_, -1);
  bumpHealthStat(health_state, 1);

  bool was_healthy = last_health_state_ != FmajorityResult::NONE;
  bool is_healthy = health_state != FmajorityResult::NONE;
  if (is_healthy != was_healthy) {
    owner_->deps_->healthCallback(is_healthy);
  }
  last_health_state_ = health_state;
}

void ClientReadStreamConnectionHealth::traceStall(const std::string& reason) {
  if (!stalled_read_tracer_) {
    stalled_read_tracer_ = std::make_unique<ClientStalledReadTracer>(
        Worker::onThisThread(false) ? Worker::onThisThread()->getTraceLogger()
                                    : nullptr);
  }

  stalled_read_tracer_->traceStall(owner_->log_id_,
                                   owner_->deps_->getReadStreamID(),
                                   owner_->start_lsn_,
                                   owner_->until_lsn_,
                                   owner_->last_delivered_lsn_,
                                   owner_->last_in_record_ts_,
                                   owner_->last_received_ts_,
                                   reason,
                                   owner_->epoch_metadata_str_factory_,
                                   owner_->unavailable_shards_str_factory_,
                                   owner_->currentEpoch(),
                                   owner_->trim_point_,
                                   owner_->readSetSize());
}

void ClientReadStreamConnectionHealth::scheduleTraceStall(std::string reason) {
  if (!stalled_read_timer_) {
    stalled_read_timer_ = owner_->deps_->createTimer();
  }

  stalled_read_timer_->setCallback([this, r = std::move(reason)] {
    traceStall(r);
    stalled_read_timer_->activate(std::chrono::seconds(60));
  });
  stalled_read_timer_->activate(std::chrono::microseconds(0));
}

void ClientReadStreamConnectionHealth::recalculate(bool grace_period_expired) {
  bool was_healthy = last_health_state_ != FmajorityResult::NONE;

  bool is_healthy = false;
  FmajorityResult health_state = FmajorityResult::NONE;

  // If this is true, we are currently reading the metadata log to figure out
  // the next storage set to read from. In that case, we will consider the read
  // stream stalled only after a grace period expires (see 3.c/ below).
  bool waiting_for_metadata = !owner_->healthy_node_set_ ||
      (owner_->epoch_metadata_requested_.hasValue() &&
       owner_->last_epoch_with_metadata_ <
           owner_->epoch_metadata_requested_.value());

  if (!waiting_for_metadata) {
    // If the subset of shards in ConnectionState::READING is a f-majority
    // subset, the connection is healthy (we are expecting to receive all data).
    // Otherwise, there are some records that live only on shards that we are
    // not connected to, resulting in read interruption.
    health_state = owner_->healthy_node_set_->isFmajority(true);
    is_healthy = health_state != FmajorityResult::NONE;
  }

  // 1/ If the health state has not changed, do nothing;
  // 2/ If the health state has changed to "healthy", change the health stats
  //    and call the `health_callback_` immediately;
  // 3/ if the health state has changed to "unhealthy", then:
  //    a/ activate a grace timer;
  //    b/ If we were in a/ but then the grace period expired, change the health
  //    stats and call the `health_callback_` immediately.

  std::string reason;
  if (health_state == last_health_state_) {
    // 1/
  } else if (is_healthy) {
    // 2/
    stall_grace_period_->reset();

    if (stalled_read_timer_) {
      stalled_read_timer_->cancel();
    }

    updateHealthState(health_state);
    // do not log if the change is from one healthy state to another
    if (!was_healthy) {
      ld_info("Read stream (%p) for log %lu is now healthy. %s",
              owner_,
              owner_->log_id_.val_,
              owner_->getDebugInfoStr().c_str());
    }
  } else {
    // 3/
    ld_check(was_healthy && !is_healthy);
    if (grace_period_expired) {
      // 3.a/
      if (waiting_for_metadata) {
        ld_warning(
            "Could not read metadata for log %lu for more than %lums, now "
            "considering the read stream (%p) unhealthy. %s",
            owner_->log_id_.val_,
            stall_grace_period_->getNextDelay().count(),
            owner_,
            owner_->getDebugInfoStr().c_str());
        reason = "could_not_read_metadata";
      } else {
        ld_warning("Read stream (%p) for log %lu is now unhealthy "
                   "because too many shards are down. %s",
                   owner_,
                   owner_->log_id_.val_,
                   owner_->getDebugInfoStr().c_str());
        reason = "too_many_shards_down";
      }
      updateHealthState(health_state);

      // log stalled reads to scuba periodically
      if (stalled_read_timer_) {
        stalled_read_timer_->setCallback([this, r = std::move(reason)] {
          traceStall(r);
          stalled_read_timer_->activate(std::chrono::seconds(60));
        });
        stalled_read_timer_->activate(std::chrono::microseconds(0));
      }
    } else {
      // 3.b/
      ld_check(!is_healthy);
      if (!stall_grace_period_->isActive()) {
        stall_grace_period_->activate();
      }
    }
  }
}

}} // namespace facebook::logdevice
