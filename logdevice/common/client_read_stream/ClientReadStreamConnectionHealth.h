/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/FailureDomainNodeSet.h"

/*
 * @file ClientReadStreamConnectionHealth.h
 *
 * Tracks the health of the connection, ie whether we have a healthy TCP
 * connection to an f-majority of shards in the read set.
 *
 * Uses a ClientStalledReadTracer to log debug information when the read stream
 * does not have a good connection health.
 *
 * Calls the connection health callback provided by the user when it changes.
 *
 * Finally, this bumps various counters that the operator can use to alert when
 * readers do not have a good connection:
 *   - client.read_streams_healthy: number of read streams that have a healthy
 *     connection to enough shards in the read set;
 *   - client.read_streams_non_authoritative: number of read streams that are
 *      not stalled but may see dataloss because too many shards lost data and
 *      the data is not recoverable;
 *   - client.read_streams_stalled: number of read streams that are stalled
 *     because they don't have a healthy connection to a f-majority of shards in
 *     the read set.
 *
 * This metric provides important signal but it is better to use the metrics by
 * ClientReadersFlowTracer to measure read availability.
 */

namespace facebook { namespace logdevice {

class BackoffTimer;
class ClientReadStream;
class ClientStalledReadTracer;
class Timer;

class ClientReadStreamConnectionHealth {
 public:
  explicit ClientReadStreamConnectionHealth(ClientReadStream* owner);
  ~ClientReadStreamConnectionHealth();

  /**
   * Called when the number of shards in the READING state changes, or the
   * config is updated.  If a health callback was set, this recalculates the
   * connection health bit and invokes the callback. This also helps maintain
   * some stats on read stream health (@see bumpHealthStat).
   */
  void recalculate(bool grace_period_expired = false);

 private:
  ClientReadStream* owner_;

  // Keep track of the last health state we computed. Each time the state of a
  // shard changes, we recompute the connection health. If it changed, we call
  // health_callback_ and bumpHealthStat().
  FmajorityResult last_health_state_{FmajorityResult::AUTHORITATIVE_COMPLETE};

  // a Throttled tracelogger for tracing stalled reads
  std::unique_ptr<ClientStalledReadTracer> stalled_read_tracer_;

  // When recalculate() detects that we don't have a healthy connection, we kick
  // off this time to only bump the stat if we remain in that state for
  // some amount of time defined by Settings::reader_stalled_grace_period .
  std::unique_ptr<BackoffTimer> stall_grace_period_;

  // a timer to publish periodically the stalled reads
  std::unique_ptr<Timer> stalled_read_timer_;

  // change last_health_state_, send notifications, update stats
  void updateHealthState(const FmajorityResult& health_state);

  /**
   * Bump stat that keeps track of the number of streams in a given health
   * state.
   */
  void bumpHealthStat(FmajorityResult health, int d);

  void traceStall(const std::string& reason);
  void scheduleTraceStall(std::string reason);

  friend class ClientReadStreamTest;
};

}} // namespace facebook::logdevice
