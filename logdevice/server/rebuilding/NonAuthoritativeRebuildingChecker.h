/**
 * Copyright (c) 2018-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/event_log/EventLogWriter.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"

namespace facebook { namespace logdevice {

/**
 * @file NonAuthoritativeRebuildingChecker encapsulates the logic of checking if
 * rebuilding processes are taking too long in non-authoritative state.
 *
 * Let T =
 * rebuildingSettings_->auto_mark_unrecoverable_timeout,
 * NonAuthoritativeRebuildingChecker checks every T/2 msecs for shards in
 * non-authorative rebuilding. For every shard found in that state, it schedules
 * a specific check for that shard T msecs after it first entered in
 * non-authoritative state.
 *
 */

class NonAuthoritativeRebuildingChecker {
 public:
  NonAuthoritativeRebuildingChecker(
      const UpdateableSettings<RebuildingSettings>& rebuilding_settings,
      EventLogStateMachine* event_log,
      node_index_t node_id);

  void updateLocalSettings();

 private:
  void doPass();
  void schedulePass();
  void checkShard(ShardID);
  void onSettingsUpdated();

  std::unique_ptr<Timer> timer_;
  std::unordered_map<ShardID, std::unique_ptr<Timer>> shardTimers_;
  std::unordered_map<ShardID, RecordTimestamp> lastMarkedUnrecoverable_;
  std::chrono::milliseconds period_;

  UpdateableSettings<RebuildingSettings> rebuilding_settings_;
  UpdateableSettings<RebuildingSettings>::SubscriptionHandle subhandle_;
  WorkerCallbackHelper<NonAuthoritativeRebuildingChecker> callback_helper_;

  EventLogStateMachine* event_log_;
  std::unique_ptr<EventLogWriter> writer_;
  node_index_t myNodeId_{-1};

  bool active_;
};

}} // namespace facebook::logdevice
