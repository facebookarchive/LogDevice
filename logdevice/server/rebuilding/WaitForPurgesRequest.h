/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/small_vector.h>

#include "logdevice/common/ExponentialBackoffTimer.h"
#include "logdevice/common/FireAndForgetRequest.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * WaitForPurgesRequest is used by RebuildingPlanner to wait for purging to
 * complete on all local shards that are part of the rebuilding plan.
 */

class WaitForPurgesRequest : public FireAndForgetRequest {
 public:
  /**
   * @param status can be one of:
   *               - E::OK purges completed for all shards;
   *               - E::NOTFOUND log was removed from the config.
   */
  using Callback = std::function<void(Status status)>;

  /**
   * @param logid  log to purge
   * @param seq    Sequencer node that we retrieved last released lsn from
   * @param up_to  LSN to purge up to
   * @param shards List of local shards that should perform purging
   * @param cb     @see Callback. Called once all shards have purged the log up
   *               to the given LSN.
   */
  WaitForPurgesRequest(
      logid_t logid,
      NodeID seq,
      lsn_t up_to,
      folly::small_vector<shard_index_t> shards,
      Callback cb,
      UpdateableSettings<RebuildingSettings> rebuilding_settings);

  ~WaitForPurgesRequest() override {}

  void executionBody() override;

  int getThreadAffinity(int nthreads) override;

 private:
  logid_t logid_;
  NodeID seq_;
  lsn_t upTo_;
  folly::small_vector<shard_index_t> shards_;
  Callback cb_;
  UpdateableSettings<RebuildingSettings> rebuildingSettings_;

  // Timer for retrying calls to check().
  std::unique_ptr<ExponentialBackoffTimer> retryTimer_;

  void onComplete(Status status);

  void tryAgain();

  // Check if all shards in `shard_` have completed purging for the log.
  // If a shard has not performed purging, make sure purging is triggered.
  // @returns true if all shards have performed purging.
  bool check();

  // Checks if a shard has completed purging for the log.
  // If the shard has not performed purging, make sure purging is triggered.
  // @returns whether the shard has completed purging.
  bool checkShardHasPurged(shard_index_t shard_idx) const;
};

}} // namespace facebook::logdevice
