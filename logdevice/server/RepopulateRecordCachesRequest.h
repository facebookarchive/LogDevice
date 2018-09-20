/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/Request.h"
#include "logdevice/common/WeakRefHolder.h"

namespace facebook { namespace logdevice {

/**
 * @file  A request to repopulate the record caches from stable storage.
 *        Regardless of success or failure, any persistent cache data is
 *        destroyed so we have a clean slate for persisting record cache data
 *        on the next shutdown.
 */
class RepopulateRecordCachesRequest : public Request {
 public:
  /**
   * @param callback  Will be called with the resulting status for each shard
   * @param repopulate_record_caches  If false, will only remove snapshots (not
   *                                  repopulate)
   */
  explicit RepopulateRecordCachesRequest(
      std::function<void(Status, int)> callback,
      bool repopulate_record_caches);

  Execution execute() override;

  // called by each RecordCacheRepopulationTask when it is done executing
  void onRepopulationTaskDone(Status status, shard_index_t shard_idx);

 private:
  // count of remaining RecordCache repopulation tasks
  int remaining_record_cache_repopulations_;

  /**
   * Callback function to notify caller of status from each shard. Possible
   * outcomes:
   *  - OK        if everything went well
   *  - PARTIAL   if there were some snapshot blobs that we could not read
   *  - FAILED    if we were not able to delete all existing snapshots (in
   *              this case, logdeviced should exit)
   *  - DISABLED  if the shard is disabled
   */
  std::function<void(Status, int)> callback_;

  // for creating weakref to itself for storage tasks created.
  WeakRefHolder<RepopulateRecordCachesRequest> ref_holder_;

  // If true: repopulate caches, in either case: drop snapshots column family.
  const bool repopulate_record_caches_;
};

}} // namespace facebook::logdevice
