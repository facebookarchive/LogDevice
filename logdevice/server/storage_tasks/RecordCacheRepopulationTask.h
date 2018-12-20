/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include <folly/Memory.h>

#include "logdevice/server/RecordCache.h"
#include "logdevice/server/RepopulateRecordCachesRequest.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

class RecordCacheRepopulationTask : public StorageTask {
 public:
  RecordCacheRepopulationTask(shard_index_t shard_idx,
                              WeakRef<RepopulateRecordCachesRequest> parent,
                              bool repopulate_record_caches)
      : StorageTask(StorageTask::Type::RECORD_CACHE_REPOPULATION),
        shard_idx_(shard_idx),
        status_(E::UNKNOWN),
        parent_(parent),
        repopulate_record_caches_(repopulate_record_caches) {}

  // see StorageTask.h
  void execute() override;
  void onDone() override;
  bool isDroppable() const override {
    return false;
  }
  void onDropped() override {
    ld_check(false);
  }

 private:
  const shard_index_t shard_idx_;
  // Status that will be sent to the caller to indicate the final result,
  // see notes on callback_ in RepopulateRecordCachesRequest
  Status status_;
  WeakRef<RepopulateRecordCachesRequest> parent_;
  bool repopulate_record_caches_;
};
}} // namespace facebook::logdevice
