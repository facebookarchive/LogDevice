/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/server/RebuildingLogEnumerator.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Task created by RebuildingLogEnumerator to identify all metadata logs
 *       that are stored in a given shard.
 */

class RebuildingEnumerateMetadataLogsTask : public StorageTask {
 public:
  /**
   * @param ref   weak reference to the RebuildingLogEnumerator object, used
   *              to check if it's still alive
   */
  explicit RebuildingEnumerateMetadataLogsTask(
      WeakRefHolder<RebuildingLogEnumerator>::Ref ref,
      size_t num_shards)
      : StorageTask(StorageTask::Type::REBUILDING_ENUMERATE_LOGS),
        ref_(std::move(ref)),
        num_shards_(num_shards) {}

  void execute() override;

  void onDone() override;

  void onDropped() override;

  ThreadType getThreadType() const override {
    // Read tasks may take a while to execute, so they shouldn't block fast
    // write operations.
    return ThreadType::SLOW;
  }

  StorageTaskPriority getPriority() const override {
    // Rebuilding reads should be lo-pri compared to regular reads
    return StorageTaskPriority::LOW;
  }

  Principal getPrincipal() const override {
    return Principal::REBUILD;
  }

  WeakRefHolder<RebuildingLogEnumerator>::Ref ref_;
  size_t num_shards_;

  Status status_;
  std::vector<logid_t> result_;
};

}} // namespace facebook::logdevice
