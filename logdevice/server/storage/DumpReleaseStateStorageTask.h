/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/Err.h"
#include "logdevice/server/locallogstore/WriteOps.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file A task used to store the release state for the node in the log store.
 */

class DumpReleaseStateStorageTask : public StorageTask {
 public:
  using ReleaseStates = std::vector<std::pair<logid_t, lsn_t>>;

  explicit DumpReleaseStateStorageTask(ReleaseStates&& states)
      : StorageTask(StorageTask::Type::DUMP_RELEASE_STATE),
        op_(std::move(states)) {}

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  void execute() override;

  void onDone() override {}
  void onDropped() override {}

 private:
  DumpReleaseStateWriteOp op_;
};

}} // namespace facebook::logdevice
