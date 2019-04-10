/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <atomic>
#include <memory>
#include <vector>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/OffsetMap.h"
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/ServerReadStream.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Task created by worker threads when they need data read from the
 *       local log store.  Upon completion, the task (including the result)
 *       gets sent back to the worker thread.
 */

class LocalLogStore;
class CatchupQueue;

class EpochOffsetStorageTask : public StorageTask {
 public:
  /**
   * @param stream                     Object that can be used to check if the
   *                                   ServerReadStream this task is for was
   *                                   destroyed, in which case this task won't
   *                                   be processed or its results will be
   *                                   discarded when it comes back to the
   *                                   worker thread.
   * @param catchup_queue              Weak reference to catchup queue.
   * @param log_id                     Log ID to read metadata of.
   * @param epoch                      Target epoch for which offset was
   *                                   requested.
   */
  explicit EpochOffsetStorageTask(WeakRef<ServerReadStream> stream,
                                  logid_t log_id,
                                  epoch_t epoch,
                                  ThreadType thread_type,
                                  StorageTaskPriority priority);

  void execute() override;

  void onDone() override;

  void onDropped() override;

  void releaseRecords();

  ThreadType getThreadType() const override {
    return thread_type_;
  }
  StorageTaskPriority getPriority() const override {
    return priority_;
  }

  Principal getPrincipal() const override {
    return Principal::METADATA;
  }

  // Used to track if the ServerReadStream for which this task is for has been
  // destroyed.
  WeakRef<ServerReadStream> stream_;
  logid_t log_id_;
  epoch_t epoch_;
  ThreadType thread_type_;
  StorageTaskPriority priority_;

  Status status_{E::UNKNOWN};
  OffsetMap result_offsets_;
};
}} // namespace facebook::logdevice
