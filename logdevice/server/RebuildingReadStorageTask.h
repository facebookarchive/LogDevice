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
#include "logdevice/common/ResourceBudget.h"
#include "logdevice/common/Timestamp.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"
#include "logdevice/include/types.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/read_path/LocalLogStoreReader.h"
#include "logdevice/server/read_path/ServerReadStream.h"
#include "logdevice/server/storage_tasks/StorageTask.h"

namespace facebook { namespace logdevice {

/**
 * @file Task created by LogRebuilding state machines when they need data read
 *       from the local log store.  Upon completion, the task (including the
 *       result) gets sent back to the worker thread.
 */

class LocalLogStore;

class RebuildingReadStorageTask : public StorageTask {
 public:
  /**
   * @param restart_version Version passed by `LogRebuilding`. `LogRebuilding`
   *                        uses to discard this storage task if it rewound
   *                        after issuing it.
   * @param seek_timestamp  If set, seek to this timestamp prior to reading
   *                        records.
   * @param read_ctx        ReadContext object which defines from which LSN to
   *                        read and the filter to use.
   * @param options         If iterator is nullptr, Options to be used to create
   *                        an iterator.
   * @param iterator        Iterator object to be passed to
   *                        LocalLogStoreReader::read(). If nullptr, a new
   *                        iterator object will be created.
   */
  RebuildingReadStorageTask(
      lsn_t restart_version,
      folly::Optional<RecordTimestamp> seek_timestamp,
      LocalLogStoreReader::ReadContext read_ctx,
      LocalLogStore::ReadOptions options,
      std::weak_ptr<LocalLogStore::ReadIterator> iterator);

  /**
   * Executes the storage task on a storage thread.  Wrapper around
   * LocalLogStoreReader::read() that puts the result into status and
   * records.
   */
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

  lsn_t restartVersion;
  folly::Optional<RecordTimestamp> seekTimestamp;
  LocalLogStoreReader::ReadContext readCtx;
  LocalLogStore::ReadOptions options;

  // weak ptr to an iterator passed in by CatchupOneStream (if any).
  std::weak_ptr<LocalLogStore::ReadIterator> iteratorFromCache;

  // Owned iterator created through a call to `iterator_.lock()` or if `lock()`
  // failed, new iterator that will later be handed over to CatchupOneStream.
  std::shared_ptr<LocalLogStore::ReadIterator> ownedIterator;

  //
  // These will hold the result after execute()
  //
  using RecordContainer = std::vector<RawRecord>;
  Status status;
  RecordContainer records;
  // Total amount of record bytes that were allocated by this storage task.
  // Used for stats.
  size_t totalBytes{0};

 private:
  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;
};

}} // namespace facebook::logdevice
