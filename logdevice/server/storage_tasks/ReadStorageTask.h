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

class ReadStorageTask : public StorageTask {
 public:
  /**
   * @param stream                     Object that can be used to check if the
   *                                   ServerReadStream this task is for was
   *                                   destroyed, in which case this task won't
   *                                   be processed or its results will be
   *                                   discarded when it comes back to the
   *                                   worker thread.
   * @param catchup_queue              Weak reference to catchup queue.
   * @param server_read_stream_version ServerReadStream version at time of
   *                                   creation.
   * @param filter_version             Filter version at the time of creation.
   * @param options                    If iterator is nullptr, Options to be
   *                                   used to create an iterator.
   * @param iterator                   Iterator object to be passed to
   *                                   LocalLogStoreReader::read(). If nullptr,
   *                                   a new iterator object will be created.
   * @param client_address             Address of the client who initiated this
   *                                   read.
   */
  ReadStorageTask(WeakRef<ServerReadStream> stream,
                  WeakRef<CatchupQueue> catchup_queue,
                  server_read_stream_version_t server_read_stream_version,
                  filter_version_t filter_version,
                  LocalLogStoreReader::ReadContext read_ctx,
                  LocalLogStore::ReadOptions options,
                  std::weak_ptr<LocalLogStore::ReadIterator> iterator,
                  size_t throttling_debit,
                  Priority rp,
                  StorageTaskType type,
                  ThreadType thread_type,
                  StorageTaskPriority priority,
                  Principal principal,
                  Sockaddr client_address = Sockaddr());

  /**
   * Called by AllServerReadStreams once it budgeted some memory for this task.
   */
  void setMemoryToken(ResourceBudget::Token memory_token);

  /**
   * Executes the storage task on a storage thread.  Wrapper around
   * LocalLogStoreReader::read() that puts the result into status_ and
   * records_.
   */
  void execute() override;

  void onDone() override;

  void onDropped() override;

  void releaseRecords();

  ThreadType getThreadType() const override {
    // Read tasks may take a while to execute, so they shouldn't block fast
    // write operations.
    return thread_type_;
  }

  StorageTaskPriority getPriority() const override {
    return priority_;
  }

  Principal getPrincipal() const override {
    return principal_;
  }

  // Used to track if the ServerReadStream for which this task is for has been
  // destroyed.
  WeakRef<ServerReadStream> stream_;

  // Used to track if the CatchupQueue for which this task is for has been
  // destroyed.
  WeakRef<CatchupQueue> catchup_queue_;

  const server_read_stream_version_t server_read_stream_version_;
  const filter_version_t filter_version_;
  LocalLogStoreReader::ReadContext read_ctx_;
  ResourceBudget::Token memory_token_;
  LocalLogStore::ReadOptions options_;

  // weak ptr to an iterator passed in by CatchupOneStream (if any).
  std::weak_ptr<LocalLogStore::ReadIterator> iterator_from_cache_;

  // Owned iterator created through a call to `iterator_.lock()` or if `lock()`
  // failed, new iterator that will later be handed over to CatchupOneStream.
  std::shared_ptr<LocalLogStore::ReadIterator> owned_iterator_;

  //
  // These will hold the result after execute()
  //
  typedef std::vector<RawRecord> RecordContainer;
  Status status_{E::UNKNOWN};
  RecordContainer records_;
  // Total amount of record bytes that were allocated by this storage task.
  // Used for stats.
  size_t total_bytes_{0};

  ThreadType thread_type_;
  StorageTaskPriority priority_;
  Principal principal_;

  size_t getThrottlingEstimate() const {
    return throttling_estimate_;
  }

  Priority getReadPriority() const {
    return rpriority_;
  }

 private:
  void getDebugInfoDetailed(StorageTaskDebugInfo&) const override;

  // The following fields store some information about the read stream for debug
  // output
  read_stream_id_t stream_id_;
  ClientID client_id_;
  Sockaddr client_address_;
  lsn_t stream_start_lsn_;
  std::chrono::steady_clock::time_point stream_creation_time_;
  bool stream_scd_enabled_;
  small_shardset_t stream_known_down_;
  // Amount of bytes that were deducted from Read Throttling framework
  // while issuing this storage task.
  size_t throttling_estimate_{0};
  Priority rpriority_{Priority::MAX};
};
}} // namespace facebook::logdevice
