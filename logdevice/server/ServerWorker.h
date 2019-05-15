/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include "logdevice/common/Worker.h"

/**
 * @file Subclass of Worker containing state specific to servers.
 */

namespace facebook { namespace logdevice {

class AllCachedDigests;
class AllServerReadStreams;
class NodeStatsControllerCallback;
class PerWorkerStorageTaskQueue;
class PurgeScheduler;
class StorageThreadPool;
class ServerProcessor;
class ServerWorkerImpl;

struct ChunkRebuildingMap;
struct PurgeUncleanEpochsMap;

struct SettingOverrideTTLRequestMap {
  std::unordered_map<std::string, std::unique_ptr<Request>> map;
};

class ServerWorker : public Worker {
 public:
  ServerWorker(WorkContext::KeepAlive,
               ServerProcessor*,
               worker_id_t,
               const std::shared_ptr<UpdateableConfig>&,
               StatsHolder*,
               WorkerType type);
  ~ServerWorker() override;
  void subclassFinishWork() override;
  void subclassWorkFinished() override;

  /**
   * If current thread is running a ServerWorker, return it.  Otherwise,
   * assert false (when enforce is true) and/or return nullptr.
   */
  static ServerWorker* onThisThread(bool enforce = true) {
    if (ThreadID::getType() == ThreadID::SERVER_WORKER ||
        ThreadID::getType() == ThreadID::CPU_EXEC) {
      return checked_downcast<ServerWorker*>(Worker::onThisThread());
    }
    if (enforce) {
      // Not on a server worker == assert failure
      ld_check(false);
    }
    return nullptr;
  }

  AllServerReadStreams& serverReadStreams() const;

  //
  // Getters for state contained on a ServerWorker
  //
  AllCachedDigests& cachedDigests() const;

  // Map of PurgeUncleanEpochs state machines running on this worker.  These
  // need to get shutdown with the worker.
  PurgeUncleanEpochsMap& activePurges() const;

  // a map of all currently running SettingOverrideTTLRequest
  SettingOverrideTTLRequestMap& activeSettingOverrides() const;

  ChunkRebuildingMap& runningChunkRebuildings() const;

  // Intentionally shadows `Worker::processor_' to expose a more specific
  // subclass of Processor
  ServerProcessor* const processor_;

  // used to limit the number of concurrent purge machines for releases
  std::unique_ptr<PurgeScheduler> purge_scheduler_;

  std::unique_ptr<AllServerReadStreams> server_read_streams_;

  // This overrides Worker::onSettingsUpdated() but calls it first thing
  void onSettingsUpdated() override;

  void onServerConfigUpdated() override;

  // This overrides Worker::setupWorker() but calls it first thing
  void setupWorker() override;

  /**
   * Gets the storage task queue. This is the main access point
   * for workers to create storage tasks.
   */
  PerWorkerStorageTaskQueue*
  getStorageTaskQueueForShard(shard_index_t shard) const;

  /**
   * Gets the storage thread pool assigned to the given shard.
   */
  StorageThreadPool& getStorageThreadPoolForShard(shard_index_t shard) const;

  /**
   * If this worker is responsible for the NodeStatsController, return the
   * callback. Otherwise return nullptr.
   */
  NodeStatsControllerCallback* nodeStatsControllerCallback() const;

 private:
  // Pimpl, contains most of the objects we provide getters for
  friend class ServerWorkerImpl;
  std::unique_ptr<ServerWorkerImpl> impl_;

  std::unique_ptr<MessageDispatch> createMessageDispatch() override;
  void noteShuttingDownNoPendingRequests() override;
  void initializeNodeStatsController();

  // Coordinator for tasks to storage threads to read from the local log store
  // and their replies, sharded by log ID to match ShardedStorageThreadPool
  // sharding
  std::vector<std::unique_ptr<PerWorkerStorageTaskQueue>> storage_task_queues_;
};
}} // namespace facebook::logdevice
