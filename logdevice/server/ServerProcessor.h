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

#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/settings/GossipSettings.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/LocalLogFile.h"
#include "logdevice/server/ServerSettings.h"
#include "logdevice/server/ServerWorker.h"
#include "logdevice/server/read_path/LogStorageStateMap.h"

/**
 * @file Subclass of Processor containing state specific to servers, also
 * spawning ServerWorker instances instead of plain Worker.
 */

namespace facebook { namespace logdevice {

class ShardedStorageThreadPool;

class ServerProcessor : public Processor {
 public:
  // Factory method just forwards to the private constructor.  We do this to
  // ensure init() gets called subsequently, allowing the base class to call
  // virtual methods to complete initialization.
  template <typename... Args>
  static std::shared_ptr<ServerProcessor> create(Args&&... args) {
    auto p = std::make_shared<ServerProcessor>(std::forward<Args>(args)...);
    p->init();
    return p;
  }

  ServerWorker* createWorker(WorkContext::KeepAlive executor,
                             worker_id_t i,
                             WorkerType type) override;

  void applyToWorkers(folly::Function<void(ServerWorker&)> func,
                      Order order = Order::FORWARD) {
    Processor::applyToWorkers(
        [&func](Worker& worker) {
          func(checked_downcast<ServerWorker&>(worker));
        },
        order);
  }

  /**
   * Returns the reference to the worker instance with the given index.
   */
  ServerWorker& getWorker(worker_id_t worker_id, WorkerType type) {
    return checked_downcast<ServerWorker&>(
        Processor::getWorker(worker_id, type));
  }

  const std::shared_ptr<LocalLogFile>& getAuditLog() {
    return audit_log_;
  }

  LogStorageStateMap& getLogStorageStateMap() const;

  // Alternative factory for tests that need to construct a half-baked
  // Processor (no workers etc).
  template <typename... Args>
  static std::unique_ptr<ServerProcessor> createNoInit(Args&&... args) {
    return std::unique_ptr<ServerProcessor>(
        new ServerProcessor(std::forward<Args>(args)...));
  }

  void init() override;

  int getWorkerCount(WorkerType type = WorkerType::GENERAL) const override;

  bool runningOnStorageNode() const override {
    return Processor::runningOnStorageNode() ||
        sharded_storage_thread_pool_ != nullptr;
  }

  UpdateableSettings<ServerSettings> updateableServerSettings() {
    return server_settings_;
  }

  void getClusterDeadNodeStats(size_t* effective_dead_cnt,
                               size_t* effective_cluster_size) override;

  virtual bool isNodeAlive(node_index_t index) const override;

  virtual bool isNodeBoycotted(node_index_t index) const override;

  virtual bool isNodeIsolated() const override;

  virtual bool isFailureDetectorRunning() const override;
  // Pointer to sharded storage thread pool, if this is a storage node
  // (nullptr if not).  Unowned.
  ShardedStorageThreadPool* const sharded_storage_thread_pool_;

  std::unique_ptr<LogStorageState_PurgeCoordinator_Bridge>
  createPurgeCoordinator(logid_t, shard_index_t, LogStorageState*);

  template <typename... Args>
  ServerProcessor(std::shared_ptr<LocalLogFile> audit_log,
                  ShardedStorageThreadPool* const sharded_storage_thread_pool,
                  UpdateableSettings<ServerSettings> server_settings,
                  UpdateableSettings<GossipSettings> gossip_settings,
                  UpdateableSettings<AdminServerSettings> admin_server_settings,
                  Args&&... args)
      : Processor(std::forward<Args>(args)...),
        sharded_storage_thread_pool_(sharded_storage_thread_pool),
        audit_log_(audit_log),
        server_settings_(std::move(server_settings)),
        gossip_settings_(std::move(gossip_settings)),
        admin_server_settings_(std::move(admin_server_settings)) {
    maybeCreateLogStorageStateMap();
  }

  ~ServerProcessor() {}
  std::unique_ptr<FailureDetector> failure_detector_;

 private:
  void maybeCreateLogStorageStateMap();

  std::shared_ptr<LocalLogFile> audit_log_;
  UpdateableSettings<ServerSettings> server_settings_;
  UpdateableSettings<GossipSettings> gossip_settings_;
  UpdateableSettings<AdminServerSettings> admin_server_settings_;
  std::unique_ptr<LogStorageStateMap> log_storage_state_map_;
};
}} // namespace facebook::logdevice
