/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/CheckpointStore.h"
#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {
/*
 * @file CheckpointStoreFactory is the way to create CheckpointStore instances.
 */
class CheckpointStoreFactory {
 public:
  /**
   * Creates a file based CheckpointStore.
   *
   * @param root_path: a path to a root file, where the checkpoints are
   *   stored.
   */
  std::unique_ptr<CheckpointStore>
  createFileBasedCheckpointStore(std::string root_path);

  /**
   * Creates a zookeeper based CheckpointStore.
   *
   * @param client: the instance of the client which contains the zookeeper
   *   config.
   */
  std::unique_ptr<CheckpointStore>
  createZookeeperBasedCheckpointStore(std::shared_ptr<Client>& client);

  /**
   * Creates a Replicated State Machine based CheckpointStore. The checkpoints
   * are stored in a given log as records. One record represents a single
   * update of the checkpoint for a given customer id. The entire log is
   * a sequence of updates which forms the current state of the store.
   *
   * @param client: the instance of the client which will read and write the
   *   checkpoints.
   * @param log_id: the id of the log, where the checkpoints will be stored.
   * @param stop_timeout: timeout for the RSM to stop after calling shutdown.
   */
  std::unique_ptr<CheckpointStore>
  createRSMBasedCheckpointStore(std::shared_ptr<Client>& client,
                                logid_t log_id,
                                std::chrono::milliseconds stop_timeout);
};

}} // namespace facebook::logdevice
