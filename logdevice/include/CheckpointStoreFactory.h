/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/CheckpointStore.h"

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
   * stored.
   */
  std::unique_ptr<CheckpointStore>
  createFileBasedCheckpointStore(std::string root_path);
};

}} // namespace facebook::logdevice
