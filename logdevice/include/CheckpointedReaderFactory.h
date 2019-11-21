/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/AsyncCheckpointedReader.h"
#include "logdevice/include/SyncCheckpointedReader.h"

namespace facebook { namespace logdevice {
/*
 * @file CheckpointedReaderFactory is the way to create CheckpointedReader
 *   instances - either SyncCheckpointedReader or AsyncCheckpointedReader.
 */
class CheckpointedReaderFactory {
 public:
  /**
   * Creates a sync CheckpointedReader.
   *
   * @param reader: an underlying reader.
   */
  std::unique_ptr<SyncCheckpointedReader>
  createCheckpointedReader(const std::string& reader_name,
                           std::unique_ptr<Reader> reader,
                           std::unique_ptr<CheckpointStore> store,
                           CheckpointedReaderBase::CheckpointingOptions opts);

  /**
   * Creates an async CheckpointedReader.
   *
   * @param reader: an underlying reader.
   */
  std::unique_ptr<AsyncCheckpointedReader>
  createCheckpointedReader(const std::string& reader_name,
                           std::unique_ptr<AsyncReader> reader,
                           std::unique_ptr<CheckpointStore> store,
                           CheckpointedReaderBase::CheckpointingOptions opts);
};

}} // namespace facebook::logdevice
