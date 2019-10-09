/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/CheckpointedReaderFactory.h"

#include "logdevice/lib/checkpointing/AsyncCheckpointedReaderImpl.h"
#include "logdevice/lib/checkpointing/SyncCheckpointedReaderImpl.h"

namespace facebook { namespace logdevice {

std::unique_ptr<SyncCheckpointedReader>
CheckpointedReaderFactory::createSyncCheckpointedReader(
    const std::string& reader_name,
    std::unique_ptr<Reader> reader,
    std::unique_ptr<CheckpointStore> store,
    CheckpointedReaderBase::CheckpointingOptions opts) {
  return std::make_unique<SyncCheckpointedReaderImpl>(
      reader_name, std::move(reader), std::move(store), opts);
}

std::unique_ptr<AsyncCheckpointedReader>
CheckpointedReaderFactory::createAsyncCheckpointedReader(
    const std::string& reader_name,
    std::unique_ptr<AsyncReader> reader,
    std::unique_ptr<CheckpointStore> store,
    CheckpointedReaderBase::CheckpointingOptions opts) {
  return std::make_unique<AsyncCheckpointedReaderImpl>(
      reader_name, std::move(reader), std::move(store), opts);
}

}} // namespace facebook::logdevice
