/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/CheckpointStoreFactory.h"

#include "logdevice/common/FileBasedVersionedConfigStore.h"
#include "logdevice/common/VersionedConfigStore.h"
#include "logdevice/lib/checkpointing/CheckpointStoreImpl.h"

namespace facebook { namespace logdevice {

std::unique_ptr<CheckpointStore>
CheckpointStoreFactory::createFileBasedCheckpointStore(std::string root_path) {
  // TODO: implement version managing
  auto extract_version_function = [](folly::StringPiece) {
    return static_cast<FileBasedVersionedConfigStore::version_t>(1);
  };
  auto versioned_config_store = std::make_unique<FileBasedVersionedConfigStore>(
      root_path, extract_version_function);
  return std::make_unique<CheckpointStoreImpl>(
      std::move(versioned_config_store));
}

}} // namespace facebook::logdevice
