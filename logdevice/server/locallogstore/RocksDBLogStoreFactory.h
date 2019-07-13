/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/settings/RebuildingSettings.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/LocalLogStoreFactory.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreConfig.h"

namespace facebook { namespace logdevice {

class StatsHolder;
class IOTracing;

class RocksDBLogStoreFactory : public LocalLogStoreFactory {
 public:
  explicit RocksDBLogStoreFactory(RocksDBLogStoreConfig rocksdb_config,
                                  const Settings& server_settings,
                                  const std::shared_ptr<Configuration> config,
                                  StatsHolder* stats)
      : rocksdb_config_(std::move(rocksdb_config)),
        server_settings_(server_settings),
        config_(std::move(config)),
        stats_(stats) {}

  std::unique_ptr<LocalLogStore> create(uint32_t shard_idx,
                                        uint32_t num_shards,
                                        std::string path,
                                        IOTracing* io_tracing) const override;

 private:
  RocksDBLogStoreConfig rocksdb_config_;
  Settings server_settings_;
  const std::shared_ptr<Configuration> config_;
  StatsHolder* stats_;
};

}} // namespace facebook::logdevice
